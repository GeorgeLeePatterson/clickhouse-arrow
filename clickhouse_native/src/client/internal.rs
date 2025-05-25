use std::collections::VecDeque;
use std::sync::Arc;

use strum::{AsRefStr, IntoStaticStr};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::Event;
use super::connection::ConnectionMetadata;
use super::reader::Reader;
use super::writer::Writer;
use crate::client::Query;
use crate::client_info::ClientInfo;
use crate::errors::*;
use crate::io::{ClickhouseRead, ClickhouseWrite};
use crate::native::block::Block;
use crate::native::block_info::BlockInfo;
use crate::native::protocol::{
    ClientHello, QueryProcessingStage, ServerData, ServerHello, ServerPacket, ServerPacketId,
};
use crate::prelude::*;
use crate::settings::Settings;
use crate::{ClickhouseEvent, ClientOptions, ServerError};

type ResponseReceiver<T> = mpsc::Receiver<Result<T>>;
type ResponseSender<T> = mpsc::Sender<Result<T>>;

#[derive(AsRefStr, IntoStaticStr)]
pub(crate) enum Message<Data: Send + Sync + 'static> {
    Ping { response: oneshot::Sender<Result<()>> },
    Operation { qid: Qid, op: Operation<Data> },
    Shutdown,
}

#[derive(AsRefStr, IntoStaticStr)]
pub(crate) enum Operation<Data: Send + Sync + 'static> {
    #[strum(serialize = "Query")]
    Query {
        query:    String,
        settings: Option<Arc<Settings>>,
        response: oneshot::Sender<Result<ResponseReceiver<Data>>>,
        header:   Option<oneshot::Sender<Vec<(String, crate::Type)>>>,
    },
    #[strum(serialize = "Insert")]
    Insert { data: Data, response: oneshot::Sender<Result<()>> },
    #[strum(serialize = "InsertMany")]
    InsertMany { data: Vec<Data>, response: oneshot::Sender<Result<()>> },
}

/// Internal tracking
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
pub(super) enum QueryState {
    // Waiting for header block
    Header,
    #[default]
    InProgress,
    Failed,
    Finished,
}

impl QueryState {
    fn next<T>(self, packet: &ServerPacket<T>) -> Self {
        let packet_id = ServerPacketId::from(packet);
        match (self, packet_id) {
            (QueryState::Finished | QueryState::Failed, _)
            | (QueryState::Header, ServerPacketId::Progress | ServerPacketId::TableColumns) => self,
            (_, ServerPacketId::EndOfStream) => QueryState::Finished,
            (_, ServerPacketId::Exception) => QueryState::Failed,
            (_, _) => QueryState::InProgress,
        }
    }

    fn is_finished(self) -> bool { matches!(self, QueryState::Finished | QueryState::Failed) }
}

/// Internal enum for inserts
#[derive(AsRefStr)]
pub(super) enum InsertState<T: Send + Sync + 'static> {
    Data(T),
    Batch(Vec<T>),
}

pub(super) struct ExecutingQuery<T: Send + Sync + 'static> {
    qid:             Qid,
    query:           QueryState,
    header:          Option<Vec<(String, crate::Type)>>,
    header_response: Option<oneshot::Sender<Vec<(String, crate::Type)>>>,
    response:        ResponseSender<T>,
}

pub(super) struct PendingQuery<T: Send + Sync + 'static> {
    qid:      Qid,
    query:    String,
    settings: Option<Arc<Settings>>,
    response: oneshot::Sender<Result<ResponseReceiver<T>>>,
    header:   Option<oneshot::Sender<Vec<(String, crate::Type)>>>,
}

impl<T: Send + Sync + 'static> std::fmt::Debug for PendingQuery<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl<T: Send + Sync + 'static> std::fmt::Display for PendingQuery<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PendingQuery(qid={}, query={}, settings={:?}, channel={})",
            self.qid,
            self.query,
            self.settings,
            if self.response.is_closed() { &"CHANNEL_CLOSED" } else { &"CHANNEL_OPEN" },
        )
    }
}

pub(super) struct InternalClient<T: ClientFormat> {
    pending:   VecDeque<PendingQuery<T::Data>>,
    executing: Option<ExecutingQuery<T::Data>>,
    metadata:  ConnectionMetadata,
    events:    Arc<broadcast::Sender<Event>>,
}

impl<T: ClientFormat> InternalClient<T> {
    pub(super) const CAPACITY: usize = 1024;

    pub(super) fn new(metadata: ConnectionMetadata, events: Arc<broadcast::Sender<Event>>) -> Self {
        InternalClient {
            pending: VecDeque::with_capacity(Self::CAPACITY),
            executing: None,
            metadata,
            events,
        }
    }

    // // TODO: Use this for reconnect
    // #[expect(unused)]
    // pub(super) fn new_with_pending(
    //     pending: VecDeque<PendingQuery<T::Data>>,
    //     metadata: ConnectionMetadata,
    // ) -> Self {
    //     InternalClient { pending, metadata }
    // }

    #[instrument(level = "trace", skip_all, fields(clickhouse.client.id = self.metadata.client_id))]
    pub(super) async fn run<R: ClickhouseRead + 'static, W: ClickhouseWrite>(
        &mut self,
        mut reader: R,
        mut writer: W,
        mut operations: mpsc::Receiver<Message<T::Data>>,
    ) -> Result<()> {
        let cid = self.metadata.client_id;

        loop {
            tokio::select! {
                // Write loop
                Some(op) = operations.recv() => {
                    match op {
                        // Ping
                        Message::Ping { response } => {
                            Writer::send_ping(&mut writer).await?;
                            Self::receive_ping(&mut reader, self.metadata).await.inspect_err(|error|
                                error!(?error, {ATT_CID} = cid, "Failed to receive pong")
                            )?;
                            let _ = response.send(Ok(())).ok();
                        }
                        // Operation
                        Message::Operation { qid, op } => {
                            let o: &'static str = (&op).into();
                            if let Err(error) = self.handle_operation(&mut writer, op, qid).await {
                                error!(?error, {ATT_QID} = %qid, {ATT_CID} = cid, "Operation {o}");
                            }
                        }
                        // Shutdown
                        Message::Shutdown => {
                            info!({ ATT_CID } = cid, "Client is shutting down");
                            return Ok(());
                        }
                    }
                }

                // Read loop
                result = self.handle_read(&mut reader) => {
                    result.inspect_err(|error| {
                        error!(?error, { ATT_CID } = cid, "Fatal error, connection exiting");
                    })?;

                    if self.executing.as_ref().is_some_and(|e| e.query.is_finished()) {
                        drop(self.executing.take());
                        if let Some(query) = self.pending.pop_front() {
                            self.send_query(&mut writer, query).await?;
                        }
                    }
                }
            }
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(clickhouse.query.id = %qid, operation = op.as_ref(), pending = self.pending.len())
        err
    )]
    async fn handle_operation<W: ClickhouseWrite>(
        &mut self,
        writer: &mut W,
        op: Operation<T::Data>,
        qid: Qid,
    ) -> Result<()> {
        let cid = self.metadata.client_id;
        let (result, response) = match op {
            Operation::Query { query, settings, response, header } => {
                let pending = PendingQuery { qid, query, settings, response, header };
                if self.pending.is_empty() && self.executing.is_none() {
                    self.send_query(writer, pending).await?;
                } else {
                    self.pending.push_back(pending);
                }

                return Ok(());
            }
            Operation::Insert { data, response } => {
                let insert = InsertState::Data(data);
                let header = self.executing.as_ref().and_then(|e| e.header.as_deref());
                let result = self.send_insert(writer, insert, header, qid).await;
                (result, response)
            }
            Operation::InsertMany { data, response } => {
                let insert = InsertState::Batch(data);
                let header = self.executing.as_ref().and_then(|e| e.header.as_deref());
                let result = self.send_insert(writer, insert, header, qid).await;
                (result, response)
            }
        };

        if let Err(error) = result {
            error!(?error, { ATT_CID } = cid, { ATT_QID } = %qid, "Insert failed");
            let _ = response.send(Err(Error::Client(error.to_string()))).ok();

            if let Some(exec) = self.executing.as_mut() {
                exec.query = QueryState::Failed;
            }

            return Err(error);
        }

        let _ = response.send(Ok(())).ok();

        Ok(())
    }

    // READ

    #[instrument(
        level = "trace",
        skip_all,
        fields(clickhouse.client.id = self.metadata.client_id, clickhouse.query.id)
    )]
    async fn handle_read<R: ClickhouseRead + 'static>(&mut self, reader: &mut R) -> Result<()> {
        if let Some(qid) = self.executing.as_ref().map(|e| e.qid) {
            let _ = Span::current().record(ATT_QID, tracing::field::display(qid));
        }

        if self.executing.as_ref().is_some_and(|e| matches!(e.query, QueryState::Header)) {
            self.receive_header(reader).await?;
        } else {
            self.receive_packet(reader).await?;
        }

        Ok(())
    }

    async fn receive_packet<R: ClickhouseRead + 'static>(&mut self, reader: &mut R) -> Result<()> {
        let cid = self.metadata.client_id;
        let packet = Reader::receive_packet::<T>(reader, self.metadata).await?;
        let query_state = self.executing.as_ref().map(|e| e.query);
        let qid = self.executing.as_ref().map(|e| e.qid);
        debug!(
            { ATT_CID } = cid,
            { ATT_QID } = ?qid,
            packet = packet.as_ref(),
            query_state = ?query_state,
            "Received packet"
        );

        if let Some(e) = self.executing.as_mut() {
            e.query = std::mem::take(&mut e.query).next(&packet);
        }

        match packet {
            ServerPacket::Hello(_) => {
                return Err(Error::ProtocolError("Unexpected Server Hello".to_string()));
            }
            ServerPacket::Data(ServerData { block }) => {
                let Some(exec) = self.executing.as_ref() else {
                    return Err(Error::ProtocolError("Data block but no executing query".into()));
                };
                let _ = exec.response.send(Ok(block)).await.ok();
            }
            ServerPacket::ProfileEvents(info) => {
                if let Some(exec) = self.executing.as_ref() {
                    let _ = self
                        .events
                        .send(Event {
                            qid:       exec.qid,
                            event:     ClickhouseEvent::Profile(info),
                            client_id: cid,
                        })
                        .ok();
                }
            }
            ServerPacket::Progress(progress) => {
                if let Some(exec) = self.executing.as_ref() {
                    let _ = self
                        .events
                        .send(Event {
                            qid:       exec.qid,
                            event:     ClickhouseEvent::Progress(progress),
                            client_id: cid,
                        })
                        .ok();
                }
            }
            ServerPacket::Exception(exception) => {
                let error = exception.emit();
                self.handle_exception(error).await?;
            }
            ServerPacket::EndOfStream => {
                let qid = self.executing.as_ref().map(|e| e.qid);
                debug!({ ATT_CID } = cid, { ATT_QID } = ?qid, "Received END OF STREAM");
            }
            _ => {}
        }

        Ok(())
    }

    async fn receive_header<R: ClickhouseRead + 'static>(&mut self, reader: &mut R) -> Result<()> {
        let cid = self.metadata.client_id;
        let exec = self.executing.as_mut().unwrap();
        let qid = exec.qid;

        let packet = Reader::receive_header::<T>(reader, self.metadata).await?;
        exec.query = std::mem::take(&mut exec.query).next(&packet);
        debug!(
            { ATT_CID } = cid,
            { ATT_QID } = %qid,
            packet = packet.as_ref(),
            query_state = exec.query.as_ref(),
            "Received header packet"
        );

        match packet {
            ServerPacket::Header(block) => {
                let header = block.block.column_types;
                debug!(?header, { ATT_QID } = %qid, { ATT_CID } = cid, "Received HEADER");
                if let Some(respond) = exec.header_response.take() {
                    let _ = respond.send(header.clone()).ok();
                }
                exec.header = Some(header);
            }
            ServerPacket::ProfileEvents(info) => {
                let _ = self
                    .events
                    .send(Event {
                        qid:       exec.qid,
                        event:     ClickhouseEvent::Profile(info),
                        client_id: cid,
                    })
                    .ok();
            }
            ServerPacket::Progress(progress) => {
                let _ = self
                    .events
                    .send(Event {
                        qid:       exec.qid,
                        event:     ClickhouseEvent::Progress(progress),
                        client_id: cid,
                    })
                    .ok();
            }
            ServerPacket::Exception(exception) => {
                let error = exception.emit();
                self.handle_exception(error).await?;
            }
            ServerPacket::EndOfStream | ServerPacket::TableColumns(_) => {}
            packet => {
                return Err(Error::ProtocolError(format!(
                    "expected header block, got: {}",
                    ServerPacketId::from(&packet).as_ref()
                )));
            }
        }

        Ok(())
    }

    /// Wait for pong
    async fn receive_ping<R: ClickhouseRead + 'static>(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<()> {
        let packet = Reader::receive_packet::<T>(reader, metadata).await?;
        if !matches!(packet, ServerPacket::Pong) {
            return Err(Error::ProtocolError("Expected Pong".to_string()));
        }
        trace!({ ATT_CID } = metadata.client_id, "Pong received");
        Ok(())
    }

    async fn handle_exception(&mut self, error: ServerError) -> Result<()> {
        let cid = self.metadata.client_id;
        if let Some(exec) = self.executing.take() {
            error!({ATT_QID} = %exec.qid, {ATT_CID} = cid, "Received EXCEPTION: {error}");
            let _ = exec.response.send(Err(error.clone().into())).await.ok();
            if error.is_fatal() {
                return Err(error.into());
            }
        } else {
            return Err(error.into());
        }
        Ok(())
    }

    // WRITE

    async fn send_query<W: ClickhouseWrite>(
        &mut self,
        writer: &mut W,
        query: PendingQuery<T::Data>,
    ) -> Result<()> {
        let PendingQuery { qid, query, settings, response, header } = query;
        // Send initial query
        let query = Query {
            qid,
            query: &query,
            settings,
            stage: QueryProcessingStage::Complete,
            info: ClientInfo::default(),
        };

        debug!({ ATT_QID } = %qid, query.query, "sending query");
        if let Err(error) = Writer::send_query(writer, query, self.metadata).await {
            error!(?error, "Query failed to send");
            drop(response.send(Err(Error::Client(error.to_string()))));
            return Err(error);
        }

        // Send back the data response channel
        let (sender, receiver) = mpsc::channel(32);
        let _ = response.send(Ok(receiver)).ok();

        self.executing = Some(ExecutingQuery {
            qid,
            query: QueryState::Header,
            header: None,
            header_response: header,
            response: sender,
        });

        self.send_delimiter(writer, qid).await?;
        trace!({ ATT_QID } = %qid, "sent query and delimiter");

        Ok(())
    }

    #[instrument(skip_all, fields(clickhouse.query.id = %qid))]
    async fn send_insert<W: ClickhouseWrite>(
        &self,
        writer: &mut W,
        insert: InsertState<T::Data>,
        header: Option<&[(String, crate::Type)]>,
        qid: Qid,
    ) -> Result<()> {
        trace!({ ATT_QID } = %qid, ?header, insert = insert.as_ref(), "Inserting data");
        match insert {
            InsertState::Data(data) => {
                Writer::send_data::<T>(writer, data, qid, header, self.metadata).await?;
                self.send_delimiter(writer, qid).await?;
            }
            InsertState::Batch(data) => {
                if !data.is_empty() {
                    for block in data {
                        Writer::send_data::<T>(writer, block, qid, header, self.metadata).await?;
                    }
                }
                self.send_delimiter(writer, qid).await?;
            }
        }
        Ok(())
    }

    async fn send_delimiter<W: ClickhouseWrite>(&self, writer: &mut W, qid: Qid) -> Result<()> {
        Writer::send_data::<NativeFormat>(
            writer,
            Block { info: BlockInfo::default(), rows: 0, ..Default::default() },
            qid,
            None,
            self.metadata,
        )
        .await
    }

    pub(crate) async fn perform_handshake<RW: ClickhouseRead + ClickhouseWrite + Send + 'static>(
        stream: &mut RW,
        client_id: u16,
        options: &ClientOptions,
    ) -> Result<ServerHello> {
        use crate::client::reader::Reader;
        use crate::client::writer::Writer;

        Writer::send_hello(stream, ClientHello {
            default_database: options.default_database.to_string(),
            username:         options.username.to_string(),
            password:         options.password.get().to_string(),
        })
        .await
        .inspect_err(|error| error!(?error, { ATT_CID } = client_id, "Failed to send hello"))?;

        let packet_id = Reader::read_packet(stream).await?;

        let hello = match packet_id {
            ServerPacketId::Hello => {
                Reader::read_hello(stream, client_id).await.inspect_err(|error| {
                    error!(?error, { ATT_CID } = client_id, "Failed to receive hello");
                })?
            }
            ServerPacketId::Exception => {
                return Err(Reader::read_exception(stream).await?.emit().into());
            }
            packet => {
                return Err(Error::ProtocolError(format!(
                    "unexpected packet {packet:?}, expected server hello"
                )));
            }
        };

        trace!({ ATT_CID } = client_id, "Finished handshake");

        // No-op if revision doesn't match
        Writer::send_addendum(stream, hello.revision_version).await?;

        Ok(hello)
    }
}
