use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU16;

use strum::{AsRefStr, IntoStaticStr};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::Event;
use super::chunk::ChunkWriter;
use super::connection::ClientMetadata;
use super::reader::Reader;
use super::writer::{Query, Writer};
use crate::ClickHouseEvent;
use crate::errors::*;
use crate::formats::DeserializerState;
use crate::io::{ClickHouseRead, ClickHouseWrite};
use crate::native::block::Block;
use crate::native::block_info::BlockInfo;
use crate::native::client_info::ClientInfo;
use crate::native::protocol::{QueryProcessingStage, ServerData, ServerHello, ServerPacket};
use crate::prelude::*;
use crate::query::QueryParams;
use crate::settings::Settings;

type ResponseReceiver<T> = mpsc::Receiver<Result<T>>;
type ResponseSender<T> = mpsc::Sender<Result<T>>;

static CONN_ID: AtomicU16 = AtomicU16::new(0);

pub(crate) enum Message<Data: Send + Sync> {
    Operation { qid: Qid, op: Operation<Data> },
    Shutdown,
}

#[derive(AsRefStr, IntoStaticStr)]
pub(crate) enum Operation<Data: Send + Sync> {
    #[strum(serialize = "Ping")]
    Ping { response: oneshot::Sender<Result<()>> },
    #[strum(serialize = "Query")]
    Query {
        query:    String,
        settings: Option<Arc<Settings>>,
        params:   Option<QueryParams>,
        response: oneshot::Sender<Result<ResponseReceiver<Data>>>,
        header:   Option<oneshot::Sender<Vec<(String, Type)>>>,
    },
    #[strum(serialize = "Insert")]
    Insert { data: Data, response: oneshot::Sender<Result<()>> },
    #[strum(serialize = "InsertMany")]
    InsertMany { data: Vec<Data>, response: oneshot::Sender<Result<()>> },
}

// Track operation tasks
#[allow(variant_size_differences)] // Expect doesn't work here
enum OperationTask {
    Chunk(ChunkBoundary),
    Ping(oneshot::Sender<Result<()>>),
    Shutdown,
}

impl Default for OperationTask {
    fn default() -> Self { Self::Chunk(ChunkBoundary::default()) }
}

/// Track chunk boundaries. NOTE: Only relevant with chunked protocol for writing
#[derive(Clone, Default, Copy, Debug, PartialEq, Eq, Hash)]
enum ChunkBoundary {
    #[default]
    None,
    Flush,
}

/// Internal tracking
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, Hash, AsRefStr)]
pub(super) enum QueryState {
    // Waiting for header block
    Header,
    #[default]
    InProgress,
}

/// Internal enum for inserts
#[derive(AsRefStr)]
pub(super) enum InsertState<T> {
    Data(T),
    Batch(Vec<T>),
}

pub(super) struct ExecutingQuery<T: Send + Sync> {
    qid:             Qid,
    state:           QueryState,
    header:          Option<Vec<(String, Type)>>,
    header_response: Option<oneshot::Sender<Vec<(String, Type)>>>,
    response:        ResponseSender<T>,
}

pub(super) struct PendingQuery<T: Send + Sync> {
    qid:      Qid,
    query:    String,
    settings: Option<Arc<Settings>>,
    params:   Option<QueryParams>,
    response: oneshot::Sender<Result<ResponseReceiver<T>>>,
    header:   Option<oneshot::Sender<Vec<(String, Type)>>>,
}

pub(super) struct InternalConn<T: ClientFormat> {
    cid:          &'static str,
    server_hello: Arc<ServerHello>,
    pending:      VecDeque<PendingQuery<T::Data>>,
    executing:    Option<ExecutingQuery<T::Data>>,
    events:       Arc<broadcast::Sender<Event>>,
    metadata:     ClientMetadata,
    state:        DeserializerState<T::Deser>,
}

impl<T: ClientFormat> InternalConn<T> {
    pub(super) const CAPACITY: usize = 1024;

    pub(super) fn new(
        metadata: ClientMetadata,
        events: Arc<broadcast::Sender<Event>>,
        server_hello: Arc<ServerHello>,
    ) -> Self {
        // Generate a unique connection id. Since `Connection` supports 2 connections in `fast_mode`
        // it's helpful to distinguish.
        let conn_id = CONN_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let cid = Box::leak(format!("{}.{conn_id}", metadata.client_id).into_boxed_str());
        let state = DeserializerState::default().with_arrow_options(metadata.arrow_options);
        InternalConn {
            cid,
            server_hello,
            pending: VecDeque::with_capacity(Self::CAPACITY),
            executing: None,
            metadata,
            events,
            state,
        }
    }

    #[instrument(
        level = "trace",
        name = "run",
        skip_all,
        fields(clickhouse.connection.id = self.cid),
        err
    )]
    pub(super) async fn run<R: ClickHouseRead + 'static, W: ClickHouseWrite>(
        &mut self,
        mut reader: R,
        mut writer: W,
        mut operations: mpsc::Receiver<Message<T::Data>>,
    ) -> Result<()> {
        loop {
            match self.run_inner(&mut reader, &mut writer, &mut operations).await? {
                OperationTask::Shutdown => return Ok(()),
                OperationTask::Ping(response) => {
                    let cid = self.cid;
                    let revision = self.server_hello.revision_version;
                    let result =
                        Self::receive_ping(&mut reader, revision, self.metadata, cid).await;
                    let _ = response.send(result).ok();
                }
                OperationTask::Chunk(_) => {}
            }
        }
    }

    #[instrument(
        level = "trace",
        name = "run_chunked",
        skip_all,
        fields(clickhouse.connection.id = self.cid),
        err
    )]
    pub(super) async fn run_chunked<R: ClickHouseRead + 'static, W: ClickHouseWrite>(
        &mut self,
        mut reader: R,
        mut writer: ChunkWriter<W>,
        mut operations: mpsc::Receiver<Message<T::Data>>,
    ) -> Result<()> {
        loop {
            match self.run_inner(&mut reader, &mut writer, &mut operations).await? {
                OperationTask::Ping(response) => {
                    // Be sure to flush the Ping
                    writer.finish_chunk().await?;
                    let cid = self.cid;
                    let revision = self.server_hello.revision_version;
                    let result =
                        Self::receive_ping(&mut reader, revision, self.metadata, cid).await;
                    let _ = response.send(result).ok();
                }
                // Logical chunk boundary, flush
                OperationTask::Chunk(ChunkBoundary::Flush) => writer.finish_chunk().await?,
                OperationTask::Chunk(ChunkBoundary::None) => {}
                OperationTask::Shutdown => return Ok(()),
            }
        }
    }

    async fn run_inner<R: ClickHouseRead + 'static, W: ClickHouseWrite>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
        operations: &mut mpsc::Receiver<Message<T::Data>>,
    ) -> Result<OperationTask> {
        let cid = self.cid;

        // Track whether logical chunk boundaries are encountered
        let mut flush = OperationTask::default();

        tokio::select! {
            // Write loop
            Some(op) = operations.recv() => {
                trace!(message = ?op, { ATT_CON } = cid, "Received operation");
                match op {
                    // Operation
                    Message::Operation { qid, op } => {
                        flush = self.handle_operation(writer, op, qid).await?;
                    }
                    // Shutdown
                    Message::Shutdown => {
                        info!({ ATT_CON } = cid, "Client is shutting down");
                        return Ok(OperationTask::Shutdown);
                    }
                }
            }

            // Read loop
            result = self.receive_packet(reader), if self.executing.is_some() => {
                result.inspect_err(|error| error!(?error, { ATT_CID } = cid, "Fatal error"))?;

                // Queue up next query if any
                if self.executing.is_none() {
                    if let Some(query) = self.pending.pop_front() {
                        self.send_query(writer, query).await?;
                        flush = OperationTask::Chunk(ChunkBoundary::Flush);
                    }
                }
            }
            else => {}
        };

        Ok(flush)
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            clickhouse.connection.id = self.cid,
            clickhouse.query.id = %qid,
            operation = op.as_ref(),
            pending = self.pending.len()
        )
        err
    )]
    async fn handle_operation<W: ClickHouseWrite>(
        &mut self,
        writer: &mut W,
        op: Operation<T::Data>,
        qid: Qid,
    ) -> Result<OperationTask> {
        // Track logical chunk boundaries
        let (result, response) = match op {
            // Ping
            Operation::Ping { response } => {
                if self.pending.is_empty() && self.executing.is_none() {
                    Writer::send_ping(writer).await?;
                    return Ok(OperationTask::Ping(response));
                }
                return Ok(OperationTask::default());
            }
            // Query - NOTE: May be any type of query, ie DDL, DML, Settings, etc.
            Operation::Query { query, settings, params, response, header } => {
                let pending = PendingQuery { qid, query, settings, params, response, header };
                if self.pending.is_empty() && self.executing.is_none() {
                    self.send_query(writer, pending).await?;
                    return Ok(OperationTask::Chunk(ChunkBoundary::Flush));
                }
                self.pending.push_back(pending);
                return Ok(OperationTask::default());
            }
            // Inserts
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

        // Return result to caller
        if let Err(error) = result {
            error!(?error, { ATT_CON } = self.cid, { ATT_QID } = %qid, "Insert failed");
            if let Some(exec) = self.executing.as_ref() {
                let _ = exec.response.send(Err(Error::Client(error.to_string()))).await.ok();
            }
            return Err(error);
        }

        // Insert successful
        trace!({ ATT_CON } = self.cid, { ATT_QID } = %qid, "Insert sent successfully");
        let _ = response.send(Ok(())).ok();

        Ok(OperationTask::Chunk(ChunkBoundary::Flush))
    }

    // READ

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            clickhouse.connection.id = self.cid,
            clickhouse.query.id,
            clickhouse.packet.id,
            executing.query,
        ),
        err
    )]
    async fn receive_packet<R: ClickHouseRead + 'static>(&mut self, reader: &mut R) -> Result<()> {
        let cid = self.cid;
        let client_id = self.metadata.client_id;
        let revision = self.server_hello.revision_version;
        let Some(exec) = self.executing.as_mut() else {
            return Err(Error::Protocol("No executing query, would block".into()));
        };

        let qid = exec.qid;
        let _ = Span::current().record("executing.query", tracing::field::display(&exec));
        let _ = Span::current().record(ATT_QID, tracing::field::display(qid));
        trace!({ ATT_CON } = cid, { ATT_QID } = %qid, state = exec.state.as_ref(), "receiving");

        // Wait for packet from server
        let packet = if matches!(exec.state, QueryState::Header) {
            Reader::receive_header::<T>(reader, revision, self.metadata).await?
        } else {
            Reader::receive_packet::<T>(reader, revision, self.metadata, &mut self.state).await?
        };

        let _ = Span::current().record(ATT_PID, packet.as_ref());
        debug!({ ATT_CON } = cid, { ATT_QID } = %qid, packet = packet.as_ref(), "packet");

        match packet {
            ServerPacket::Header(block) => {
                exec.state = QueryState::InProgress;
                let header = block.block.column_types;
                debug!(?header, { ATT_QID } = %qid, { ATT_CON } = cid, "HEADER");
                if let Some(respond) = exec.header_response.take() {
                    let _ = respond.send(header.clone()).ok();
                }
                exec.header = Some(header);
            }
            ServerPacket::Data(ServerData { block }) => {
                let _ = exec.response.send(Ok(block)).await.ok();
            }
            ServerPacket::ProfileEvents(info) => {
                let event = ClickHouseEvent::Profile(info);
                let _ = self.events.send(Event { event, qid, client_id }).ok();
            }
            ServerPacket::Progress(progress) => {
                let event = ClickHouseEvent::Progress(progress);
                let _ = self.events.send(Event { event, qid, client_id }).ok();
            }
            ServerPacket::Exception(exception) => {
                let error = exception.emit();
                error!({ ATT_QID } = %exec.qid, { ATT_CON } = cid, "EXCEPTION: {error}");
                let _ = exec.response.send(Err(error.clone().into())).await.ok();
                drop(self.executing.take());
                if error.is_fatal() {
                    return Err(error.into());
                }
                T::reset_state(&mut self.state);
            }
            ServerPacket::EndOfStream => {
                debug!({ ATT_CON } = cid, { ATT_QID } = %qid, "END OF STREAM");
                drop(self.executing.take());
                T::reset_state(&mut self.state);
            }
            ServerPacket::Hello(_) => {
                return Err(Error::Protocol("Unexpected Server Hello".to_string()));
            }
            // Ignored
            // TODO: Should profile info be returned to caller?
            ServerPacket::ProfileInfo(info) => {
                debug!(?info, "Profile info");
            }
            ServerPacket::Ignore(ignored) => trace!(ignored = ignored.as_ref(), "Ignored packet"),

            _ => {}
        }
        Ok(())
    }

    async fn receive_ping<R: ClickHouseRead + 'static>(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
        cid: &'static str,
    ) -> Result<()> {
        let mut state = DeserializerState::default();
        let packet = Reader::receive_packet::<T>(reader, revision, metadata, &mut state)
            .await
            .inspect_err(|error| error!(?error, { ATT_CON } = cid, "Failed pong"))?;

        if !matches!(packet, ServerPacket::Pong) {
            return Err(Error::Protocol("Expected Pong".to_string()));
        }

        trace!({ ATT_CON } = metadata.client_id, "Pong received");

        Ok(())
    }

    // WRITE

    #[instrument(skip_all, fields(clickhouse.query.id = %query.qid), err)]
    async fn send_query<W: ClickHouseWrite>(
        &mut self,
        writer: &mut W,
        query: PendingQuery<T::Data>,
    ) -> Result<()> {
        let PendingQuery { qid, query, settings, params, response, header } = query;
        debug!({ ATT_CON } = self.cid, { ATT_QID } = %qid, query, "sending query");

        // Send initial query
        if let Err(error) = Writer::send_query(
            writer,
            Query {
                qid,
                query: &query,
                settings,
                params,
                stage: QueryProcessingStage::Complete,
                info: ClientInfo::default(),
            },
            self.server_hello.settings.as_ref(),
            self.server_hello.revision_version,
            self.metadata,
        )
        .await
        {
            error!(?error, { ATT_CON } = self.cid, { ATT_QID } = %qid, "Query failed to send");
            drop(response.send(Err(Error::Client(error.to_string()))));
            return Err(error);
        }

        trace!({ ATT_CON } = self.cid, { ATT_QID } = %qid, "query sent");

        // Send back the data response channel
        let (sender, receiver) = mpsc::channel(32);
        let _ = response.send(Ok(receiver)).ok();

        self.executing = Some(ExecutingQuery {
            qid,
            state: QueryState::Header,
            header: None,
            header_response: header,
            response: sender,
        });

        self.send_delimiter(writer, qid).await?;
        trace!({ ATT_CON } = self.cid, { ATT_QID } = %qid, "sent query and delimiter");

        Ok(())
    }

    #[instrument(skip_all, fields(clickhouse.query.id = %qid), err)]
    async fn send_insert<W: ClickHouseWrite>(
        &self,
        writer: &mut W,
        insert: InsertState<T::Data>,
        header: Option<&[(String, Type)]>,
        qid: Qid,
    ) -> Result<()> {
        let revision = self.server_hello.revision_version;
        trace!({ ATT_CID } = self.cid, { ATT_QID } = %qid, insert = insert.as_ref(), "Inserting");
        match insert {
            InsertState::Data(data) => {
                Writer::send_data::<T>(writer, data, qid, header, revision, self.metadata).await?;
                self.send_delimiter(writer, qid).await?;
            }
            InsertState::Batch(data) => {
                if !data.is_empty() {
                    for block in data {
                        Writer::send_data::<T>(writer, block, qid, header, revision, self.metadata)
                            .await?;
                    }
                }
                self.send_delimiter(writer, qid).await?;
            }
        }

        Ok(())
    }

    async fn send_delimiter<W: ClickHouseWrite>(&self, writer: &mut W, qid: Qid) -> Result<()> {
        Writer::send_data::<NativeFormat>(
            writer,
            Block { info: BlockInfo::default(), rows: 0, ..Default::default() },
            qid,
            None,
            self.server_hello.revision_version,
            self.metadata,
        )
        .await
    }
}

#[cfg(feature = "fast_mode")]
impl<Data: Send + Sync + 'static> Operation<Data> {
    /// Default helper method to calculate the "load" or weight an operation incurs
    pub(crate) fn weight(&self, finished: bool) -> u8 {
        match self {
            Operation::Query { .. } if finished => 1,
            Operation::Query { .. } | Operation::InsertMany { .. } => 3,
            Operation::Insert { .. } => 2,
            Operation::Ping { .. } => 0,
        }
    }

    // Helper functions to account for full weight across common operations
    pub(crate) fn weight_query() -> u8 { 1 }

    pub(crate) fn weight_insert() -> u8 { 5 }

    pub(crate) fn weight_insert_many() -> u8 { 6 }
}

impl<Data: Send + Sync + 'static> std::fmt::Debug for Message<Data> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Shutdown => write!(f, "Message(Shutdown)"),
            Message::Operation { qid, op } => write!(f, "Message({}, qid={qid})", op.as_ref()),
        }
    }
}

impl<T: Send + Sync + 'static> std::fmt::Debug for ExecutingQuery<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ExecutingQuery(qid={}, header={:?}, header_response={:?}, response={})",
            self.qid,
            self.header,
            if self.header_response.as_ref().is_some_and(|h| !h.is_closed()) {
                &"CHANNEL_OPEN"
            } else {
                &"CHANNEL_CLOSED"
            },
            if self.response.is_closed() { &"CHANNEL_CLOSED" } else { &"CHANNEL_OPEN" },
        )
    }
}

impl<T: Send + Sync + 'static> std::fmt::Display for ExecutingQuery<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ExecutingQuery(qid={}, columns={}, header_response={:?}, response={})",
            self.qid,
            self.header.as_ref().map(Vec::len).unwrap_or_default(),
            if self.header_response.as_ref().is_some_and(|h| !h.is_closed()) {
                &"OPEN"
            } else {
                &"CLOSED"
            },
            if self.response.is_closed() { &"CLOSED" } else { &"OPEN" },
        )
    }
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
            "PendingQuery(qid={}, query={}, settings={:?}, params={:?}, channel={})",
            self.qid,
            self.query,
            self.settings,
            self.params,
            if self.response.is_closed() { &"CLOSED" } else { &"OPEN" },
        )
    }
}
