use std::collections::VecDeque;
use std::sync::Arc;

use strum::{AsRefStr, IntoStaticStr};
use tokio::sync::{mpsc, oneshot};
use tracing::field;

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
    ClientHello, QueryProcessingStage, ServerHello, ServerPacket, ServerPacketId,
};
use crate::prelude::*;
use crate::settings::Settings;
use crate::{ClientOptions, Response};

pub(crate) type SerializeData<T> =
    Box<dyn FnOnce(Vec<(String, crate::Type)>) -> Result<T> + Send + Sync + 'static>;

/// Query response after initial query
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryResponse {
    pub qid:    Qid,
    pub header: Option<Vec<(String, crate::Type)>>,
}

#[derive(AsRefStr, IntoStaticStr)]
pub(crate) enum Operation<Data: Send + Sync + 'static> {
    #[strum(serialize = "Ping")]
    Ping,
    #[strum(serialize = "Query")]
    Query {
        query:    String,
        settings: Option<Arc<Settings>>,
        metadata: oneshot::Sender<QueryResponse>,
    },
    #[strum(serialize = "Insert")]
    Insert {
        query:    String,
        data:     Data,
        settings: Option<Arc<Settings>>,
        metadata: oneshot::Sender<QueryResponse>,
    },
    #[strum(serialize = "InsertMany")]
    InsertMany {
        query:    String,
        data:     Vec<Data>,
        settings: Option<Arc<Settings>>,
        metadata: oneshot::Sender<QueryResponse>,
    },
    #[strum(serialize = "InsertRows")]
    InsertRows {
        query:    String,
        get_data: SerializeData<Data>,
        settings: Option<Arc<Settings>>,
        metadata: oneshot::Sender<QueryResponse>,
    },
}

#[derive(AsRefStr, IntoStaticStr)]
pub(crate) enum Message<Data: Send + Sync + 'static> {
    Operation { qid: Qid, op: Operation<Data>, response: mpsc::Sender<Response<Data>> },
    Shutdown,
}

/// Internal tracking
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
pub(super) enum QueryState {
    #[default]
    InProgress,
    Broken,
    Failed,
    Finished,
    // TODO: Implement this below when cancel query is figured out.
    #[expect(unused)]
    Cancelled,
}

impl QueryState {
    fn next<T>(self, packet: &ServerPacket<T>) -> Self {
        let packet_id = ServerPacketId::from(packet);
        match (self, packet_id) {
            (QueryState::Finished | QueryState::Failed | QueryState::Cancelled, _) => self,
            (_, ServerPacketId::EndOfStream) => QueryState::Finished,
            (_, ServerPacketId::Exception) => QueryState::Failed,
            (QueryState::Broken, _) => QueryState::Broken,
            (_, _) => QueryState::InProgress,
        }
    }

    fn broken(self) -> Self { if self.is_finished() { self } else { QueryState::Broken } }

    fn is_finished(self) -> bool { matches!(self, QueryState::Finished | QueryState::Failed) }

    fn is_broken(self) -> bool { matches!(self, QueryState::Broken) }

    fn is_cancelled(self) -> bool { matches!(self, QueryState::Cancelled) }
}

/// Internal enum for inserts
#[derive(AsRefStr)]
pub(super) enum InsertState<T: Send + Sync + 'static> {
    Data(T),
    Batch(Vec<T>),
    Rows(SerializeData<T>),
}

/// Internal enum for operation state
#[derive(Default, AsRefStr)]
pub(super) enum OperationState<T: Send + Sync + 'static> {
    /// Signals waiting for a pong from the server
    Pong,
    /// Signals waiting for a header block from server for inserts
    InsertHeader((Option<oneshot::Sender<QueryResponse>>, InsertState<T>)),
    /// Signals received header block, ready to insert
    Data((Vec<(String, crate::Type)>, InsertState<T>)),
    /// Signals sent simple query, waiting for header block from server
    QueryHeader(Option<oneshot::Sender<QueryResponse>>),
    /// Signals operation is in waiting state
    #[default]
    Waiting,
}

pub(super) struct PendingQuery<T: Send + Sync + 'static> {
    qid:      Qid,
    query:    QueryState,
    response: mpsc::Sender<Response<T>>,
    op_state: OperationState<T>,
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
            "PendingQuery(qid={}, query={}, state={}, channel={})",
            self.qid,
            self.query.as_ref(),
            self.op_state.as_ref(),
            if self.response.is_closed() { &"CHANNEL_CLOSED" } else { &"CHANNEL_OPEN" },
        )
    }
}

/// Represents the different type of "initial" blocks that can be received
enum HeaderBlock {
    Header(Vec<(String, crate::Type)>),
    Other(oneshot::Sender<QueryResponse>),
    EndOfStream,
    Ignore,
}

pub(super) struct InternalClient<T: ClientFormat> {
    pending:  VecDeque<PendingQuery<T::Data>>,
    metadata: ConnectionMetadata,
}

impl<T: ClientFormat> InternalClient<T> {
    pub(super) const CAPACITY: usize = 1024;

    pub(super) fn new(metadata: ConnectionMetadata) -> Self {
        InternalClient { pending: VecDeque::with_capacity(Self::CAPACITY), metadata }
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
        let client_id = self.metadata.client_id;

        loop {
            tokio::select! {
                // Inbound loop
                Some(op) = operations.recv(), if self.pending.len() < Self::CAPACITY => {
                    match op {
                        // Response
                        Message::Operation { qid, op, response } => {
                            let operation: &'static str = (&op).into();
                            self
                                .handle_operation(&mut writer, op, qid, response)
                                .await
                                .inspect_err(|error| error!(
                                    ?error,
                                    operation,
                                    { ATT_QID } = %qid,
                                    { ATT_CID } = client_id,
                                    "Operation error"
                                ))?;
                        }
                        // Shutdown
                        Message::Shutdown => {
                            info!({ ATT_CID } = client_id, "Client is shutting down");
                            return Ok(());
                        }
                    }
                }

                // Outbound loop
                next_state = self.handle_query(&mut reader), if !self.pending.is_empty() => {
                    if let Err(error) = self.handle_responses(next_state, &mut writer).await {
                        error!(
                            ?error, { ATT_CID } = client_id, pending = ?self.pending,
                            "Query failed, closing connection"
                        );
                        return Err(error);
                    }
                }

                else => if self.pending.is_empty() {
                    break Ok(())
                } else {
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Drain any queries in progress for reconnect
    pub(super) fn drain_pending(mut self) -> VecDeque<PendingQuery<T::Data>> {
        self.pending.drain(..).filter(|p| matches!(p.query, QueryState::InProgress)).collect()
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
        response: mpsc::Sender<Response<T::Data>>,
    ) -> Result<()> {
        let cid = self.metadata.client_id;
        let operation: &'static str = (&op).into();
        debug!(operation, { ATT_QID } = %qid, { ATT_CID } = cid, "Running operation");
        let op_state = match op {
            Operation::Ping => {
                Writer::send_ping(writer).await?;
                OperationState::Pong
            }
            Operation::Query { query, settings, metadata } => {
                self.send_query(writer, &query, settings, qid).await?;
                OperationState::QueryHeader(Some(metadata))
            }
            Operation::Insert { query, data, settings, metadata } => {
                self.send_query(writer, &query, settings, qid).await?;
                OperationState::InsertHeader((Some(metadata), InsertState::Data(data)))
            }
            Operation::InsertMany { query, data, settings, metadata } => {
                self.send_query(writer, &query, settings, qid).await?;
                OperationState::InsertHeader((Some(metadata), InsertState::Batch(data)))
            }
            Operation::InsertRows { query, get_data, settings, metadata } => {
                self.send_query(writer, &query, settings, qid).await?;
                OperationState::InsertHeader((Some(metadata), InsertState::Rows(get_data)))
            }
        };

        self.pending.push_back(PendingQuery {
            qid,
            query: QueryState::InProgress,
            response,
            op_state,
        });

        Ok(())
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(query.state = ?state, pending = self.pending.len())
        err(Display)
    )]
    async fn handle_responses<W: ClickhouseWrite>(
        &mut self,
        state: Result<QueryState>,
        writer: &mut W,
    ) -> Result<()> {
        let cid = self.metadata.client_id;
        trace!(?state, pending = self.pending.len(), { ATT_CID } = cid, "Next state");
        let next_state = match state {
            Ok(s) => s,
            Err(error) => {
                error!(?error, { ATT_CID } = cid, "Error receiving packet");

                // Fatal errors should kill the connection
                if let ClickhouseNativeError::ServerException(exc) = &error {
                    if exc.is_fatal() {
                        error!(?error, { ATT_CID } = cid, "Fatal error encountered, exiting");
                        return Err(error);
                    }
                }

                // Pending queries will just finish out the query and continue
                if self.pending.len() > 1 {
                    drop(self.pending.pop_front());
                    return Ok(());
                }

                // Otherwise consider fatal
                return Err(error);
            }
        };

        let maybe_insert =
            self.pending.front_mut().and_then(|p| match std::mem::take(&mut p.op_state) {
                OperationState::Data((h, i)) => Some((h, i, p.qid)),
                state => {
                    p.op_state = state;
                    None
                }
            });

        // If an insert pending, insert the data
        if let Some((header, insert, qid)) = maybe_insert {
            // Failure of insert is client side, most likely data related, ie serialization.
            if let Err(error) = self.send_insert(writer, insert, &header, qid).await {
                error!(?error, { ATT_CID } = cid, "Insert failed");
                // Fail the query, send cancel, send error to client.
                if let Some(pending) = self.pending.pop_front() {
                    let qid = pending.qid;
                    trace!({ ATT_CID } = cid, { ATT_QID } = %qid, "Sending cancel to server");

                    // Set the query to cancelled, ignores if response is closed
                    let _ = pending
                        .response
                        .send(Response::ClientError(ClickhouseNativeError::Client(
                            error.to_string(),
                        )))
                        .await
                        .inspect_err(|_| error!("Failed to send error, response closed"))
                        .ok();
                }

                // As soon as cancel is figured out, remove this error as the underlying client is
                // still good
                return Err(error);
            }

        // Remove the query if finished
        } else if next_state.is_finished() {
            if let Some(previous) = self.pending.pop_front() {
                debug!(%previous, { ATT_CID } = cid, "Query FINISHED");
            } else {
                debug!({ ATT_CID } = cid, "Query FINISHED");
            }
        }

        Ok(())
    }

    // READ

    #[instrument(
        level = "trace",
        skip_all,
        fields(clickhouse.client.id = self.metadata.client_id, clickhouse.query.id)
    )]
    async fn handle_query<R: ClickhouseRead + 'static>(
        &mut self,
        reader: &mut R,
    ) -> Result<QueryState> {
        let Some(pending) = self.pending.front_mut() else {
            return Err(ClickhouseNativeError::ProtocolError(
                "No pending queries, unexpected read".into(),
            ));
        };

        let cid = self.metadata.client_id;
        let qid = pending.qid;
        let _ = Span::current().record(ATT_QID, field::display(qid));
        trace!(%pending, { ATT_CID } = cid, { ATT_QID } = %qid, "Handling current query");

        match std::mem::take(&mut pending.op_state) {
            OperationState::Pong => {
                Self::receive_ping(reader, qid, self.metadata).await?;
                pending.query = QueryState::Finished;
            }
            OperationState::InsertHeader((mut respond, data)) => {
                let respond = respond.take().unwrap();
                let responses = &pending.response;
                let header =
                    Self::receive_header(reader, qid, respond, responses, self.metadata).await?;

                // The 3 types of header blocks that can be received
                match header {
                    HeaderBlock::Header(header) => {
                        pending.op_state = OperationState::Data((header, data));
                    }
                    HeaderBlock::Other(respond) => {
                        pending.op_state = OperationState::InsertHeader((Some(respond), data));
                    }
                    HeaderBlock::Ignore | HeaderBlock::EndOfStream => {
                        return Err(ClickhouseNativeError::ProtocolError(
                            "Unexpected response while waiting for header block".into(),
                        ));
                    }
                }
            }
            OperationState::QueryHeader(mut respond) => {
                let respond = respond.take().unwrap();
                let responses = &pending.response;
                let header =
                    Self::receive_header(reader, qid, respond, responses, self.metadata).await?;

                if let HeaderBlock::EndOfStream = header {
                    pending.query = QueryState::Finished;
                }
            }
            _ => {
                pending.query = Self::receive_responses(
                    reader,
                    qid,
                    pending.query,
                    &pending.response,
                    self.metadata,
                )
                .await?;
            }
        }

        Ok(pending.query)
    }

    async fn receive_responses<R: ClickhouseRead + 'static>(
        reader: &mut R,
        qid: Qid,
        state: QueryState,
        response: &mpsc::Sender<Response<T::Data>>,
        metadata: ConnectionMetadata,
    ) -> Result<QueryState> {
        let cid = metadata.client_id;
        let mut state = state;
        trace!(?state, "Recieving response packets");

        loop {
            let packet = Reader::receive_packet::<T>(reader, metadata).await?;
            state = state.next(&packet);
            trace!(
                query_state = state.as_ref(),
                { ATT_QID } = %qid,
                { ATT_CID } = cid,
                { ATT_PID } = packet.as_ref(),
                "Received Packet",
            );

            // If cancelled, return
            if state.is_cancelled() {
                match packet {
                    ServerPacket::Exception(exc) => {
                        let error = exc.emit();
                        error!({ATT_QID} = %qid, {ATT_CID} = cid, "EXCEPTION AFTER CANCEL: {error}");
                        state = QueryState::Finished;
                    }
                    _ => {
                        return Err(ClickhouseNativeError::ProtocolError(
                            "Unexpected packet after cancellation".into(),
                        ));
                    }
                }

            // If not broken (receiver gone) means the packet must still be read
            } else if !state.is_broken() {
                let mut fatal = None;

                let message = match packet {
                    ServerPacket::Data(data) => Some(Response::Data(data.block)),
                    ServerPacket::ProfileEvents(info) => Some(Response::Profile(info)),
                    ServerPacket::Progress(progress) => Some(Response::Progress(progress)),
                    ServerPacket::Exception(exc) => {
                        let error = exc.emit();
                        error!({ATT_QID} = %qid, {ATT_CID} = cid, "Received EXCEPTION: {error}");
                        if error.is_fatal() {
                            fatal = Some(error.clone());
                        }
                        Some(Response::Exception(error))
                    }
                    _ => None,
                };

                if let Some(msg) = message {
                    if response.send(msg).await.ok().is_none() {
                        debug!({ ATT_QID } = %qid, { ATT_CID } = cid, "QUERY CONN BROKEN");
                        state = state.broken();
                    }
                }

                if let Some(fatal) = fatal {
                    return Err(ClickhouseNativeError::from(fatal));
                }
            }

            if state.is_finished() {
                debug!({ ATT_QID } = %qid, { ATT_CID } = cid, "END OF STREAM");
                break;
            }

            // Give time to handle other tasks
            tokio::task::yield_now().await;
        }

        Ok(state)
    }

    /// Wait for initial block (header block)
    async fn receive_header<R: ClickhouseRead + 'static>(
        reader: &mut R,
        qid: Qid,
        response: oneshot::Sender<QueryResponse>,
        // In the case where progress is returned, send it to the consumer
        responses: &mpsc::Sender<Response<T::Data>>,
        metadata: ConnectionMetadata,
    ) -> Result<HeaderBlock> {
        let cid = metadata.client_id;
        match Reader::receive_header::<T>(reader, metadata).await? {
            ServerPacket::Header(block) => {
                let header = block.block.column_types;
                debug!(?header, { ATT_QID } = %qid, { ATT_CID } = cid, "Received HEADER");

                // Send metadata to consumer
                let _ = response
                    .send(QueryResponse { qid, header: Some(header.clone()) })
                    .inspect_err(|_| warn!("failed to send header response"))
                    .ok();

                Ok(HeaderBlock::Header(header))
            }
            // Clickhouse sends progress for DDL queries.
            ServerPacket::Progress(progress) => {
                debug!({ ATT_QID } = %qid, { ATT_CID } = cid, "Received PROGRESS (header)");

                // Send metadata to consumer
                let _ = response
                    .send(QueryResponse { qid, header: None })
                    .inspect_err(|_| warn!("failed to send header response"))
                    .ok();

                let _ = responses.send(Response::Progress(progress)).await.ok();

                Ok(HeaderBlock::Ignore)
            }
            // For inserts, Clickhouse sends TableColumns first. There might be a slight
            // optimization here since table columns can be parsed for header data. Not sure if it's
            // worth it.
            ServerPacket::TableColumns(_) => {
                debug!({ ATT_QID } = %qid, { ATT_CID } = cid, "Received TABLE COLUMNS (header)");
                Ok(HeaderBlock::Other(response))
            }
            // For settings queries and other queries, EoS is received immediately
            ServerPacket::EndOfStream => {
                debug!({ ATT_QID } = %qid, { ATT_CID } = cid, "Received END OF STREAM (header)");

                // Send metadata to consumer
                let _ = response
                    .send(QueryResponse { qid, header: None })
                    .inspect_err(|_| warn!("failed to send header response"))
                    .ok();

                Ok(HeaderBlock::EndOfStream)
            }
            ServerPacket::Exception(exc) => {
                let error = exc.emit();
                error!({ ATT_QID } = %qid, { ATT_CID } = cid, "Received EXCEPTION: {error}");
                Err(error.into())
            }
            packet => Err(ClickhouseNativeError::ProtocolError(format!(
                "expected header block, got: {}",
                ServerPacketId::from(&packet).as_ref()
            ))),
        }
    }

    /// Wait for pong
    async fn receive_ping<R: ClickhouseRead + 'static>(
        reader: &mut R,
        qid: Qid,
        metadata: ConnectionMetadata,
    ) -> Result<()> {
        let packet = Reader::receive_packet::<T>(reader, metadata).await?;
        if !matches!(packet, ServerPacket::Pong) {
            return Err(ClickhouseNativeError::ProtocolError("Expected Pong".to_string()));
        }
        trace!({ ATT_QID } = %qid, { ATT_CID } = metadata.client_id, "Pong received");
        Ok(())
    }

    // WRITE

    async fn send_query<W: ClickhouseWrite>(
        &self,
        writer: &mut W,
        query: &str,
        settings: Option<Arc<Settings>>,
        qid: Qid,
    ) -> Result<()> {
        // Send initial query
        let query = Query {
            qid,
            query,
            settings,
            stage: QueryProcessingStage::Complete,
            info: ClientInfo::default(),
        };

        debug!({ ATT_QID } = %qid, query.query, "sending query");
        Writer::send_query(writer, query, self.metadata).await?;

        self.send_delimiter(writer, qid).await?;
        trace!({ ATT_QID } = %qid, "sent query and delimiter");

        Ok(())
    }

    #[instrument(skip_all, fields(clickhouse.query.id = %qid))]
    async fn send_insert<W: ClickhouseWrite>(
        &mut self,
        writer: &mut W,
        insert: InsertState<T::Data>,
        header: &[(String, crate::Type)],
        qid: Qid,
    ) -> Result<()> {
        trace!({ ATT_QID } = %qid, ?header, insert = insert.as_ref(), "Inserting data");
        match insert {
            InsertState::Data(data) => {
                self.send_data(writer, data, qid, Some(header)).await?;
                self.send_delimiter(writer, qid).await?;
            }
            InsertState::Batch(data) => {
                if !data.is_empty() {
                    for block in data {
                        self.send_data(writer, block, qid, Some(header)).await?;
                    }
                }
                self.send_delimiter(writer, qid).await?;
            }
            InsertState::Rows(get_data) => {
                let data = get_data(header.to_vec())?;
                self.send_data(writer, data, qid, Some(header)).await?;
                self.send_delimiter(writer, qid).await?;
            }
        }
        Ok(())
    }

    async fn send_data<W: ClickhouseWrite>(
        &self,
        writer: &mut W,
        data: T::Data,
        qid: Qid,
        header: Option<&[(String, crate::Type)]>,
    ) -> Result<()> {
        Writer::send_data::<T>(writer, data, qid, header, self.metadata).await
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
                return Err(ClickhouseNativeError::ProtocolError(format!(
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
