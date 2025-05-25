use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use strum::Display;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_rustls::rustls;

use super::internal::{InternalClient, PendingQuery};
use super::{ArrowOptions, CompressionMethod, Event};
use crate::io::{ClickhouseRead, ClickhouseWrite};
use crate::prelude::*;
use crate::{ClientOptions, Message, Operation};

// Type alias for the JoinSet used to spawn inner connections
type IoHandle<T> = JoinSet<VecDeque<PendingQuery<T>>>;

/// The status of the underlying connection to `ClickHouse`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum ConnectionStatus {
    Open,
    Closed,
    Error,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionMetadata {
    pub(crate) revision:      u64,
    pub(crate) client_id:     u16,
    pub(crate) compression:   CompressionMethod,
    pub(crate) arrow_options: ArrowOptions,
}

impl ConnectionMetadata {
    /// Helper function to disable compression on the metadata.
    pub(crate) fn disable_compression(self) -> Self {
        Self {
            revision:      self.revision,
            client_id:     self.client_id,
            compression:   CompressionMethod::None,
            arrow_options: self.arrow_options,
        }
    }
}

/// A struct defining the information needed to connect over tcp.
///
/// TODO: Support reconnects
#[derive(Clone, Debug)]
pub(super) struct ConnectState<T: ClientFormat> {
    #[expect(unused)]
    addrs:   Arc<[SocketAddr]>,
    status:  Arc<RwLock<ConnectionStatus>>,
    channel: mpsc::Sender<Message<T::Data>>,
}

#[derive(Clone, Debug)]
pub(super) struct Connection<T: ClientFormat> {
    options:  Arc<ClientOptions>,
    state:    ConnectState<T>,
    io_task:  Arc<Mutex<IoHandle<T::Data>>>,
    metadata: ConnectionMetadata,
}

impl<T: ClientFormat> Connection<T> {
    #[instrument(
        level = "trace",
        name = "clickhouse.connection.create",
        skip_all,
        fields(
            clickhouse.client.id = client_id,
            db.system = "clickhouse",
            db.operation = "connect",
            network.transport = ?if options.use_tls { "tls" } else { "tcp" }
        ),
        err
    )]
    pub(crate) async fn connect(
        client_id: u16,
        addrs: Vec<SocketAddr>,
        options: ClientOptions,
        events: Arc<broadcast::Sender<Event>>,
        trace_ctx: TraceContext,
    ) -> Result<Self> {
        let span = Span::current();
        span.in_scope(|| trace!({ {ATT_CID} = client_id }, "connecting stream"));
        let _ = trace_ctx.link(&span);

        // Create joinset
        let mut io_task = JoinSet::new();

        // Initialize the status to allow the io loop to signal broken/closed connections
        let status = Arc::new(RwLock::new(ConnectionStatus::Open));

        // Establish tcp connection, perform handshake, and spawn io task
        let (metadata, channel) = Self::connect_inner(
            client_id,
            &addrs,
            &mut io_task,
            events,
            Arc::clone(&status),
            &options,
        )
        .await?;

        // Initialize connection status and state
        let state = ConnectState { addrs: Arc::from(addrs.as_slice()), status, channel };

        Ok(Self {
            options: Arc::new(options),
            io_task: Arc::new(Mutex::new(io_task)),
            state,
            metadata,
        })
    }

    async fn connect_inner(
        client_id: u16,
        addrs: &[SocketAddr],
        io_task: &mut IoHandle<T::Data>,
        events: Arc<broadcast::Sender<Event>>,
        status: Arc<RwLock<ConnectionStatus>>,
        options: &ClientOptions,
    ) -> Result<(ConnectionMetadata, mpsc::Sender<Message<T::Data>>)> {
        if options.use_tls {
            let domain = options
                .domain
                .as_ref()
                .map_or_else(|| addrs[0].ip().to_string(), String::to_string);
            debug!(%domain, "Initiating TLS connection");

            // Install rustls provider
            drop(rustls::crypto::aws_lc_rs::default_provider().install_default());
            let stream = super::tcp::connect_socket(addrs).await?;
            let tls_stream = super::tcp::tls_stream(domain, stream).await?;

            Self::establish_connection(client_id, tls_stream, io_task, events, status, options)
                .await
        } else {
            debug!(?addrs, "Initiating TCP connection");
            let tcp_stream = super::tcp::connect_socket(addrs).await?;

            Self::establish_connection(client_id, tcp_stream, io_task, events, status, options)
                .await
        }
    }

    async fn establish_connection<RW: ClickhouseRead + ClickhouseWrite + Send + 'static>(
        client_id: u16,
        mut stream: RW,
        io_task: &mut IoHandle<T::Data>,
        events: Arc<broadcast::Sender<Event>>,
        status: Arc<RwLock<ConnectionStatus>>,
        options: &ClientOptions,
    ) -> Result<(ConnectionMetadata, mpsc::Sender<Message<T::Data>>)> {
        // Perform connection handshake
        let hello = Box::new(
            InternalClient::<T>::perform_handshake(&mut stream, client_id, options).await?,
        );

        // Construct connection metadata
        let metadata = ConnectionMetadata {
            client_id,
            revision: hello.revision_version,
            compression: options.compression,
            arrow_options: options.arrow.unwrap_or_default(),
        };

        // Create operation channel
        let (operations, op_rx) = mpsc::channel(InternalClient::<T>::CAPACITY);

        // Split stream
        let (reader, writer) = tokio::io::split(stream);

        // Spawn read loop
        drop(
            io_task.spawn(
                async move {
                    let reader = BufReader::with_capacity(1024 * 1024, reader);
                    let writer = BufWriter::with_capacity(10 * 1024 * 1024, writer);

                    // Create and run internal client
                    let mut internal = InternalClient::<T>::new(metadata, events);
                    if let Err(error) = internal.run(reader, writer, op_rx).await {
                        error!(?error, "Internal connection lost");
                        *status.write() = ConnectionStatus::Error;
                    } else {
                        *status.write() = ConnectionStatus::Closed;
                    }

                    trace!("Exiting inner connection");

                    // TODO: Drain inner of pending queries
                    VecDeque::new()
                }
                .instrument(trace_span!(
                    "clickhouse.connection.io",
                    { ATT_CID } = client_id,
                    otel.kind = "server",
                    peer.service = "clickhouse",
                )),
            ),
        );

        trace!({ ATT_CID } = client_id, "spawned connection loop");

        Ok((metadata, operations))
    }

    pub(crate) fn metadata(&self) -> ConnectionMetadata { self.metadata }

    pub(crate) fn database(&self) -> &str { &self.options.default_database }

    pub(crate) fn status(&self) -> ConnectionStatus { *self.state.status.read() }

    pub(crate) async fn send_request(&self, op: Operation<T::Data>, qid: Qid) -> Result<()> {
        let client_id = self.metadata.client_id;
        let operation: &'static str = (&op).into();

        // First check if the underlying connection is ok (until re-connects are impelemented)
        if !matches!(self.status(), ConnectionStatus::Open) {
            return Err(Error::Client("No active connection".into()));
        }

        if self
            .state
            .channel
            .send(Message::Operation { qid, op })
            .instrument(trace_span!(
                "clickhouse.connection.send_request",
                { ATT_CID } = client_id,
                { ATT_QID } = %qid,
                db.system = "clickhouse",
                db.operation = operation,
            ))
            .await
            .is_err()
        {
            error!({ ATT_CID } = client_id, { ATT_QID } = %qid, "failed to send message");
            *self.state.status.write() = ConnectionStatus::Closed;
            return Err(Error::ChannelClosed);
        }

        Ok(())
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        let client_id = self.metadata.client_id;
        trace!({ ATT_CID } = client_id, "Shutting down connection");
        self.state.channel.send(Message::Shutdown).await.map_err(|_| {
            error!({ ATT_CID } = client_id, "Failed to send shutdown message");
            *self.state.status.write() = ConnectionStatus::Closed;
            Error::ChannelClosed
        })?;

        self.io_task.lock().abort_all();

        Ok(())
    }

    pub(crate) fn check_channel(&self) -> Result<()> {
        if self.state.channel.is_closed() {
            *self.state.status.write() = ConnectionStatus::Closed;
            Err(Error::ChannelClosed)
        } else {
            Ok(())
        }
    }

    pub(crate) async fn check_connection(&self, ping: bool) -> Result<()> {
        // First check that internal channels are ok
        self.check_channel()?;

        if !ping {
            return Ok(());
        }

        // Then ping
        let cid = self.metadata.client_id;
        let qid = Qid::default();

        let (response, rx) = tokio::sync::oneshot::channel();
        if self
            .state
            .channel
            .send(Message::Ping { response })
            .instrument(trace_span!(
                "clickhouse.connection.ping",
                { ATT_CID } = cid,
                { ATT_QID } = %qid,
                db.system = "clickhouse",
            ))
            .await
            .is_err()
        {
            error!({ ATT_CID } = cid, { ATT_QID } = %qid, "failed to send ping");
            *self.state.status.write() = ConnectionStatus::Closed;
            return Err(Error::ChannelClosed);
        }

        let result = rx.await.map_err(|_| {
            *self.state.status.write() = ConnectionStatus::Error;
            Error::ChannelClosed
        })?;

        result.inspect_err(|error| error!(?error, { ATT_CID } = cid, "Ping failed"))?;

        trace!({ ATT_CID } = cid, "Sent ping");

        Ok(())
    }
}

impl<T: ClientFormat> Drop for Connection<T> {
    fn drop(&mut self) {
        trace!({ ATT_CID } = self.metadata.client_id, "Connection dropped");
        self.io_task.lock().abort_all();
    }
}
