use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(feature = "fast_mode")]
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use strum::Display;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{broadcast, mpsc};
use tokio::task::{AbortHandle, JoinSet};
use tokio_rustls::rustls;

use super::internal::{InternalConn, PendingQuery};
use super::{ArrowOptions, CompressionMethod, Event};
use crate::client::chunk::{ChunkReader, ChunkWriter};
use crate::flags::{conn_read_buffer_size, conn_write_buffer_size};
use crate::io::{ClickhouseRead, ClickhouseWrite};
use crate::native::protocol::{
    ClientHello, DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM, DBMS_TCP_PROTOCOL_VERSION, ServerHello,
};
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

impl From<u8> for ConnectionStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Open,
            1 => Self::Closed,
            _ => Self::Error,
        }
    }
}

impl From<ConnectionStatus> for u8 {
    fn from(value: ConnectionStatus) -> u8 { value as u8 }
}

/// Client metadata passed around the internal client
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientMetadata {
    pub(crate) client_id:     u16,
    pub(crate) compression:   CompressionMethod,
    pub(crate) arrow_options: ArrowOptions,
}

impl ClientMetadata {
    /// Helper function to disable compression on the metadata.
    pub(crate) fn disable_compression(self) -> Self {
        Self {
            client_id:     self.client_id,
            compression:   CompressionMethod::None,
            arrow_options: self.arrow_options,
        }
    }

    /// Helper function to provide settings for compression
    pub(crate) fn compression_settings(self) -> Settings {
        match self.compression {
            CompressionMethod::None | CompressionMethod::LZ4 => Settings::default(),
            CompressionMethod::ZSTD => vec![
                ("network_compression_method", "zstd"),
                ("network_zstd_compression_level", "1"),
            ]
            .into(),
        }
    }
}

/// A struct defining the information needed to connect over TCP.
#[derive(Debug)]
struct ConnectState<T: Send + Sync + 'static> {
    status:  Arc<AtomicU8>,
    channel: mpsc::Sender<Message<T>>,
    #[expect(unused)]
    handle:  AbortHandle,
}

// NOTE: ArcSwaps are used to support reconnects in the future.
#[derive(Debug)]
pub(super) struct Connection<T: ClientFormat> {
    #[expect(unused)]
    addrs:         Arc<[SocketAddr]>,
    options:       Arc<ClientOptions>,
    io_task:       Arc<Mutex<IoHandle<T::Data>>>,
    metadata:      ClientMetadata,
    #[cfg(not(feature = "fast_mode"))]
    state:         Arc<ConnectState<T::Data>>,
    /// NOTE: Max connections must remain at 4, unless algorithm changes
    #[cfg(feature = "fast_mode")]
    state:         Vec<ArcSwap<ConnectState<T::Data>>>,
    #[cfg(feature = "fast_mode")]
    load_balancer: Arc<load::AtomicLoad>,
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

        // Construct connection metadata
        let metadata = ClientMetadata {
            client_id,
            compression: options.compression,
            arrow_options: options.ext.arrow.unwrap_or_default(),
        };

        // Install rustls provider if using tls
        if options.use_tls {
            drop(rustls::crypto::aws_lc_rs::default_provider().install_default());
        }

        // Establish tcp connection, perform handshake, and spawn io task
        let state = Arc::new(
            Self::connect_inner(&addrs, &mut io_task, Arc::clone(&events), &options, metadata)
                .await?,
        );

        #[cfg(feature = "fast_mode")]
        let mut state = vec![ArcSwap::from(state)];

        // Currently "fast_mode" = 2 connections. But this can support up to 4 (possibly more with
        // u64 load_counter)
        #[cfg(feature = "fast_mode")]
        state.push(ArcSwap::from(Arc::new(
            Self::connect_inner(&addrs, &mut io_task, events, &options, metadata).await?,
        )));

        Ok(Self {
            addrs: Arc::from(addrs.as_slice()),
            io_task: Arc::new(Mutex::new(io_task)),
            options: Arc::new(options),
            metadata,
            state,
            // Currently only using 2 connections
            // TODO: Provide inner pool configuration option
            #[cfg(feature = "fast_mode")]
            load_balancer: Arc::new(load::AtomicLoad::new(2)),
        })
    }

    async fn connect_inner(
        addrs: &[SocketAddr],
        io_task: &mut IoHandle<T::Data>,
        events: Arc<broadcast::Sender<Event>>,
        options: &ClientOptions,
        metadata: ClientMetadata,
    ) -> Result<ConnectState<T::Data>> {
        if options.use_tls {
            let tls_stream = super::tcp::connect_tls(addrs, options.domain.as_deref()).await?;
            Self::establish_connection(tls_stream, io_task, events, options, metadata).await
        } else {
            let tcp_stream = super::tcp::connect_socket(addrs).await?;
            Self::establish_connection(tcp_stream, io_task, events, options, metadata).await
        }
    }

    async fn establish_connection<RW: ClickhouseRead + ClickhouseWrite + Send + 'static>(
        mut stream: RW,
        io_task: &mut IoHandle<T::Data>,
        events: Arc<broadcast::Sender<Event>>,
        options: &ClientOptions,
        metadata: ClientMetadata,
    ) -> Result<ConnectState<T::Data>> {
        let cid = metadata.client_id;

        // Initialize the status to allow the io loop to signal broken/closed connections
        let status = Arc::new(AtomicU8::new(ConnectionStatus::Open.into()));
        let internal_status = Arc::clone(&status);

        // Perform connection handshake
        let server_hello = Arc::new(Self::perform_handshake(&mut stream, cid, options).await?);

        // Create operation channel
        let (operations, op_rx) = mpsc::channel(InternalConn::<T>::CAPACITY);

        // Split stream
        let (reader, writer) = tokio::io::split(stream);

        // Spawn read loop
        let handle = io_task.spawn(
            async move {
                let chunk_send = server_hello.supports_chunked_send();
                let chunk_recv = server_hello.supports_chunked_recv();

                // Create and run internal client
                let mut internal = InternalConn::<T>::new(metadata, events, server_hello);

                let reader = BufReader::with_capacity(conn_read_buffer_size(), reader);
                let writer = BufWriter::with_capacity(conn_write_buffer_size(), writer);

                let result = match (chunk_send, chunk_recv) {
                    (true, true) => {
                        // let reader = ChunkReader::new(reader);
                        let reader = ChunkReader::new(reader);
                        let writer = ChunkWriter::new(writer);
                        internal.run_chunked(reader, writer, op_rx).await
                    }
                    (true, false) => {
                        let writer = ChunkWriter::new(writer);
                        internal.run_chunked(reader, writer, op_rx).await
                    }
                    (false, true) => {
                        // let reader = ChunkReader::new(reader);
                        let reader = ChunkReader::new(reader);
                        internal.run(reader, writer, op_rx).await
                    }
                    (false, false) => internal.run(reader, writer, op_rx).await,
                };

                if let Err(error) = result {
                    error!(?error, "Internal connection lost");
                    internal_status.store(ConnectionStatus::Error.into(), Ordering::Release);
                } else {
                    info!("Internal connection closed");
                    internal_status.store(ConnectionStatus::Closed.into(), Ordering::Release);
                }
                trace!("Exiting inner connection");
                // TODO: Drain inner of pending queries
                VecDeque::new()
            }
            .instrument(trace_span!(
                "clickhouse.connection.io",
                { ATT_CID } = cid,
                otel.kind = "server",
                peer.service = "clickhouse",
            )),
        );

        trace!({ ATT_CID } = cid, "spawned connection loop");
        Ok(ConnectState { status, channel: operations, handle })
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            db.system = "clickhouse",
            db.operation = op.as_ref(),
            clickhouse.client.id = self.metadata.client_id,
            clickhouse.query.id = %qid,
        )
    )]
    pub(crate) async fn send_operation(
        &self,
        op: Operation<T::Data>,
        qid: Qid,
        finished: bool,
    ) -> Result<usize> {
        #[cfg(not(feature = "fast_mode"))]
        let conn_idx = 0; // Dummy for non-fast mode
        #[cfg(feature = "fast_mode")]
        let conn_idx = {
            let key = (matches!(op, Operation::Query { .. } if !finished)
                || matches!(op, Operation::Insert { .. } | Operation::InsertMany { .. }))
            .then(|| qid.key());
            self.load_balancer.assign(key, op.weight(finished) as usize)
        };

        let span = trace_span!(
            "clickhouse.connection.send_operation",
            { ATT_CID } = self.metadata.client_id,
            { ATT_QID } = %qid,
            db.system = "clickhouse",
            db.operation = op.as_ref(),
            finished
        );

        // Get the current state
        #[cfg(not(feature = "fast_mode"))]
        let state = &self.state;
        #[cfg(feature = "fast_mode")]
        let state = self.state[conn_idx].load();

        // Get the current status
        #[cfg(not(feature = "fast_mode"))]
        let status = self.state.status.load(Ordering::Acquire);
        #[cfg(feature = "fast_mode")]
        let status = state.status.load(Ordering::Acquire);

        // First check if the underlying connection is ok (until re-connects are impelemented)
        if status > 0 {
            return Err(Error::Client("No active connection".into()));
        }

        let result = state.channel.send(Message::Operation { qid, op }).instrument(span).await;
        if result.is_err() {
            error!({ ATT_QID } = %qid, "failed to send message");
            self.update_status(conn_idx, ConnectionStatus::Closed);
            return Err(Error::ChannelClosed);
        }

        Ok(conn_idx)
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(db.system = "clickhouse", clickhouse.client.id = self.metadata.client_id)
    )]
    pub(crate) async fn shutdown(&self) -> Result<()> {
        trace!({ ATT_CID } = self.metadata.client_id, "Shutting down connections");
        #[cfg(not(feature = "fast_mode"))]
        {
            if self.state.channel.send(Message::Shutdown).await.is_err() {
                error!("Failed to shutdown connection");
            }
        }
        #[cfg(feature = "fast_mode")]
        {
            for (i, conn_state) in self.state.iter().enumerate() {
                let state = conn_state.load();
                debug!("Shutting down connection {i}");
                // Send the message again to shutdown the next internal connection
                if state.channel.send(Message::Shutdown).await.is_err() {
                    error!("Failed to shutdown connection {i}");
                }
            }
        }
        self.io_task.lock().abort_all();
        Ok(())
    }

    pub(crate) async fn check_connection(&self, ping: bool) -> Result<()> {
        // First check that internal channels are ok
        self.check_channel()?;

        if !ping {
            return Ok(());
        }

        // Then ping
        let (response, rx) = tokio::sync::oneshot::channel();
        let cid = self.metadata.client_id;
        let qid = Qid::default();
        let idx = self
            .send_operation(Operation::Ping { response }, qid, true)
            .instrument(trace_span!(
                "clickhouse.connection.ping",
                { ATT_CID } = cid,
                { ATT_QID } = %qid,
                db.system = "clickhouse",
            ))
            .await?;

        rx.await
            .map_err(|_| {
                self.update_status(idx, ConnectionStatus::Closed);
                Error::ChannelClosed
            })?
            .inspect_err(|error| {
                self.update_status(idx, ConnectionStatus::Error);
                error!(?error, { ATT_CID } = cid, "Ping failed");
            })?;

        Ok(())
    }

    fn update_status(&self, idx: usize, status: ConnectionStatus) {
        trace!({ ATT_CID } = self.metadata.client_id, ?status, "Updating status conn {idx}");

        #[cfg(not(feature = "fast_mode"))]
        let state = &self.state;
        #[cfg(feature = "fast_mode")]
        let state = self.state[idx].load();

        state.status.store(status.into(), Ordering::Release);
    }

    async fn perform_handshake<RW: ClickhouseRead + ClickhouseWrite + Send + 'static>(
        stream: &mut RW,
        client_id: u16,
        options: &ClientOptions,
    ) -> Result<ServerHello> {
        use crate::client::reader::Reader;
        use crate::client::writer::Writer;

        let client_hello = ClientHello {
            default_database: options.default_database.to_string(),
            username:         options.username.to_string(),
            password:         options.password.get().to_string(),
        };

        // Send client hello
        Writer::send_hello(stream, client_hello)
            .await
            .inspect_err(|error| error!(?error, { ATT_CID } = client_id, "Failed to send hello"))?;

        // Receive server hello
        let chunked_modes = (options.ext.chunked_send, options.ext.chunked_recv);
        let server_hello =
            Reader::receive_hello(stream, DBMS_TCP_PROTOCOL_VERSION, chunked_modes, client_id)
                .await?;
        trace!({ ATT_CID } = client_id, ?server_hello, "Finished handshake");

        if server_hello.revision_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM {
            Writer::send_addendum(stream, server_hello.revision_version, &server_hello).await?;
            stream.flush().await.inspect_err(|error| error!(?error, "Error writing addendum"))?;
        }

        Ok(server_hello)
    }
}

impl<T: ClientFormat> Connection<T> {
    pub(crate) fn metadata(&self) -> ClientMetadata { self.metadata }

    pub(crate) fn database(&self) -> &str { &self.options.default_database }

    #[cfg(feature = "fast_mode")]
    pub(crate) fn finish(&self, conn_idx: usize, weight: u8) {
        self.load_balancer.finish(usize::from(weight), conn_idx);
    }

    pub(crate) fn status(&self) -> ConnectionStatus {
        #[cfg(not(feature = "fast_mode"))]
        let status = ConnectionStatus::from(self.state.status.load(Ordering::Acquire));

        // TODO: Status is strange if we have an internal pool. Figure this out.
        // Just use the first channel for now
        #[cfg(feature = "fast_mode")]
        let status = ConnectionStatus::from(self.state[0].load().status.load(Ordering::Acquire));

        status
    }

    fn check_channel(&self) -> Result<()> {
        #[cfg(not(feature = "fast_mode"))]
        {
            if self.state.channel.is_closed() {
                self.update_status(0, ConnectionStatus::Closed);
                Err(Error::ChannelClosed)
            } else {
                Ok(())
            }
        }

        // TODO: Checking channel is strange if we have an internal pool. Figure this out.
        // Just return status of first connection for now
        #[cfg(feature = "fast_mode")]
        if self.state[0].load().channel.is_closed() {
            self.update_status(0, ConnectionStatus::Closed);
            Err(Error::ChannelClosed)
        } else {
            Ok(())
        }
    }
}

impl<T: ClientFormat> Drop for Connection<T> {
    fn drop(&mut self) {
        trace!({ ATT_CID } = self.metadata.client_id, "Connection dropped");
        self.io_task.lock().abort_all();
    }
}

#[cfg(feature = "fast_mode")]
mod load {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    pub(super) struct AtomicLoad {
        load_counter:    AtomicUsize,
        max_connections: u8,
    }

    impl AtomicLoad {
        /// Try and create the load balancer.
        ///
        /// # Panics
        /// - Currently only 4 connections are supported. Errors if max > 4.
        pub(super) fn new(max_connections: u8) -> Self {
            assert!(max_connections <= 4, "Max 4 connections supported");
            assert!(max_connections > 0, "At leat 1 connection required");
            Self { load_counter: AtomicUsize::new(0), max_connections }
        }

        /// Assign a connection index, incrementing load by weight
        /// If key is Some, use key % `max_connections` (deterministic)
        /// If key is None, use least-loaded connection
        /// Returns connection index
        pub(super) fn assign(&self, key: Option<usize>, weight: usize) -> usize {
            let idx = if let Some(k) = key {
                k % usize::from(self.max_connections) // Deterministic assignment
            } else {
                // Select least-loaded connection
                let load = self.load_counter.load(Ordering::Acquire);
                usize::from(
                    (0..self.max_connections)
                        .min_by_key(|&i| (load >> (i * 8)) & 0xFF)
                        .unwrap_or(0),
                )
            };
            if weight == 0 {
                return idx;
            }

            // Increment load
            let _ = self.load_counter.fetch_add(weight << (idx * 8), Ordering::SeqCst);
            idx
        }

        /// Finish an operation, decrementing load by weight for index
        pub(crate) fn finish(&self, weight: usize, idx: usize) {
            if weight == 0 {
                return;
            }

            let _ = self.load_counter.fetch_sub(weight << (idx * 8), Ordering::SeqCst);
        }
    }
}
