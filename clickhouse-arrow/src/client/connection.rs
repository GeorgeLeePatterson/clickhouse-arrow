use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(feature = "inner_pool")]
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
use crate::io::{ClickHouseRead, ClickHouseWrite};
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
    #[cfg(not(feature = "inner_pool"))]
    state:         Arc<ConnectState<T::Data>>,
    /// NOTE: Max connections must remain at 4, unless algorithm changes
    #[cfg(feature = "inner_pool")]
    state:         Vec<ArcSwap<ConnectState<T::Data>>>,
    #[cfg(feature = "inner_pool")]
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

        #[cfg(feature = "inner_pool")]
        let mut state = vec![ArcSwap::from(state)];

        // Inner pool: Spawn additional connections for improved concurrency.
        // Default is 4, max is 16. User can configure via fast_mode_size option.
        #[cfg(feature = "inner_pool")]
        let inner_pool_size = options
            .ext
            .fast_mode_size
            .map_or(load::DEFAULT_MAX_CONNECTIONS, |s| s.clamp(2, load::ABSOLUTE_MAX_CONNECTIONS));

        #[cfg(feature = "inner_pool")]
        for _ in 0..inner_pool_size.saturating_sub(1) {
            let events = Arc::clone(&events);
            state.push(ArcSwap::from(Arc::new(
                Self::connect_inner(&addrs, &mut io_task, events, &options, metadata).await?,
            )));
        }

        Ok(Self {
            addrs: Arc::from(addrs.as_slice()),
            io_task: Arc::new(Mutex::new(io_task)),
            options: Arc::new(options),
            metadata,
            state,
            #[cfg(feature = "inner_pool")]
            load_balancer: Arc::new(load::AtomicLoad::new(inner_pool_size)),
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

    async fn establish_connection<RW: ClickHouseRead + ClickHouseWrite + Send + 'static>(
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
        #[cfg(not(feature = "inner_pool"))]
        let conn_idx = 0; // Dummy for non-fast mode
        #[cfg(feature = "inner_pool")]
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
        #[cfg(not(feature = "inner_pool"))]
        let state = &self.state;
        #[cfg(feature = "inner_pool")]
        let state = self.state[conn_idx].load();

        // Get the current status
        #[cfg(not(feature = "inner_pool"))]
        let status = self.state.status.load(Ordering::Acquire);
        #[cfg(feature = "inner_pool")]
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
        #[cfg(not(feature = "inner_pool"))]
        {
            if self.state.channel.send(Message::Shutdown).await.is_err() {
                error!("Failed to shutdown connection");
            }
        }
        #[cfg(feature = "inner_pool")]
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

        #[cfg(not(feature = "inner_pool"))]
        let state = &self.state;
        #[cfg(feature = "inner_pool")]
        let state = self.state[idx].load();

        state.status.store(status.into(), Ordering::Release);
    }

    async fn perform_handshake<RW: ClickHouseRead + ClickHouseWrite + Send + 'static>(
        stream: &mut RW,
        client_id: u16,
        options: &ClientOptions,
    ) -> Result<ServerHello> {
        use crate::client::reader::Reader;
        use crate::client::writer::Writer;

        let client_hello = ClientHello {
            default_database: options.default_database.clone(),
            username:         options.username.clone(),
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

    #[cfg(feature = "inner_pool")]
    pub(crate) fn finish(&self, conn_idx: usize, weight: u8) {
        self.load_balancer.finish(usize::from(weight), conn_idx);
    }

    pub(crate) fn status(&self) -> ConnectionStatus {
        #[cfg(not(feature = "inner_pool"))]
        let status = ConnectionStatus::from(self.state.status.load(Ordering::Acquire));

        // TODO: Status is strange if we have an internal pool. Figure this out.
        // Just use the first channel for now
        #[cfg(feature = "inner_pool")]
        let status = ConnectionStatus::from(self.state[0].load().status.load(Ordering::Acquire));

        status
    }

    fn check_channel(&self) -> Result<()> {
        #[cfg(not(feature = "inner_pool"))]
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
        #[cfg(feature = "inner_pool")]
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

#[cfg(feature = "inner_pool")]
mod load {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(super) const DEFAULT_MAX_CONNECTIONS: u8 = 4;
    pub(super) const ABSOLUTE_MAX_CONNECTIONS: u8 = 16;

    /// Array-based load balancer for distributing operations across multiple connections.
    ///
    /// Each connection has a dedicated 64-bit atomic counter tracking its current load.
    /// This prevents the overflow issues inherent in bit-packed approaches and allows
    /// scaling up to 16 concurrent connections.
    #[derive(Debug)]
    pub(super) struct AtomicLoad {
        load_counters:   Box<[AtomicUsize]>,
        max_connections: u8,
    }

    impl AtomicLoad {
        /// Create a new load balancer with the specified maximum connections.
        ///
        /// # Panics
        /// - If `max_connections` is 0
        /// - If `max_connections` exceeds 16
        pub(super) fn new(max_connections: u8) -> Self {
            assert!(max_connections > 0, "At least 1 connection required");
            assert!(
                max_connections <= ABSOLUTE_MAX_CONNECTIONS,
                "Max {ABSOLUTE_MAX_CONNECTIONS} connections supported"
            );

            let load_counters = (0..max_connections)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice();

            Self { load_counters, max_connections }
        }

        /// Assign a connection index, incrementing its load by the specified weight.
        ///
        /// If `key` is Some, uses deterministic assignment (key % `max_connections`).
        /// If `key` is None, selects the least-loaded connection.
        ///
        /// Returns the selected connection index.
        pub(super) fn assign(&self, key: Option<usize>, weight: usize) -> usize {
            let idx = if let Some(k) = key {
                k % usize::from(self.max_connections)
            } else {
                // Select least-loaded connection
                (0..self.max_connections)
                    .min_by_key(|&i| self.load_counters[usize::from(i)].load(Ordering::Acquire))
                    .unwrap_or(0)
                    .into()
            };

            if weight > 0 {
                let _ = self.load_counters[idx].fetch_add(weight, Ordering::SeqCst);
            }
            idx
        }

        /// Decrement load by weight for the connection at the specified index.
        pub(crate) fn finish(&self, weight: usize, idx: usize) {
            if weight == 0 || idx >= self.load_counters.len() {
                return;
            }
            let _ = self.load_counters[idx].fetch_sub(weight, Ordering::SeqCst);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_atomic_load_supports_16_connections() {
            let load = AtomicLoad::new(16);

            // Assign 1000 tasks across 16 connections
            let assignments: Vec<_> = (0..1000).map(|_| load.assign(None, 1)).collect();

            // Verify reasonable distribution (should be ~62-63 per connection)
            for i in 0..16 {
                let count = assignments.iter().filter(|&&idx| idx == i).count();
                assert!(
                    (50..=75).contains(&count),
                    "Connection {i} got {count} assignments (expected ~62)"
                );
            }
        }

        #[test]
        fn test_no_overflow_with_heavy_inserts() {
            let load = AtomicLoad::new(4);

            // Simulate 1000 concurrent InsertMany operations (weight=7)
            for _ in 0..1000 {
                let idx = load.assign(None, 7);
                // Immediately finish to prevent unbounded growth
                load.finish(7, idx);
            }

            // All counters should be back to 0
            for i in 0..4 {
                assert_eq!(load.load_counters[i].load(Ordering::Acquire), 0);
            }
        }

        #[test]
        fn test_deterministic_assignment_by_key() {
            let load = AtomicLoad::new(8);

            // Same key should always go to same connection
            let key = 12345;
            let idx1 = load.assign(Some(key), 1);
            let idx2 = load.assign(Some(key), 1);
            let idx3 = load.assign(Some(key), 1);

            assert_eq!(idx1, idx2);
            assert_eq!(idx2, idx3);
            assert_eq!(idx1, key % 8);
        }

        #[test]
        fn test_least_loaded_selection() {
            let load = AtomicLoad::new(4);

            // Manually set load: [100, 50, 200, 75]
            load.load_counters[0].store(100, Ordering::Release);
            load.load_counters[1].store(50, Ordering::Release);
            load.load_counters[2].store(200, Ordering::Release);
            load.load_counters[3].store(75, Ordering::Release);

            // Next assignment should go to connection 1 (load=50)
            let idx = load.assign(None, 1);
            assert_eq!(idx, 1);
        }

        #[test]
        #[should_panic(expected = "Max 16 connections")]
        fn test_rejects_too_many_connections() { drop(AtomicLoad::new(17)); }

        #[test]
        #[should_panic(expected = "At least 1 connection")]
        fn test_rejects_zero_connections() { drop(AtomicLoad::new(0)); }

        #[test]
        fn test_zero_weight_returns_index_without_increment() {
            let load = AtomicLoad::new(4);

            let idx = load.assign(None, 0);
            assert!(idx < 4);

            // All counters should still be 0
            for i in 0..4 {
                assert_eq!(load.load_counters[i].load(Ordering::Acquire), 0);
            }
        }

        #[test]
        fn test_finish_with_invalid_index() {
            let load = AtomicLoad::new(4);

            // Assign some load
            let idx = load.assign(None, 10);
            load.load_counters[idx].store(10, Ordering::Release);

            // Finish with out-of-bounds index should not panic
            load.finish(5, 999);

            // Original load should be unchanged
            assert_eq!(load.load_counters[idx].load(Ordering::Acquire), 10);
        }

        #[test]
        fn test_finish_with_zero_weight() {
            let load = AtomicLoad::new(4);

            // Assign some load
            let idx = load.assign(None, 10);
            load.load_counters[idx].store(10, Ordering::Release);

            // Finish with zero weight should not modify counters
            load.finish(0, idx);

            // Load should be unchanged
            assert_eq!(load.load_counters[idx].load(Ordering::Acquire), 10);

            // Also test zero weight with invalid index (covers both branches)
            load.finish(0, 999);
        }
    }
}
