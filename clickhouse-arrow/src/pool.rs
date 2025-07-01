use std::num::NonZeroU64;
use std::time::Duration;

use bb8::ManageConnection;
use tokio::time::timeout;

use crate::prelude::*;
use crate::settings::Settings;
use crate::{Client, ClientBuilder, ClientOptions, ConnectionStatus, Destination, Error, Result};

/// Alias for `ConnectionPoolBuilder<NativeFormat>`
pub type NativeConnectionPoolBuilder = ConnectionPoolBuilder<NativeFormat>;
/// Alias for `ConnectionPoolBuilder<ArrowFormat>`
pub type ArrowConnectionPoolBuilder = ConnectionPoolBuilder<ArrowFormat>;
/// Alias for `ConnectionManager<NativeFormat>`
pub type NativeConnectionManager = ConnectionManager<NativeFormat>;
/// Alias for `ConnectionManager<ArrowFormat>`
pub type ArrowConnectionManager = ConnectionManager<ArrowFormat>;
/// Alias for [`bb8::Builder<ConnectionManager<T>>`]
pub type PoolBuilder<T> = bb8::Builder<ConnectionManager<T>>;
/// Alias for [`bb8::Builder<ConnectionManager<NativeFormat>>`]
pub type NativePoolBuilder = bb8::Builder<ConnectionManager<NativeFormat>>;
/// Alias for [`bb8::Builder<ConnectionManager<ArrowFormat>>`]
pub type ArrowPoolBuilder = bb8::Builder<ConnectionManager<ArrowFormat>>;
/// Alias for [`bb8::Pool<ConnectionManager<T>>`]
pub type ConnectionPool<T> = bb8::Pool<ConnectionManager<T>>;

/// Helper to construct a bb8 connection pool
pub struct ConnectionPoolBuilder<T: ClientFormat> {
    client_builder: ClientBuilder,
    pool:           PoolBuilder<T>,
    check_health:   bool,
}

impl<T: ClientFormat> ConnectionPoolBuilder<T> {
    /// Initialize by providing a destination. Use [`Self::configure_client`] to configure the
    /// underlying [`ClientBuilder`].
    pub fn new<A: Into<Destination>>(destination: A) -> Self {
        let client_builder = ClientBuilder::new().with_destination(destination);
        Self { pool: bb8::Builder::new(), client_builder, check_health: false }
    }

    /// Initialize by providing a [`ClientBuilder`] directly.
    pub fn with_client_builder(client_builder: ClientBuilder) -> Self {
        Self { pool: bb8::Builder::new(), client_builder, check_health: false }
    }

    /// Get the underlying client builder's unique identifier.
    pub fn connection_identifier(&self) -> String { self.client_builder.connection_identifier() }

    /// Get a reference to the current configured [`ClientOptions`]
    pub fn client_options(&self) -> &ClientOptions { self.client_builder.options() }

    /// Get a reference to the current configured [`Settings`]
    pub fn client_settings(&self) -> Option<&Settings> { self.client_builder.settings() }

    /// Whether the underlying connection will issue a `ping` when checking health.
    #[must_use]
    pub fn with_check(mut self) -> Self {
        self.check_health = true;
        self
    }

    /// Configure the underlying client through the [`ClientBuilder`].
    #[must_use]
    pub fn configure_client<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ClientBuilder) -> ClientBuilder,
    {
        self.client_builder = f(self.client_builder);
        self
    }

    /// Configure the underlying [`PoolBuilder`]
    #[must_use]
    pub fn configure_pool<F>(mut self, f: F) -> Self
    where
        F: FnOnce(PoolBuilder<T>) -> PoolBuilder<T>,
    {
        self.pool = f(self.pool);
        self
    }

    /// Builds a connection manager with the given configuration.
    ///
    /// # Errors
    /// Returns an error if the connection manager build fails, ie `Destination` fails to verify.
    pub async fn build_manager(&self) -> Result<ConnectionManager<T>> {
        Ok(ConnectionManager::try_new_with_builder(self.client_builder.clone())
            .await?
            .with_check(self.check_health))
    }

    /// Builds a connection pool with the given configuration.
    ///
    /// # Errors
    /// Returns an error if the connection manager build fails or the pool build fails, ie
    /// `Destination` fails to verify.
    pub async fn build(self) -> Result<ConnectionPool<T>> {
        let manager = ConnectionManager::try_new_with_builder(self.client_builder)
            .await?
            .with_check(self.check_health);
        self.pool.build(manager).await
    }
}

/// `ConnectionManager` is the underlying manager that `bb8::Pool` uses to manage connections.
#[derive(Clone)]
pub struct ConnectionManager<T: ClientFormat> {
    builder:      ClientBuilder,
    check_health: bool,
    _phantom:     std::marker::PhantomData<Client<T>>,
}

impl<T: ClientFormat> ConnectionManager<T> {
    /// Creates a new connection manager for the pool.
    ///
    /// This method builds a `ConnectionManager` that the pool will use to create
    /// and manage connections to `ClickHouse`. Each connection will be built using
    /// the provided destination, options, and settings.
    ///
    /// # Arguments
    /// * `destination` - The `ClickHouse` server address (host:port or socket address)
    /// * `options` - Client configuration options
    /// * `settings` - Optional `ClickHouse` settings to apply to all connections
    /// * `span` - Optional tracing span ID for distributed tracing
    ///
    /// # Errors
    /// Returns an error if the destination cannot be resolved or is invalid
    #[instrument(
        level = "trace",
        name = "clickhouse.pool.try_new",
        fields(db.system = "clickhouse"),
        skip_all
    )]
    pub async fn try_new<A: Into<Destination>, S: Into<Settings>>(
        destination: A,
        options: ClientOptions,
        settings: Option<S>,
        span: Option<NonZeroU64>,
    ) -> Result<Self> {
        let builder = ClientBuilder::new()
            .with_options(options)
            .with_destination(destination)
            .with_trace_context(TraceContext::from(span))
            .with_settings(settings.map(Into::into).unwrap_or_default());
        Self::try_new_with_builder(builder).await
    }

    /// Creates a new connection manager from an existing `ClientBuilder`.
    ///
    /// This is an alternative constructor that allows you to pre-configure a
    /// `ClientBuilder` with custom settings before creating the connection manager.
    /// This is useful when you need fine-grained control over the client configuration.
    ///
    /// Unlike [`Self::try_new`], which creates a `ClientBuilder` internally,
    /// this method accepts a pre-configured builder directly.
    ///
    /// # Errors
    /// Returns an error if the builder's destination cannot be verified
    #[instrument(
         level = "trace",
         name = "clickhouse.pool.try_new_with_builder",
         fields(db.system = "clickhouse"),
         skip_all
     )]
    pub async fn try_new_with_builder(builder: ClientBuilder) -> Result<Self> {
        // Verify the connection settings
        let builder = builder.verify().await?;
        Ok(Self { builder, check_health: false, _phantom: std::marker::PhantomData })
    }

    /// Whether the underlying connection will issue a `ping` when checking health.
    #[must_use]
    pub fn with_check(mut self, check: bool) -> Self {
        self.check_health = check;
        self
    }

    /// Provide a thread-safe atomic boolean to track whether a ping has already been issued to the
    /// cloud.
    #[cfg(feature = "cloud")]
    #[must_use]
    pub fn with_cloud_track(
        mut self,
        track: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.builder = self.builder.with_cloud_track(track);
        self
    }

    /// Useful to determine if 2 connections are essentially the same
    pub fn connection_identifier(&self) -> String { self.builder.connection_identifier() }

    async fn connect(&self) -> Result<Client<T>> { self.builder.clone().build().await }
}

impl<T: ClientFormat> ManageConnection for ConnectionManager<T> {
    type Connection = Client<T>;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        debug!("Connecting to ClickHouse...");
        self.connect()
            .await
            .inspect(|c| trace!({ { ATT_CID } = c.client_id }, "Connection established"))
            .inspect_err(|error| error!(?error, "Connection failed"))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        match conn.status() {
            ConnectionStatus::Error => {
                error!("Connection validation failed: Error");
                Err(Error::ConnectionGone("Connection in error state"))
            }
            ConnectionStatus::Closed => {
                warn!("Connection validation failed: Closed");
                Err(Error::ConnectionGone("Connection in closed state"))
            }
            ConnectionStatus::Open => {
                let id = conn.client_id;
                let timeout_duration = Duration::from_secs(2);
                // A health check is always done (despite the value of check_health) since it will
                // spot check the underlying connection thread. The check_health flag indicates
                // whether to issue an "expensive" ping or not.
                return match timeout(timeout_duration, conn.health_check(self.check_health)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => {
                        warn!(?error, { ATT_CID } = id, "Health check failed");
                        Err(error)
                    }
                    Err(_) => Err(Error::ConnectionTimeout("Health check timed out".into())),
                };
            }
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        matches!(conn.status(), ConnectionStatus::Error | ConnectionStatus::Closed)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExponentialBackoff {
    current_interval: Duration,
    factor:           f64,
    max_interval:     Duration,
    max_elapsed_time: Option<Duration>,
    attempts:         u32,
}

impl ExponentialBackoff {
    pub fn new() -> Self {
        ExponentialBackoff {
            current_interval: Duration::from_millis(10), // Start with 100ms
            factor:           2.0,
            max_interval:     Duration::from_secs(60),
            max_elapsed_time: Some(Duration::from_secs(900)), // 15 minutes
            attempts:         0,
        }
    }

    pub fn next_backoff(&mut self) -> Option<Duration> {
        self.attempts += 1;

        if let Some(max_time) = self.max_elapsed_time
            && self.current_interval * self.attempts > max_time
        {
            return None;
        }

        #[expect(clippy::cast_possible_wrap)]
        let next_interval =
            self.current_interval.mul_f64(self.factor.powi(self.attempts as i32 - 1));

        Some(next_interval.min(self.max_interval))
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self { Self::new() }
}
