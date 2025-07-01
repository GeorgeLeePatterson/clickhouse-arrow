use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use tracing::error;

use super::tcp::Destination;
use super::{
    ArrowOptions, Client, ClientFormat, CompressionMethod, ConnectionContext, Extension, Secret,
};
#[cfg(feature = "pool")]
use crate::pool::ConnectionManager;
use crate::prelude::SettingValue;
use crate::settings::Settings;
use crate::telemetry::TraceContext;
use crate::{ArrowFormat, ClientOptions, Error, NativeFormat, Result};

/// A builder for configuring and creating a `ClickHouse` client.
///
/// The `ClientBuilder` provides a fluent interface to set up a [`Client`] with
/// custom connection parameters, such as the server address, credentials, TLS,
/// compression, and session settings. It supports creating either a single
/// [`Client`] (via [`ClientBuilder::build`]) or a connection pool (via
/// [`ClientBuilder::build_pool_manager`], with the `pool` feature enabled).
///
/// Use this builder for fine-grained control over the client configuration. The
/// builder ensures that the destination address is verified before establishing a
/// connection, either explicitly via [`ClientBuilder::verify`] or implicitly during
/// the build process.
///
/// # Examples
/// ```rust,ignore
/// use clickhouse_arrow::prelude::*;
///
/// let client = ClientBuilder::new()
///     .with_endpoint("localhost:9000")
///     .with_username("default")
///     .with_password("")
///     .build_arrow()
///     .await
///     .unwrap();
///
/// // Use the client to query `ClickHouse`
/// client.query("SELECT 1").await.unwrap();
/// ```
#[derive(Default, Debug, Clone)]
pub struct ClientBuilder {
    destination: Option<Destination>,
    options:     ClientOptions,
    settings:    Option<Settings>,
    context:     Option<ConnectionContext>,
    verified:    bool,
}

impl ClientBuilder {
    /// Creates a new `ClientBuilder` with default configuration.
    ///
    /// This method initializes a builder with default [`ClientOptions`], no destination,
    /// settings, or context. Use this as the starting point to configure a `ClickHouse`
    /// client with methods like [`ClientBuilder::with_endpoint`],
    /// [`ClientBuilder::with_username`], or [`ClientBuilder::with_tls`].
    ///
    /// # Returns
    /// A new [`ClientBuilder`] instance ready for configuration.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_username("default");
    /// ```
    pub fn new() -> Self {
        ClientBuilder {
            destination: None,
            options:     ClientOptions::default(),
            settings:    None,
            context:     None,
            verified:    false,
        }
    }

    /// Retrieves the configured destination, if set.
    ///
    /// This method returns the `ClickHouse` server address (as a [`Destination`]) that
    /// has been configured via methods like [`ClientBuilder::with_endpoint`] or
    /// [`ClientBuilder::with_socket_addr`]. Returns `None` if no destination is set.
    ///
    /// # Returns
    /// An `Option<&Destination>` containing the configured destination, or `None` if
    /// not set.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000");
    /// if let Some(dest) = builder.destination() {
    ///     println!("Destination: {:?}", dest);
    /// }
    /// ```
    pub fn destination(&self) -> Option<&Destination> { self.destination.as_ref() }

    /// Retrieves the current connection options.
    ///
    /// This method returns a reference to the [`ClientOptions`] configured for the
    /// builder, which includes settings like username, password, TLS, and compression.
    /// These options can be set via methods like [`ClientBuilder::with_username`],
    /// [`ClientBuilder::with_tls`], or [`ClientBuilder::with_options`].
    ///
    /// # Returns
    /// A reference to the current [`ClientOptions`].
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_username("default")
    ///     .with_endpoint("localhost:9000");
    /// let options = builder.options();
    /// println!("Username: {}", options.username);
    /// ```
    pub fn options(&self) -> &ClientOptions { &self.options }

    /// Retrieves the configured session settings, if set.
    ///
    /// This method returns the `ClickHouse` session settings (as `Settings`)
    /// that have been configured via [`ClientBuilder::with_settings`]. These settings
    /// control query behavior, such as timeouts or maximum rows. Returns `None` if no
    /// settings are set.
    ///
    /// # Returns
    /// An `Option<&Settings>` containing the configured settings, or `None` if not set.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    /// use std::sync::Arc;
    ///
    /// let settings = Settings::default();
    /// let builder = ClientBuilder::new()
    ///     .with_settings(settings.clone());
    /// if let Some(config_settings) = builder.settings() {
    ///     println!("Settings: {config_settings:?}");
    ///     assert_eq!(config_settings, &settings)
    /// }
    /// ```
    pub fn settings(&self) -> Option<&Settings> { self.settings.as_ref() }

    /// Checks whether the builder's destination has been verified.
    ///
    /// A verified builder indicates that the `ClickHouse` server's destination address
    /// has been resolved into valid socket addresses (via [`ClientBuilder::verify`] or
    /// during [`ClientBuilder::build`]). Verification ensures that the address is
    /// reachable before attempting to connect. This method returns `false` if the
    /// destination is unset or has been modified since the last verification.
    ///
    /// # Returns
    /// A `bool` indicating whether the destination is verified.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000");
    /// println!("Verified: {}", builder.verified()); // false
    ///
    /// let verified_builder = builder.verify().await.unwrap();
    /// println!("Verified: {}", verified_builder.verified()); // true
    /// ```
    pub fn verified(&self) -> bool { self.verified }

    /// Sets the `ClickHouse` server address using a socket address.
    ///
    /// This method configures the destination using a [`SocketAddr`] (e.g.,
    /// `127.0.0.1:9000`). It is useful when the address is already resolved. For
    /// hostname-based addresses, use [`ClientBuilder::with_endpoint`] or
    /// [`ClientBuilder::with_host_port`].
    ///
    /// # Parameters
    /// - `addr`: The socket address of the `ClickHouse` server.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated destination.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    /// use std::net::{Ipv4Addr, SocketAddr};
    ///
    /// let addr = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 9000);
    /// let builder = ClientBuilder::new()
    ///     .with_socket_addr(addr);
    /// ```
    #[must_use]
    pub fn with_socket_addr(self, addr: SocketAddr) -> Self { self.with_destination(addr) }

    /// Sets the `ClickHouse` server address using a hostname and port.
    ///
    /// This method configures the destination using a hostname (e.g., `"localhost"`) and
    /// port number (e.g., `9000`). The hostname will be resolved during verification
    /// (via [`ClientBuilder::verify`] or [`ClientBuilder::build`]). For resolved
    /// addresses, use [`ClientBuilder::with_socket_addr`].
    ///
    /// # Parameters
    /// - `host`: The hostname or IP address of the `ClickHouse` server.
    /// - `port`: The port number of the `ClickHouse` server.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated destination.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_host_port("localhost", 9000);
    /// ```
    #[must_use]
    pub fn with_host_port(self, host: impl Into<String>, port: u16) -> Self {
        self.with_destination((host.into(), port))
    }

    /// Sets the `ClickHouse` server address using a string endpoint.
    ///
    /// This method configures the destination using a string in the format
    /// `"host:port"` (e.g., `"localhost:9000"`). The endpoint will be resolved during
    /// verification (via [`ClientBuilder::verify`] or [`ClientBuilder::build`]). For
    /// separate host and port, use [`ClientBuilder::with_host_port`].
    ///
    /// # Parameters
    /// - `endpoint`: The `ClickHouse` server address as a string (e.g., `"localhost:9000"`).
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated destination.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000");
    /// ```
    #[must_use]
    pub fn with_endpoint(self, endpoint: impl Into<String>) -> Self {
        self.with_destination(endpoint.into())
    }

    /// Sets the `ClickHouse` server address using any compatible destination type.
    ///
    /// This method configures the destination using a type that can be converted into
    /// a [`Destination`], such as a string endpoint (e.g., `"localhost:9000"`), a
    /// `(host, port)` tuple, or a [`SocketAddr`]. It is the most flexible way to set
    /// the destination. For specific formats, use [`ClientBuilder::with_endpoint`],
    /// [`ClientBuilder::with_host_port`], or [`ClientBuilder::with_socket_addr`].
    ///
    /// # Parameters
    /// - `destination`: The `ClickHouse` server address, convertible to [`Destination`].
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated destination.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_destination("localhost:9000");
    /// ```
    #[must_use]
    pub fn with_destination<D>(mut self, destination: D) -> Self
    where
        D: Into<Destination>,
    {
        self.destination = Some(destination.into());
        self.verified = false;
        self
    }

    /// Sets the connection options directly.
    ///
    /// This method replaces the current [`ClientOptions`] with the provided options,
    /// overriding any settings configured via methods like
    /// [`ClientBuilder::with_username`] or [`ClientBuilder::with_tls`]. Use this when
    /// you have a pre-configured [`ClientOptions`] instance or need to set multiple
    /// options at once.
    ///
    /// # Parameters
    /// - `options`: The [`ClientOptions`] to use for the connection.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated options.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let options = ClientOptions::default()
    ///     .username("default")
    ///     .password("");
    /// let builder = ClientBuilder::new()
    ///     .with_options(options);
    /// ```
    #[must_use]
    pub fn with_options(mut self, options: ClientOptions) -> Self {
        self.options = options;
        self.verified = false;
        self
    }

    #[must_use]
    pub fn with_ext<F>(mut self, cb: F) -> Self
    where
        F: FnOnce(Extension) -> Extension,
    {
        self.options.ext = cb(self.options.ext);
        self
    }

    /// Enables or disables TLS for the `ClickHouse` connection.
    ///
    /// This method configures whether the client will use a secure TLS connection to
    /// communicate with `ClickHouse`. If TLS is enabled, you may also need to set a CA
    /// file (via [`ClientBuilder::with_cafile`]) or domain (via
    /// [`ClientBuilder::with_domain`]) for secure connections.
    ///
    /// # Parameters
    /// - `tls`: If `true`, enables TLS; if `false`, uses plain TCP.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated TLS setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_tls(true);
    /// ```
    #[must_use]
    pub fn with_tls(mut self, tls: bool) -> Self {
        if self.options.use_tls != tls {
            self.options.use_tls = tls;
            self.verified = false;
        }
        self
    }

    /// Sets the CA file for TLS connections to `ClickHouse`.
    ///
    /// This method specifies the path to a certificate authority (CA) file used to
    /// verify the `ClickHouse` server's certificate during TLS connections. It is
    /// required when TLS is enabled (via [`ClientBuilder::with_tls`]) and the server
    /// uses a custom or self-signed certificate.
    ///
    /// # Parameters
    /// - `cafile`: The path to the CA file.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated CA file setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_tls(true)
    ///     .with_cafile("/path/to/ca.crt");
    /// ```
    #[must_use]
    pub fn with_cafile<P: AsRef<Path>>(mut self, cafile: P) -> Self {
        self.options.cafile = Some(cafile.as_ref().to_path_buf());
        self
    }

    /// Forces the use of IPv4-only resolution for the `ClickHouse` server address.
    ///
    /// This method configures whether the destination address resolution (during
    /// [`ClientBuilder::verify`] or [`ClientBuilder::build`]) should use only IPv4
    /// addresses. By default, both IPv4 and IPv6 are considered. Enable this for
    /// environments where IPv6 is unsupported.
    ///
    /// # Parameters
    /// - `enabled`: If `true`, restricts resolution to IPv4; if `false`, allows both IPv4 and IPv6.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated IPv4 setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_ipv4_only(true);
    /// ```
    #[must_use]
    pub fn with_ipv4_only(mut self, enabled: bool) -> Self {
        self.options.ipv4_only = enabled;
        self
    }

    /// Sets the `ClickHouse` session settings.
    ///
    /// This method configures the session settings (e.g., query timeouts, max rows) for
    /// the client. These settings are applied to all queries executed by the client.
    /// Use this to customize query behavior beyond the defaults.
    ///
    /// # Parameters
    /// - `settings`: The session settings as an `impl Into<Settings>`.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated settings.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    /// use std::sync::Arc;
    ///
    /// let settings = vec![
    ///     ("select_sequential_consistency", 1)
    /// ];
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_settings(settings);
    /// ```
    #[must_use]
    pub fn with_settings(mut self, settings: impl Into<Settings>) -> Self {
        self.settings = Some(settings.into());
        self
    }

    /// Set a `ClickHouse` session setting.
    ///
    /// This method configures the session settings (e.g., query timeouts, max rows) for
    /// the client. These settings are applied to all queries executed by the client.
    /// Use this to customize query behavior beyond the defaults.
    ///
    /// # Parameters
    /// - `key`: The session setting's name.
    /// - `setting`: The session setting as an `impl Into<SettingValue>`.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated settings.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    /// use std::sync::Arc;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_setting("select_sequential_consistency", 1);
    /// ```
    #[must_use]
    pub fn with_setting(
        mut self,
        name: impl Into<String>,
        setting: impl Into<SettingValue>,
    ) -> Self {
        let setting: SettingValue = setting.into();
        let settings = self.settings.unwrap_or_default().with_setting(name, setting);
        self.settings = Some(settings);
        self
    }

    /// Sets the username for authenticating with `ClickHouse`.
    ///
    /// This method configures the username used to authenticate the client with the
    /// `ClickHouse` server. The default username is typically `"default"`, but this
    /// can be customized based on the server's configuration.
    ///
    /// # Parameters
    /// - `username`: The username for authentication.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated username.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_username("admin");
    /// ```
    #[must_use]
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.options.username = username.into();
        self
    }

    /// Sets the password for authenticating with `ClickHouse`.
    ///
    /// This method configures the password used to authenticate the client with the
    /// `ClickHouse` server. The password is stored securely as a [`Secret`]. If no
    /// password is required, an empty string can be used.
    ///
    /// # Parameters
    /// - `password`: The password for authentication, convertible to [`Secret`].
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated password.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_username("admin")
    ///     .with_password("secret");
    /// ```
    #[must_use]
    pub fn with_password<T>(mut self, password: T) -> Self
    where
        Secret: From<T>,
    {
        self.options.password = Secret::from(password);
        self
    }

    /// Sets the default database for the `ClickHouse` connection.
    ///
    /// This method configures the default database used by the client for queries and
    /// operations. If not set, the `ClickHouse` server defaults to the `"default"`
    /// database. Specifying a database is optional but recommended when working with a
    /// specific database to avoid explicit database prefixes in queries.
    ///
    /// # Parameters
    /// - `database`: The name of the default database.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated database.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_database("my_db");
    /// ```
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.options.default_database = database.into();
        self
    }

    /// Sets the domain for secure TLS connections to `ClickHouse`.
    ///
    /// This method specifies the domain name used for TLS verification when connecting
    /// to a `ClickHouse` server with TLS enabled (via [`ClientBuilder::with_tls`]). If
    /// not set, the domain is inferred from the destination during verification. Use
    /// this to explicitly set the domain for cloud or custom TLS configurations.
    ///
    /// # Parameters
    /// - `domain`: The domain name for TLS verification.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated domain.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_tls(true)
    ///     .with_domain("clickhouse.example.com");
    /// ```
    #[must_use]
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.options.domain = Some(domain.into());
        self.verified = false;
        self
    }

    /// Sets the compression method for data exchange with `ClickHouse`.
    ///
    /// This method configures the compression algorithm used for sending and receiving
    /// data to/from `ClickHouse`. The default is [`CompressionMethod::LZ4`], which
    /// balances performance and compression ratio. Other options may be available
    /// depending on the `ClickHouse` server configuration.
    ///
    /// # Parameters
    /// - `compression`: The compression method to use.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated compression setting.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_compression(CompressionMethod::LZ4);
    /// ```
    #[must_use]
    pub fn with_compression(mut self, compression: CompressionMethod) -> Self {
        self.options.compression = compression;
        self
    }

    /// Sets the Arrow-specific options for `ClickHouse` connections.
    ///
    /// This method configures options specific to the Arrow format (used by
    /// [`ArrowClient`]), such as schema mapping or data type conversions. These options
    /// are applied when the client is built with [`ClientBuilder::build`] for
    /// [`ArrowFormat`]. Use this to customize Arrow interoperability.
    ///
    /// # Parameters
    /// - `options`: The Arrow-specific options.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated Arrow options.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let arrow_options = ArrowOptions::default();
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_arrow_options(arrow_options);
    /// ```
    #[must_use]
    pub fn with_arrow_options(mut self, options: ArrowOptions) -> Self {
        self.options.ext.arrow = Some(options);
        self
    }

    /// Sets a tracing context for `ClickHouse` connections and queries.
    ///
    /// This method configures a [`TraceContext`] to enable distributed tracing for
    /// client operations, such as connections and queries. The tracing context is
    /// included in logs and can be used to monitor and debug client behavior across
    /// distributed systems.
    ///
    /// # Parameters
    /// - `trace_context`: The tracing context to use.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated tracing context.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let trace_context = TraceContext::default();
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_trace_context(trace_context);
    /// ```
    #[must_use]
    pub fn with_trace_context(mut self, trace_context: TraceContext) -> Self {
        let mut context = self.context.unwrap_or_default();
        context.trace = Some(trace_context);
        self.context = Some(context);
        self
    }

    /// Resolves and verifies the `ClickHouse` server destination early.
    ///
    /// This method resolves the configured destination (set via
    /// [`ClientBuilder::with_endpoint`], etc.) into socket addresses and verifies that
    /// it is valid and reachable. It also ensures that a domain is set for TLS
    /// connections if required. Verification is performed automatically during
    /// [`ClientBuilder::build`], but calling this method explicitly allows early error
    /// detection.
    ///
    /// # Returns
    /// A [`Result`] containing the verified [`ClientBuilder`], or an error if
    /// verification fails.
    ///
    /// # Errors
    /// - Fails if no destination is set ([`Error::MissingConnectionInformation`]).
    /// - Fails if the destination cannot be resolved ([`Error::MalformedConnectionInformation`]).
    /// - Fails if TLS is enabled but no domain is provided and cannot be inferred
    ///   ([`Error::MalformedConnectionInformation`]).
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000");
    /// let verified_builder = builder.verify().await.unwrap();
    /// println!("Destination verified!");
    /// ```
    pub async fn verify(mut self) -> Result<Self> {
        let (addrs, domain) = {
            let destination =
                self.destination.as_ref().ok_or(Error::MissingConnectionInformation)?;
            let addrs = destination
                .resolve(self.options.ipv4_only)
                .await
                .inspect_err(|error| error!(?error, "Failed to resolve destination"))?;
            if addrs.is_empty() {
                return Err(Error::MalformedConnectionInformation(
                    "Socket addresses cannot be empty".into(),
                ));
            }

            if self.options.use_tls && self.options.domain.is_none() {
                let domain = destination.domain();
                if domain.is_empty() {
                    return Err(Error::MalformedConnectionInformation(
                        "Domain required for TLS, couldn't be determined from destination".into(),
                    ));
                }
                (addrs, Some(domain))
            } else {
                (addrs, self.options.domain)
            }
        };

        self.options.domain = domain;
        self.destination = Some(Destination::from(addrs));
        self.verified = true;

        Ok(self)
    }

    /// Builds a `ClickHouse` client by connecting to the configured destination.
    ///
    /// This method creates a [`Client`] using the configured destination, options,
    /// settings, and context. It verifies the destination (via
    /// [`ClientBuilder::verify`] if not already verified) and establishes a connection
    /// to the `ClickHouse` server. The client type (`NativeClient` or `ArrowClient`)
    /// is determined by the format `T` (e.g., [`NativeFormat`] or [`ArrowFormat`]).
    ///
    /// # Parameters
    /// - `T`: The client format, either [`NativeFormat`] or [`ArrowFormat`].
    ///
    /// # Returns
    /// A [`Result`] containing the connected [`Client<T>`], or an error if the
    /// connection fails.
    ///
    /// # Errors
    /// - Fails if the destination is unset or invalid ([`Error::MissingConnectionInformation`],
    ///   [`Error::MalformedConnectionInformation`]).
    /// - Fails if the connection cannot be established (e.g., network issues, authentication
    ///   failure).
    /// - Fails if TLS or cloud-specific configurations are invalid.
    ///
    /// # Panics
    /// - Shouldn't panic, verification guarantees destination.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let client = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_username("default")
    ///     .build_arrow()
    ///     .await
    ///     .unwrap();
    /// client.query("SELECT 1").await.unwrap();
    /// ```
    pub async fn build<T: ClientFormat>(self) -> Result<Client<T>> {
        let verified_builder = if self.verified { self } else { self.verify().await? };

        Client::connect(
            verified_builder.destination.unwrap(), // Guaranteed in verify above
            verified_builder.options,
            verified_builder.settings.map(Arc::new),
            verified_builder.context,
        )
        .await
    }

    /// A helper method to build a [`Client<ArrowFormat>`] directly
    ///
    /// # Errors
    /// - Returns an error if destination verification fails.
    ///
    /// # Panics
    /// - Shouldn't panic, verification guarantees destination.
    pub async fn build_arrow(self) -> Result<Client<ArrowFormat>> {
        Self::build::<ArrowFormat>(self).await
    }

    /// A helper method to build a [`Client<NativeFormat>`] directly
    ///
    /// # Errors
    /// - Returns an error if destination verification fails.
    ///
    /// # Panics
    /// - Shouldn't panic, verification guarantees destination.
    pub async fn build_native(self) -> Result<Client<NativeFormat>> {
        Self::build::<NativeFormat>(self).await
    }

    /// Builds a connection pool manager for `ClickHouse` clients.
    ///
    /// This method creates a [`ConnectionManager<T>`] for managing a pool of
    /// [`Client<T>`] instances, allowing efficient reuse of connections. It verifies
    /// the destination and configures the manager with the specified health check
    /// behavior. The client type (`NativeClient` or `ArrowClient`) is determined by
    /// the format `T` (e.g., [`NativeFormat`] or [`ArrowFormat`]).
    ///
    /// # Parameters
    /// - `check_health`: If `true`, the manager performs health checks on connections before
    ///   reusing them.
    ///
    /// # Returns
    /// A [`Result`] containing the [`ConnectionManager<T>`], or an error if
    /// verification or setup fails.
    ///
    /// # Errors
    /// - Fails if the destination is unset or invalid ([`Error::MissingConnectionInformation`],
    ///   [`Error::MalformedConnectionInformation`]).
    /// - Fails if the manager cannot be initialized (e.g., invalid configuration).
    ///
    /// # Feature
    /// Requires the `pool` feature to be enabled.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let manager = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_username("default")
    ///     .build_pool_manager::<ArrowFormat>(true)
    ///     .await
    ///     .unwrap();
    /// // Use the manager to get a client from the pool
    /// let client = manager.get().await.unwrap();
    /// client.query("SELECT 1").await.unwrap();
    /// ```
    #[cfg(feature = "pool")]
    pub async fn build_pool_manager<T: ClientFormat>(
        self,
        check_health: bool,
    ) -> Result<ConnectionManager<T>> {
        let manager =
            ConnectionManager::<T>::try_new_with_builder(self).await?.with_check(check_health);
        Ok(manager)
    }
}

impl ClientBuilder {
    /// Generates a unique identifier for the connection configuration.
    ///
    /// This method creates a string identifier based on the destination, username,
    /// password, database, and domain configured in the builder. It is useful for
    /// distinguishing between different connection configurations, especially when
    /// managing multiple clients or pools. The identifier includes a hashed password
    /// for security.
    ///
    /// # Returns
    /// A `String` representing the connection configuration.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_username("default")
    ///     .with_password("secret");
    /// let id = builder.connection_identifier();
    /// println!("Connection ID: {}", id);
    /// ```
    pub fn connection_identifier(&self) -> String {
        let mut dest_str = self.destination.as_ref().map_or(String::new(), Destination::domain);
        dest_str.push_str(&self.options.username);
        let mut hasher = rustc_hash::FxHasher::default();
        self.options.password.hash(&mut hasher);
        dest_str.push_str(&hasher.finish().to_string());
        dest_str.push_str(&self.options.default_database);
        if let Some(d) = self.options.domain.as_ref() {
            dest_str.push_str(d);
        }
        dest_str
    }
}

// Cloud related configuration
#[cfg(feature = "cloud")]
impl ClientBuilder {
    /// Enables or disables a cloud wakeup ping for `ClickHouse` cloud instances.
    ///
    /// This method configures whether the client will send a lightweight ping to wake
    /// up a `ClickHouse` cloud instance before connecting. This is useful for ensuring
    /// the instance is active, reducing connection latency. The ping is sent during
    /// [`ClientBuilder::build`] if enabled.
    ///
    /// # Parameters
    /// - `ping`: If `true`, enables the cloud wakeup ping; if `false`, disables it.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated cloud wakeup setting.
    ///
    /// # Feature
    /// Requires the `cloud` feature to be enabled.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("cloud.clickhouse.com:9000")
    ///     .with_cloud_wakeup(true);
    /// ```
    #[must_use]
    pub fn with_cloud_wakeup(mut self, ping: bool) -> Self {
        self.options.ext.cloud.wakeup = ping;
        self
    }

    /// Sets the maximum timeout for the cloud wakeup ping.
    ///
    /// This method configures the maximum time (in seconds) that the cloud wakeup ping
    /// (enabled via [`ClientBuilder::with_cloud_wakeup`]) will wait for a response from
    /// the `ClickHouse` cloud instance. If not set, a default timeout is used.
    ///
    /// # Parameters
    /// - `timeout`: The timeout in seconds.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated cloud timeout setting.
    ///
    /// # Feature
    /// Requires the `cloud` feature to be enabled.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("cloud.clickhouse.com:9000")
    ///     .with_cloud_wakeup(true)
    ///     .with_cloud_timeout(10);
    /// ```
    #[must_use]
    pub fn with_cloud_timeout(mut self, timeout: u64) -> Self {
        self.options.ext.cloud.timeout = Some(timeout);
        self
    }

    /// Sets a tracker for monitoring cloud wakeup pings.
    ///
    /// This method configures a shared `Arc<AtomicBool>` to track whether a
    /// `ClickHouse` cloud instance has been pinged across multiple client instances.
    /// This is useful for coordinating wakeup operations in a multi-client
    /// environment. The tracker is used when cloud wakeup is enabled (via
    /// [`ClientBuilder::with_cloud_wakeup`]).
    ///
    /// # Parameters
    /// - `track`: A shared `Arc<AtomicBool>` to track ping status.
    ///
    /// # Returns
    /// A new [`ClientBuilder`] with the updated cloud tracker.
    ///
    /// # Feature
    /// Requires the `cloud` feature to be enabled.
    ///
    /// # Examples
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    /// use std::sync::{Arc, atomic::AtomicBool};
    ///
    /// let tracker = Arc::new(AtomicBool::new(false));
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("cloud.clickhouse.com:9000")
    ///     .with_cloud_wakeup(true)
    ///     .with_cloud_track(tracker);
    /// ```
    #[must_use]
    pub fn with_cloud_track(mut self, track: Arc<std::sync::atomic::AtomicBool>) -> Self {
        let mut context = self.context.unwrap_or_default();
        context.cloud = Some(track);
        self.context = Some(context);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;

    use super::*;

    fn default_builder() -> ClientBuilder { ClientBuilder::new() }

    #[test]
    fn test_accessors_empty() {
        let builder = default_builder();
        assert_eq!(builder.destination(), None);
        assert!(!builder.options().use_tls);
        assert_eq!(builder.settings(), None);
        assert!(!builder.verified());
    }

    #[test]
    fn test_accessors_configured() {
        let settings = Settings::default();
        let builder = default_builder()
            .with_socket_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000))
            .with_settings(settings.clone())
            .with_options(ClientOptions { use_tls: true, ..Default::default() });
        assert!(builder.destination().is_some());
        assert!(builder.options().use_tls);
        assert_eq!(builder.settings(), Some(&settings));
        assert!(!builder.verified());
    }

    #[test]
    fn test_with_socket_addr() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000);
        let builder = default_builder().with_socket_addr(addr);
        assert_eq!(builder.destination(), Some(&Destination::from(addr)));
        assert!(!builder.verified());
    }

    #[test]
    fn test_with_host_port() {
        let builder = default_builder().with_host_port("localhost", 9000);
        assert_eq!(
            builder.destination(),
            Some(&Destination::from(("localhost".to_string(), 9000)))
        );
        assert!(!builder.verified());
    }

    #[test]
    fn test_with_options() {
        let options =
            ClientOptions { username: "test".to_string(), use_tls: true, ..Default::default() };
        let builder = default_builder().with_options(options.clone());
        assert_eq!(builder.options(), &options);
        assert!(!builder.verified());
    }

    #[test]
    fn test_with_tls() {
        let builder = default_builder().with_tls(true);
        assert!(builder.options().use_tls);
        assert!(!builder.verified());

        let builder = builder.with_tls(true); // Same value, no change
        assert!(!builder.verified());

        let builder = builder.with_tls(false); // Change value
        assert!(!builder.options().use_tls);
        assert!(!builder.verified());
    }

    #[test]
    fn test_with_cafile() {
        let cafile = PathBuf::from("/path/to/ca.pem");
        let builder = default_builder().with_cafile(&cafile);
        assert_eq!(builder.options().cafile, Some(cafile));
    }

    #[test]
    fn test_with_settings() {
        let settings = Settings::default();
        let builder = default_builder().with_settings(settings.clone());
        assert_eq!(builder.settings(), Some(&settings));
    }

    #[test]
    fn test_with_database() {
        let builder = default_builder().with_database("test_db");
        assert_eq!(builder.options().default_database, "test_db");
    }

    #[test]
    fn test_with_domain() {
        let builder = default_builder().with_domain("example.com");
        assert_eq!(builder.options().domain, Some("example.com".to_string()));
        assert!(!builder.verified());
    }

    #[test]
    fn test_with_trace_context() {
        let trace_context = TraceContext::default();
        let builder = default_builder().with_trace_context(trace_context);
        assert_eq!(builder.context.unwrap().trace, Some(trace_context));
    }

    #[test]
    fn test_connection_identifier() {
        let builder = default_builder()
            .with_endpoint("localhost:9000")
            .with_username("user")
            .with_password("pass")
            .with_database("db")
            .with_domain("example.com");
        let id = builder.connection_identifier();
        assert!(id.contains("localhost"));
        assert!(id.contains("user"));
        assert!(id.contains("db"));
        assert!(id.contains("example.com"));

        let empty_builder = default_builder();
        assert_eq!(empty_builder.connection_identifier(), "default13933120620573868840");
    }

    #[tokio::test]
    async fn test_verify_empty_addrs() {
        let builder = default_builder()
            .with_destination(Destination::from(vec![])) // Empty SocketAddrs
            .verify()
            .await;
        assert!(matches!(
            builder,
            Err(Error::MalformedConnectionInformation(msg))
            if msg.contains("Socket addresses cannot be empty")
        ));
    }

    #[tokio::test]
    async fn test_verify_no_connection_information() {
        let builder = default_builder().verify().await;
        assert!(matches!(builder, Err(Error::MissingConnectionInformation)));
    }

    #[cfg(feature = "pool")]
    #[tokio::test]
    async fn test_build_pool_manager() {
        use crate::formats::ArrowFormat;

        let builder = default_builder()
            .with_endpoint("localhost:9000")
            .with_username("user")
            .verify()
            .await
            .unwrap();
        let manager = builder.build_pool_manager::<ArrowFormat>(true).await;
        assert!(manager.is_ok());
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_with_cloud_timeout() {
        let builder = default_builder().with_cloud_timeout(5000);
        assert_eq!(builder.options().ext.cloud.timeout, Some(5000));
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_with_cloud_wakeup() {
        let builder = default_builder().with_cloud_wakeup(true);
        assert!(builder.options().ext.cloud.wakeup);
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_with_cloud_track() {
        use std::sync::atomic::Ordering;

        let track = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let builder = default_builder().with_cloud_track(Arc::clone(&track));
        assert_eq!(
            builder.context.unwrap().cloud.unwrap().load(Ordering::SeqCst),
            track.load(Ordering::SeqCst)
        );
    }
}
