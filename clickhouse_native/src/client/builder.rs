use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tracing::error;

use super::tcp::Destination;
use super::{ArrowOptions, Client, ClientFormat, CompressionMethod, ConnectionContext, Secret};
#[cfg(feature = "pool")]
use crate::pool::ConnectionManager;
use crate::settings::Settings;
use crate::telemetry::TraceContext;
use crate::{ClickhouseNativeError, ClientOptions, Result};

/// Builder for [`crate::Client`]
#[derive(Default, Debug, Clone)]
pub struct ClientBuilder {
    destination: Option<Destination>,
    options:     ClientOptions,
    settings:    Option<Arc<Settings>>,
    context:     Option<ConnectionContext>,
    verified:    bool,
}

impl ClientBuilder {
    /// Create a new builder with default options
    pub fn new() -> Self {
        ClientBuilder {
            destination: None,
            options:     ClientOptions::default(),
            settings:    None,
            context:     None,
            verified:    false,
        }
    }

    /// Access inner destination
    pub fn destination(&self) -> Option<&Destination> { self.destination.as_ref() }

    /// Access inner options
    pub fn options(&self) -> &ClientOptions { &self.options }

    /// Access inner settings
    pub fn settings(&self) -> Option<&Arc<Settings>> { self.settings.as_ref() }

    /// Whether the client builder has been verified
    pub fn verified(&self) -> bool { self.verified }

    /// Set destination with a [`SocketAddr`] (e.g., 127.0.0.1:9000)
    #[must_use]
    pub fn with_socket_addr(self, addr: SocketAddr) -> Self { self.with_destination(addr) }

    /// Set destination with an IP or hostname and port (e.g., "localhost", 9000)
    #[must_use]
    pub fn with_host_port(self, host: impl Into<String>, port: u16) -> Self {
        self.with_destination((host.into(), port))
    }

    /// Set destination with a string endpoint (e.g., "localhost:9000")
    #[must_use]
    pub fn with_endpoint(self, endpoint: impl Into<String>) -> Self {
        self.with_destination(endpoint.into())
    }

    /// Set destination with a Destination
    #[must_use]
    pub fn with_destination<D>(mut self, destination: D) -> Self
    where
        D: Into<Destination>,
    {
        self.destination = Some(destination.into());
        self.verified = false;
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_options(mut self, options: ClientOptions) -> Self {
        self.options = options;
        self.verified = false;
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_tls(mut self, tls: bool) -> Self {
        if self.options.use_tls != tls {
            self.options.use_tls = tls;
            self.verified = false;
        }
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_cafile<P: AsRef<Path>>(mut self, cafile: P) -> Self {
        self.options.cafile = Some(cafile.as_ref().to_path_buf());
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_ipv4_only(mut self, enabled: bool) -> Self {
        self.options.ipv4_only = enabled;
        self
    }

    /// Set settings
    #[must_use]
    pub fn with_settings(mut self, settings: Arc<Settings>) -> Self {
        self.settings = Some(settings);
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.options.username = username.into();
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_password<T>(mut self, password: T) -> Self
    where
        Secret: From<T>,
    {
        self.options.password = Secret::from(password);
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.options.default_database = database.into();
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.options.domain = Some(domain.into());
        self.verified = false;
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_compression(mut self, compression: CompressionMethod) -> Self {
        self.options.compression = compression;
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_arrow_options(mut self, options: ArrowOptions) -> Self {
        self.options.arrow = Some(options);
        self
    }

    /// Set client options
    #[cfg(feature = "cloud")]
    #[must_use]
    pub fn with_cloud_wakeup(mut self, ping: bool) -> Self {
        self.options.cloud.wakeup = ping;
        self
    }

    /// Set client options
    #[must_use]
    pub fn with_cloud_timeout(mut self, timeout: u64) -> Self {
        self.options.cloud.timeout = Some(timeout);
        self
    }

    /// Set cloud ping tracker
    #[cfg(feature = "cloud")]
    #[must_use]
    pub fn with_cloud_track(mut self, track: Arc<AtomicBool>) -> Self {
        let mut context = self.context.unwrap_or_default();
        context.cloud = Some(track);
        self.context = Some(context);
        self
    }

    /// Set trace context
    #[must_use]
    pub fn with_trace_context(mut self, trace_context: TraceContext) -> Self {
        let mut context = self.context.unwrap_or_default();
        context.trace = Some(trace_context);
        self.context = Some(context);
        self
    }

    /// Helper to resolve the destination early
    ///
    /// # Errors
    ///
    /// Returns an error if destination verification fails.
    pub async fn verify(mut self) -> Result<Self> {
        let (addrs, domain) = {
            let destination = self
                .destination
                .as_ref()
                .ok_or(ClickhouseNativeError::MissingConnectionInformation)?;
            let addrs = destination
                .resolve(self.options.ipv4_only)
                .await
                .inspect_err(|error| error!(?error, "Failed to resolve destination"))?;
            if addrs.is_empty() {
                return Err(ClickhouseNativeError::MalformedConnectionInformation(
                    "Socket addresses cannot be empty".into(),
                ));
            }

            if self.options.use_tls && self.options.domain.is_none() {
                let domain = destination.domain();
                if domain.is_empty() {
                    return Err(ClickhouseNativeError::MalformedConnectionInformation(
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

    /// Build the client by connecting to the destination
    ///
    /// # Errors
    ///
    /// Returns an error if destination verification fails.
    ///
    /// # Panics
    ///
    /// Shouldn't panic, verification guarantees destination.
    pub async fn build<T: ClientFormat>(self) -> Result<Client<T>> {
        let verified_builder = if self.verified { self } else { self.verify().await? };

        Client::connect(
            verified_builder.destination.unwrap(), // Guaranteed in verify above
            verified_builder.options,
            verified_builder.settings,
            verified_builder.context,
        )
        .await
    }

    /// Build a client pool manager directly from the builder
    ///
    /// # Errors
    ///
    /// Returns an error if destination verification fails.
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

/// Helpful to deteremine if 2 builders refer to the same underlying connection destination
impl ClientBuilder {
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
