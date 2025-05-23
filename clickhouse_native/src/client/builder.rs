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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

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
        let settings = Arc::new(Settings::default());
        let builder = default_builder()
            .with_socket_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000))
            .with_settings(Arc::clone(&settings))
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
        let settings = Arc::new(Settings::default());
        let builder = default_builder().with_settings(Arc::clone(&settings));
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
    fn test_with_cloud_timeout() {
        let builder = default_builder().with_cloud_timeout(5000);
        assert_eq!(builder.options().cloud.timeout, Some(5000));
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

    #[cfg(feature = "cloud")]
    #[test]
    fn test_with_cloud_wakeup() {
        let builder = default_builder().with_cloud_wakeup(true);
        assert!(builder.options().cloud.wakeup);
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_with_cloud_track() {
        use std::sync::atomic::Ordering;

        let track = Arc::new(AtomicBool::new(false));
        let builder = default_builder().with_cloud_track(Arc::clone(&track));
        assert_eq!(
            builder.context.unwrap().cloud.unwrap().load(Ordering::SeqCst),
            track.load(Ordering::SeqCst)
        );
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

    #[tokio::test]
    async fn test_verify_empty_addrs() {
        let builder = default_builder()
            .with_destination(Destination::from(vec![])) // Empty SocketAddrs
            .verify()
            .await;
        assert!(matches!(
            builder,
            Err(ClickhouseNativeError::MalformedConnectionInformation(msg))
            if msg.contains("Socket addresses cannot be empty")
        ));
    }

    #[tokio::test]
    async fn test_verify_no_connection_information() {
        let builder = default_builder().verify().await;
        assert!(matches!(builder, Err(ClickhouseNativeError::MissingConnectionInformation)));
    }
}
