use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};

use crate::constants::*;
use crate::prelude::*;
use crate::{Error, Result};

// Custom Destination type
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Destination {
    inner: DestinationInner,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum DestinationInner {
    SocketAddrs(Vec<SocketAddr>), // It is important this is guaranteed to be non-empty
    SocketAddr(SocketAddr),       // Direct SocketAddr (e.g., 127.0.0.1:9000)
    HostPort(String, u16),        // Hostname and port (e.g., "localhost", 9000)
    Endpoint(String),             // String to parse (e.g., "localhost:9000")
}

impl Destination {
    /// Resolve to Vec<SocketAddr> using [`tokio::net::lookup_host`]
    pub(crate) async fn resolve(&self, ipv4_only: bool) -> Result<Vec<SocketAddr>> {
        let addrs: Vec<SocketAddr> = match &self.inner {
            DestinationInner::SocketAddrs(addrs) => return Ok(addrs.clone()),
            DestinationInner::SocketAddr(addr) => return Ok(vec![*addr]),
            DestinationInner::HostPort(host, port) => {
                tokio::net::lookup_host((host.as_str(), *port)).await.map(Iterator::collect)
            }
            DestinationInner::Endpoint(endpoint) => {
                tokio::net::lookup_host(endpoint).await.map(Iterator::collect)
            }
        }
        .map_err(|_| {
            Error::MalformedConnectionInformation("Could not resolve destination".into())
        })?;

        Ok(addrs
            .into_iter()
            .filter(|addr| !ipv4_only || matches!(addr, SocketAddr::V4(_)))
            .collect())
    }

    // Create a domain from this Destination
    pub(crate) fn domain(&self) -> String {
        match &self.inner {
            DestinationInner::SocketAddrs(addrs) => {
                addrs.iter().next().map(|addr| addr.ip().to_string()).unwrap_or_default()
            }
            DestinationInner::SocketAddr(addr) => addr.ip().to_string(),
            DestinationInner::HostPort(host, _) => host.clone(),
            DestinationInner::Endpoint(endpoint) => {
                endpoint.split(':').next().map(ToString::to_string).unwrap_or(endpoint.clone())
            }
        }
    }
}

/// Connects to `ClickHouse`'s native server port over TLS.
pub(super) async fn connect_tls(
    addrs: &[SocketAddr],
    domain: Option<&str>,
) -> Result<TlsStream<TcpStream>> {
    let domain: String =
        domain.as_ref().map_or_else(|| addrs[0].ip().to_string(), ToString::to_string);
    debug!(%domain, "Initiating TLS connection");
    let stream = connect_socket(addrs).await?;
    tls_stream(domain, stream).await
}

/// Connects to `ClickHouse`'s native server port and configures common socket options.
#[instrument(level = "trace", name = "clickhouse._connect_socket", skip_all)]
pub(crate) async fn connect_socket(addrs: &[SocketAddr]) -> Result<TcpStream> {
    debug!(?addrs, "Initiating TCP connection");
    let addr = addrs.first().ok_or(Error::MissingConnectionInformation)?;
    let domain = if addr.is_ipv4() { socket2::Domain::IPV4 } else { socket2::Domain::IPV6 };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    // Increase buffer sizes for high-throughput data transfer
    socket.set_recv_buffer_size(TCP_READ_BUFFER_SIZE as usize)?;
    socket.set_send_buffer_size(TCP_WRITE_BUFFER_SIZE as usize)?;
    // Configure TCP keepalive
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(TCP_KEEP_ALIVE_SECS))
        .with_interval(Duration::from_secs(TCP_KEEP_ALIVE_INTERVAL))
        .with_retries(TCP_KEEP_ALIVE_RETRIES);
    socket.set_tcp_keepalive(&keepalive)?;

    // Connect with a timeout
    let sock_addr = socket2::SockAddr::from(*addr);
    socket.connect_timeout(&sock_addr, Duration::from_secs(TCP_CONNECT_TIMEOUT))?;
    trace!("Connected socket for {addr}");

    // Convert to TcpStream
    let stream = std::net::TcpStream::from(socket);
    stream.set_nodelay(true)?;
    stream.set_nonblocking(true)?;

    Ok(TcpStream::from_std(stream)?)
}

// Helper function to facilitate TLS connection setup
async fn tls_stream(domain: String, stream: TcpStream) -> Result<TlsStream<TcpStream>> {
    let root_store = RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.into() };

    let mut tls_config =
        ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth();

    // Enable session resumption by default
    tls_config.resumption = rustls::client::Resumption::in_memory_sessions(256);

    let connector = TlsConnector::from(Arc::new(tls_config));
    let dnsname =
        ServerName::try_from(domain.clone()).map_err(|e| Error::InvalidDnsName(e.to_string()))?;
    Ok(connector.connect(dnsname, stream).await?)
}

impl std::fmt::Display for Destination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            DestinationInner::SocketAddrs(addrs) => {
                write!(f, "{}", addrs.first().map(ToString::to_string).unwrap_or_default())
            }
            DestinationInner::SocketAddr(addr) => write!(f, "{addr}"),
            DestinationInner::HostPort(host, port) => write!(f, "{host}:{port}"),
            DestinationInner::Endpoint(endpoint) => write!(f, "{endpoint}"),
        }
    }
}

// From implementations for common destination types
impl From<Vec<SocketAddr>> for Destination {
    fn from(addrs: Vec<SocketAddr>) -> Self {
        Destination { inner: DestinationInner::SocketAddrs(addrs) }
    }
}

// From implementations for common destination types
impl From<SocketAddr> for Destination {
    fn from(addr: SocketAddr) -> Self { Destination { inner: DestinationInner::SocketAddr(addr) } }
}

impl From<(String, u16)> for Destination {
    fn from((host, port): (String, u16)) -> Self {
        Destination { inner: DestinationInner::HostPort(host, port) }
    }
}

impl From<&(String, u16)> for Destination {
    fn from((host, port): &(String, u16)) -> Self {
        Destination { inner: DestinationInner::HostPort(host.clone(), *port) }
    }
}

impl From<(&String, u16)> for Destination {
    fn from((host, port): (&String, u16)) -> Self {
        Destination { inner: DestinationInner::HostPort(host.clone(), port) }
    }
}

impl From<(&str, u16)> for Destination {
    fn from((host, port): (&str, u16)) -> Self {
        Destination { inner: DestinationInner::HostPort(host.to_string(), port) }
    }
}

impl From<String> for Destination {
    fn from(endpoint: String) -> Self {
        Destination { inner: DestinationInner::Endpoint(endpoint) }
    }
}

impl From<&String> for Destination {
    fn from(endpoint: &String) -> Self {
        Destination { inner: DestinationInner::Endpoint(endpoint.clone()) }
    }
}

impl From<&str> for Destination {
    fn from(endpoint: &str) -> Self {
        Destination { inner: DestinationInner::Endpoint(endpoint.to_string()) }
    }
}

impl From<std::borrow::Cow<'_, str>> for Destination {
    fn from(endpoint: std::borrow::Cow<'_, str>) -> Self {
        Destination { inner: DestinationInner::Endpoint(endpoint.into_owned()) }
    }
}

impl From<(Ipv4Addr, u16)> for Destination {
    fn from((host, port): (Ipv4Addr, u16)) -> Self {
        Destination { inner: DestinationInner::SocketAddr((host, port).into()) }
    }
}

impl From<(Ipv6Addr, u16)> for Destination {
    fn from((host, port): (Ipv6Addr, u16)) -> Self {
        Destination { inner: DestinationInner::SocketAddr((host, port).into()) }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    // Helper to create Destination variants
    fn socket_addr() -> SocketAddr { SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000) }

    #[tokio::test]
    async fn test_resolve_socket_addrs() {
        let addrs = vec![socket_addr()];
        let dest = Destination { inner: DestinationInner::SocketAddrs(addrs.clone()) };
        let result = dest.resolve(false).await.unwrap();
        assert_eq!(result, addrs);
    }

    #[tokio::test]
    async fn test_resolve_socket_addr() {
        let addr = socket_addr();
        let dest = Destination { inner: DestinationInner::SocketAddr(addr) };
        let result = dest.resolve(false).await.unwrap();
        assert_eq!(result, vec![addr]);
    }

    #[tokio::test]
    async fn test_resolve_host_port_valid() {
        let dest = Destination { inner: DestinationInner::HostPort("localhost".to_string(), 9000) };
        let result = dest.resolve(false).await.unwrap();
        assert!(!result.is_empty());
        assert!(result.iter().all(|addr| addr.port() == 9000));
    }

    #[tokio::test]
    async fn test_resolve_host_port_invalid() {
        let dest =
            Destination { inner: DestinationInner::HostPort("invalid-host-xyz".to_string(), 9000) };
        let result = dest.resolve(false).await;
        assert!(matches!(
            result,
            Err(Error::MalformedConnectionInformation(msg))
            if msg == "Could not resolve destination"
        ));
    }

    #[tokio::test]
    async fn test_resolve_endpoint_valid() {
        let dest = Destination { inner: DestinationInner::Endpoint("localhost:9000".to_string()) };
        let result = dest.resolve(false).await.unwrap();
        assert!(!result.is_empty());
        assert!(result.iter().all(|addr| addr.port() == 9000));
    }

    #[tokio::test]
    async fn test_resolve_endpoint_invalid() {
        let dest =
            Destination { inner: DestinationInner::Endpoint("invalid-host-xyz:9000".to_string()) };
        let result = dest.resolve(false).await;
        assert!(matches!(
            result,
            Err(Error::MalformedConnectionInformation(msg))
            if msg == "Could not resolve destination"
        ));
    }

    #[tokio::test]
    async fn test_resolve_ipv4_only() {
        let dest = Destination { inner: DestinationInner::Endpoint("localhost:9000".to_string()) };
        let result = dest.resolve(true).await.unwrap();
        assert!(!result.is_empty());
        assert!(result.iter().all(|addr| matches!(addr, SocketAddr::V4(_))));
    }

    #[test]
    fn test_domain_socket_addrs() {
        let addrs = vec![socket_addr()];
        let dest = Destination { inner: DestinationInner::SocketAddrs(addrs) };
        assert_eq!(dest.domain(), "127.0.0.1");
    }

    #[test]
    fn test_domain_socket_addrs_empty() {
        let dest = Destination { inner: DestinationInner::SocketAddrs(vec![]) };
        assert_eq!(dest.domain(), "");
    }

    #[test]
    fn test_domain_socket_addr() {
        let addr = socket_addr();
        let dest = Destination { inner: DestinationInner::SocketAddr(addr) };
        assert_eq!(dest.domain(), "127.0.0.1");
    }

    #[test]
    fn test_domain_host_port() {
        let dest = Destination { inner: DestinationInner::HostPort("localhost".to_string(), 9000) };
        assert_eq!(dest.domain(), "localhost");
    }

    #[test]
    fn test_domain_endpoint() {
        let dest = Destination { inner: DestinationInner::Endpoint("localhost:9000".to_string()) };
        assert_eq!(dest.domain(), "localhost");
    }

    #[test]
    fn test_domain_endpoint_no_port() {
        let dest = Destination { inner: DestinationInner::Endpoint("localhost".to_string()) };
        assert_eq!(dest.domain(), "localhost");
    }
}
