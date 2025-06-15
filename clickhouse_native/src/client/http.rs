use std::env::consts::OS;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{self, BufMut, Bytes, BytesMut};
use futures_util::{Stream, ready};
use hyper::body::{Body, Frame, SizeHint};
use hyper::header::{CONTENT_LENGTH, HeaderValue, USER_AGENT};
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::connect::{Connect, HttpConnector};
use hyper_util::client::legacy::{Client as HyperClient, ResponseFuture};
use hyper_util::rt::TokioExecutor;
use pin_project::pin_project;
use strum::AsRefStr;
use url::Url;

use super::ClientOptions;
use super::connection::ClientMetadata;
use crate::constants::TCP_KEEP_ALIVE_SECS;
use crate::prelude::*;
use crate::row::protocol::{
    CLICKHOUSE_DEFAULT_CHUNK_BYTES, DEFAULT_SETTINGS, HttpSummary, X_CLICKHOUSE_FORMAT,
    X_CLICKHOUSE_KEY, X_CLICKHOUSE_SUMMARY, X_CLICKHOUSE_USER,
};

static LIB_USER_AGENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}/{} (lv:rust/{} {OS})",
        option_env!("CARGO_PKG_NAME").unwrap_or("unknown"),
        option_env!("CARGO_PKG_VERSION").unwrap_or("unknown"),
        option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unknown")
    )
});
// Estimated based on standard ClickHouse block sizes
const RECV_BUFFER_SIZE: usize = 2 * CLICKHOUSE_DEFAULT_CHUNK_BYTES;
const TARGET_BUFFER_SIZE: usize = RECV_BUFFER_SIZE;
const MIN_COMPRESSED_TARGET_BUFFER_SIZE: usize = 64 * 1024; // Allow serving bytes early
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2); // Based on ClickHouse's 3s
const MAX_QUERY_LEN_FOR_GET: usize = 8192;
const DEFAULT_FORMAT: &str = "RowBinary";

#[derive(Debug, Clone)]
struct HttpOptions {
    username: String,
    password: Secret,
    metadata: ClientMetadata,
}

// Trait for HTTP client implementation to avoid drilling generics through.
trait HttpClientConnect: std::fmt::Debug + Send + Sync + 'static {
    fn request(&self, req: Request<QueryRequest>) -> ResponseFuture;
}

impl<C> HttpClientConnect for HyperClient<C, QueryRequest>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    fn request(&self, req: Request<QueryRequest>) -> ResponseFuture { self.request(req) }
}

#[derive(Debug, Clone)]
pub(crate) struct HttpClient {
    client:   Arc<dyn HttpClientConnect>,
    endpoint: String,
    params:   Vec<(String, String)>,
    options:  HttpOptions,
}

impl HttpClient {
    /// Constructor for `HttpClient` (HTTP, port 8123 or 8443).
    ///
    /// # Errors
    /// - Returns an error if the endpoint is invalid or fails to resolve
    pub(crate) fn try_new(
        domain: impl Into<String>,
        options: &ClientOptions,
        settings: Option<&Settings>,
        cid: u16,
    ) -> Result<Self> {
        // Endpoint
        let endpoint = options
            .ext
            .http
            .as_ref()
            .and_then(|h| h.proxy.as_deref())
            .map(ToString::to_string)
            .unwrap_or(domain.into());

        // Port
        let port = options.ext.http.as_ref().and_then(|h| h.port).unwrap_or(if options.use_tls {
            8443
        } else {
            8123
        });

        let mut connector = HttpConnector::new();
        connector.set_keepalive(Some(Duration::from_secs(TCP_KEEP_ALIVE_SECS)));
        connector.set_nodelay(true);
        connector.set_recv_buffer_size(Some(RECV_BUFFER_SIZE));

        let mut client_builder = HyperClient::builder(TokioExecutor::new());
        let _ = client_builder.pool_idle_timeout(POOL_IDLE_TIMEOUT);

        let client = if options.use_tls {
            connector.enforce_http(false);
            let tls_connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_http1()
                .wrap_connector(connector);
            Arc::new(client_builder.build(tls_connector)) as Arc<dyn HttpClientConnect>
        } else {
            Arc::new(client_builder.build(connector)) as Arc<dyn HttpClientConnect>
        };

        let (endpoint, params) = build_endpoint(&endpoint, port, options, settings)?;
        debug!(endpoint, ?params, port, "Built http client");

        let metadata = ClientMetadata {
            client_id:     cid,
            compression:   options.compression,
            arrow_options: options.ext.arrow.unwrap_or_default(),
        };

        // Zstd compression is not supported. If enabled, switch to LZ4.
        let metadata = if matches!(metadata.compression, CompressionMethod::ZSTD) {
            metadata.with_compression(CompressionMethod::LZ4)
        } else {
            metadata
        };

        // Create http options
        let options = HttpOptions {
            username: options.username.clone(),
            password: options.password.clone(),
            metadata,
        };

        Ok(HttpClient { client, endpoint, params, options })
    }

    pub(crate) async fn query<Format: ClientFormat, P: Into<QueryParams>>(
        &self,
        query: &str,
        params: Option<P>,
        schema: Format::Schema,
        overrides: Option<SchemaConversions>,
        qid: Qid,
    ) -> Result<Vec<Format::Data>> {
        let mut reader = self.send_query(query, params, qid).await?;
        let summary = reader.summary();
        Format::read_rows(&mut reader, schema, overrides, self.options.metadata, summary).await
    }

    async fn send_query<P: Into<QueryParams>>(
        &self,
        query: &str,
        params: Option<P>,
        qid: Qid,
    ) -> Result<HyperBodyReader> {
        let endpoint = &self.endpoint;

        let mut url =
            Url::parse(endpoint).map_err(|e| Error::Protocol(format!("Invalid URI: {e}")))?;

        // Add query_id. Should be cheap-ish since the impl takes into account heap allocs
        let _ = url
            .query_pairs_mut()
            .append_pair("query_id", &qid.to_string())
            .extend_pairs(self.params.iter().map(|(k, v)| (k.as_str(), v.as_str())));

        // Interpolated params
        if let Some(s) = params.map(Into::<QueryParams>::into).map(Settings::from) {
            let _ = url.query_pairs_mut().extend_pairs(
                s.encode_to_key_value_strings().into_iter().map(|(k, v)| (format!("param_{k}"), v)),
            );
        }

        // GET if small query, POST otherwise
        let method = if query.len() < MAX_QUERY_LEN_FOR_GET { Method::GET } else { Method::POST };

        // Add query for GET requests
        if matches!(method, Method::GET) {
            let _ = url.query_pairs_mut().append_pair("query", query);
        }

        debug!(url = url.as_str(), query, ?method, "Building request");

        // Begin creating request
        let req = Request::builder()
            .method(&method)
            .uri(url.as_str())
            .header(USER_AGENT, LIB_USER_AGENT.as_str())
            .header(X_CLICKHOUSE_USER, &self.options.username)
            .header(X_CLICKHOUSE_KEY, self.options.password.get())
            .header(X_CLICKHOUSE_FORMAT, HeaderValue::from_static(DEFAULT_FORMAT));

        let req = if matches!(method, Method::POST) {
            req.header(CONTENT_LENGTH, query.len().to_string())
        } else {
            req
        };

        // Build req body
        let req = if matches!(method, Method::POST) {
            req.body(QueryRequest::full(query.into()))
        } else {
            req.body(QueryRequest::empty())
        };

        let body = req.map_err(|e| Error::Protocol(format!("Failed to build request: {e}")))?;

        // Issue query
        let response = self
            .client
            .request(body)
            .await
            .map_err(|e| Error::Protocol(format!("HTTP request failed: {e}")))?;

        // Convert to ClickhouseRead, aka HyperBodyReader
        HyperBodyReader::try_new(response, self.options.metadata.compression)
    }
}

fn build_endpoint(
    endpoint: &str,
    port: u16,
    options: &ClientOptions,
    settings: Option<&Settings>,
) -> Result<(String, Vec<(String, String)>)> {
    let database = &options.default_database;

    let mut params = Vec::with_capacity(32);
    if !database.is_empty() {
        params.push(("database".to_string(), database.into()));
    }

    let settings = settings.cloned().unwrap_or_default().encode_to_key_value_strings();

    // Encode default settings first to allow their overrides
    params.extend(
        DEFAULT_SETTINGS
            .iter()
            .filter(|(k, _)| !settings.iter().any(|(k2, _)| k == k2))
            .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
    );
    params.extend(settings);

    match options.compression {
        CompressionMethod::LZ4 => params.push(("compress".into(), "1".into())),
        // TODO: Remove - add docs that ZSTD isn't currently supported
        CompressionMethod::ZSTD => {
            warn!("ZSTD compression is not currently supported for RowBinary, falling back to LZ4");
            params.push(("compress".into(), "1".into()));
        }
        CompressionMethod::None => params.push(("compress".into(), "0".into())),
    }

    let scheme = if options.use_tls { "https" } else { "http" };
    let endpoint = format!("{scheme}://{endpoint}:{port}");

    // Parse to ensure endpoint is valid
    drop(
        Url::parse_with_params(&endpoint, params.iter())
            .map_err(|e| Error::Protocol(format!("Invalid URI: {e}")))?,
    );

    Ok((endpoint, params))
}

#[derive(Clone, Debug, AsRefStr)]
enum QueryRequest {
    Full(Bytes),
    Empty,
}

impl QueryRequest {
    pub(crate) fn empty() -> Self { Self::Empty }

    pub(crate) fn full(query: String) -> Self { Self::Full(Bytes::from(query)) }
}

impl Body for QueryRequest {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.get_mut() {
            Self::Full(bytes) => Poll::Ready(Some(Ok(Frame::data(std::mem::take(bytes))))),
            Self::Empty => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Full(bytes) => bytes.is_empty(),
            Self::Empty => true,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Full(bytes) => SizeHint::with_exact(bytes.len() as u64),
            Self::Empty => SizeHint::with_exact(0),
        }
    }
}

#[pin_project]
pub(crate) struct HyperBodyReader {
    #[pin]
    body:        hyper::body::Incoming,
    buffer:      BytesMut,
    summary:     HttpSummary,
    is_finished: bool,
}

impl HyperBodyReader {
    pub(crate) fn try_new(
        response: Response<hyper::body::Incoming>,
        compression: CompressionMethod,
    ) -> Result<Self> {
        if !response.status().is_success() {
            return Err(Error::Protocol(format!("HTTP error: {}", response.status())));
        }

        info!(headers = ?response.headers(), "reading response");

        let summary = response
            .headers()
            .get(X_CLICKHOUSE_SUMMARY)
            .map(HeaderValue::to_str)
            .transpose()
            .ok()
            .flatten()
            .map(HttpSummary::from_str)
            .transpose()
            .ok()
            .flatten()
            .unwrap_or_default()
            .with_compression(compression);

        Ok(HyperBodyReader {
            body: response.into_body(),
            buffer: BytesMut::with_capacity(TARGET_BUFFER_SIZE),
            summary,
            is_finished: false,
        })
    }

    pub(crate) fn summary(&self) -> HttpSummary { self.summary }
}

impl Stream for HyperBodyReader {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let target = TARGET_BUFFER_SIZE;
        let min_target = matches!(self.summary.compression, CompressionMethod::None)
            .then_some(CLICKHOUSE_DEFAULT_CHUNK_BYTES)
            .unwrap_or(MIN_COMPRESSED_TARGET_BUFFER_SIZE);

        if self.is_finished && self.buffer.is_empty() {
            return Poll::Ready(None);
        }

        let mut this = self.as_mut().project();

        if !*this.is_finished {
            // Poll for data, tracking if inner body is finished
            *this.is_finished = loop {
                let res = this.body.as_mut().poll_frame(cx);
                if matches!(res, Poll::Pending) {
                    if this.buffer.len() > min_target {
                        break false;
                    }
                    return Poll::Pending;
                }

                match ready!(res) {
                    Some(Ok(frame)) => {
                        if let Ok(data) = frame.into_data() {
                            this.buffer.put(data);

                            // Keep the chunks manageable
                            if this.buffer.len() >= target {
                                break false;
                            }
                        }
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(Error::Protocol(e.to_string())))),
                    None => break true,
                }
            };
        }

        // Keep the capacity
        Poll::Ready(Some(Ok(self.buffer.split().freeze())))
    }
}
