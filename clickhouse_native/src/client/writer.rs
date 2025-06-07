use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::connection::ClientMetadata;
use crate::Result;
use crate::io::ClickhouseWrite;
use crate::native::client_info::ClientInfo;
use crate::native::protocol::{
    ClientHello, CompressionMethod, DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS, DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY,
    DBMS_MIN_REVISION_WITH_CLIENT_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET,
    DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL,
    DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION, QueryProcessingStage, ServerHello,
};
use crate::prelude::*;
use crate::query::QueryParams;
use crate::settings::Settings;

#[derive(Debug)]
pub(super) struct Query<'a> {
    pub qid:      Qid,
    pub info:     ClientInfo<'a>,
    pub settings: Option<Arc<Settings>>,
    pub stage:    QueryProcessingStage,
    #[expect(clippy::struct_field_names)]
    pub query:    &'a str,
    pub params:   Option<QueryParams>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Writer<W: ClickhouseWrite> {
    _phantom: std::marker::PhantomData<W>,
}

impl<W: ClickhouseWrite> Writer<W> {
    pub(super) async fn send_hello(writer: &mut W, params: ClientHello) -> Result<()> {
        writer.write_var_uint(ClientPacketId::Hello as u64).await?;
        writer
            .write_string(format!("ClickhouseNative Rust {}", env!("CARGO_PKG_VERSION")))
            .await?;
        writer.write_var_uint(crate::constants::VERSION_MAJOR).await?;
        writer.write_var_uint(crate::constants::VERSION_MINOR).await?;
        writer.write_var_uint(DBMS_TCP_PROTOCOL_VERSION).await?;
        writer.write_string(params.default_database).await?;
        writer.write_string(params.username).await?;
        writer.write_string(params.password).await?;
        writer.flush().instrument(trace_span!("flush_hello")).await?;
        Ok(())
    }

    pub(super) async fn send_query(
        writer: &mut W,
        params: Query<'_>,
        server_settings: Option<&Settings>,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<()> {
        writer.write_var_uint(ClientPacketId::Query as u64).await?;
        params.qid.write_id(writer).await?;

        if revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
            params.info.write(writer, revision).await?;
        }

        // Compression settings
        //
        // Boolean flagging that compression is used below is not enough, at least for zstd. We must
        // provide settings that indicate the compression type and optionally other related
        // settings.
        metadata.compression_settings().encode(writer, revision).await?;

        // Settings
        if let Some(settings) = &params.settings {
            if let Some(ignore) = server_settings {
                settings.as_ref().encode_with_ignore(writer, revision, ignore).await?;
            } else {
                settings.as_ref().encode(writer, revision).await?;
            }
        }

        writer.write_string("").await?; // end of settings

        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES {
            writer.write_string("").await?;
        }

        if revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET {
            //todo interserver secret
            writer.write_string("").await?;
        }

        writer.write_var_uint(params.stage as u64).await?;
        writer
            .write_u8(u8::from(!matches!(metadata.compression, CompressionMethod::None)))
            .await?;
        writer.write_string(params.query).await?;

        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS {
            if let Some(query_params) = params.params {
                // Convert query parameters to Settings format and encode them
                Settings::from(query_params).encode(writer, revision).await?;
            }
            writer.write_string("").await?; // end of params
        }

        writer
            .flush()
            .instrument(trace_span!(
                "flush_query",
                { ATT_QID } = %params.qid,
                { attribute::DB_QUERY_TEXT } = params.query,
            ))
            .await?;

        Ok(())
    }

    pub(super) async fn send_data<T: ClientFormat>(
        writer: &mut W,
        data: T::Data,
        qid: Qid,
        header: Option<&[(String, crate::Type)]>,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<()> {
        writer.write_var_uint(ClientPacketId::Data as u64).await?;
        writer.write_string("").await?; // Table name
        T::write(writer, data, qid, header, revision, metadata).await?;
        writer
            .flush()
            .instrument(trace_span!("flush_data", { ATT_QID } = %qid))
            .await
            .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "send_data"))?;
        Ok(())
    }

    pub(super) async fn send_addendum(
        writer: &mut W,
        revision: u64,
        server_hello: &ServerHello,
    ) -> Result<()> {
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY {
            writer.write_string("").await?;
        }

        // Send chunked protocol negotiation results
        if server_hello.revision_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
            let send_mode = server_hello.chunked_send.as_ref();
            let recv_mode = server_hello.chunked_recv.as_ref();
            trace!("Sending chunked protocol addendum: send={send_mode}, recv={recv_mode}");
            writer.write_string(send_mode).await?;
            writer.write_string(recv_mode).await?;
        }

        if server_hello.revision_version
            >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
        {
            writer.write_var_uint(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION).await?;
        }

        Ok(())
    }

    pub(super) async fn send_ping(writer: &mut W) -> Result<()> {
        writer.write_var_uint(ClientPacketId::Ping as u64).await?;
        writer.flush().instrument(trace_span!("flush_ping")).await?;
        Ok(())
    }

    // NOTE: Not used currently
    #[expect(unused)]
    pub(super) async fn send_cancel(writer: &mut W) -> Result<()> {
        writer.write_var_uint(ClientPacketId::Cancel as u64).await?;
        writer.flush().instrument(trace_span!("flush_cancel")).await?;
        Ok(())
    }
}

/// A wrapper around a [`ClickhouseWrite`] that logs all writes. Useful for testing.
struct LoggingWriter<W> {
    inner: W,
}

#[expect(unused)]
impl<W: ClickhouseWrite + Unpin> LoggingWriter<W> {
    async fn flush_with_timeout(&mut self) -> Result<()> {
        debug!("Attempting flush with timeout");
        if let Ok(result) = tokio::time::timeout(Duration::from_secs(5), self.inner.flush()).await {
            match result {
                Ok(()) => {
                    debug!("Flush completed successfully within timeout");
                    Ok(())
                }
                Err(e) => {
                    error!("Flush error within timeout: {:?}", e);
                    Err(e.into())
                }
            }
        } else {
            error!("Flush operation timed out");
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Flush timed out").into())
        }
    }
}

impl<W: AsyncWrite + ClickhouseWrite + Unpin> AsyncWrite for LoggingWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        debug!("poll_write called with {} bytes", buf.len());
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                debug!("poll_write wrote {} bytes", n);
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => {
                error!("poll_write error: {:?}", e);
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                debug!("poll_write pending");
                Poll::Pending
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        debug!("poll_flush called");
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                debug!("poll_flush completed");
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                error!("poll_flush error: {:?}", e);
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                debug!("poll_flush pending");
                Poll::Pending
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        debug!("poll_shutdown called");
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
