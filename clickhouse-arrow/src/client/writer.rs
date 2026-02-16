use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::connection::ClientMetadata;
use crate::Result;
use crate::io::ClickHouseWrite;
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
pub(crate) struct Writer<W: ClickHouseWrite> {
    _phantom: std::marker::PhantomData<W>,
}

impl<W: ClickHouseWrite> Writer<W> {
    pub(super) async fn send_hello(writer: &mut W, params: ClientHello) -> Result<()> {
        writer.write_var_uint(ClientPacketId::Hello as u64).await?;
        writer.write_string(format!("ClickHouseArrow Rust {}", env!("CARGO_PKG_VERSION"))).await?;
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
                // Encode query parameters directly (not as Settings)
                tracing::debug!("Sending {} query parameters", query_params.len());
                query_params.encode(writer, revision).await?;
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
        header: Option<&[(String, Type)]>,
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
    pub(super) async fn send_cancel(writer: &mut W) -> Result<()> {
        writer.write_var_uint(ClientPacketId::Cancel as u64).await?;
        writer.flush().instrument(trace_span!("flush_cancel")).await?;
        Ok(())
    }
}

/// A wrapper around a [`ClickHouseWrite`] that logs all writes. Useful for testing.
struct LoggingWriter<W> {
    inner: W,
}

impl<W: ClickHouseWrite + Unpin> LoggingWriter<W> {
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

impl<W: AsyncWrite + ClickHouseWrite + Unpin> AsyncWrite for LoggingWriter<W> {
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::{Buf, Bytes};
    use tokio::io::AsyncWriteExt;
    use uuid::Uuid;

    use super::*;
    use crate::formats::NativeFormat;
    use crate::io::ClickHouseBytesRead;
    use crate::native::block::Block;
    use crate::native::block_info::BlockInfo;
    use crate::native::protocol::{ClientPacketId, DBMS_TCP_PROTOCOL_VERSION};

    fn has_subslice(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|window| window == needle)
    }

    fn metadata(compression: CompressionMethod) -> ClientMetadata {
        ClientMetadata {
            client_id: 42,
            compression,
            arrow_options: super::super::ArrowOptions::default(),
        }
    }

    #[tokio::test]
    async fn send_hello_writes_expected_packet_layout() {
        let mut writer = Cursor::new(Vec::new());
        Writer::<Cursor<Vec<u8>>>::send_hello(
            &mut writer,
            ClientHello {
                default_database: "default".to_string(),
                username: "user".to_string(),
                password: "secret".to_string(),
            },
        )
        .await
        .unwrap();

        let mut buf = Bytes::from(writer.into_inner());
        assert_eq!(buf.try_get_var_uint().unwrap(), ClientPacketId::Hello as u64);
        let client_name = String::from_utf8(buf.try_get_string().unwrap().to_vec()).unwrap();
        assert!(client_name.starts_with("ClickHouseArrow Rust "));
        assert_eq!(
            buf.try_get_var_uint().unwrap(),
            crate::constants::VERSION_MAJOR
        );
        assert_eq!(
            buf.try_get_var_uint().unwrap(),
            crate::constants::VERSION_MINOR
        );
        assert_eq!(buf.try_get_var_uint().unwrap(), DBMS_TCP_PROTOCOL_VERSION);
        assert_eq!(
            String::from_utf8(buf.try_get_string().unwrap().to_vec()).unwrap(),
            "default"
        );
        assert_eq!(
            String::from_utf8(buf.try_get_string().unwrap().to_vec()).unwrap(),
            "user"
        );
        assert_eq!(
            String::from_utf8(buf.try_get_string().unwrap().to_vec()).unwrap(),
            "secret"
        );
        assert!(!buf.has_remaining());
    }

    #[tokio::test]
    async fn send_query_encodes_settings_and_params_for_newer_revisions() {
        let mut writer = Cursor::new(Vec::new());
        let qid = Qid::from(Uuid::from_u128(0x1234));
        let query_text = "SELECT {name:String}, {count:Int32}";
        let params = QueryParams(vec![
            ("name".to_string(), "alice".into()),
            ("count".to_string(), 5_i32.into()),
        ]);
        let query = Query {
            qid,
            info: ClientInfo::default(),
            settings: Some(Arc::new(Settings::from([("max_threads", 8_i32)]))),
            stage: QueryProcessingStage::Complete,
            query: query_text,
            params: Some(params),
        };
        let server_settings = Settings::from([("max_threads", 8_i32)]);

        Writer::<Cursor<Vec<u8>>>::send_query(
            &mut writer,
            query,
            Some(&server_settings),
            DBMS_TCP_PROTOCOL_VERSION,
            metadata(CompressionMethod::ZSTD),
        )
        .await
        .unwrap();

        let bytes = writer.into_inner();
        let mut buf = Bytes::from(bytes.clone());
        assert_eq!(buf.try_get_var_uint().unwrap(), ClientPacketId::Query as u64);

        let qid_hex = qid.to_string();
        assert!(has_subslice(&bytes, qid_hex.as_bytes()));
        assert!(has_subslice(&bytes, b"network_compression_method"));
        assert!(has_subslice(&bytes, b"network_zstd_compression_level"));
        assert!(has_subslice(&bytes, query_text.as_bytes()));
        assert!(has_subslice(&bytes, b"name"));
        assert!(has_subslice(&bytes, b"count"));
        assert!(has_subslice(&bytes, b"'alice'"));
        assert!(has_subslice(&bytes, b"'5'"));
    }

    #[tokio::test]
    async fn send_query_skips_params_for_older_revisions() {
        let mut writer = Cursor::new(Vec::new());
        let query = Query {
            qid: Qid::default(),
            info: ClientInfo::default(),
            settings: None,
            stage: QueryProcessingStage::Complete,
            query: "SELECT 1",
            params: Some(QueryParams::from([("name", "alice")])),
        };

        Writer::<Cursor<Vec<u8>>>::send_query(
            &mut writer,
            query,
            None,
            DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS - 1,
            metadata(CompressionMethod::None),
        )
        .await
        .unwrap();

        let bytes = writer.into_inner();
        assert!(has_subslice(&bytes, b"SELECT 1"));
        assert!(!has_subslice(&bytes, b"'alice'"));
    }

    #[tokio::test]
    async fn send_data_addendum_and_control_packets_encode_expected_markers() {
        let mut data_writer = Cursor::new(Vec::new());
        let block = Block {
            info: BlockInfo::default(),
            rows: 0,
            column_types: vec![],
            column_data: vec![],
        };
        let qid = Qid::from(Uuid::from_u128(0x7777));

        Writer::<Cursor<Vec<u8>>>::send_data::<NativeFormat>(
            &mut data_writer,
            block,
            qid,
            None,
            DBMS_TCP_PROTOCOL_VERSION,
            metadata(CompressionMethod::None),
        )
        .await
        .unwrap();

        let mut data_bytes = Bytes::from(data_writer.into_inner());
        assert_eq!(data_bytes.try_get_var_uint().unwrap(), ClientPacketId::Data as u64);

        let mut ping_writer = Cursor::new(Vec::new());
        Writer::<Cursor<Vec<u8>>>::send_ping(&mut ping_writer)
            .await
            .unwrap();
        let mut ping_bytes = Bytes::from(ping_writer.into_inner());
        assert_eq!(ping_bytes.try_get_var_uint().unwrap(), ClientPacketId::Ping as u64);

        let mut cancel_writer = Cursor::new(Vec::new());
        Writer::<Cursor<Vec<u8>>>::send_cancel(&mut cancel_writer)
            .await
            .unwrap();
        let mut cancel_bytes = Bytes::from(cancel_writer.into_inner());
        assert_eq!(
            cancel_bytes.try_get_var_uint().unwrap(),
            ClientPacketId::Cancel as u64
        );

        let mut addendum_writer = Cursor::new(Vec::new());
        let server_hello = ServerHello {
            revision_version: DBMS_TCP_PROTOCOL_VERSION,
            chunked_send: ChunkedProtocolMode::Chunked,
            chunked_recv: ChunkedProtocolMode::NotChunked,
            ..ServerHello::default()
        };
        Writer::<Cursor<Vec<u8>>>::send_addendum(
            &mut addendum_writer,
            DBMS_TCP_PROTOCOL_VERSION,
            &server_hello,
        )
        .await
        .unwrap();
        addendum_writer.flush().await.unwrap();

        let mut addendum = Bytes::from(addendum_writer.into_inner());
        assert_eq!(
            String::from_utf8(addendum.try_get_string().unwrap().to_vec()).unwrap(),
            ""
        );
        assert_eq!(
            String::from_utf8(addendum.try_get_string().unwrap().to_vec()).unwrap(),
            "chunked"
        );
        assert_eq!(
            String::from_utf8(addendum.try_get_string().unwrap().to_vec()).unwrap(),
            "notchunked"
        );
        assert_eq!(
            addendum.try_get_var_uint().unwrap(),
            DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
        );

        let mut old_writer = Cursor::new(Vec::new());
        let old_hello = ServerHello {
            revision_version: DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS - 1,
            ..ServerHello::default()
        };
        Writer::<Cursor<Vec<u8>>>::send_addendum(
            &mut old_writer,
            DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY - 1,
            &old_hello,
        )
        .await
        .unwrap();
        old_writer.flush().await.unwrap();
        assert!(old_writer.into_inner().is_empty());
    }

    #[tokio::test]
    async fn logging_writer_wraps_async_write_operations() {
        let mut writer = LoggingWriter {
            inner: Cursor::new(Vec::new()),
        };

        writer.flush_with_timeout().await.unwrap();
        writer.write_all(b"payload").await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
    }
}
