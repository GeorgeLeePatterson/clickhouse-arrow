use std::str::FromStr;

use tokio::io::AsyncReadExt;

use super::connection::ClientMetadata;
use crate::formats::DeserializerState;
use crate::formats::sealed::ClientFormatImpl;
use crate::io::ClickHouseRead;
use crate::native::block::Block;
use crate::native::progress::Progress;
use crate::native::protocol::{
    ChunkedProtocolMode, DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT,
    DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS,
    DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2,
    DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION,
    DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION, DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME,
    DBMS_MIN_REVISION_WITH_SERVER_LOGS, DBMS_MIN_REVISION_WITH_SERVER_SETTINGS,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE, DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL, LogData, MAX_STRING_SIZE,
    ProfileEvent, ProfileInfo, ServerData, ServerException, ServerHello, ServerPacket,
    ServerPacketId, TableColumns, TableStatus, TablesStatusResponse,
};
use crate::prelude::*;
use crate::{Error, FxIndexMap, Result};

#[derive(Debug, Clone, Copy)]
pub(super) struct Reader<R: ClickHouseRead> {
    _phantom: std::marker::PhantomData<R>,
}

impl<R: ClickHouseRead + 'static> Reader<R> {
    pub(super) async fn receive_hello(
        reader: &mut R,
        client_revision: u64,
        chunked_modes: (ChunkedProtocolMode, ChunkedProtocolMode),
        cid: u16,
    ) -> Result<ServerHello> {
        let packet = ServerPacketId::from_u64(reader.read_var_uint().await?)
            .inspect(|id| trace!({ ATT_PID } = id.as_ref(), "Reading packet ID"))
            .inspect_err(|error| error!(?error, "Failed to read packet ID"))?;
        match packet {
            ServerPacketId::Hello => Self::read_hello(reader, client_revision, chunked_modes, cid)
                .await
                .inspect_err(|error| {
                    error!(?error, { ATT_CID } = cid, "Failed to receive hello");
                }),
            ServerPacketId::Exception => Err(Self::read_exception(reader).await?.emit().into()),
            packet => {
                Err(Error::Protocol(format!("Unexpected packet {packet:?}, expected server hello")))
            }
        }
    }

    /// Receive header packet (empty native block)
    pub(super) async fn receive_header<T: ClientFormat>(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<ServerPacket<T::Data>> {
        let packet = ServerPacketId::from_u64(reader.read_var_uint().await?)
            .inspect_err(|error| error!(?error, "Failed to read packet ID"))?;
        trace!({ ATT_PID } = packet.as_ref(), "Read packet ID (header)");
        match packet {
            ServerPacketId::Data => Self::read_block(reader, revision, metadata)
                .await?
                .ok_or(Error::Protocol("Expected valid block for header".into()))
                .map(ServerPacket::Header),
            // NOTE: For DDL queries and some other cases, the server will not send a header but
            // will send a progress packet or table columns instead.
            ServerPacketId::Progress => {
                Self::read_progress(reader, revision).await.map(ServerPacket::Progress)
            }
            ServerPacketId::TableColumns => {
                Self::read_table_columns(reader).await.map(ServerPacket::TableColumns)
            }
            ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
            // When query parameters are used, ClickHouse may send ProfileEvents before the header
            ServerPacketId::ProfileEvents => Self::read_profile_events(reader, revision, metadata)
                .await
                .map(ServerPacket::ProfileEvents),
            // Errors
            ServerPacketId::Exception => {
                Self::read_exception(reader).await.map(ServerPacket::Exception)
            }
            ServerPacketId::Hello => {
                Err(Error::Protocol("Unexpected hello received from server".to_string()))
            }
            packet => {
                Err(Error::Protocol(format!("expected header packet, got: {}", packet.as_ref())))
            }
        }
    }

    /// Receive any packet from the server
    pub(super) async fn receive_packet<T: ClientFormat>(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
        state: &mut DeserializerState<T::Deser>,
    ) -> Result<ServerPacket<T::Data>> {
        let packet = ServerPacketId::from_u64(reader.read_var_uint().await?)
            .inspect_err(|error| error!(?error, "Failed to read packet ID"))?;
        trace!({ ATT_PID } = packet.as_ref(), "Read packet ID");
        match packet {
            ServerPacketId::Pong => Ok(ServerPacket::Pong),
            ServerPacketId::Data => Ok(Self::read_data::<T>(reader, revision, metadata, state)
                .await?
                .map_or(ServerPacket::Ignore(ServerPacketId::Data), ServerPacket::Data)),
            ServerPacketId::Exception => {
                Self::read_exception(reader).await.map(ServerPacket::Exception)
            }
            ServerPacketId::Progress => {
                Self::read_progress(reader, revision).await.map(ServerPacket::Progress)
            }
            ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
            ServerPacketId::ProfileInfo => {
                Self::read_profile_info(reader, revision).await.map(ServerPacket::ProfileInfo)
            }
            ServerPacketId::Totals => Ok(Self::read_data::<T>(reader, revision, metadata, state)
                .await?
                .map_or(ServerPacket::Ignore(ServerPacketId::Totals), ServerPacket::Totals)),
            ServerPacketId::Extremes => Ok(Self::read_data::<T>(reader, revision, metadata, state)
                .await?
                .map_or(ServerPacket::Ignore(ServerPacketId::Extremes), ServerPacket::Extremes)),
            ServerPacketId::TablesStatusResponse => Self::read_table_status_response(reader)
                .await
                .map(ServerPacket::TablesStatusResponse),
            ServerPacketId::Log => {
                Self::read_log_data(reader, revision, metadata).await.map(ServerPacket::Log)
            }
            ServerPacketId::TableColumns => {
                Self::read_table_columns(reader).await.map(ServerPacket::TableColumns)
            }
            ServerPacketId::PartUUIDs => {
                Self::read_part_uuids(reader).await.map(ServerPacket::PartUUIDs)
            }
            ServerPacketId::ReadTaskRequest => {
                Self::read_task_request(reader).await.map(ServerPacket::ReadTaskRequest)
            }
            ServerPacketId::ProfileEvents => Self::read_profile_events(reader, revision, metadata)
                .await
                .map(ServerPacket::ProfileEvents),
            // TODO: These currently are not correct. They are placeholders but must be deserialized
            ServerPacketId::MergeTreeAllRangesAnnouncement => {
                Ok(ServerPacket::MergeTreeAllRangesAnnouncement)
            }
            ServerPacketId::MergeTreeReadTaskRequest => Ok(ServerPacket::MergeTreeReadTaskRequest),
            ServerPacketId::TimezoneUpdate => Ok(ServerPacket::TimezoneUpdate),
            ServerPacketId::SSHChallenge => Ok(ServerPacket::SSHChallenge),
            ServerPacketId::Hello => {
                Err(Error::Protocol("Uexpected hello received from server".to_string()))
            }
        }
    }

    pub(super) async fn read_exception(reader: &mut R) -> Result<ServerException> {
        let code = reader.read_i32_le().await?;
        let name = reader.read_utf8_string().await?;
        let message = String::from_utf8_lossy(reader.read_string().await?.as_ref()).to_string();
        let stack_trace = reader.read_utf8_string().await?;
        let has_nested = reader.read_u8().await? != 0;

        Ok(ServerException { code, name, message, stack_trace, has_nested })
    }

    async fn read_hello(
        reader: &mut R,
        client_revision: u64,
        // (send, recv)
        chunked_modes: (ChunkedProtocolMode, ChunkedProtocolMode),
        cid: u16,
    ) -> Result<ServerHello> {
        trace!({ ATT_CID } = cid, "Receiving server hello packet");

        let server_name = reader.read_utf8_string().await?;
        let major_version = reader.read_var_uint().await?;
        let minor_version = reader.read_var_uint().await?;

        let server_revision = reader.read_var_uint().await?;
        let revision_version = std::cmp::min(server_revision, client_revision);

        if revision_version >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL {
            let _ = reader.read_var_uint().await?;
        }

        let timezone = if revision_version >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            Some(reader.read_utf8_string().await?)
        } else {
            None
        };

        let display_name = if revision_version >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
            Some(reader.read_utf8_string().await?)
        } else {
            None
        };
        let patch_version = if revision_version >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            reader.read_var_uint().await?
        } else {
            revision_version
        };

        let (chunked_send, chunked_recv) =
            if revision_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS {
                // proto_send_chunked_srv
                let srv_chunked_send = ChunkedProtocolMode::from_str(
                    String::from_utf8_lossy(&reader.read_string().await?).as_ref(),
                )
                .ok()
                .unwrap_or_default();
                // proto_recv_chunked_srv
                let srv_chunked_recv = ChunkedProtocolMode::from_str(
                    String::from_utf8_lossy(&reader.read_string().await?).as_ref(),
                )
                .ok()
                .unwrap_or_default();

                let cl_chunked_send = chunked_modes.0;
                let cl_chunked_recv = chunked_modes.1;

                (
                    ChunkedProtocolMode::negotiate(srv_chunked_send, cl_chunked_send, "send")?,
                    ChunkedProtocolMode::negotiate(srv_chunked_recv, cl_chunked_recv, "recv")?,
                )
            } else {
                (ChunkedProtocolMode::default(), ChunkedProtocolMode::default())
            };

        tracing::trace!(
            recv = chunked_recv.as_ref(),
            send = chunked_send.as_ref(),
            "Negotiated chunking"
        );

        if revision_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES {
            let rules_size = reader.read_var_uint().await?;
            for _ in 0..rules_size {
                drop(reader.read_utf8_string().await?); // original_pattern
                drop(reader.read_utf8_string().await?); // exception_message
            }
        }

        if revision_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
            let _ = reader.read_u64_le().await?;
        }

        // Read server settings if supported
        let settings = if revision_version >= DBMS_MIN_REVISION_WITH_SERVER_SETTINGS {
            Some(Settings::decode(reader).await?)
        } else {
            None
        };

        let _query_plan_version =
            if revision_version >= DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION {
                Some(reader.read_var_uint().await?)
            } else {
                None
            };

        let _server_cluster_function_porotocl_version =
            if revision_version >= DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL {
                Some(reader.read_var_uint().await?)
            } else {
                None
            };

        trace!(
            server_name,
            version = format!("{major_version}.{minor_version}.{patch_version}"),
            revision = revision_version,
            chunked_send = chunked_send.as_ref(),
            chunked_recv = chunked_recv.as_ref(),
            { ATT_CID } = cid,
            "Connected to server",
        );

        Ok(ServerHello {
            server_name,
            version: (major_version, minor_version, patch_version),
            revision_version,
            timezone,
            display_name,
            settings,
            chunked_send,
            chunked_recv,
        })
    }

    async fn read_log_data(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<Vec<LogData>> {
        let mut state = DeserializerState::default();
        let Some(data) = Self::read_data::<NativeFormat>(
            reader,
            revision,
            metadata.disable_compression(),
            &mut state,
        )
        .await?
        else {
            return Ok(vec![]);
        };
        Ok(LogData::from_block(data.block)
            .inspect_err(|error| error!(?error, "Log data parsing failed"))
            .unwrap_or_default())
    }

    async fn read_progress(reader: &mut R, revision: u64) -> Result<Progress> {
        let read_rows = reader.read_var_uint().await?;
        let read_bytes = reader.read_var_uint().await?;
        let total_rows_to_read = if revision >= DBMS_MIN_REVISION_WITH_SERVER_LOGS {
            reader.read_var_uint().await?
        } else {
            0
        };
        let total_bytes_to_read =
            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS {
                Some(reader.read_var_uint().await?)
            } else {
                None
            };

        let written = if revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
            Some((reader.read_var_uint().await?, reader.read_var_uint().await?))
        } else {
            None
        };
        let elapsed_ns = if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS
        {
            Some(reader.read_var_uint().await?)
        } else {
            None
        };

        Ok(Progress {
            read_rows,
            read_bytes,
            total_rows_to_read,
            total_bytes_to_read,
            written_rows: written.map(|w| w.0),
            written_bytes: written.map(|w| w.1),
            elapsed_ns,
        })
    }

    async fn read_profile_info(reader: &mut R, revision: u64) -> Result<ProfileInfo> {
        let rows = reader.read_var_uint().await?;
        let blocks = reader.read_var_uint().await?;
        let bytes = reader.read_var_uint().await?;
        let applied_limit = reader.read_u8().await? != 0;
        let rows_before_limit = reader.read_var_uint().await?;
        // Obsolete according to ClickHouse
        let calculated_rows_before_limit = reader.read_u8().await? != 0;

        let (applied_aggregation, rows_before_aggregation) =
            if revision >= DBMS_MIN_REVISION_WITH_ROWS_BEFORE_AGGREGATION {
                (reader.read_u8().await? != 0, reader.read_var_uint().await?)
            } else {
                (false, 0)
            };

        Ok(ProfileInfo {
            rows,
            blocks,
            bytes,
            applied_limit,
            rows_before_limit,
            calculated_rows_before_limit,
            applied_aggregation,
            rows_before_aggregation,
        })
    }

    async fn read_profile_events(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<Vec<ProfileEvent>> {
        if revision < DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT {
            return Err(Error::Protocol(format!(
                "unexpected profile events for revision {revision}"
            )));
        }
        let mut state = DeserializerState::default();
        let Some(data) = Self::read_data::<NativeFormat>(
            reader,
            revision,
            metadata.disable_compression(),
            &mut state,
        )
        .await?
        else {
            return Ok(vec![]);
        };
        Ok(ProfileEvent::from_block(data.block)
            .inspect_err(|error| error!(?error, "Profile event parsing failed"))
            .unwrap_or_default())
    }

    async fn read_table_status_response(reader: &mut R) -> Result<TablesStatusResponse> {
        let mut response = TablesStatusResponse { database_tables: FxIndexMap::default() };
        let size = reader.read_var_uint().await?;

        #[expect(clippy::cast_possible_truncation)]
        if size as usize > MAX_STRING_SIZE {
            return Err(Error::Protocol(format!(
                "table status response size too large. {size} > {MAX_STRING_SIZE}"
            )));
        }
        for _ in 0..size {
            let database_name = reader.read_utf8_string().await?;
            let table_name = reader.read_utf8_string().await?;
            let is_replicated = reader.read_u8().await? != 0;
            #[expect(clippy::cast_possible_truncation)]
            let absolute_delay =
                if is_replicated { reader.read_var_uint().await? as u32 } else { 0 };
            let _ = response
                .database_tables
                .entry(database_name)
                .or_default()
                .insert(table_name, TableStatus { is_replicated, absolute_delay });
        }
        Ok(response)
    }

    async fn read_task_request(reader: &mut R) -> Result<Option<String>> {
        Ok(reader
            .read_utf8_string()
            .await
            .inspect_err(|error| error!(?error, "Error reading task request"))
            .ok())
    }

    async fn read_part_uuids(reader: &mut R) -> Result<Vec<uuid::Uuid>> {
        #[expect(clippy::cast_possible_truncation)]
        let len = reader.read_var_uint().await? as usize;
        if len > MAX_STRING_SIZE {
            return Err(Error::Protocol(format!(
                "PartUUIDs response size too large. {len} > {MAX_STRING_SIZE}"
            )));
        }
        let mut out = Vec::with_capacity(len);
        let mut bytes = [0u8; 16];
        for _ in 0..len {
            let _ = reader.read_exact(&mut bytes[..]).await?;
            out.push(uuid::Uuid::from_bytes(bytes));
        }
        Ok(out)
    }

    async fn read_table_columns(reader: &mut R) -> Result<TableColumns> {
        Ok(TableColumns {
            name:        reader.read_utf8_string().await?,
            description: reader.read_utf8_string().await?,
        })
    }

    /// Read a data packet from the server and deserialize into [`crate::Block`]
    async fn read_block(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<Option<ServerData<Block>>> {
        drop(reader.read_string().await?);
        let mut state = DeserializerState::default();
        let Some(block) = NativeFormat::read(reader, revision, metadata.clone(), &mut state)
            .await
            .inspect_err(|error| {
                error!(?error, { ATT_CID } = metadata.client_id, "Block read fail");
            })?
        else {
            return Ok(None);
        };
        Ok(Some(ServerData { block }))
    }

    /// Read a data packet from the server and deserialize into [`ClientFormat`]
    async fn read_data<T: ClientFormat>(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
        state: &mut DeserializerState<T::Deser>,
    ) -> Result<Option<ServerData<T::Data>>> {
        drop(reader.read_string().await?);
        let Some(block) =
            T::read(reader, revision, metadata.clone(), state).await.inspect_err(|error| {
                error!(?error, { ATT_CID } = metadata.client_id, "Data read fail");
            })?
        else {
            return Ok(None);
        };
        Ok(Some(ServerData { block }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncWriteExt;
    use uuid::Uuid;

    use super::*;
    use crate::formats::NativeFormat;
    use crate::io::ClickHouseWrite;
    use crate::native::protocol::{CompressionMethod, DBMS_TCP_PROTOCOL_VERSION, ServerPacketId};

    fn metadata() -> ClientMetadata {
        ClientMetadata {
            client_id:     7,
            compression:   CompressionMethod::None,
            arrow_options: super::super::ArrowOptions::default(),
        }
    }

    #[tokio::test]
    async fn receive_hello_reads_basic_server_hello() {
        let mut buf = Cursor::new(Vec::new());
        buf.write_var_uint(ServerPacketId::Hello as u64).await.unwrap();
        buf.write_string("test-server").await.unwrap();
        buf.write_var_uint(24).await.unwrap();
        buf.write_var_uint(8).await.unwrap();
        buf.write_var_uint(0).await.unwrap();

        let mut reader = Cursor::new(buf.into_inner());
        let hello = Reader::<Cursor<Vec<u8>>>::receive_hello(
            &mut reader,
            DBMS_TCP_PROTOCOL_VERSION,
            (ChunkedProtocolMode::ChunkedOptional, ChunkedProtocolMode::ChunkedOptional),
            1,
        )
        .await
        .unwrap();

        assert_eq!(hello.server_name, "test-server");
        assert_eq!(hello.version, (24, 8, 0));
        assert_eq!(hello.revision_version, 0);
        assert_eq!(hello.chunked_send, ChunkedProtocolMode::ChunkedOptional);
        assert_eq!(hello.chunked_recv, ChunkedProtocolMode::ChunkedOptional);
    }

    #[tokio::test]
    async fn receive_hello_propagates_server_exception() {
        let mut buf = Cursor::new(Vec::new());
        buf.write_var_uint(ServerPacketId::Exception as u64).await.unwrap();
        buf.write_i32_le(43).await.unwrap();
        buf.write_string("DB::Exception").await.unwrap();
        buf.write_string("boom").await.unwrap();
        buf.write_string("stack").await.unwrap();
        buf.write_u8(0).await.unwrap();

        let mut reader = Cursor::new(buf.into_inner());
        let err = Reader::<Cursor<Vec<u8>>>::receive_hello(
            &mut reader,
            DBMS_TCP_PROTOCOL_VERSION,
            (ChunkedProtocolMode::ChunkedOptional, ChunkedProtocolMode::ChunkedOptional),
            99,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn receive_header_handles_progress_table_columns_and_eof() {
        let mut progress_buf = Cursor::new(Vec::new());
        progress_buf.write_var_uint(ServerPacketId::Progress as u64).await.unwrap();
        progress_buf.write_var_uint(10).await.unwrap();
        progress_buf.write_var_uint(20).await.unwrap();

        let mut reader = Cursor::new(progress_buf.into_inner());
        let packet =
            Reader::<Cursor<Vec<u8>>>::receive_header::<NativeFormat>(&mut reader, 0, metadata())
                .await
                .unwrap();
        match packet {
            ServerPacket::Progress(progress) => {
                assert_eq!(progress.read_rows, 10);
                assert_eq!(progress.read_bytes, 20);
                assert_eq!(progress.total_rows_to_read, 0);
            }
            other => panic!("unexpected packet: {other:?}"),
        }

        let mut table_columns_buf = Cursor::new(Vec::new());
        table_columns_buf.write_var_uint(ServerPacketId::TableColumns as u64).await.unwrap();
        table_columns_buf.write_string("table").await.unwrap();
        table_columns_buf.write_string("id UInt64").await.unwrap();
        let mut reader = Cursor::new(table_columns_buf.into_inner());
        let packet =
            Reader::<Cursor<Vec<u8>>>::receive_header::<NativeFormat>(&mut reader, 0, metadata())
                .await
                .unwrap();
        match packet {
            ServerPacket::TableColumns(columns) => {
                assert_eq!(columns.name, "table");
                assert_eq!(columns.description, "id UInt64");
            }
            other => panic!("unexpected packet: {other:?}"),
        }

        let mut eos_buf = Cursor::new(Vec::new());
        eos_buf.write_var_uint(ServerPacketId::EndOfStream as u64).await.unwrap();
        let mut reader = Cursor::new(eos_buf.into_inner());
        let packet =
            Reader::<Cursor<Vec<u8>>>::receive_header::<NativeFormat>(&mut reader, 0, metadata())
                .await
                .unwrap();
        assert!(matches!(packet, ServerPacket::EndOfStream));
    }

    #[tokio::test]
    async fn receive_packet_handles_control_packets_and_errors() {
        let mut state = DeserializerState::default();

        let mut pong_buf = Cursor::new(Vec::new());
        pong_buf.write_var_uint(ServerPacketId::Pong as u64).await.unwrap();
        let mut reader = Cursor::new(pong_buf.into_inner());
        let packet = Reader::<Cursor<Vec<u8>>>::receive_packet::<NativeFormat>(
            &mut reader,
            0,
            metadata(),
            &mut state,
        )
        .await
        .unwrap();
        assert!(matches!(packet, ServerPacket::Pong));

        let mut task_buf = Cursor::new(Vec::new());
        task_buf.write_var_uint(ServerPacketId::ReadTaskRequest as u64).await.unwrap();
        task_buf.write_string("task-1").await.unwrap();
        let mut reader = Cursor::new(task_buf.into_inner());
        let packet = Reader::<Cursor<Vec<u8>>>::receive_packet::<NativeFormat>(
            &mut reader,
            0,
            metadata(),
            &mut state,
        )
        .await
        .unwrap();
        assert!(matches!(packet, ServerPacket::ReadTaskRequest(Some(task)) if task == "task-1"));

        let uuid = Uuid::from_u128(0x1122_3344_5566_7788_9900_aabb_ccdd_eeff);
        let mut part_buf = Cursor::new(Vec::new());
        part_buf.write_var_uint(ServerPacketId::PartUUIDs as u64).await.unwrap();
        part_buf.write_var_uint(1).await.unwrap();
        part_buf.write_all(uuid.as_bytes()).await.unwrap();
        let mut reader = Cursor::new(part_buf.into_inner());
        let packet = Reader::<Cursor<Vec<u8>>>::receive_packet::<NativeFormat>(
            &mut reader,
            0,
            metadata(),
            &mut state,
        )
        .await
        .unwrap();
        assert!(matches!(packet, ServerPacket::PartUUIDs(parts) if parts == vec![uuid]));

        let mut hello_buf = Cursor::new(Vec::new());
        hello_buf.write_var_uint(ServerPacketId::Hello as u64).await.unwrap();
        let mut reader = Cursor::new(hello_buf.into_inner());
        let err = Reader::<Cursor<Vec<u8>>>::receive_packet::<NativeFormat>(
            &mut reader,
            0,
            metadata(),
            &mut state,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("Uexpected hello"));
    }

    #[tokio::test]
    async fn read_progress_and_profile_info_cover_optional_fields() {
        let mut progress_high = Cursor::new(Vec::new());
        progress_high.write_var_uint(11).await.unwrap();
        progress_high.write_var_uint(22).await.unwrap();
        progress_high.write_var_uint(33).await.unwrap();
        progress_high.write_var_uint(44).await.unwrap();
        progress_high.write_var_uint(55).await.unwrap();
        progress_high.write_var_uint(66).await.unwrap();
        progress_high.write_var_uint(77).await.unwrap();
        let mut reader = Cursor::new(progress_high.into_inner());
        let progress =
            Reader::<Cursor<Vec<u8>>>::read_progress(&mut reader, DBMS_TCP_PROTOCOL_VERSION)
                .await
                .unwrap();
        assert_eq!(progress.read_rows, 11);
        assert_eq!(progress.total_rows_to_read, 33);
        assert_eq!(progress.total_bytes_to_read, Some(44));
        assert_eq!(progress.written_rows, Some(55));
        assert_eq!(progress.written_bytes, Some(66));
        assert_eq!(progress.elapsed_ns, Some(77));

        let mut profile_old = Cursor::new(Vec::new());
        profile_old.write_var_uint(1).await.unwrap();
        profile_old.write_var_uint(2).await.unwrap();
        profile_old.write_var_uint(3).await.unwrap();
        profile_old.write_u8(1).await.unwrap();
        profile_old.write_var_uint(4).await.unwrap();
        profile_old.write_u8(0).await.unwrap();
        let mut reader = Cursor::new(profile_old.into_inner());
        let info = Reader::<Cursor<Vec<u8>>>::read_profile_info(&mut reader, 0).await.unwrap();
        assert_eq!(info.rows, 1);
        assert!(!info.applied_aggregation);
        assert_eq!(info.rows_before_aggregation, 0);

        let mut profile_new = Cursor::new(Vec::new());
        profile_new.write_var_uint(10).await.unwrap();
        profile_new.write_var_uint(11).await.unwrap();
        profile_new.write_var_uint(12).await.unwrap();
        profile_new.write_u8(1).await.unwrap();
        profile_new.write_var_uint(13).await.unwrap();
        profile_new.write_u8(1).await.unwrap();
        profile_new.write_u8(1).await.unwrap();
        profile_new.write_var_uint(14).await.unwrap();
        let mut reader = Cursor::new(profile_new.into_inner());
        let info =
            Reader::<Cursor<Vec<u8>>>::read_profile_info(&mut reader, DBMS_TCP_PROTOCOL_VERSION)
                .await
                .unwrap();
        assert!(info.applied_aggregation);
        assert_eq!(info.rows_before_aggregation, 14);
    }

    #[tokio::test]
    async fn helper_readers_handle_valid_and_invalid_payloads() {
        let mut status_buf = Cursor::new(Vec::new());
        status_buf.write_var_uint(1).await.unwrap();
        status_buf.write_string("db").await.unwrap();
        status_buf.write_string("tbl").await.unwrap();
        status_buf.write_u8(1).await.unwrap();
        status_buf.write_var_uint(9).await.unwrap();
        let mut reader = Cursor::new(status_buf.into_inner());
        let statuses =
            Reader::<Cursor<Vec<u8>>>::read_table_status_response(&mut reader).await.unwrap();
        let table_status =
            statuses.database_tables.get("db").and_then(|tables| tables.get("tbl")).unwrap();
        assert!(table_status.is_replicated);
        assert_eq!(table_status.absolute_delay, 9);

        let mut oversized_status = Cursor::new(Vec::new());
        oversized_status.write_var_uint((MAX_STRING_SIZE as u64) + 1).await.unwrap();
        let mut reader = Cursor::new(oversized_status.into_inner());
        let err =
            Reader::<Cursor<Vec<u8>>>::read_table_status_response(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("size too large"));

        let mut valid_task = Cursor::new(Vec::new());
        valid_task.write_string("work").await.unwrap();
        let mut reader = Cursor::new(valid_task.into_inner());
        assert_eq!(
            Reader::<Cursor<Vec<u8>>>::read_task_request(&mut reader).await.unwrap(),
            Some("work".to_string())
        );

        let mut invalid_task = Cursor::new(Vec::new());
        invalid_task.write_var_uint(1).await.unwrap();
        invalid_task.write_all(&[0xFF]).await.unwrap();
        let mut reader = Cursor::new(invalid_task.into_inner());
        assert!(Reader::<Cursor<Vec<u8>>>::read_task_request(&mut reader).await.unwrap().is_none());

        let uuid = Uuid::from_u128(0xaaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa);
        let mut part_buf = Cursor::new(Vec::new());
        part_buf.write_var_uint(1).await.unwrap();
        part_buf.write_all(uuid.as_bytes()).await.unwrap();
        let mut reader = Cursor::new(part_buf.into_inner());
        let parts = Reader::<Cursor<Vec<u8>>>::read_part_uuids(&mut reader).await.unwrap();
        assert_eq!(parts, vec![uuid]);

        let mut oversized_parts = Cursor::new(Vec::new());
        oversized_parts.write_var_uint((MAX_STRING_SIZE as u64) + 1).await.unwrap();
        let mut reader = Cursor::new(oversized_parts.into_inner());
        let err = Reader::<Cursor<Vec<u8>>>::read_part_uuids(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("size too large"));

        let mut columns = Cursor::new(Vec::new());
        columns.write_string("my_table").await.unwrap();
        columns.write_string("x UInt32").await.unwrap();
        let mut reader = Cursor::new(columns.into_inner());
        let cols = Reader::<Cursor<Vec<u8>>>::read_table_columns(&mut reader).await.unwrap();
        assert_eq!(cols.name, "my_table");
        assert_eq!(cols.description, "x UInt32");
    }

    #[tokio::test]
    async fn read_exception_parses_binary_message_payload() {
        let mut buf = Cursor::new(Vec::new());
        buf.write_i32_le(123).await.unwrap();
        buf.write_string("Name").await.unwrap();
        buf.write_var_uint(3).await.unwrap();
        buf.write_all(&[0x66, 0x6F, 0x80]).await.unwrap(); // "fo" + invalid UTF-8 tail
        buf.write_string("trace").await.unwrap();
        buf.write_u8(1).await.unwrap();

        let mut reader = Cursor::new(buf.into_inner());
        let exception = Reader::<Cursor<Vec<u8>>>::read_exception(&mut reader).await.unwrap();
        assert_eq!(exception.code, 123);
        assert_eq!(exception.name, "Name");
        assert!(exception.message.starts_with("fo"));
        assert!(exception.has_nested);
    }
}
