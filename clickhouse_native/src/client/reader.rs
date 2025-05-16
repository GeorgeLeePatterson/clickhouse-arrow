use tokio::io::AsyncReadExt;

use super::connection::ConnectionMetadata;
use crate::formats::sealed::ClientFormatImpl;
use crate::io::ClickhouseRead;
use crate::native::block::Block;
use crate::native::progress::Progress;
use crate::native::protocol::{
    BlockStreamProfileInfo, DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT,
    DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS,
    DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO, DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2,
    DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME, DBMS_MIN_REVISION_WITH_SERVER_LOGS,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE, DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_TCP_PROTOCOL_VERSION, LogData, MAX_STRING_SIZE, ProfileEvent, ServerData, ServerException,
    ServerHello, ServerPacket, ServerPacketId, TableColumns, TableStatus, TablesStatusResponse,
};
use crate::prelude::*;
use crate::{ClickhouseNativeError, FxIndexMap, Result};

#[derive(Debug, Clone, Copy)]
pub(super) struct Reader<R: ClickhouseRead> {
    _phantom: std::marker::PhantomData<R>,
}

impl<R: ClickhouseRead + 'static> Reader<R> {
    pub(super) async fn read_hello(reader: &mut R, cid: u16) -> Result<ServerHello> {
        trace!({ ATT_CID } = cid, "Receiving server hello packet");

        let server_name = reader.read_utf8_string().await?;
        let major_version = reader.read_var_uint().await?;
        let minor_version = reader.read_var_uint().await?;

        let revision_version = reader.read_var_uint().await?;
        let revision_version = std::cmp::min(revision_version, DBMS_TCP_PROTOCOL_VERSION);

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

        if revision_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES {
            let rules_size = reader.read_var_uint().await?;
            for _ in 0..rules_size {
                drop(reader.read_utf8_string().await?);
            }
        }

        if revision_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 {
            let _ = reader.read_u64_le().await?;
        }

        trace!(
            server_name,
            version = format!("{major_version}.{minor_version}.{patch_version}"),
            revision = revision_version,
            { ATT_CID } = cid,
            "Connected to server",
        );

        Ok(ServerHello {
            server_name,
            major_version,
            minor_version,
            patch_version,
            revision_version,
            timezone,
            display_name,
        })
    }

    pub(super) async fn read_exception(reader: &mut R) -> Result<ServerException> {
        let code = reader.read_i32_le().await?;
        let name = reader.read_utf8_string().await?;
        let message = String::from_utf8_lossy(reader.read_string().await?.as_ref()).to_string();
        let stack_trace = reader.read_utf8_string().await?;
        let has_nested = reader.read_u8().await? != 0;

        Ok(ServerException { code, name, message, stack_trace, has_nested })
    }

    pub(super) async fn read_log_data(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<Vec<LogData>> {
        let Some(data) =
            Self::read_data::<NativeFormat>(reader, metadata.disable_compression()).await?
        else {
            return Ok(vec![]);
        };
        Ok(LogData::from_block(data.block)
            .inspect_err(|error| error!(?error, "Log data parsing failed"))
            .unwrap_or_default())
    }

    pub(super) async fn read_progress(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<Progress> {
        let read_rows = reader.read_var_uint().await?;
        let read_bytes = reader.read_var_uint().await?;
        let new_total_rows_to_read = if metadata.revision >= DBMS_MIN_REVISION_WITH_SERVER_LOGS {
            reader.read_var_uint().await?
        } else {
            0
        };
        let new_written_rows = if metadata.revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
            Some(reader.read_var_uint().await?)
        } else {
            None
        };
        let new_written_bytes = if metadata.revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
            Some(reader.read_var_uint().await?)
        } else {
            None
        };
        let elapsed_ns =
            if metadata.revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS {
                Some(reader.read_var_uint().await?)
            } else {
                None
            };

        Ok(Progress {
            read_rows,
            read_bytes,
            new_total_rows_to_read,
            new_written_rows,
            new_written_bytes,
            elapsed_ns,
        })
    }

    pub(super) async fn read_profile_info(reader: &mut R) -> Result<BlockStreamProfileInfo> {
        let rows = reader.read_var_uint().await?;
        let blocks = reader.read_var_uint().await?;
        let bytes = reader.read_var_uint().await?;
        let applied_limit = reader.read_u8().await? != 0;
        let rows_before_limit = reader.read_var_uint().await?;
        let calculated_rows_before_limit = reader.read_u8().await? != 0;
        Ok(BlockStreamProfileInfo {
            rows,
            blocks,
            bytes,
            applied_limit,
            rows_before_limit,
            calculated_rows_before_limit,
        })
    }

    pub(super) async fn read_profile_events(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<Vec<ProfileEvent>> {
        let revision = metadata.revision;
        if revision < DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT {
            return Err(ClickhouseNativeError::ProtocolError(format!(
                "unexpected profile events for revision {revision}"
            )));
        }
        let Some(data) =
            Self::read_data::<NativeFormat>(reader, metadata.disable_compression()).await?
        else {
            return Ok(vec![]);
        };
        Ok(ProfileEvent::from_block(data.block)
            .inspect_err(|error| error!(?error, "Profile event parsing failed"))
            .unwrap_or_default())
    }

    pub(super) async fn read_table_status_response(reader: &mut R) -> Result<TablesStatusResponse> {
        let mut response = TablesStatusResponse { database_tables: FxIndexMap::default() };
        let size = reader.read_var_uint().await?;

        #[expect(clippy::cast_possible_truncation)]
        if size as usize > MAX_STRING_SIZE {
            return Err(ClickhouseNativeError::ProtocolError(format!(
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

    pub(super) async fn read_task_request(reader: &mut R) -> Result<Option<String>> {
        Ok(reader
            .read_utf8_string()
            .await
            .inspect_err(|error| error!(?error, "Error reading task request"))
            .ok())
    }

    pub(super) async fn read_part_uuids(reader: &mut R) -> Result<Vec<uuid::Uuid>> {
        #[expect(clippy::cast_possible_truncation)]
        let len = reader.read_var_uint().await? as usize;
        if len > MAX_STRING_SIZE {
            return Err(ClickhouseNativeError::ProtocolError(format!(
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

    pub(super) async fn read_table_columns(reader: &mut R) -> Result<TableColumns> {
        Ok(TableColumns {
            name:        reader.read_utf8_string().await?,
            description: reader.read_utf8_string().await?,
        })
    }

    /// Read a data packet from the server and deserialize into [`crate::Block`]
    async fn read_block(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<Option<ServerData<Block>>> {
        drop(reader.read_utf8_string().await?);
        let Some(block) = NativeFormat::read(reader, metadata).await.inspect_err(|error| {
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
        metadata: ConnectionMetadata,
    ) -> Result<Option<ServerData<T::Data>>> {
        drop(reader.read_utf8_string().await?);
        let Some(block) = T::read(reader, metadata).await.inspect_err(|error| {
            error!(?error, { ATT_CID } = metadata.client_id, "Data read fail");
        })?
        else {
            return Ok(None);
        };
        Ok(Some(ServerData { block }))
    }

    /// Read the packet id from the reader
    pub(super) async fn read_packet(reader: &mut R) -> Result<ServerPacketId> {
        ServerPacketId::from_u64(reader.read_var_uint().await?)
            .inspect(|id| trace!({ ATT_PID } = id.as_ref(), "Reading packet ID"))
            .inspect_err(|error| error!(?error, "Failed to read packet ID"))
    }

    /// Receive header packet (empty native block)
    #[instrument(
        level = "trace",
        skip_all,
        fields(clickhouse.client.id = metadata.client_id, clickhouse.packet.id)
    )]
    pub(super) async fn receive_header<T: ClientFormat>(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<ServerPacket<T::Data>> {
        let packet = ServerPacketId::from_u64(reader.read_var_uint().await?)
            .inspect_err(|error| error!(?error, "Failed to read packet ID"))?;
        let _ = Span::current().record(ATT_PID, packet.as_ref());
        trace!({ ATT_PID } = packet.as_ref(), "Read packet ID (header)");

        // Read the packet ID from the server
        return match packet {
            ServerPacketId::Data => Self::read_block(reader, metadata)
                .await?
                .ok_or(ClickhouseNativeError::ProtocolError(
                    "Expected valid block for header".into(),
                ))
                .map(ServerPacket::Header),
            // NOTE: For DDL queries and some other cases, the server will not send a header but
            // will send a progress packet or table columns instead.
            ServerPacketId::Progress => {
                Self::read_progress(reader, metadata).await.map(ServerPacket::Progress)
            }
            ServerPacketId::TableColumns => {
                Self::read_table_columns(reader).await.map(ServerPacket::TableColumns)
            }
            ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
            // Errors
            ServerPacketId::Exception => {
                Self::read_exception(reader).await.map(ServerPacket::Exception)
            }
            packet => Err(ClickhouseNativeError::ProtocolError(format!(
                "expected header packet, got: {}",
                packet.as_ref()
            ))),
        };
    }

    /// Receive any packet from the server
    #[instrument(
        level = "trace",
        skip_all,
        fields(clickhouse.client.id = metadata.client_id, clickhouse.packet.id)
    )]
    pub(super) async fn receive_packet<T: ClientFormat>(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<ServerPacket<T::Data>> {
        let packet = ServerPacketId::from_u64(reader.read_var_uint().await?)
            .inspect_err(|error| error!(?error, "Failed to read packet ID"))?;
        let _ = Span::current().record(ATT_PID, packet.as_ref());
        trace!({ ATT_PID } = packet.as_ref(), "Read packet ID");

        // Otherwise process packet as usual
        match packet {
            ServerPacketId::Hello => {
                Self::read_hello(reader, metadata.client_id).await.map(ServerPacket::Hello)
            }
            ServerPacketId::Pong => Ok(ServerPacket::Pong),
            ServerPacketId::Data => Ok(Self::read_data::<T>(reader, metadata)
                .await?
                .map_or(ServerPacket::Ignore, ServerPacket::Data)),
            ServerPacketId::Exception => {
                Self::read_exception(reader).await.map(ServerPacket::Exception)
            }
            ServerPacketId::Progress => {
                Self::read_progress(reader, metadata).await.map(ServerPacket::Progress)
            }
            ServerPacketId::EndOfStream => Ok(ServerPacket::EndOfStream),
            ServerPacketId::ProfileInfo => {
                Self::read_profile_info(reader).await.map(ServerPacket::ProfileInfo)
            }
            ServerPacketId::Totals => Ok(Self::read_data::<T>(reader, metadata)
                .await?
                .map_or(ServerPacket::Ignore, ServerPacket::Totals)),
            ServerPacketId::Extremes => Ok(Self::read_data::<T>(reader, metadata)
                .await?
                .map_or(ServerPacket::Ignore, ServerPacket::Extremes)),
            ServerPacketId::TablesStatusResponse => Self::read_table_status_response(reader)
                .await
                .map(ServerPacket::TablesStatusResponse),
            ServerPacketId::Log => {
                Self::read_log_data(reader, metadata).await.map(ServerPacket::Log)
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
            ServerPacketId::ProfileEvents => {
                Self::read_profile_events(reader, metadata).await.map(ServerPacket::ProfileEvents)
            }
            ServerPacketId::ServerTreeReadTaskRequest => {
                Ok(ServerPacket::ServerTreeReadTaskRequest)
            }
            ServerPacketId::Ignore => Err(ClickhouseNativeError::ProtocolError(
                "Unknown packet received from server".into(),
            )),
        }
    }
}
