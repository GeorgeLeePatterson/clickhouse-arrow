use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use super::protocol::{
    DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER, DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS,
};
use crate::io::ClickHouseWrite;
use crate::native::protocol::{
    DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH,
    DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS,
    DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME, DBMS_MIN_REVISION_WITH_OPENTELEMETRY,
    DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO, DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    DBMS_TCP_PROTOCOL_VERSION,
};
use crate::prelude::*;

#[repr(u8)]
#[derive(PartialEq, Clone, Copy, Debug)]
#[allow(unused, clippy::enum_variant_names)]
pub(crate) enum QueryKind {
    NoQuery,
    InitialQuery,
    SecondaryQuery,
}

#[derive(Debug)]
pub(crate) struct OpenTelemetry<'a> {
    trace_id:    Uuid,
    span_id:     u64,
    tracestate:  &'a str,
    trace_flags: u8,
}

#[derive(Debug)]
pub(crate) struct ClientInfo<'a> {
    pub kind:                        QueryKind,
    pub initial_user:                &'a str,
    pub initial_query_id:            &'a str,
    pub initial_address:             &'a str,
    // interface = TCP = 1
    pub os_user:                     &'a str,
    pub client_hostname:             &'a str,
    pub client_name:                 &'a str,
    pub client_version_major:        u64,
    pub client_version_minor:        u64,
    pub client_tcp_protocol_version: u64,
    // DBMS_MIN_PROTOCOL_VERSION_WITH_INITIAL_QUERY_START_TIME
    pub query_start_time:            u64,
    // if DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
    pub quota_key:                   &'a str,
    // if DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH
    pub distributed_depth:           u64,
    // if DBMS_MIN_REVISION_WITH_VERSION_PATCH
    pub client_version_patch:        u64,
    // if DBMS_MIN_REVISION_WITH_OPENTELEMETRY
    pub open_telemetry:              Option<OpenTelemetry<'a>>,
}

impl Default for ClientInfo<'_> {
    fn default() -> Self {
        ClientInfo {
            kind: QueryKind::InitialQuery,
            initial_user: "",
            initial_query_id: "",
            initial_address: "0.0.0.0:0",
            os_user: "",
            client_hostname: "localhost",
            client_name: "ClickHouseArrow",
            client_version_major: crate::constants::VERSION_MAJOR,
            client_version_minor: crate::constants::VERSION_MINOR,
            client_version_patch: crate::constants::VERSION_PATCH,
            client_tcp_protocol_version: DBMS_TCP_PROTOCOL_VERSION,
            #[expect(clippy::cast_possible_truncation)]
            query_start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(std::time::Duration::from_secs(0))
                .as_micros() as u64,
            quota_key: "",
            distributed_depth: 1,
            open_telemetry: None,
        }
    }
}

impl ClientInfo<'_> {
    pub(crate) async fn write<W: ClickHouseWrite>(&self, to: &mut W, revision: u64) -> Result<()> {
        to.write_u8(self.kind as u8).await?;
        if self.kind == QueryKind::NoQuery {
            return Ok(());
        }
        to.write_string(self.initial_user).await?;
        to.write_string(self.initial_query_id).await?;
        to.write_string(self.initial_address).await?;

        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME {
            to.write_u64_le(self.query_start_time).await?;
        }

        // interface = TCP = 1
        to.write_u8(1).await?;

        to.write_string(self.os_user).await?;
        to.write_string(self.client_hostname).await?;
        to.write_string(self.client_name).await?;

        to.write_var_uint(self.client_version_major).await?;
        to.write_var_uint(self.client_version_minor).await?;
        to.write_var_uint(self.client_tcp_protocol_version).await?;

        if revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            to.write_string(self.quota_key).await?;
        }
        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH {
            to.write_var_uint(self.distributed_depth).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            to.write_var_uint(self.client_version_patch).await?;
        }
        if revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
            if let Some(telemetry) = &self.open_telemetry {
                to.write_u8(1u8).await?;
                to.write_all(&telemetry.trace_id.as_bytes()[..]).await?;
                to.write_u64(telemetry.span_id).await?;
                to.write_string(telemetry.tracestate).await?;
                to.write_u8(telemetry.trace_flags).await?;
            } else {
                to.write_u8(0u8).await?;
            }
        }

        if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS {
            to.write_var_uint(0).await?; // collaborate_with_initiator
            to.write_var_uint(0).await?; // count_participating_replicas
            to.write_var_uint(0).await?; // number_of_current_replica
        }

        if revision >= DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS {
            to.write_var_uint(0).await?; // script_query_number
            to.write_var_uint(0).await?; // script_line_number
        }

        if revision >= DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER {
            // TODO: Support jwt
            to.write_u8(0).await?;
        }

        Ok(())
    }
}
