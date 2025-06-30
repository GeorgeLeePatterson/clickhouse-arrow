//! ## Tracing (telemetry) utilities and constants.
//!
//! `clickhouse-arrow` uses the `tracing` crate to emit spans and events for debugging and
//! monitoring. To enable tracing, add a subscriber in your application:
//!
//! ```rust
//! use tracing_subscriber;
//!
//! tracing_subscriber::fmt()
//!     .with_env_filter("clickhouse_arrow=debug")
//!     .init();
//! // Use clickhouse_arrow
//! ```
use std::num::NonZeroU64;

pub use opentelemetry_semantic_conventions::*;
use tracing::Span;

/// Commonly used attribute names
pub const ATT_CID: &str = "clickhouse.client.id";
pub const ATT_CON: &str = "clickhouse.connection.id";
pub const ATT_CREQ: &str = "clickhouse.client.request";
pub const ATT_QID: &str = "clickhouse.query.id";
pub const ATT_PCOUNT: &str = "clickhouse.packet.count";
pub const ATT_PID: &str = "clickhouse.packet.id";
pub const ATT_MSGTYPE: &str = "clickhouse.message.type";
pub const ATT_FIELD_NAME: &str = "clickhouse.field.name";
pub const ATT_FIELD_TYPE: &str = "clickhouse.field.type";

/// A helper to link spans to various actions, namely connection. Sometimes, clients are spawned on
/// separate tasks. This provides a simple way to link traces if a link is preferred in some
/// situations over other types of instrumentation.
#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub struct TraceContext(Option<NonZeroU64>);

impl TraceContext {
    pub(super) fn link(&self, span: &Span) -> &Self {
        let _ = span.follows_from(self.get_id());
        self
    }

    pub(super) fn get_id(self) -> Option<tracing::span::Id> {
        self.0.map(tracing::span::Id::from_non_zero_u64)
    }
}

impl From<NonZeroU64> for TraceContext {
    fn from(id: NonZeroU64) -> Self { Self(Some(id)) }
}

impl From<Option<NonZeroU64>> for TraceContext {
    fn from(id: Option<NonZeroU64>) -> Self { Self(id) }
}
