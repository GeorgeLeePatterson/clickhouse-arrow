//! Per-column `SerializationInfo` kind, read from the native wire.
//!
//! See the dialect note at the top of [`crate::native::protocol`] for the
//! contract: kind 0 is fully supported, kind 1 is reconstructed to a dense
//! column by the arrow path, every other kind is rejected. This module owns
//! the wire-format decode for the kind byte (plus the kind 5 stack drain) so
//! both block readers — arrow and native — share one source of truth.

use tokio::io::AsyncReadExt;

use super::protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::{Error, Result};

/// Which `SerializationInfo` kind the server declared for a column.
///
/// CH writes one byte after the per-column `has_custom` flag (when the
/// protocol revision is >= [`DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION`]).
/// Bytes 0..=4 are predefined stacks; 5 is `COMBINATION` followed by a varint
/// count + that many u8 kinds (which `read_serialization_kind` drains so the
/// next column header stays aligned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SerializationKind {
    Default,
    Sparse,
    Detached,
    DetachedOverSparse,
    Replicated,
    Combination,
}

const KIND_DEFAULT: u8 = 0;
const KIND_SPARSE: u8 = 1;
const KIND_DETACHED: u8 = 2;
const KIND_DETACHED_OVER_SPARSE: u8 = 3;
const KIND_REPLICATED: u8 = 4;
const KIND_COMBINATION: u8 = 5;

/// Read the per-column `SerializationInfo` flag + kind stack.
///
/// Returns `Default` for protocols that pre-date the gate or when the
/// `has_custom` flag is zero. For `COMBINATION` (5), consumes the
/// varint-length kind list off the wire so the next column header stays
/// in frame, then returns `Combination` — dispatching is up to the
/// caller.
pub(crate) async fn read_serialization_kind<R: ClickHouseRead>(
    reader: &mut R,
    revision: u64,
) -> Result<SerializationKind> {
    if revision < DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
        return Ok(SerializationKind::Default);
    }
    let has_custom = reader.read_u8().await?;
    if has_custom == 0 {
        return Ok(SerializationKind::Default);
    }
    let kind = reader.read_u8().await?;
    Ok(match kind {
        KIND_DEFAULT => SerializationKind::Default,
        KIND_SPARSE => SerializationKind::Sparse,
        KIND_DETACHED => SerializationKind::Detached,
        KIND_DETACHED_OVER_SPARSE => SerializationKind::DetachedOverSparse,
        KIND_REPLICATED => SerializationKind::Replicated,
        KIND_COMBINATION => {
            #[allow(clippy::cast_possible_truncation)]
            let stack_len = reader.read_var_uint().await? as usize;
            for _ in 0..stack_len {
                let _ = reader.read_u8().await?;
            }
            SerializationKind::Combination
        }
        other => {
            return Err(Error::Protocol(format!("unknown SerializationInfo kind: {other}")));
        }
    })
}

/// Sync counterpart of [`read_serialization_kind`] for callers using
/// the bytes-buffer reader path. Mirrors the async logic exactly. The
/// sync block reader currently has no callers in the wider tree (every
/// path goes through the async reader), but we keep this symmetric so a
/// future sync caller stays kind-aware on day one rather than silently
/// mis-framing the wire.
#[allow(dead_code)]
pub(crate) fn read_serialization_kind_sync<R: ClickHouseBytesRead>(
    reader: &mut R,
    revision: u64,
) -> Result<SerializationKind> {
    if revision < DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
        return Ok(SerializationKind::Default);
    }
    let has_custom = reader.try_get_u8()?;
    if has_custom == 0 {
        return Ok(SerializationKind::Default);
    }
    let kind = reader.try_get_u8()?;
    Ok(match kind {
        KIND_DEFAULT => SerializationKind::Default,
        KIND_SPARSE => SerializationKind::Sparse,
        KIND_DETACHED => SerializationKind::Detached,
        KIND_DETACHED_OVER_SPARSE => SerializationKind::DetachedOverSparse,
        KIND_REPLICATED => SerializationKind::Replicated,
        KIND_COMBINATION => {
            #[allow(clippy::cast_possible_truncation)]
            let stack_len = reader.try_get_var_uint()? as usize;
            for _ in 0..stack_len {
                let _ = reader.try_get_u8()?;
            }
            SerializationKind::Combination
        }
        other => {
            return Err(Error::Protocol(format!("unknown SerializationInfo kind: {other}")));
        }
    })
}
