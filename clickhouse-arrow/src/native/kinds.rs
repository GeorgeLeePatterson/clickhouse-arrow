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

#[cfg(test)]
mod tests_async {
    use std::io::Cursor;

    use super::*;

    const GATE: u64 = DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;

    async fn read(bytes: &[u8], revision: u64) -> Result<SerializationKind> {
        let mut reader = Cursor::new(bytes.to_vec());
        read_serialization_kind(&mut reader, revision).await
    }

    #[tokio::test]
    async fn pre_gate_revision_is_default_and_reads_nothing() {
        // Below the gate, no byte is consumed — pass an empty buffer to prove it.
        assert_eq!(read(&[], GATE - 1).await.unwrap(), SerializationKind::Default);
    }

    #[tokio::test]
    async fn has_custom_zero_is_default() {
        assert_eq!(read(&[0], GATE).await.unwrap(), SerializationKind::Default);
    }

    #[tokio::test]
    async fn each_predefined_kind() {
        // has_custom=1, then the kind byte.
        assert_eq!(read(&[1, 0], GATE).await.unwrap(), SerializationKind::Default);
        assert_eq!(read(&[1, 1], GATE).await.unwrap(), SerializationKind::Sparse);
        assert_eq!(read(&[1, 2], GATE).await.unwrap(), SerializationKind::Detached);
        assert_eq!(read(&[1, 3], GATE).await.unwrap(), SerializationKind::DetachedOverSparse);
        assert_eq!(read(&[1, 4], GATE).await.unwrap(), SerializationKind::Replicated);
    }

    #[tokio::test]
    async fn combination_drains_stack_and_leaves_frame_aligned() {
        // has_custom=1, kind=5 (COMBINATION), varint stack_len=3, then 3 kind
        // bytes, then a sentinel that must remain unread.
        let mut reader = Cursor::new(vec![1, 5, 3, 0, 1, 2, 0xAB]);
        let kind = read_serialization_kind(&mut reader, GATE).await.unwrap();
        assert_eq!(kind, SerializationKind::Combination);
        // The next byte must be the sentinel — the 3-entry stack was consumed.
        assert_eq!(reader.read_u8().await.unwrap(), 0xAB);
    }

    #[tokio::test]
    async fn unknown_kind_is_protocol_error() {
        let err = read(&[1, 6], GATE).await.unwrap_err();
        assert!(
            matches!(err, Error::Protocol(msg) if msg.contains("unknown SerializationInfo kind"))
        );
    }
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

#[cfg(test)]
mod tests_sync {
    use super::*;

    const GATE: u64 = DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;

    // `ClickHouseBytesRead` is implemented for any `bytes::Buf`; `&[u8]` is one.
    fn read(mut bytes: &[u8], revision: u64) -> Result<SerializationKind> {
        read_serialization_kind_sync(&mut bytes, revision)
    }

    #[test]
    fn pre_gate_revision_is_default_and_reads_nothing() {
        assert_eq!(read(&[], GATE - 1).unwrap(), SerializationKind::Default);
    }

    #[test]
    fn has_custom_zero_is_default() {
        assert_eq!(read(&[0], GATE).unwrap(), SerializationKind::Default);
    }

    #[test]
    fn each_predefined_kind() {
        assert_eq!(read(&[1, 0], GATE).unwrap(), SerializationKind::Default);
        assert_eq!(read(&[1, 1], GATE).unwrap(), SerializationKind::Sparse);
        assert_eq!(read(&[1, 2], GATE).unwrap(), SerializationKind::Detached);
        assert_eq!(read(&[1, 3], GATE).unwrap(), SerializationKind::DetachedOverSparse);
        assert_eq!(read(&[1, 4], GATE).unwrap(), SerializationKind::Replicated);
    }

    #[test]
    fn combination_drains_stack_and_leaves_frame_aligned() {
        // has_custom=1, kind=5, stack_len=3, 3 kind bytes, then a sentinel.
        let mut buf: &[u8] = &[1, 5, 3, 0, 1, 2, 0xAB];
        let kind = read_serialization_kind_sync(&mut buf, GATE).unwrap();
        assert_eq!(kind, SerializationKind::Combination);
        // The cursor advanced past the 3-entry stack; the sentinel remains.
        assert_eq!(buf, &[0xAB]);
    }

    #[test]
    fn unknown_kind_is_protocol_error() {
        let err = read(&[1, 6], GATE).unwrap_err();
        assert!(
            matches!(err, Error::Protocol(msg) if msg.contains("unknown SerializationInfo kind"))
        );
    }
}
