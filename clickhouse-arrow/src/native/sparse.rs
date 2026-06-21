use crate::io::ClickHouseRead;
use crate::{Error, Result};

/// End-of-granule marker bit in sparse offsets stream.
pub(crate) const END_OF_GRANULE_FLAG: u64 = 1 << 62;

/// Read sparse offsets for a full block column payload.
///
/// Each varint encodes a run of defaults; non-terminal groups are followed by one non-default
/// value. The terminal group is marked with [`END_OF_GRANULE_FLAG`].
pub(crate) async fn read_sparse_offsets<R: ClickHouseRead>(
    reader: &mut R,
    num_rows: usize,
) -> Result<Vec<usize>> {
    let mut offsets = Vec::new();
    let mut current_position = 0_u64;
    let total_rows = u64::try_from(num_rows)
        .map_err(|_| Error::Protocol("sparse row count out of range".to_string()))?;

    loop {
        let group = reader.read_var_uint().await?;
        let end_of_granule = (group & END_OF_GRANULE_FLAG) != 0;
        let defaults = group & !END_OF_GRANULE_FLAG;
        current_position = current_position
            .checked_add(defaults)
            .ok_or_else(|| Error::Protocol("sparse offsets overflow".to_string()))?;

        if end_of_granule {
            break;
        }

        if current_position >= total_rows {
            return Err(Error::Protocol(format!(
                "sparse offset points past row count: position={current_position} \
                 total={total_rows}"
            )));
        }
        offsets.push(usize::try_from(current_position).map_err(|_| {
            Error::Protocol("sparse offset cannot be represented as usize".to_string())
        })?);
        current_position = current_position
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("sparse offsets overflow".to_string()))?;
    }

    if current_position > total_rows {
        return Err(Error::Protocol(format!(
            "sparse offsets overshot row count: position={current_position} total={total_rows}"
        )));
    }

    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::io::{ClickHouseBytesWrite, ClickHouseRead};

    #[tokio::test]
    async fn test_read_sparse_offsets_roundtrip_shape() {
        // groups: 2 defaults -> value at 2, 1 default -> value at 4, trailing 3 defaults.
        let mut raw = Vec::new();
        raw.put_var_uint(2).unwrap();
        raw.put_var_uint(1).unwrap();
        raw.put_var_uint(END_OF_GRANULE_FLAG | 3).unwrap();

        let mut reader = Cursor::new(raw);
        let offsets = read_sparse_offsets(&mut reader, 8).await.unwrap();
        assert_eq!(offsets, vec![2, 4]);
        assert!(reader.read_var_uint().await.err().is_some());
    }

    #[tokio::test]
    async fn test_read_sparse_offsets_rejects_non_terminal_overshoot() {
        let mut raw = Vec::new();
        raw.put_var_uint(9).unwrap();

        let mut reader = Cursor::new(raw);
        let err = read_sparse_offsets(&mut reader, 8).await.unwrap_err();
        assert!(
            matches!(err, Error::Protocol(ref message) if message.contains("past row count")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_read_sparse_offsets_rejects_terminal_overshoot() {
        let mut raw = Vec::new();
        raw.put_var_uint(END_OF_GRANULE_FLAG | 9).unwrap();

        let mut reader = Cursor::new(raw);
        let err = read_sparse_offsets(&mut reader, 8).await.unwrap_err();
        assert!(
            matches!(err, Error::Protocol(ref message) if message.contains("overshot row count")),
            "{err:?}"
        );
    }
}
