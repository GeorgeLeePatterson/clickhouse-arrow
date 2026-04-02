use crate::Result;
use crate::io::ClickHouseRead;

/// End-of-granule marker bit in sparse offsets stream.
pub(crate) const END_OF_GRANULE_FLAG: u64 = 1 << 62;

/// Read sparse offsets for a full block column payload.
///
/// Each varint encodes a run of defaults; non-terminal groups are followed by one non-default
/// value. The terminal group is marked with [`END_OF_GRANULE_FLAG`].
#[expect(clippy::cast_possible_truncation, reason = "Offsets are bounded by row count")]
pub(crate) async fn read_sparse_offsets<R: ClickHouseRead>(
    reader: &mut R,
    num_rows: usize,
) -> Result<Vec<usize>> {
    let mut offsets = Vec::new();
    let mut current_position = 0_u64;

    loop {
        let group = reader.read_var_uint().await?;
        let end_of_granule = (group & END_OF_GRANULE_FLAG) != 0;
        let defaults = group & !END_OF_GRANULE_FLAG;
        current_position = current_position.saturating_add(defaults);

        if end_of_granule {
            break;
        }

        if current_position < num_rows as u64 {
            offsets.push(current_position as usize);
        }
        current_position = current_position.saturating_add(1);
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
}
