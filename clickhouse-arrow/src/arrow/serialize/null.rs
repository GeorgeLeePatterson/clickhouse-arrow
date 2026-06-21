/// Serialization logic for nullability bitmaps in `ClickHouse`’s native format.
///
/// This module provides a function to serialize nullability bitmaps for Arrow arrays, used by
/// the `ClickHouseArrowSerializer` implementation in `types.rs` for nullable types. It writes
/// a bitmap where `1` represents a null value and `0` represents a non-null value, as expected
/// by `ClickHouse`.
///
/// The `write_nullability` function handles arrays with or without a null buffer, writing the
/// appropriate bitmap before serializing the inner values.
///
/// # Examples
/// ```rust,ignore
/// use arrow::array::Int32Array;
/// use clickhouse_arrow::types::null::write_nullability;
/// use std::sync::Arc;
/// use tokio::io::AsyncWriteExt;
///
/// let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
/// let mut buffer = Vec::new();
/// write_nullability(&mut buffer, &array).await.unwrap();
/// ```
use arrow::array::ArrayRef;
use tokio::io::AsyncWriteExt;

use crate::arrow::simd::expand_null_bitmap;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Result, Type};

const STACK_NULL_MASK_CAPACITY: usize = 1024;
const STACK_NULL_MASK_ZEROES: [u8; STACK_NULL_MASK_CAPACITY] = [0; STACK_NULL_MASK_CAPACITY];

/// Serializes the nullability bitmap for an Arrow array to `ClickHouse`’s native format.
///
/// Writes a bitmap where `1` indicates a null value and `0` indicates a non-null value. If the
/// array has a null buffer, it constructs the bitmap based on valid indices. If no null buffer
/// exists, it writes a zeroed bitmap (all `0`).
///
/// # Arguments
/// - `writer`: The async writer to serialize to (e.g., a TCP stream).
/// - `array`: The Arrow array containing the nullability information.
///
/// # Returns
/// A `Result` indicating success or a `Error` if writing fails.
///
/// # Errors
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn serialize_nulls_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    array: &ArrayRef,
) -> Result<()> {
    if matches!(type_hint.strip_null(), Type::Array(_) | Type::Map(_, _)) {
        return Ok(());
    }

    let len = array.len();
    if len == 0 {
        return Ok(());
    }

    if let Some(null_buffer) = array.nulls() {
        let bitmap_bytes = null_buffer.validity();
        if len <= STACK_NULL_MASK_CAPACITY {
            let mut null_mask = [0_u8; STACK_NULL_MASK_CAPACITY];
            expand_null_bitmap(bitmap_bytes, &mut null_mask[..len], len);
            writer.write_all(&null_mask[..len]).await?;
        } else {
            let mut null_mask = vec![0_u8; len];
            expand_null_bitmap(bitmap_bytes, &mut null_mask, len);
            writer.write_all(&null_mask).await?;
        }
    } else {
        let mut remaining = len;
        while remaining > 0 {
            let chunk_len = remaining.min(STACK_NULL_MASK_CAPACITY);
            writer.write_all(&STACK_NULL_MASK_ZEROES[..chunk_len]).await?;
            remaining -= chunk_len;
        }
    }

    Ok(())
}
pub(super) fn serialize_nulls<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    array: &ArrayRef,
) {
    // ClickHouse: Arrays cannot be nullable
    if matches!(type_hint.strip_null(), Type::Array(_) | Type::Map(_, _)) {
        return;
    }

    let len = array.len();
    if len == 0 {
        return;
    }

    if let Some(null_buffer) = array.nulls() {
        let bitmap_bytes = null_buffer.validity();
        if len <= STACK_NULL_MASK_CAPACITY {
            let mut null_mask = [0_u8; STACK_NULL_MASK_CAPACITY];
            expand_null_bitmap(bitmap_bytes, &mut null_mask[..len], len);
            writer.put_slice(&null_mask[..len]);
        } else {
            let mut null_mask = vec![0_u8; len];
            expand_null_bitmap(bitmap_bytes, &mut null_mask, len);
            writer.put_slice(&null_mask);
        }
    } else {
        let mut remaining = len;
        while remaining > 0 {
            let chunk_len = remaining.min(STACK_NULL_MASK_CAPACITY);
            writer.put_slice(&STACK_NULL_MASK_ZEROES[..chunk_len]);
            remaining -= chunk_len;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, ListArray, StringArray};
    use arrow::datatypes::Int32Type;

    use super::*;

    // Helper to create a mock writer
    type MockWriter = Vec<u8>;

    #[tokio::test]
    async fn test_write_nullability_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::Int32, &mut writer, &array).await.unwrap();
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }

    #[tokio::test]
    async fn test_write_nullability_without_nulls() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::Int32, &mut writer, &array).await.unwrap();
        assert_eq!(writer, vec![0, 0, 0]); // All 0 for non-null
    }

    #[tokio::test]
    async fn test_write_nullability_empty() {
        let array = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::Int32, &mut writer, &array).await.unwrap();
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_write_nullability_nullable_string() {
        let array = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::String, &mut writer, &array).await.unwrap();
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }

    // ClickHouse doesn't support nullable arrays
    #[tokio::test]
    async fn test_write_nullability_nullable_array() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
        let array = Arc::new(list_array) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(
            &Type::Nullable(Type::Array(Type::Int32.into()).into()),
            &mut writer,
            &array,
        )
        .await
        .unwrap();
        assert!(writer.is_empty());
    }

    #[test]
    fn test_write_nullability_sync_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(&Type::Int32, &mut writer, &array);
        assert_eq!(writer, vec![0, 1, 0]);
    }

    #[test]
    fn test_write_nullability_sync_without_nulls() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(&Type::Int32, &mut writer, &array);
        assert_eq!(writer, vec![0, 0, 0]);
    }

    #[test]
    fn test_write_nullability_sync_array_and_map_are_skipped() {
        let int_array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(&Type::Array(Box::new(Type::Int32)), &mut writer, &int_array);
        assert!(writer.is_empty());

        let mut writer = MockWriter::new();
        serialize_nulls(
            &Type::Map(Box::new(Type::Int32), Box::new(Type::String)),
            &mut writer,
            &int_array,
        );
        assert!(writer.is_empty());
    }
}
