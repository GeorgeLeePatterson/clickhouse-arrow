/// Serialization logic for nullability bitmaps in ClickHouse’s native format.
///
/// This module provides a function to serialize nullability bitmaps for Arrow arrays, used by
/// the `ClickhouseArrowSerializer` implementation in `types.rs` for nullable types. It writes
/// a bitmap where `1` represents a null value and `0` represents a non-null value, as expected
/// by `ClickHouse`.
///
/// The `write_nullability` function handles arrays with or without a null buffer, writing the
/// appropriate bitmap before serializing the inner values.
///
/// # Examples
/// ```
/// use arrow::array::Int32Array;
/// use clickhouse_native::types::null::write_nullability;
/// use std::sync::Arc;
/// use tokio::io::AsyncWriteExt;
///
/// #[tokio::main]
/// async fn main() {
///     let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
///     let mut buffer = Vec::new();
///     write_nullability(&mut buffer, &array).await.unwrap();
/// }
/// ```
use arrow::array::ArrayRef;

use crate::Result;
use crate::formats::SerializerState;
use crate::io::ClickhouseWrite;

/// Serializes the nullability bitmap for an Arrow array to ClickHouse’s native format.
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
/// A `Result` indicating success or a `ClickhouseNativeError` if writing fails.
///
/// # Errors
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn write_nullability<W: ClickhouseWrite>(
    writer: &mut W,
    array: &ArrayRef,
    _state: &mut SerializerState,
) -> Result<()> {
    // Write null bitmap
    if let Some(null_buffer) = array.nulls() {
        let mut null_mask = vec![1_u8; array.len()]; // Initialize with all NULLs (1 in Clickhouse)
        for valid_idx in null_buffer.valid_indices() {
            null_mask[valid_idx] = 0; // Set to 0 for non-NULL values
        }
        writer.write_all(&null_mask).await?;
    } else {
        let nulls = vec![0_u8; array.len()];
        writer.write_all(&nulls).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};

    use super::*;

    // Helper to create a mock writer
    type MockWriter = Vec<u8>;

    #[tokio::test]
    async fn test_write_nullability_with_nulls() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut writer = MockWriter::new();
        write_nullability(&mut writer, &array, &mut state).await.unwrap();
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }

    #[tokio::test]
    async fn test_write_nullability_without_nulls() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        write_nullability(&mut writer, &array, &mut state).await.unwrap();
        assert_eq!(writer, vec![0, 0, 0]); // All 0 for non-null
    }

    #[tokio::test]
    async fn test_write_nullability_empty() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let mut writer = MockWriter::new();
        write_nullability(&mut writer, &array, &mut state).await.unwrap();
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_write_nullability_nullable_string() {
        let mut state = SerializerState::default();
        let array = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let mut writer = MockWriter::new();
        write_nullability(&mut writer, &array, &mut state).await.unwrap();
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }
}
