/// Deserialization logic for `ClickHouse` `Nullable` types into Arrow arrays.
///
/// This module provides a function to deserialize ClickHouse’s native format for `Nullable`
/// types into Arrow arrays, handling nullability for any inner type (e.g., `Nullable(Int32)`,
/// `Nullable(String)`, `Nullable(Array(T))`). It is used by the `ClickhouseArrowDeserializer`
/// implementation in `deserialize.rs` to process nullable columns.
use arrow::array::ArrayRef;
use tokio::io::AsyncReadExt;

use super::ClickhouseArrowDeserializer;
use crate::formats::DeserializerState;
use crate::io::ClickhouseRead;
use crate::{ClickhouseNativeError, Result, Type};

/// Deserializes a `ClickHouse` `Nullable` type into an Arrow array.
///
/// Reads a null mask (`1`=null, `0`=non-null) of `rows` bytes from the input stream, then
/// delegates to the inner type’s deserializer with the mask to mark null values. Supports any
/// inner type, including primitives (e.g., `Nullable(Int32)`), strings (e.g.,
/// `Nullable(String)`), and nested types (e.g., `Nullable(Array(T))`). Ensures the mask length
/// matches the expected number of rows before proceeding.
///
/// # Arguments
/// - `inner`: The `ClickHouse` type of the inner elements (e.g., `Int32`, `String`, `Array(T)`).
/// - `reader`: The async reader providing the `ClickHouse` native format data.
/// - `rows`: The number of rows to deserialize.
/// - `state`: A mutable `DeserializerState` for deserialization context.
///
/// # Returns
/// A `Result` containing the deserialized `ArrayRef` with nulls marked according to the mask, or
/// a `ClickhouseNativeError` if deserialization fails.
///
/// # Errors
/// - Returns `Io` if reading the null mask fails (e.g., EOF).
/// - Returns `DeserializeError` if the mask length doesn’t match `rows`.
/// - Returns `ArrowDeserialize` if the inner type deserialization fails.
///
/// # Example
/// ```rust,ignore
/// use arrow::array::{ArrayRef, Int32Array};
/// use clickhouse_native::types::{Type, DeserializerState};
/// use std::io::Cursor;
///
/// let data = vec![
///     // Null mask: [0, 1, 0] (0=non-null, 1=null)
///     0, 1, 0,
///     // Values: [1, 0, 3] (0 for null)
///     1, 0, 0, 0, // 1
///     0, 0, 0, 0, // null
///     3, 0, 0, 0, // 3
/// ];
/// let mut reader = Cursor::new(data);
/// let mut state = DeserializerState::default();
/// let array = crate::arrow::deserialize::null::deserialize(
///     &Type::Int32,
///     &mut reader,
///     3,
///     &mut state,
/// )
/// .await
/// .unwrap();
/// let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(int32_array, &Int32Array::from(vec![Some(1), None, Some(3)]));
/// ```
pub(crate) async fn deserialize<R: ClickhouseRead>(
    inner: &Type,
    reader: &mut R,
    rows: usize,
    state: &mut DeserializerState,
) -> Result<ArrayRef> {
    let mut mask = vec![0u8; rows];
    let _ = reader.read_exact(&mut mask).await?;
    if mask.len() != rows {
        return Err(ClickhouseNativeError::DeserializeError(format!(
            "Mask length {} does not match rows {rows}",
            mask.len()
        )));
    }
    inner.deserialize(reader, rows, &mask, state).await
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{Array, Int32Array, ListArray, StringArray};

    use super::*;
    use crate::native::types::Type;

    /// Tests deserialization of `Nullable(Int32)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int32() {
        let inner_type = Type::Int32;
        let rows = 3;
        let input = vec![
            // Null mask: [0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, // Values: [1, 0, 3] (0 for null)
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int32)");
        let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array, &Int32Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Nullable(String)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_string() {
        let inner_type = Type::String;
        let rows = 3;
        let input = vec![
            // Null mask: [0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, // Values: ["a", "", "c"] (empty string for null)
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &mut state)
            .await
            .expect("Failed to deserialize Nullable(String)");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array, &StringArray::from(vec![Some("a"), None, Some("c")]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Nullable(Array(Int32))` with null arrays.
    #[tokio::test]
    async fn test_deserialize_nullable_array_int32() {
        let inner_type = Type::Array(Box::new(Type::Int32));
        let rows = 3;
        let input = vec![
            // Null mask: [0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Array(Int32))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(
            list_array.nulls().unwrap().iter().collect::<Vec<bool>>(),
            vec![true, false, true] // 0=non-null, 1=null
        );
    }

    /// Tests deserialization of `Nullable(Int32)` with zero rows.
    #[tokio::test]
    async fn test_deserialize_nullable_int32_zero_rows() {
        let inner_type = Type::Int32;
        let rows = 0;
        let input = vec![]; // No data for zero rows
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int32) with zero rows");
        let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array.len(), 0);
        assert_eq!(array, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(array.nulls(), None);
    }

    #[tokio::test]
    async fn test_null_mask_length() {
        assert!(
            deserialize(
                &Type::String,
                &mut Cursor::new(vec![0_u8; 50]),
                100,
                &mut DeserializerState::default()
            )
            .await
            .is_err()
        );
    }
}
