/// Deserialization logic for `ClickHouse` `Array` types into Arrow `ListArray`.
///
/// This module provides a function to deserialize ClickHouse’s native format for `Array` types
/// into an Arrow `ListArray`, which represents variable-length lists of inner values. It is
/// used by the `ClickhouseArrowDeserializer` implementation in `deserialize.rs` to handle
/// array data, including `Array(T)` and `Nullable(Array(T))` types.
///
/// The `deserialize` function reads offsets and inner values from the input stream,
/// constructing a `ListArray` with the specified inner type. It handles nullability for the
/// outer array via the provided null mask and delegates to the inner type’s deserializer for
/// value processing.
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use tokio::io::AsyncReadExt;

use super::ClickhouseArrowDeserializer;
use crate::arrow::types::LIST_ITEM_FIELD_NAME;
use crate::formats::DeserializerState;
use crate::io::ClickhouseRead;
use crate::{ClickhouseNativeError, Result, Type};

/// Deserializes a `ClickHouse` `Array` type into an Arrow `ListArray`.
///
/// Reads offsets (skipping the first `0`, as in serialization) and inner values from the input
/// stream, constructing a `ListArray` with the specified inner type. Applies the outer null mask
/// to set the array’s nullability, supporting `Nullable(Array(T))`. Delegates to the inner type’s
/// deserializer for value processing, handling nested types like `Array(Nullable(T))`.
///
/// # Arguments
/// - `inner`: The `ClickHouse` type of the array’s inner elements (e.g., `Int32`,
///   `Nullable(Int32)`).
/// - `reader`: The async reader providing the `ClickHouse` native format data.
/// - `rows`: The number of lists to deserialize.
/// - `nulls`: A slice indicating null values for the outer array (`1` for null, `0` for non-null).
/// - `state`: A mutable `DeserializerState` for deserialization context.
///
/// # Returns
/// A `Result` containing the deserialized `ListArray` as an `ArrayRef` or a
/// `ClickhouseNativeError` if deserialization fails.
///
/// # Errors
/// - Returns `Io` if reading from the reader fails (e.g., EOF).
/// - Returns `DeserializeError` if the deserialized array length doesn’t match `rows`.
/// - Returns `ArrowDeserialize` if the inner type deserialization fails.
///
/// # Example
/// ```
/// use arrow::array::{ArrayRef, Int32Array, ListArray};
/// use clickhouse_native::types::{Type, ClickhouseArrowDeserializer, DeserializerState};
/// use std::io::Cursor;
///
/// #[tokio::test]
/// async fn test_deserialize_list_int32() {
///     let inner_type = Type::Int32;
///     let rows = 3;
///     let nulls = vec![];
///     let input = vec![
///         // Offsets: [2, 3, 5] (skipping first 0)
///         2, 0, 0, 0, 0, 0, 0, 0, // 2
///         3, 0, 0, 0, 0, 0, 0, 0, // 3
///         5, 0, 0, 0, 0, 0, 0, 0, // 5
///         // Values: [1, 2, 3, 4, 5]
///         1, 0, 0, 0, // 1
///         2, 0, 0, 0, // 2
///         3, 0, 0, 0, // 3
///         4, 0, 0, 0, // 4
///         5, 0, 0, 0, // 5
///     ];
///     let mut reader = Cursor::new(input);
///     let mut state = DeserializerState::default();
///
///     let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
///         .await
///         .expect("Failed to deserialize List(Int32)");
///     let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
///     let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();
///
///     assert_eq!(list_array.len(), 3);
///     assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
///     assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 3, 5]);
///     assert_eq!(list_array.nulls(), None);
/// }
/// ```
#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::cast_sign_loss)]
pub(crate) async fn deserialize<R: ClickhouseRead>(
    inner_type: &Type,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    state: &mut DeserializerState,
) -> Result<ArrayRef> {
    // Read outer offsets
    let mut offsets = Vec::with_capacity(rows);
    offsets.push(0); // Initial offset
    for _ in 0..rows {
        offsets.push(reader.read_u64_le().await? as i32);
    }

    let total_values = *offsets.last().unwrap_or(&0) as usize;

    // Recursively deserialize the inner array
    let inner_array = inner_type.deserialize(reader, total_values, &[], state).await?;

    // The null mask provides the null buffer for THIS list
    let null_buffer = if nulls.is_empty() {
        None
    } else {
        Some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()))
    };

    // Construct the ListArray directly
    let field = Arc::new(Field::new(
        LIST_ITEM_FIELD_NAME,
        inner_array.data_type().clone(),
        inner_type.strip_low_cardinality().is_nullable(),
    ));
    let list_array =
        ListArray::new(field, OffsetBuffer::new(offsets.into()), inner_array, null_buffer);

    // Verify length matches expected rows
    if list_array.len() != rows {
        return Err(ClickhouseNativeError::DeserializeError(format!(
            "ListArray length {} does not match expected rows {}",
            list_array.len(),
            rows
        )));
    }

    Ok(Arc::new(list_array))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::*;
    use arrow::datatypes::*;
    use chrono_tz::Tz;

    use super::*;
    use crate::native::types::Type;

    #[tokio::test]
    async fn test_deserialize_list_int32() {
        let inner_type = Type::Int32;
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
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

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize List(Int32)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    #[tokio::test]
    async fn test_deserialize_nullable_list_int32() {
        let inner_type = Type::Int32;
        let rows = 3;
        let nulls = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
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

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize nullable List(Int32)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![
            true, false, true
        ]);
    }

    #[tokio::test]
    async fn test_deserialize_list_nullable_int32() {
        let inner_type = Type::Nullable(Box::new(Type::Int32));
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0, // Inner values: [1, 0, 3, 0, 5] (0 for nulls)
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
            0, 0, 0, 0, // null
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize List(Nullable(Int32))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    #[tokio::test]
    async fn test_deserialize_list_zero_rows() {
        let inner_type = Type::Int32;
        let rows = 0;
        let nulls = vec![];
        let input = vec![]; // Initial offset
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize List(Int32) with zero rows");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 0);
        assert_eq!(values, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(String)` with non-zero rows.
    #[tokio::test]
    async fn test_deserialize_list_string() {
        let inner_type = Type::String;
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(String)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Nullable(String))` with nullable inner values.
    #[tokio::test]
    async fn test_deserialize_list_nullable_string() {
        let inner_type = Type::Nullable(Box::new(Type::String));
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0, // Inner values: ["a", "", "c", "", "e"] (empty string for nulls)
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
            0,    // null (empty string)
            1, b'e', // "e"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Nullable(String))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &StringArray::from(vec![Some("a"), None, Some("c"), None, Some("e")]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Array(Int32))` with nested arrays.
    #[tokio::test]
    async fn test_deserialize_nested_list_int32() {
        let inner_type = Type::Array(Box::new(Type::Int32));
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Outer offsets: [2, 3] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            // Inner offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Array(Int32))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner_list_array = list_array.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = inner_list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 2);
        assert_eq!(inner_list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3]);
        assert_eq!(inner_list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 3, 5
        ]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Nullable(Array(Int32)))` with nullable inner arrays.
    #[tokio::test]
    async fn test_deserialize_list_nullable_array_int32() {
        let inner_type = Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))));
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Outer offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0,
            // Inner array offsets: [2, 3, 5] (skipping first 0, for non-null arrays)
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (first non-null)
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (null)
            3, 0, 0, 0, 0, 0, 0, 0, // 3 (second non-null)
            3, 0, 0, 0, 0, 0, 0, 0, // 3 (null)
            5, 0, 0, 0, 0, 0, 0, 0, // 5 (third non-null)
            // Inner array values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Nullable(Array(Int32)))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner_list_array = list_array.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = inner_list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(inner_list_array.len(), 5);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(inner_list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 2, 3, 3, 5
        ]);
        assert_eq!(
            inner_list_array.nulls().unwrap().iter().collect::<Vec<bool>>(),
            vec![true, false, true, false, true] // 0=non-null, 1=null
        );
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Array(Nullable(Int32)))` with nullable innermost values.
    #[tokio::test]
    async fn test_deserialize_list_array_nullable_int32() {
        let inner_type = Type::Array(Box::new(Type::Nullable(Box::new(Type::Int32))));
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Outer offsets: [2, 3] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            // Inner offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0, // Inner values: [1, 0, 3, 0, 5] (0 for nulls)
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
            0, 0, 0, 0, // null
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Array(Nullable(Int32)))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner_list_array = list_array.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = inner_list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 2);
        assert_eq!(inner_list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3]);
        assert_eq!(inner_list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 3, 5
        ]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Array(String))` with nested string arrays.
    #[tokio::test]
    async fn test_deserialize_nested_list_string() {
        let inner_type = Type::Array(Box::new(Type::String));
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Outer offsets: [2, 3] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            // Inner offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner values: ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Array(String))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner_list_array = list_array.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = inner_list_array.values().as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(list_array.len(), 2);
        assert_eq!(inner_list_array.len(), 3);
        assert_eq!(values, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3]);
        assert_eq!(inner_list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 3, 5
        ]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Nullable(Array(String)))` with nullable inner string arrays.
    #[tokio::test]
    async fn test_deserialize_list_nullable_array_string() {
        let inner_type = Type::Nullable(Box::new(Type::Array(Box::new(Type::String))));
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Outer offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0,
            // Inner offsets: [2, 2, 3, 3, 5] (skipping first 0, repeating for null arrays)
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (first non-null)
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (null)
            3, 0, 0, 0, 0, 0, 0, 0, // 3 (second non-null)
            3, 0, 0, 0, 0, 0, 0, 0, // 3 (null)
            5, 0, 0, 0, 0, 0, 0, 0, // 5 (third non-null)
            // Inner values: ["a", "b", "c", "d", "e"] (only for non-null arrays)
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Nullable(Array(String)))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner_list_array = list_array.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = inner_list_array.values().as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(inner_list_array.len(), 5); // Reflects total inner arrays, including nulls
        assert_eq!(values, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(
            inner_list_array.offsets().iter().copied().collect::<Vec<i32>>(),
            vec![0, 2, 2, 3, 3, 5] // Null arrays have same offset
        );
        assert_eq!(
            inner_list_array.nulls().unwrap().iter().collect::<Vec<bool>>(),
            vec![true, false, true, false, true] // 0=non-null, 1=null
        );
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Array(Nullable(String)))` with nullable innermost string
    /// values.
    #[tokio::test]
    async fn test_deserialize_list_array_nullable_string() {
        let inner_type = Type::Array(Box::new(Type::Nullable(Box::new(Type::String))));
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Outer offsets: [2, 3] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            // Inner offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0, // Inner values: ["a", "", "c", "", "e"] (empty string for nulls)
            1, b'a', // "a"
            0,    // null
            1, b'c', // "c"
            0,    // null
            1, b'e', // "e"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Array(Nullable(String)))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let inner_list_array = list_array.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = inner_list_array.values().as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(list_array.len(), 2);
        assert_eq!(inner_list_array.len(), 3);
        assert_eq!(values, &StringArray::from(vec![Some("a"), None, Some("c"), None, Some("e")]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3]);
        assert_eq!(inner_list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 3, 5
        ]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Int32)` with empty inner arrays.
    #[tokio::test]
    async fn test_deserialize_list_empty_inner() {
        let inner_type = Type::Int32;
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Offsets: [0, 0] (skipping first 0)
            0, 0, 0, 0, 0, 0, 0, 0, // 0
            0, 0, 0, 0, 0, 0, 0, 0, /* 0
                * No values */
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Int32) with empty inner arrays");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 2);
        assert_eq!(values, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 0, 0]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Float64)` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_list_float64() {
        let inner_type = Type::Float64;
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1.0, 2.0, 3.0, 4.0, 5.0] (little-endian f64)
            0, 0, 0, 0, 0, 0, 240, 63, // 1.0
            0, 0, 0, 0, 0, 0, 0, 64, // 2.0
            0, 0, 0, 0, 0, 0, 8, 64, // 3.0
            0, 0, 0, 0, 0, 0, 16, 64, // 4.0
            0, 0, 0, 0, 0, 0, 20, 64, // 5.0
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Float64)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Nullable(Float64))` with nullable inner values.
    #[tokio::test]
    async fn test_deserialize_list_nullable_float64() {
        let inner_type = Type::Nullable(Box::new(Type::Float64));
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0, // Inner values: [1.0, 0.0, 3.0, 0.0, 5.0] (0.0 for nulls)
            0, 0, 0, 0, 0, 0, 240, 63, // 1.0
            0, 0, 0, 0, 0, 0, 0, 0, // null
            0, 0, 0, 0, 0, 0, 8, 64, // 3.0
            0, 0, 0, 0, 0, 0, 0, 0, // null
            0, 0, 0, 0, 0, 0, 20, 64, // 5.0
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Nullable(Float64))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Float64Array::from(vec![Some(1.0), None, Some(3.0), None, Some(5.0)]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(DateTime)` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_list_datetime() {
        let inner_type = Type::DateTime(Tz::UTC);
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 4] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            4, 0, 0, 0, 0, 0, 0, 0, // 4
            // Values: [1000, 2000, 3000, 4000] (seconds since 1970-01-01, little-endian u32)
            232, 3, 0, 0, // 1000
            208, 7, 0, 0, // 2000
            184, 11, 0, 0, // 3000
            160, 15, 0, 0, // 4000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(DateTime)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<TimestampSecondArray>().unwrap();

        assert_eq!(list_array.len(), 2);
        assert_eq!(
            values,
            &TimestampSecondArray::from(vec![1000, 2000, 3000, 4000])
                .with_timezone_opt(Some("UTC"))
        );
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 4]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Nullable(DateTime))` with nullable inner values.
    #[tokio::test]
    async fn test_deserialize_list_nullable_datetime() {
        let inner_type = Type::Nullable(Box::new(Type::DateTime(Tz::UTC)));
        let rows = 3;
        let nulls = vec![];
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Inner null mask: [0, 1, 0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, 1, 0, // Inner values: [1000, 0, 3000, 0, 5000] (0 for nulls)
            232, 3, 0, 0, // 1000
            0, 0, 0, 0, // null
            184, 11, 0, 0, // 3000
            0, 0, 0, 0, // null
            136, 19, 0, 0, // 5000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&inner_type, &mut reader, rows, &nulls, &mut state)
            .await
            .expect("Failed to deserialize Array(Nullable(DateTime))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<TimestampSecondArray>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(
            values,
            &TimestampSecondArray::from(vec![Some(1000), None, Some(3000), None, Some(5000)],)
                .with_timezone_opt(Some("UTC"))
        );
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(LowCardinality(Nullable(DateTime)))`
    #[tokio::test]
    async fn test_deserialize_list_low_cardinality_nullable_string() {
        let inner_type = Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String))));
        let rows = 5;

        // For 5 arrays with the desired structure:
        // [["low", Null], [], ["low", "card"], ["low", Null, "test"], ["test"]]
        // The offsets should be: [0, 2, 2, 4, 7, 8]
        // But ClickHouse sends: [2, 2, 4, 7, 8] (skipping the first)
        let input = vec![
            // Array Offsets (skipping the first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // After row 1: 2 elements
            2, 0, 0, 0, 0, 0, 0, 0, // After row 2: 0 additional elements (still 2)
            4, 0, 0, 0, 0, 0, 0, 0, // After row 3: 2 additional elements (total: 4)
            7, 0, 0, 0, 0, 0, 0, 0, // After row 4: 3 additional elements (total: 7)
            8, 0, 0, 0, 0, 0, 0, 0, // After row 5: 1 additional element (total: 8)
            // LowCardinality
            0, 2, 0, 0, 0, 0, 0, 0, // Flags
            4, 0, 0, 0, 0, 0, 0, 0, // Dict length
            // Dictionary: [null, "low", "card", "test"]
            0, // Null marker
            3, b'l', b'o', b'w', // "low"
            4, b'c', b'a', b'r', b'd', // "card"
            4, b't', b'e', b's', b't', // "test"
            8, 0, 0, 0, 0, 0, 0, 0, // Key length
            // Key indices mapping to:
            // [["low", Null], [], ["low", "card"], ["low", Null, "test"], ["test"]]
            // ---
            1, 0, // Row 1: ["low", null]
            // Row 2: [] (empty)
            1, 2, // Row 3: ["low", "card"]
            1, 0, 3, // Row 4: ["low", null, "test"]
            3, // Row 5: ["test"]
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();
        let result = deserialize(&inner_type, &mut reader, rows, &[], &mut state)
            .await
            .expect("Failed to deserialize Array(LowCardinality(Nullable(String)))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values =
            list_array.values().as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();

        assert_eq!(list_array.len(), rows);

        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 2, 4, 7, 8
        ]);
        assert_eq!(list_array.nulls(), None);
        let expected_keys = Int32Array::from(vec![
            Some(1),
            Some(0), // Row 1: ["low", null]
            // Row 2: [] (empty)
            Some(1),
            Some(2), // Row 3: ["low", "card"]
            Some(1),
            Some(0),
            Some(3), // Row 4: ["low", null, "test"]
            Some(3), // Row 5: ["test"]
        ]);
        // Expected dictionary values
        let expected_values =
            StringArray::from(vec![None, Some("low"), Some("card"), Some("test")]);
        let expected_dict =
            DictionaryArray::<Int32Type>::try_new(expected_keys, Arc::new(expected_values))
                .unwrap();
        assert_eq!(values, &expected_dict);
    }
}
