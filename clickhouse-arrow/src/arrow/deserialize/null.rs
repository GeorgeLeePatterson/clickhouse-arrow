/// Deserialization logic for `ClickHouse` `Nullable` types into Arrow arrays.
///
/// This module provides a function to deserialize `ClickHouse`’s native format for `Nullable`
/// types into Arrow arrays, handling nullability for any inner type (e.g., `Nullable(Int32)`,
/// `Nullable(String)`, `Nullable(Array(T))`). It is used by the `ClickHouseArrowDeserializer`
/// implementation in `deserialize.rs` to process nullable columns.
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use tokio::io::AsyncReadExt;

use super::ClickHouseArrowDeserializer;
use crate::arrow::builder::TypedBuilder;
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::{Error, Result, Type};

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
/// a `Error` if deserialization fails.
///
/// # Errors
/// - Returns `Io` if reading the null mask fails (e.g., EOF).
/// - Returns `DeserializeError` if the mask length doesn’t match `rows`.
/// - Returns `ArrowDeserialize` if the inner type deserialization fails.
///
/// # Example
/// ```rust,ignore
/// use arrow::array::{ArrayRef, Int32Array};
/// use clickhouse_arrow::types::{Type, DeserializerState};
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
pub(crate) async fn deserialize_async<R: ClickHouseRead>(
    inner: &Type,
    builder: &mut TypedBuilder,
    data_type: &DataType,
    reader: &mut R,
    rows: usize,
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    let nulls = if rows > 0 {
        let mut mask = vec![0u8; rows];
        let _ = reader.read_exact(&mut mask).await?;
        if mask.len() != rows {
            return Err(Error::DeserializeError(format!("Nulls={}, rows={rows}", mask.len())));
        }
        mask
    } else {
        vec![]
    };
    inner.deserialize_arrow_async(builder, reader, data_type, rows, &nulls, rbuffer).await
}

pub(crate) fn deserialize<R: ClickHouseBytesRead>(
    inner: &Type,
    builder: &mut TypedBuilder,
    reader: &mut R,
    data_type: &DataType,
    rows: usize,
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    let nulls = if rows > 0 {
        let mut mask = vec![0u8; rows];
        reader.try_copy_to_slice(&mut mask)?;
        if mask.len() != rows {
            return Err(Error::DeserializeError(format!("Nulls={}, rows={rows}", mask.len())));
        }
        mask
    } else {
        vec![]
    };

    inner.deserialize_arrow(builder, reader, data_type, rows, &nulls, rbuffer)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{Array, Int32Array, ListArray, StringArray};

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::ch_to_arrow_type;
    use crate::native::types::Type;

    /// Tests deserialization of `Nullable(Int32)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int32() {
        let type_ = Type::Nullable(Box::new(Type::Int32));
        let inner_type = type_.strip_null();
        let rows = 3;
        let input = vec![
            // Null mask: [0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, // Values: [1, 0, 3] (0 for null)
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);
        let data_type = ch_to_arrow_type(inner_type, None).unwrap().0;
        let mut builder = TypedBuilder::try_new(inner_type, &data_type).unwrap();
        let result =
            deserialize_async(inner_type, &mut builder, &data_type, &mut reader, rows, &mut vec![])
                .await
                .expect("Failed to deserialize Nullable(Int32)");
        let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array, &Int32Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Nullable(String)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_string() {
        let type_ = Type::Nullable(Box::new(Type::String));
        let inner_type = type_.strip_null();
        let rows = 3;
        let input = vec![
            // Null mask: [0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, // Values: ["a", "", "c"] (empty string for null)
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
        ];
        let mut reader = Cursor::new(input);
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        let data_type = ch_to_arrow_type(inner_type, opts).unwrap().0;
        let mut builder = TypedBuilder::try_new(inner_type, &data_type).unwrap();
        let result =
            deserialize_async(inner_type, &mut builder, &data_type, &mut reader, rows, &mut vec![])
                .await
                .expect("Failed to deserialize Nullable(String)");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array, &StringArray::from(vec![Some("a"), None, Some("c")]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Nullable(Array(Int32))` with null arrays.
    #[tokio::test]
    async fn test_deserialize_nullable_array_int32() {
        let type_ = Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))));
        let inner_type = type_.strip_null();
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
        let data_type = ch_to_arrow_type(inner_type, None).unwrap().0;
        let mut builder = TypedBuilder::try_new(inner_type, &data_type).unwrap();
        let result =
            deserialize_async(inner_type, &mut builder, &data_type, &mut reader, rows, &mut vec![])
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
        let type_ = Type::Nullable(Box::new(Type::Int32));
        let inner_type = type_.strip_null();
        let rows = 0;
        let input = vec![]; // No data for zero rows
        let mut reader = Cursor::new(input);
        let data_type = ch_to_arrow_type(inner_type, None).unwrap().0;
        let mut builder = TypedBuilder::try_new(inner_type, &data_type).unwrap();
        let result =
            deserialize_async(inner_type, &mut builder, &data_type, &mut reader, rows, &mut vec![])
                .await
                .expect("Failed to deserialize Nullable(Int32) with zero rows");
        let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array.len(), 0);
        assert_eq!(array, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(array.nulls(), None);
    }

    #[tokio::test]
    async fn test_null_mask_length() {
        let type_ = Type::Nullable(Box::new(Type::String));
        let inner_type = type_.strip_null();
        let data_type = ch_to_arrow_type(inner_type, None).unwrap().0;
        let mut builder = TypedBuilder::try_new(inner_type, &data_type).unwrap();
        assert!(
            deserialize_async(
                inner_type,
                &mut builder,
                &data_type,
                &mut Cursor::new(vec![0_u8; 50]),
                100,
                &mut vec![]
            )
            .await
            .is_err()
        );
    }
}
