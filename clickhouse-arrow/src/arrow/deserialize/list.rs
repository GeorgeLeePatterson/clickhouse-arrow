/// Deserialization logic for `ClickHouse` `Array` types into Arrow `ListArray`.
///
/// This module provides a function to deserialize `ClickHouse`’s native format for `Array`
/// types into an Arrow `ListArray`, which represents variable-length lists of inner values. It
/// is used by the `ClickHouseArrowDeserializer` implementation in `deserialize.rs` to handle
/// array data, including `Array(T)` and `Nullable(Array(T))` types.
///
/// The `deserialize` function reads offsets and inner values from the input stream,
/// constructing a `ListArray` with the specified inner type. It handles nullability for the
/// outer array via the provided null mask and delegates to the inner type’s deserializer for
/// value processing.
use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::*;
use tokio::io::AsyncReadExt;

use super::ClickHouseArrowDeserializer;
use crate::arrow::builder::TypedBuilder;
use crate::arrow::builder::list::TypedListBuilder;
use crate::arrow::types::LIST_ITEM_FIELD_NAME;
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::{Error, Result, Type};

macro_rules! bulk_offsets {
    ($r:expr, $rbuf:expr, $rows:expr) => {{
        $rbuf.clear();
        // Prepare buffer for: initial offset (8 bytes) + offset data (rows * 8 bytes)
        let total_bytes_needed = 8 + ($rows * 8);
        if $rbuf.capacity() < total_bytes_needed {
            $rbuf.reserve(total_bytes_needed - $rbuf.capacity());
        }
        $rbuf.resize(total_bytes_needed, 0);
        // Set initial offset to 0
        let initial_off = 0_u64.to_le_bytes();
        $rbuf[..8].copy_from_slice(&initial_off);
        // Read offset data into the rest of the buffer
        let _ = $r.try_copy_to_slice(&mut $rbuf[8..total_bytes_needed])?;
        total_bytes_needed
    }};
    (tokio; $r:expr, $rbuf:expr, $rows:expr) => {{
        $rbuf.clear();
        // Prepare buffer for: initial offset (8 bytes) + offset data (rows * 8 bytes)
        let total_bytes_needed = 8 + ($rows * 8);
        if $rbuf.capacity() < total_bytes_needed {
            $rbuf.reserve(total_bytes_needed - $rbuf.capacity());
        }
        $rbuf.resize(total_bytes_needed, 0);
        // Set initial offset to 0
        let initial_off = 0_u64.to_le_bytes();
        $rbuf[..8].copy_from_slice(&initial_off);
        // Read offset data into the rest of the buffer
        let _ = $r.read_exact(&mut $rbuf[8..total_bytes_needed]).await?;
        total_bytes_needed
    }};
}
pub(super) use bulk_offsets;

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
/// `Error` if deserialization fails.
///
/// # Errors
/// - Returns `Io` if reading from the reader fails (e.g., EOF).
/// - Returns `DeserializeError` if the deserialized array length doesn’t match `rows`.
/// - Returns `ArrowDeserialize` if the inner type deserialization fails.
///
/// # Example
/// ```rust,ignore
/// use arrow::array::{ArrayRef, Int32Array, ListArray};
/// use clickhouse_arrow::types::{Type, ClickHouseArrowDeserializer, DeserializerState};
/// use std::io::Cursor;
///
/// let inner_type = Type::Int32;
/// let rows = 3;
/// let nulls = vec![];
/// let input = vec![
///     // Offsets: [2, 3, 5] (skipping first 0)
///     2, 0, 0, 0, 0, 0, 0, 0, // 2
///     3, 0, 0, 0, 0, 0, 0, 0, // 3
///     5, 0, 0, 0, 0, 0, 0, 0, // 5
///     // Values: [1, 2, 3, 4, 5]
///     1, 0, 0, 0, // 1
///     2, 0, 0, 0, // 2
///     3, 0, 0, 0, // 3
///     4, 0, 0, 0, // 4
///     5, 0, 0, 0, // 5
/// ];
/// let mut reader = Cursor::new(input);
/// let inner_field = Field::new("item", DataType::Int32, false);
/// let data_type = DataType::List(Arc::new(inner_field));
/// let result = deserialize(&inner_type, &data_type, &mut reader, rows, &nulls)
///     .await
///     .expect("Failed to deserialize List(Int32)");
/// let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
/// let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(list_array.len(), 3);
/// assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
/// assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 3, 5]);
/// assert_eq!(list_array.nulls(), None);
/// ```
#[expect(clippy::cast_possible_wrap)]
#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn deserialize_async<R: ClickHouseRead>(
    inner_type: &Type,
    builder: &mut TypedBuilder,
    data_type: &DataType,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    type B = TypedListBuilder;

    let (DataType::List(inner) | DataType::ListView(inner) | DataType::LargeList(inner)) =
        data_type
    else {
        return Err(Error::ArrowDeserialize(format!("Unexpected list type: {data_type:?}")));
    };

    let TypedBuilder::List(list_builder) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected builder type: {}",
            builder.as_ref()
        )));
    };

    let inner_data_type = inner.data_type();
    let inner_nullable = inner_type.strip_low_cardinality().is_nullable();

    macro_rules! list_deser {
        ($b:expr, $b_ty:ident, $t:ty) => {{
            // Offsets
            let offset_bytes = bulk_offsets!(tokio; reader, rbuffer, rows);
            let offsets: &[u64] = bytemuck::cast_slice::<u8, u64>(&rbuffer[..offset_bytes]);
            let offset_buffer =
                OffsetBuffer::new(offsets.iter().map(|&o| o as $t).collect::<ScalarBuffer<_>>());
            let total_values = *offsets.last().unwrap_or(&0) as usize;
            // Recursively deserialize the inner array
            let inner_array = inner_type.deserialize_arrow_async(
                $b,
                reader,
                inner_data_type,
                total_values,
                &[],
                rbuffer,
            ).await?;
            // The null mask provides the null buffer for THIS list
            let null_buffer = (!nulls.is_empty())
                .then_some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()));
            // Construct the ListArray directly
            let inner_dt = inner_array.data_type().clone();
            let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, inner_nullable));
            let list_array = $b_ty::new(field, offset_buffer, inner_array, null_buffer);
            // Verify length matches expected rows
            if list_array.len() != rows {
                return Err(Error::DeserializeError(format!(
                    "ListArray length {} does not match expected rows {rows}",
                    list_array.len()
                )));
            }

            Ok(Arc::new(list_array))
        }};
    }

    match list_builder {
        B::List(b) => list_deser!(b, ListArray, i32),
        B::LargeList(b) => list_deser!(b, LargeListArray, i64),
        B::FixedList((size, b)) => {
            // Recursively deserialize the inner array
            let inner_array = inner_type
                .deserialize_arrow_async(b, reader, inner_data_type, rows, &[], rbuffer)
                .await?;
            // The null mask provides the null buffer for THIS list
            let null_buffer = (!nulls.is_empty())
                .then_some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()));
            let inner_dt = inner_array.data_type().clone();
            let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, inner_nullable));
            let list_array = FixedSizeListArray::new(field, *size, inner_array, null_buffer);
            // Verify length matches expected rows
            if list_array.len() != rows {
                return Err(Error::DeserializeError(format!(
                    "ListArray length {} does not match expected rows {rows}",
                    list_array.len()
                )));
            }
            Ok(Arc::new(list_array))
        }
    }
}

#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::cast_possible_wrap)]
#[allow(dead_code)] // TODO: remove once synchronous Arrow path is fully retired
pub(super) fn deserialize<R: ClickHouseBytesRead>(
    builder: &mut TypedListBuilder,
    reader: &mut R,
    inner_type: &Type,
    data_type: &DataType,
    rows: usize,
    nulls: &[u8],
    // Reusable row buffer
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    type B = TypedListBuilder;

    let (DataType::List(inner) | DataType::ListView(inner) | DataType::LargeList(inner)) =
        data_type
    else {
        return Err(Error::ArrowDeserialize(format!("Unexpected list type: {data_type:?}")));
    };

    let inner_data_type = inner.data_type();
    let inner_nullable = inner_type.strip_low_cardinality().is_nullable();

    macro_rules! list_deser {
        ($b:expr, $b_ty:ident, $t:ty) => {{
            // Offsets
            let offset_bytes = bulk_offsets!(reader, rbuffer, rows);
            let offsets: &[u64] = bytemuck::cast_slice::<u8, u64>(&rbuffer[..offset_bytes]);
            let offset_buffer =
                OffsetBuffer::new(offsets.iter().map(|&o| o as $t).collect::<ScalarBuffer<_>>());
            let total_values = *offsets.last().unwrap_or(&0) as usize;
            // Recursively deserialize the inner array
            let inner_array = inner_type.deserialize_arrow(
                $b,
                reader,
                inner_data_type,
                total_values,
                &[],
                rbuffer,
            )?;
            // The null mask provides the null buffer for THIS list
            let null_buffer = (!nulls.is_empty())
                .then_some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()));
            // Construct the ListArray directly
            let inner_dt = inner_array.data_type().clone();
            let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, inner_nullable));
            let list_array = $b_ty::new(field, offset_buffer, inner_array, null_buffer);
            // Verify length matches expected rows
            if list_array.len() != rows {
                return Err(Error::DeserializeError(format!(
                    "ListArray length {} does not match expected rows {rows}",
                    list_array.len()
                )));
            }

            Ok(Arc::new(list_array))
        }};
    }

    match builder {
        B::List(b) => list_deser!(b, ListArray, i32),
        B::LargeList(b) => list_deser!(b, LargeListArray, i64),
        B::FixedList((size, b)) => {
            // Recursively deserialize the inner array
            let inner_array =
                inner_type.deserialize_arrow(b, reader, inner_data_type, rows, &[], rbuffer)?;
            // The null mask provides the null buffer for THIS list
            let null_buffer = (!nulls.is_empty())
                .then_some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<bool>>()));
            let inner_dt = inner_array.data_type().clone();
            let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, inner_nullable));
            let list_array = FixedSizeListArray::new(field, *size, inner_array, null_buffer);
            // Verify length matches expected rows
            if list_array.len() != rows {
                return Err(Error::DeserializeError(format!(
                    "ListArray length {} does not match expected rows {rows}",
                    list_array.len()
                )));
            }
            Ok(Arc::new(list_array))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::*;
    use arrow::datatypes::*;
    use chrono_tz::Tz;

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::block::LIST_ITEM_FIELD_NAME;
    use crate::arrow::ch_to_arrow_type;
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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

        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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

        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, false)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, true)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, true));
        let data_type = DataType::List(inner_field);
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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

        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, true));
        let data_type = DataType::List(inner_field);
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, true)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float64, false)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float64, true)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let inner_dt = DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")));
        let data_type = DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, false)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let inner_dt = DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")));
        let data_type = DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, true)));
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &nulls,
            &mut vec![],
        )
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
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        let data_type =
            ch_to_arrow_type(&Type::Array(Box::new(inner_type.clone())), opts).unwrap().0;
        let mut builder =
            TypedBuilder::try_new(&Type::Array(Box::new(inner_type.clone())), &data_type).unwrap();
        let result = deserialize_async(
            &inner_type,
            &mut builder,
            &data_type,
            &mut reader,
            rows,
            &[],
            &mut vec![],
        )
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

#[cfg(test)]
mod tests_sync {
    use std::io::Cursor;

    use arrow::array::*;
    use arrow::datatypes::*;
    use chrono_tz::Tz;

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::block::LIST_ITEM_FIELD_NAME;
    use crate::arrow::ch_to_arrow_type;
    use crate::native::types::Type;

    fn test_list_deser(
        input: Vec<u8>,
        inner_type: &Type,
        data_type: &DataType,
        rows: usize,
        nulls: &[u8],
    ) -> Result<ArrayRef> {
        let mut reader = Cursor::new(input);
        let mut builder = TypedListBuilder::try_new(inner_type, data_type).unwrap();
        deserialize(&mut builder, &mut reader, inner_type, data_type, rows, nulls, &mut vec![])
    }

    #[test]
    fn test_deserialize_list_int32() {
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize List(Int32)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    #[test]
    fn test_deserialize_nullable_list_int32() {
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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

    #[test]
    fn test_deserialize_list_nullable_int32() {
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize List(Nullable(Int32))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    #[test]
    fn test_deserialize_list_zero_rows() {
        let inner_type = Type::Int32;
        let rows = 0;
        let nulls = vec![];
        let input = vec![]; // Initial offset
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize List(Int32) with zero rows");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 0);
        assert_eq!(values, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<_>>(), vec![0]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(String)` with non-zero rows.
    #[test]
    fn test_deserialize_list_string() {
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, false)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize Array(String)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Nullable(String))` with nullable inner values.
    #[test]
    fn test_deserialize_list_nullable_string() {
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, true)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize Array(Nullable(String))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &StringArray::from(vec![Some("a"), None, Some("c"), None, Some("e")]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Array(Int32))` with nested arrays.
    #[test]
    fn test_deserialize_nested_list_int32() {
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_list_nullable_array_int32() {
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, true));
        let data_type = DataType::List(inner_field);
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_list_array_nullable_int32() {
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_nested_list_string() {
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_list_nullable_array_string() {
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, false)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, true));
        let data_type = DataType::List(inner_field);
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_list_array_nullable_string() {
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
        let inner_data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, true)));
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_data_type, false));
        let data_type = DataType::List(inner_field);
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_list_empty_inner() {
        let inner_type = Type::Int32;
        let rows = 2;
        let nulls = vec![];
        let input = vec![
            // Offsets: [0, 0] (skipping first 0)
            0, 0, 0, 0, 0, 0, 0, 0, // 0
            0, 0, 0, 0, 0, 0, 0, 0, /* 0
                * No values */
        ];
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize Array(Int32) with empty inner arrays");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 2);
        assert_eq!(values, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 0, 0]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Float64)` with non-nullable values.
    #[test]
    fn test_deserialize_list_float64() {
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float64, false)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize Array(Float64)");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(Nullable(Float64))` with nullable inner values.
    #[test]
    fn test_deserialize_list_nullable_float64() {
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
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float64, true)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
            .expect("Failed to deserialize Array(Nullable(Float64))");
        let list_array = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Float64Array::from(vec![Some(1.0), None, Some(3.0), None, Some(5.0)]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Array(DateTime)` with non-nullable values.
    #[test]
    fn test_deserialize_list_datetime() {
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
        let inner_dt = DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")));
        let data_type = DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, false)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_list_nullable_datetime() {
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
        let inner_dt = DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC")));
        let data_type = DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_dt, true)));
        let result = test_list_deser(input, &inner_type, &data_type, rows, &nulls)
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
    #[test]
    fn test_deserialize_list_low_cardinality_nullable_string() {
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
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        let data_type =
            ch_to_arrow_type(&Type::Array(Box::new(inner_type.clone())), opts).unwrap().0;
        let result = test_list_deser(input, &inner_type, &data_type, rows, &[])
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
