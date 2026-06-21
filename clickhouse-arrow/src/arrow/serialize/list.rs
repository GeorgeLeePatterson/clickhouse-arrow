/// Serialization logic for `ClickHouse` `Array` types from Arrow list arrays.
///
/// This module provides functions to serialize Arrow `ListArray`, `ListViewArray`,
/// `LargeListArray`, `LargeListViewArray`, and `FixedSizeListArray` into `ClickHouse`’s native
/// format for the `Array` type. It is used by the `ClickHouseArrowSerializer` implementation
/// in `types.rs` to handle nested data structures.
///
/// The main `serialize` function handles four cases:
/// - `ListArray`: Writes variable-length offsets and serializes inner values.
/// - `ListViewArray`: Writes variable-length offsets and serializes inner values.
/// - `LargeListArray`: Writes variable-length offsets and serializes inner values.
/// - `LargeListViewArray`: Writes variable-length offsets and serializes inner values.
/// - `FixedSizeListArray`: Writes computed offsets based on fixed length and serializes inner
///   values.
///
/// # Examples
/// ```rust,ignore
/// use arrow::array::{Int32Array, ListArray};
/// use arrow::buffer::OffsetBuffer;
/// use arrow::datatypes::{ArrayRef, DataType, Field};
/// use clickhouse_arrow::types::{Type, list::serialize, SerializerState};
/// use std::sync::Arc;
/// use tokio::io::AsyncWriteExt;
///
/// let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
/// let offsets = OffsetBuffer::new(vec![0, 2, 4].into());
/// let column = Arc::new(ListArray::new(
///     Arc::new(Field::new("item", DataType::Int32, false)),
///     offsets,
///     values,
///     None,
/// )) as ArrayRef;
/// let field = Field::new(
///     "list",
///     DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
///     false,
/// );
/// let mut buffer = Vec::new();
/// let mut state = SerializerState::default();
/// serialize(&Type::Int32, &field, &column, &mut buffer, &mut state)
///     .await
///     .unwrap();
/// ```
use arrow::array::*;
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use super::ClickHouseArrowSerializer;
use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

const STACK_OFFSET_CAPACITY: usize = 64;

/// Extracts the inner `Field` from a `List`, `ListView`, `LargeList`, `LargeListView`, or
/// `FixedSizeList` data type.
///
/// # Arguments
/// - `field`: The Arrow `Field` to extract from.
///
/// # Returns
/// A `Result` containing the inner `Field` or a `Error` if the data type is not
/// `List`, `ListView`, `LargeList`, `LargeListView`, or `FixedSizeList`.
fn unwrap_array_data_type(dt: &DataType) -> Result<&DataType> {
    match dt {
        DataType::List(f)
        | DataType::ListView(f)
        | DataType::LargeList(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => Ok(f.data_type()),
        _ => Err(Error::ArrowSerialize(format!("Expected List or FixedSizeList, got {dt:?}"))),
    }
}

async fn write_offsets_i32_async<W: ClickHouseWrite>(
    writer: &mut W,
    offsets: &[i32],
) -> Result<()> {
    let count = offsets.len().saturating_sub(1);
    if count == 0 {
        return Ok(());
    }

    if count <= STACK_OFFSET_CAPACITY {
        let mut buffer = [0_u8; STACK_OFFSET_CAPACITY * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.write_all(&buffer[..count * 8]).await?;
    } else {
        let mut buffer = vec![0_u8; count * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.write_all(&buffer).await?;
    }

    Ok(())
}

async fn write_offsets_i64_async<W: ClickHouseWrite>(
    writer: &mut W,
    offsets: &[i64],
) -> Result<()> {
    let count = offsets.len().saturating_sub(1);
    if count == 0 {
        return Ok(());
    }

    if count <= STACK_OFFSET_CAPACITY {
        let mut buffer = [0_u8; STACK_OFFSET_CAPACITY * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.write_all(&buffer[..count * 8]).await?;
    } else {
        let mut buffer = vec![0_u8; count * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.write_all(&buffer).await?;
    }

    Ok(())
}

fn write_offsets_i32<W: ClickHouseBytesWrite>(writer: &mut W, offsets: &[i32]) {
    let count = offsets.len().saturating_sub(1);
    if count == 0 {
        return;
    }

    if count <= STACK_OFFSET_CAPACITY {
        let mut buffer = [0_u8; STACK_OFFSET_CAPACITY * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.put_slice(&buffer[..count * 8]);
    } else {
        let mut buffer = vec![0_u8; count * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.put_slice(&buffer);
    }
}

fn write_offsets_i64<W: ClickHouseBytesWrite>(writer: &mut W, offsets: &[i64]) {
    let count = offsets.len().saturating_sub(1);
    if count == 0 {
        return;
    }

    if count <= STACK_OFFSET_CAPACITY {
        let mut buffer = [0_u8; STACK_OFFSET_CAPACITY * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.put_slice(&buffer[..count * 8]);
    } else {
        let mut buffer = vec![0_u8; count * 8];
        for (index, offset) in offsets[1..].iter().enumerate() {
            #[expect(clippy::cast_sign_loss, reason = "Arrow list offsets are non-negative")]
            let bytes = (*offset as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.put_slice(&buffer);
    }
}

async fn write_fixed_offsets_async<W: ClickHouseWrite>(
    writer: &mut W,
    value_len: usize,
    rows: usize,
) -> Result<()> {
    if rows == 0 {
        return Ok(());
    }

    if rows <= STACK_OFFSET_CAPACITY {
        let mut buffer = [0_u8; STACK_OFFSET_CAPACITY * 8];
        for index in 0..rows {
            let bytes = ((value_len * (index + 1)) as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.write_all(&buffer[..rows * 8]).await?;
    } else {
        let mut buffer = vec![0_u8; rows * 8];
        for index in 0..rows {
            let bytes = ((value_len * (index + 1)) as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.write_all(&buffer).await?;
    }

    Ok(())
}

fn write_fixed_offsets<W: ClickHouseBytesWrite>(writer: &mut W, value_len: usize, rows: usize) {
    if rows == 0 {
        return;
    }

    if rows <= STACK_OFFSET_CAPACITY {
        let mut buffer = [0_u8; STACK_OFFSET_CAPACITY * 8];
        for index in 0..rows {
            let bytes = ((value_len * (index + 1)) as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.put_slice(&buffer[..rows * 8]);
    } else {
        let mut buffer = vec![0_u8; rows * 8];
        for index in 0..rows {
            let bytes = ((value_len * (index + 1)) as u64).to_le_bytes();
            buffer[index * 8..(index + 1) * 8].copy_from_slice(&bytes);
        }
        writer.put_slice(&buffer);
    }
}

/// Serializes an Arrow `ListArray`, `ListViewArray`, `LargeListArray`, `LargeListViewArray`, or
/// `FixedSizeListArray` to `ClickHouse`’s native format for `Array` types.
///
/// Writes offsets (variable-length for `ListArray`, computed for `FixedSizeListArray`) followed by
/// serialized inner values. The inner values are serialized using the provided `inner_type` and
/// `inner_field`.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` `Type` of the array.
/// - `field`: The Arrow `Field` describing the list’s metadata.
/// - `values`: The `ListArray` or `FixedSizeListArray` containing the data.
/// - `writer`: The async writer to serialize to (e.g., a TCP stream).
/// - `state`: A mutable `SerializerState` for serialization context.
///
/// # Returns
/// A `Result` indicating success or a `Error` if serialization fails.
///
/// # Errors
/// - Returns `ArrowSerialize` if the `values` is not a `ListArray`, `ListViewArray`,
///   `LargeListArray`, `LargeListViewArray`, or `FixedSizeListArray`, or the field’s data type is
///   invalid.
/// - Returns an error is the type is not an `Array`
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn serialize_with_inner_async<W: ClickHouseWrite>(
    inner_type: &Type,
    writer: &mut W,
    values: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    macro_rules! write_list_array_i32 {
        ($( $array_ty:ty ),* $(,)?) => {{
            $(
            if let Some(array) = values.as_any().downcast_ref::<$array_ty>() {
                let inner_dt = unwrap_array_data_type(data_type)?;
                let values = array.values();
                let offsets = array.value_offsets();
                write_offsets_i32_async(writer, offsets.as_ref()).await?;
                // Write inner values
                inner_type.serialize_async(writer, values, inner_dt, state).await?;
                return Ok(());
            }
            )*
        }}
    }

    macro_rules! write_list_array_i64 {
        ($( $array_ty:ty ),* $(,)?) => {{
            $(
            if let Some(array) = values.as_any().downcast_ref::<$array_ty>() {
                let inner_dt = unwrap_array_data_type(data_type)?;
                let values = array.values();
                let offsets = array.value_offsets();
                write_offsets_i64_async(writer, offsets.as_ref()).await?;
                inner_type.serialize_async(writer, values, inner_dt, state).await?;
                return Ok(());
            }
            )*
        }}
    }

    write_list_array_i32!(ListArray, ListViewArray);
    write_list_array_i64!(LargeListArray, LargeListViewArray);

    // FixedSizeListArray
    if let Some(array) = values.as_any().downcast_ref::<FixedSizeListArray>() {
        let inner_dt = unwrap_array_data_type(data_type)?;

        #[expect(clippy::cast_sign_loss)]
        let value_len = array.value_length() as usize;
        let num_rows = array.len();
        write_fixed_offsets_async(writer, value_len, num_rows).await?;
        // Write inner values
        let values = array.values();
        inner_type.serialize_async(writer, values, inner_dt, state).await?;
        return Ok(());
    }

    Err(Error::ArrowSerialize(format!(
        "Expected ListArray or FixedSizeListArray: type={inner_type:?}, data_type={data_type:?}"
    )))
}

pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    values: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    // Unwrap the inner type
    let inner_type = type_hint.strip_null().unwrap_array()?;
    serialize_with_inner_async(inner_type, writer, values, data_type, state).await
}

pub(super) fn serialize_with_inner<W: ClickHouseBytesWrite>(
    inner_type: &Type,
    writer: &mut W,
    values: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    macro_rules! put_list_array_i32 {
        ($( $array_ty:ty ),* $(,)?) => {{
            $(
            if let Some(array) = values.as_any().downcast_ref::<$array_ty>() {
                let inner_dt = unwrap_array_data_type(data_type)?;
                let values = array.values();
                let offsets = array.value_offsets();
                write_offsets_i32(writer, offsets.as_ref());
                // Write inner values
                inner_type.serialize(writer, values, inner_dt, state)?;
                return Ok(());
            }
            )*
        }}
    }

    macro_rules! put_list_array_i64 {
        ($( $array_ty:ty ),* $(,)?) => {{
            $(
            if let Some(array) = values.as_any().downcast_ref::<$array_ty>() {
                let inner_dt = unwrap_array_data_type(data_type)?;
                let values = array.values();
                let offsets = array.value_offsets();
                write_offsets_i64(writer, offsets.as_ref());
                inner_type.serialize(writer, values, inner_dt, state)?;
                return Ok(());
            }
            )*
        }}
    }

    put_list_array_i32!(ListArray, ListViewArray);
    put_list_array_i64!(LargeListArray, LargeListViewArray);

    // FixedSizeListArray
    if let Some(array) = values.as_any().downcast_ref::<FixedSizeListArray>() {
        let inner_dt = unwrap_array_data_type(data_type)?;

        #[expect(clippy::cast_sign_loss)]
        let value_len = array.value_length() as usize;
        let num_rows = array.len();
        write_fixed_offsets(writer, value_len, num_rows);
        // Write inner values
        let values = array.values();
        inner_type.serialize(writer, values, inner_dt, state)?;
        return Ok(());
    }

    Err(Error::ArrowSerialize(format!(
        "Expected ListArray or FixedSizeListArray: type={inner_type:?}, data_type={data_type:?}"
    )))
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    values: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    // Unwrap the inner type
    let inner_type = type_hint.strip_null().unwrap_array()?;
    serialize_with_inner(inner_type, writer, values, data_type, state)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::*;

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::types::LIST_ITEM_FIELD_NAME;
    use crate::formats::SerializerState;
    use crate::native::types::Type;

    type MockWriter = Vec<u8>;

    fn wrap_array(typ: Type) -> Type { Type::Array(Box::new(typ)) }

    /// Helper function used by individual type serializers
    pub(crate) async fn test_type_serializer(
        expected: &[u8],
        type_: &Type,
        field: &Field,
        array: &ArrayRef,
    ) {
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default()
            .with_arrow_options(ArrowOptions::default().with_strings_as_strings(true));
        serialize_async(type_, &mut writer, array, field.data_type(), &mut state).await.unwrap();
        assert_eq!(writer, expected);
    }

    pub(crate) fn test_type_serializer_sync(
        expected: &[u8],
        type_: &Type,
        field: &Field,
        array: &ArrayRef,
    ) {
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default()
            .with_arrow_options(ArrowOptions::default().with_strings_as_strings(true));
        serialize(type_, &mut writer, array, field.data_type(), &mut state).unwrap();
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_list_int32() {
        let type_ = wrap_array(Type::Int32);
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let field = Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        let array = Arc::new(ListArray::new(
            inner_field,
            OffsetBuffer::new(vec![0, 2, 3, 5].into()),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            None,
        )) as ArrayRef;

        let expected = vec![
            // Offsets: [2, 3, 5] (u64, little-endian)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5] (i32, little-endian)
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        test_type_serializer(&expected, &type_, &field, &array).await;
    }

    #[tokio::test]
    async fn test_serialize_list_nullable_int32() {
        let type_ = wrap_array(Type::Nullable(Box::new(Type::Int32)));
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true));
        let field = Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        let array = Arc::new(ListArray::new(
            inner_field,
            offsets,
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)])) as ArrayRef,
            None,
        )) as ArrayRef;
        let expected = vec![
            // Offsets: [0, 2, 3, 5] (4 * 8 = 32 bytes, u64, little-endian)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Null mask for values: [0, 1, 0, 1, 0] (5 * 1 = 5 bytes, 0=not null, 1=null)
            0, 1, 0, 1, 0,
            // Values: [1, 0, 3, 0, 5] (5 * 4 = 20 bytes, i32, little-endian, nulls as 0)
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // 0 (null)
            3, 0, 0, 0, // 3
            0, 0, 0, 0, // 0 (null)
            5, 0, 0, 0, // 5
        ];
        test_type_serializer(&expected, &type_, &field, &array).await;
    }

    #[tokio::test]
    async fn test_serialize_list_nullable_string() {
        let type_ = wrap_array(Type::Nullable(Box::new(Type::String)));
        let values = Arc::new(StringArray::from(vec![Some("even"), Some("odd"), None, Some("odd")]))
            as ArrayRef;
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 4].into());
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Utf8, true));
        let field = Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        let list_array = ListArray::new(inner_field, offsets, values, None);
        let array = Arc::new(list_array) as ArrayRef;
        let expected = vec![
            // Offsets: [2, 3, 4]
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            4, 0, 0, 0, 0, 0, 0, 0, // 4
            // Null mask: [0, 0, 1, 0]
            0, 0, 1, 0,
            // Non-null values: ["even", "odd", "odd"] (var_uint length + string bytes)
            4, // var_uint length: 4 (1 byte)
            b'e', b'v', b'e', b'n', // "even" (4 bytes)
            3,    // var_uint length: 3 (1 byte)
            b'o', b'd', b'd', // "odd" (3 bytes)
            0,    // var_uint length: 0 (1 byte, null as empty string)
            3,    // var_uint length: 3 (1 byte)
            b'o', b'd', b'd', // "odd" (3 bytes)
        ];
        test_type_serializer(&expected, &type_, &field, &array).await;
    }

    #[tokio::test]
    async fn test_serialize_fixed_size_list_int32() {
        let type_ = wrap_array(Type::Int32);
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef;
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let field = Arc::new(Field::new(
            "list",
            DataType::FixedSizeList(Arc::clone(&inner_field), 2),
            false,
        ));
        let list_array = FixedSizeListArray::new(inner_field, 2, values, None);
        let array = Arc::new(list_array) as ArrayRef;
        let expected = vec![
            // Offsets: [0, 2, 4, 6] (u64, little-endian)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            4, 0, 0, 0, 0, 0, 0, 0, // 4
            6, 0, 0, 0, 0, 0, 0, 0, // 6
            // Values: [1, 2, 3, 4, 5, 6] (i32, little-endian)
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
            6, 0, 0, 0, // 6
        ];
        test_type_serializer(&expected, &type_, &field, &array).await;
    }

    #[tokio::test]
    async fn test_serialize_list_zero_rows() {
        let type_ = wrap_array(Type::Int32);
        let values = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let offsets = OffsetBuffer::new(vec![0].into());
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let field = Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        let list_array = ListArray::new(inner_field, offsets, values, None);
        let array = Arc::new(list_array) as ArrayRef;
        let expected: Vec<u8> = vec![
            // Offsets: [0] (u64, little-endian)
            /* No values written */
        ];
        test_type_serializer(&expected, &type_, &field, &array).await;
    }

    #[tokio::test]
    async fn test_serialize_list_empty_inner() {
        let type_ = wrap_array(Type::Int32);
        let values = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let offsets = OffsetBuffer::new(vec![0, 0].into());
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let field = Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        let list_array = ListArray::new(inner_field, offsets, values, None);
        let array = Arc::new(list_array) as ArrayRef;
        let expected = vec![
            // Offsets: [0, 0] (u64, little-endian)
            0, 0, 0, 0, 0, 0, 0, 0, /* 0
               * No values written */
        ];
        test_type_serializer(&expected, &type_, &field, &array).await;
    }

    #[tokio::test]
    async fn test_serialize_nested_list_int32() {
        let type_ = wrap_array(Type::Array(Box::new(Type::Int32)));
        // Inner ListArray: [[1, 2], [3], [4, 5]]
        let inner_values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;
        let inner_offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let inner_inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let inner_field = Arc::new(Field::new(
            "inner_list",
            DataType::List(Arc::clone(&inner_inner_field)),
            false,
        ));
        let inner_list_array = ListArray::new(inner_inner_field, inner_offsets, inner_values, None);
        let outer_field =
            Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        // Outer ListArray: [[[1, 2], [3]], [[4, 5]]]
        let array = Arc::new(ListArray::new(
            inner_field,
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(inner_list_array) as ArrayRef,
            None,
        )) as ArrayRef;
        let expected = vec![
            // Outer offsets: [0, 2, 3] (u64, little-endian)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            // Inner offsets: [0, 2, 3, 5] (u64, little-endian)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5] (i32, little-endian)
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        test_type_serializer(&expected, &type_, &outer_field, &array).await;
    }

    #[tokio::test]
    async fn test_serialize_array_nullable_low_cardinality_string() {
        let type_ =
            wrap_array(Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String)))));
        let field = Arc::new(Field::new(
            "array_low_cardinality_string_col",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        ));
        let array = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new(
                    "item",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                )),
                OffsetBuffer::new(vec![0, 2, 2, 3, 4, 6].into()),
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(vec![
                            Some(0),
                            Some(1), // Row 1: ["low", "card"]
                            Some(2), // Row 3: ["test"]
                            None,    // Row 4: [null]
                            Some(0),
                            None, // Row 5: ["low", null]
                        ]),
                        Arc::new(StringArray::from(vec!["low", "card", "test"])),
                    )
                    .unwrap(),
                ),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let expected = vec![
            2, 0, 0, 0, 0, 0, 0, 0, // Offset
            2, 0, 0, 0, 0, 0, 0, 0, // Offset
            3, 0, 0, 0, 0, 0, 0, 0, // Offset
            4, 0, 0, 0, 0, 0, 0, 0, // Offset
            6, 0, 0, 0, 0, 0, 0, 0, // Offset
            // LowCardinality
            0, 2, 0, 0, 0, 0, 0, 0, // Flags
            4, 0, 0, 0, 0, 0, 0, 0, // Dict length
            0, // Null value
            3, 108, 111, 119, // Dict value
            4, 99, 97, 114, 100, // Dict value
            4, 116, 101, 115, 116, // Dict value
            6, 0, 0, 0, 0, 0, 0, 0, // Key length
            1, 2, 3, 0, 1, 0, // Key indicies
        ];
        test_type_serializer(&expected, &type_, &field, &array).await;
    }

    #[test]
    fn test_serialize_list_int32_sync() {
        let type_ = wrap_array(Type::Int32);
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let field = Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        let array = Arc::new(ListArray::new(
            inner_field,
            OffsetBuffer::new(vec![0, 2, 3, 5].into()),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            None,
        )) as ArrayRef;

        let expected = vec![
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        test_type_serializer_sync(&expected, &type_, &field, &array);
    }

    #[test]
    fn test_serialize_list_nullable_int32_sync() {
        let type_ = wrap_array(Type::Nullable(Box::new(Type::Int32)));
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true));
        let field = Arc::new(Field::new("list", DataType::List(Arc::clone(&inner_field)), false));
        let array = Arc::new(ListArray::new(
            inner_field,
            offsets,
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)])) as ArrayRef,
            None,
        )) as ArrayRef;
        let expected = vec![
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            0, 1, 0, 1, 0, // null mask
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // 0 (null)
            3, 0, 0, 0, // 3
            0, 0, 0, 0, // 0 (null)
            5, 0, 0, 0, // 5
        ];
        test_type_serializer_sync(&expected, &type_, &field, &array);
    }

    #[test]
    fn test_serialize_list_invalid_type_sync() {
        let type_ = wrap_array(Type::Int32);
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();
        let result = serialize(&type_, &mut writer, &column, &DataType::Int32, &mut state);
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(msg))
            if msg.contains("Expected ListArray or FixedSizeListArray")
        ));
    }
}
