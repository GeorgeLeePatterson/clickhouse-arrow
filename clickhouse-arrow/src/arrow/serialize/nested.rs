use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeListArray, ListArray, StructArray};
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use super::ClickHouseArrowSerializer;
use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

#[expect(clippy::too_many_lines)]
#[expect(clippy::cast_sign_loss)]
#[expect(clippy::cast_possible_truncation)]
pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    let Type::Nested(fields) = type_hint.strip_null() else {
        return Err(Error::ArrowSerialize(format!(
            "Expected Nested type for nested serialization, got {type_hint:?}",
        )));
    };

    let struct_array = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected StructArray for Nested type".into()))?;

    let DataType::Struct(arrow_fields) = data_type else {
        return Err(Error::ArrowSerialize("Expected Struct data type for Nested type".into()));
    };

    if fields.len() != arrow_fields.len() {
        return Err(Error::ArrowSerialize(format!(
            "Nested field count mismatch: nested={}, arrow={}",
            fields.len(),
            arrow_fields.len()
        )));
    }

    if fields.is_empty() {
        return Err(Error::ArrowSerialize(
            "Nested type must contain at least one field".to_string(),
        ));
    }

    let rows = struct_array.len();
    let mut shared_offsets: Option<Vec<u64>> = None;
    let mut nested_values = Vec::with_capacity(fields.len());

    for (i, ((nested_name, _inner_type), arrow_field)) in
        fields.iter().zip(arrow_fields.iter()).enumerate()
    {
        if arrow_field.name() != nested_name {
            return Err(Error::ArrowSerialize(format!(
                "Nested field name mismatch at index {i}: nested='{nested_name}', arrow='{}'",
                arrow_field.name()
            )));
        }

        let nested_column = struct_array.column(i);
        let (offsets, values) = match arrow_field.data_type() {
            DataType::List(_item) => {
                let list = nested_column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' expected ListArray, found {:?}",
                        nested_column.data_type()
                    ))
                })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                (
                    list.value_offsets().iter().map(|offset| *offset as u64).collect::<Vec<_>>(),
                    Arc::clone(list.values()),
                )
            }
            DataType::LargeList(_item) => {
                let list =
                    nested_column.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' expected LargeListArray, found {:?}",
                            nested_column.data_type()
                        ))
                    })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                let mut offsets = Vec::with_capacity(list.value_offsets().len());
                for offset in list.value_offsets() {
                    let offset = u64::try_from(*offset).map_err(|_| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' contains negative LargeList offset: \
                             {offset}"
                        ))
                    })?;
                    offsets.push(offset);
                }
                (offsets, Arc::clone(list.values()))
            }
            other => {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' expected List/LargeList type, got {other:?}"
                )));
            }
        };

        if offsets.len() != rows + 1 {
            return Err(Error::ArrowSerialize(format!(
                "Nested field '{nested_name}' offset length mismatch: {} != {}",
                offsets.len(),
                rows + 1
            )));
        }

        if let Some(shared_offsets) = shared_offsets.as_ref() {
            if shared_offsets != &offsets {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' offsets differ across Nested children"
                )));
            }
        } else {
            shared_offsets = Some(offsets);
        }

        nested_values.push(values);
    }

    let shared_offsets = shared_offsets.expect("Nested fields are checked as non-empty");
    let total_values = shared_offsets.last().copied().unwrap_or_default() as usize;
    for (i, value_array) in nested_values.iter().enumerate() {
        if value_array.len() != total_values {
            return Err(Error::ArrowSerialize(format!(
                "Nested child '{}' values length mismatch: {} != {total_values}",
                fields[i].0,
                value_array.len()
            )));
        }
    }

    for offset in shared_offsets.iter().skip(1) {
        writer.write_u64_le(*offset).await?;
    }

    for (((nested_name, inner_type), arrow_field), values) in
        fields.iter().zip(arrow_fields.iter()).zip(nested_values)
    {
        let item_type = match arrow_field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => item.data_type(),
            other => {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' expected List/LargeList type, got {other:?}"
                )));
            }
        };
        inner_type.serialize_async(writer, &values, item_type, state).await?;
    }

    Ok(())
}

#[expect(clippy::too_many_lines)]
#[expect(clippy::cast_sign_loss)]
#[expect(clippy::cast_possible_truncation)]
pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    let Type::Nested(fields) = type_hint.strip_null() else {
        return Err(Error::ArrowSerialize(format!(
            "Expected Nested type for nested serialization, got {type_hint:?}",
        )));
    };

    let struct_array = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::ArrowSerialize("Expected StructArray for Nested type".into()))?;

    let DataType::Struct(arrow_fields) = data_type else {
        return Err(Error::ArrowSerialize("Expected Struct data type for Nested type".into()));
    };

    if fields.len() != arrow_fields.len() {
        return Err(Error::ArrowSerialize(format!(
            "Nested field count mismatch: nested={}, arrow={}",
            fields.len(),
            arrow_fields.len()
        )));
    }

    if fields.is_empty() {
        return Err(Error::ArrowSerialize(
            "Nested type must contain at least one field".to_string(),
        ));
    }

    let rows = struct_array.len();
    let mut shared_offsets: Option<Vec<u64>> = None;
    let mut nested_values = Vec::with_capacity(fields.len());

    for (i, ((nested_name, _inner_type), arrow_field)) in
        fields.iter().zip(arrow_fields.iter()).enumerate()
    {
        if arrow_field.name() != nested_name {
            return Err(Error::ArrowSerialize(format!(
                "Nested field name mismatch at index {i}: nested='{nested_name}', arrow='{}'",
                arrow_field.name()
            )));
        }

        let nested_column = struct_array.column(i);
        let (offsets, values) = match arrow_field.data_type() {
            DataType::List(_item) => {
                let list = nested_column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' expected ListArray, found {:?}",
                        nested_column.data_type()
                    ))
                })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                (
                    list.value_offsets().iter().map(|offset| *offset as u64).collect::<Vec<_>>(),
                    Arc::clone(list.values()),
                )
            }
            DataType::LargeList(_item) => {
                let list =
                    nested_column.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' expected LargeListArray, found {:?}",
                            nested_column.data_type()
                        ))
                    })?;
                if list.len() != rows {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' row count mismatch: {} != {rows}",
                        list.len()
                    )));
                }
                if list.null_count() != 0 {
                    return Err(Error::ArrowSerialize(format!(
                        "Nested field '{nested_name}' contains null lists; Nested requires \
                         non-null arrays"
                    )));
                }
                let mut offsets = Vec::with_capacity(list.value_offsets().len());
                for offset in list.value_offsets() {
                    let offset = u64::try_from(*offset).map_err(|_| {
                        Error::ArrowSerialize(format!(
                            "Nested field '{nested_name}' contains negative LargeList offset: \
                             {offset}"
                        ))
                    })?;
                    offsets.push(offset);
                }
                (offsets, Arc::clone(list.values()))
            }
            other => {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' expected List/LargeList type, got {other:?}"
                )));
            }
        };

        if offsets.len() != rows + 1 {
            return Err(Error::ArrowSerialize(format!(
                "Nested field '{nested_name}' offset length mismatch: {} != {}",
                offsets.len(),
                rows + 1
            )));
        }

        if let Some(shared_offsets) = shared_offsets.as_ref() {
            if shared_offsets != &offsets {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' offsets differ across Nested children"
                )));
            }
        } else {
            shared_offsets = Some(offsets);
        }

        nested_values.push(values);
    }

    let shared_offsets = shared_offsets.expect("Nested fields are checked as non-empty");
    let total_values = shared_offsets.last().copied().unwrap_or_default() as usize;
    for (i, value_array) in nested_values.iter().enumerate() {
        if value_array.len() != total_values {
            return Err(Error::ArrowSerialize(format!(
                "Nested child '{}' values length mismatch: {} != {total_values}",
                fields[i].0,
                value_array.len()
            )));
        }
    }

    for offset in shared_offsets.iter().skip(1) {
        writer.put_u64_le(*offset);
    }

    for (((nested_name, inner_type), arrow_field), values) in
        fields.iter().zip(arrow_fields.iter()).zip(nested_values)
    {
        let item_type = match arrow_field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => item.data_type(),
            other => {
                return Err(Error::ArrowSerialize(format!(
                    "Nested field '{nested_name}' expected List/LargeList type, got {other:?}"
                )));
            }
        };
        inner_type.serialize(writer, &values, item_type, state)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Int32Array, LargeListArray, ListArray, StringArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields};

    use super::*;
    use crate::formats::SerializerState;

    fn nested_type() -> Type {
        Type::Nested(vec![("name".to_string(), Type::String), ("score".to_string(), Type::Int32)])
    }

    fn nested_data_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new(
                "name",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "score",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]))
    }

    fn nested_nullable_data_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new(
                "name",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                true,
            ),
            Field::new(
                "score",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]))
    }

    fn nested_column() -> ArrayRef {
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into());
        let names = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            offsets.clone(),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
            None,
        )) as ArrayRef;
        let scores = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            offsets,
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            None,
        )) as ArrayRef;
        let DataType::Struct(fields) = nested_data_type() else {
            unreachable!();
        };

        Arc::new(StructArray::new(fields, vec![names, scores], None)) as ArrayRef
    }

    fn nested_large_data_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new(
                "name",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "score",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]))
    }

    fn nested_large_column() -> ArrayRef {
        let offsets = OffsetBuffer::new(vec![0_i64, 2, 2, 3].into());
        let names = Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            offsets.clone(),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
            None,
        )) as ArrayRef;
        let scores = Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            offsets,
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            None,
        )) as ArrayRef;
        let DataType::Struct(fields) = nested_large_data_type() else {
            unreachable!();
        };

        Arc::new(StructArray::new(fields, vec![names, scores], None)) as ArrayRef
    }

    fn nested_column_with_null_lists() -> ArrayRef {
        let offsets = OffsetBuffer::new(vec![0, 1, 1, 2].into());
        let names = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            offsets.clone(),
            Arc::new(StringArray::from(vec!["alice", "carol"])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )) as ArrayRef;
        let scores = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            offsets,
            Arc::new(Int32Array::from(vec![10, 30])) as ArrayRef,
            None,
        )) as ArrayRef;
        let DataType::Struct(fields) = nested_nullable_data_type() else {
            unreachable!();
        };

        Arc::new(StructArray::new(fields, vec![names, scores], None)) as ArrayRef
    }

    fn empty_nested_type() -> Type { Type::Nested(vec![]) }

    fn empty_nested_data_type() -> DataType { DataType::Struct(Fields::from(Vec::<Field>::new())) }

    fn empty_nested_column() -> ArrayRef {
        Arc::new(StructArray::new_empty_fields(0, None)) as ArrayRef
    }

    #[tokio::test]
    async fn test_serialize_nested_async_matches_sync() {
        let type_hint = nested_type();
        let data_type = nested_data_type();
        let column = nested_column();

        let mut async_writer = Cursor::new(Vec::new());
        serialize_async(
            &type_hint,
            &mut async_writer,
            &column,
            &data_type,
            &mut SerializerState::default(),
        )
        .await
        .unwrap();

        let mut sync_writer = Vec::new();
        serialize(
            &type_hint,
            &mut sync_writer,
            &column,
            &data_type,
            &mut SerializerState::default(),
        )
        .unwrap();

        assert_eq!(async_writer.into_inner(), sync_writer);
    }

    #[tokio::test]
    async fn test_serialize_nested_large_list_async_matches_sync() {
        let type_hint = nested_type();
        let data_type = nested_large_data_type();
        let column = nested_large_column();

        let mut async_writer = Cursor::new(Vec::new());
        serialize_async(
            &type_hint,
            &mut async_writer,
            &column,
            &data_type,
            &mut SerializerState::default(),
        )
        .await
        .unwrap();

        let mut sync_writer = Vec::new();
        serialize(
            &type_hint,
            &mut sync_writer,
            &column,
            &data_type,
            &mut SerializerState::default(),
        )
        .unwrap();

        assert_eq!(async_writer.into_inner(), sync_writer);
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_non_nested_type() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &Type::Tuple(vec![]),
            &mut writer,
            &nested_column(),
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Expected Nested type"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_non_struct_column() {
        let mut writer = Cursor::new(Vec::new());
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &column,
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Expected StructArray"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_non_struct_data_type() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &DataType::Utf8,
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Expected Struct data type"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_field_count_mismatch() {
        let mut writer = Cursor::new(Vec::new());
        let DataType::Struct(fields) = nested_data_type() else {
            unreachable!();
        };
        let smaller = DataType::Struct(Fields::from(vec![Arc::clone(&fields[0])]));
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &smaller,
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("field count mismatch"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_name_mismatch() {
        let mut writer = Cursor::new(Vec::new());
        let renamed = DataType::Struct(Fields::from(vec![
            Field::new(
                "not_name",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "score",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]));
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &renamed,
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("field name mismatch"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_non_list_child() {
        let mut writer = Cursor::new(Vec::new());
        let non_list = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "score",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]));
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &non_list,
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("expected List/LargeList type"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_offsets_mismatch() {
        let names_offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into());
        let scores_offsets = OffsetBuffer::new(vec![0, 1, 2, 3].into());
        let names = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            names_offsets,
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
            None,
        )) as ArrayRef;
        let scores = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            scores_offsets,
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            None,
        )) as ArrayRef;
        let DataType::Struct(fields) = nested_data_type() else {
            unreachable!();
        };
        let column = Arc::new(StructArray::new(fields, vec![names, scores], None)) as ArrayRef;

        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &column,
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("offsets differ"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_null_list_entries() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &nested_column_with_null_lists(),
            &nested_nullable_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("contains null lists"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_empty_nested_fields() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &empty_nested_type(),
            &mut writer,
            &empty_nested_column(),
            &empty_nested_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("at least one field"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_largelist_column_with_list_type() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &nested_large_column(),
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("expected ListArray"));
    }

    #[tokio::test]
    async fn test_serialize_nested_rejects_list_column_with_largelist_type() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &nested_large_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("expected LargeListArray"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_non_nested_type() {
        let mut writer = Vec::new();
        let error = serialize(
            &Type::Tuple(vec![]),
            &mut writer,
            &nested_column(),
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("Expected Nested type"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_non_struct_column() {
        let mut writer = Vec::new();
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let error = serialize(
            &nested_type(),
            &mut writer,
            &column,
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("Expected StructArray"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_non_struct_data_type() {
        let mut writer = Vec::new();
        let error = serialize(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &DataType::Utf8,
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("Expected Struct data type"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_field_count_mismatch() {
        let mut writer = Vec::new();
        let DataType::Struct(fields) = nested_data_type() else {
            unreachable!();
        };
        let smaller = DataType::Struct(Fields::from(vec![Arc::clone(&fields[0])]));
        let error = serialize(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &smaller,
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("field count mismatch"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_name_mismatch() {
        let mut writer = Vec::new();
        let renamed = DataType::Struct(Fields::from(vec![
            Field::new(
                "not_name",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "score",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]));
        let error = serialize(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &renamed,
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("field name mismatch"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_non_list_child() {
        let mut writer = Vec::new();
        let non_list = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "score",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]));
        let error = serialize(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &non_list,
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("expected List/LargeList type"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_offsets_mismatch() {
        let names_offsets = OffsetBuffer::new(vec![0, 2, 2, 3].into());
        let scores_offsets = OffsetBuffer::new(vec![0, 1, 2, 3].into());
        let names = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            names_offsets,
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
            None,
        )) as ArrayRef;
        let scores = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            scores_offsets,
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            None,
        )) as ArrayRef;
        let DataType::Struct(fields) = nested_data_type() else {
            unreachable!();
        };
        let column = Arc::new(StructArray::new(fields, vec![names, scores], None)) as ArrayRef;

        let mut writer = Vec::new();
        let error = serialize(
            &nested_type(),
            &mut writer,
            &column,
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("offsets differ"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_null_list_entries() {
        let mut writer = Vec::new();
        let error = serialize(
            &nested_type(),
            &mut writer,
            &nested_column_with_null_lists(),
            &nested_nullable_data_type(),
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("contains null lists"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_empty_nested_fields() {
        let mut writer = Vec::new();
        let error = serialize(
            &empty_nested_type(),
            &mut writer,
            &empty_nested_column(),
            &empty_nested_data_type(),
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("at least one field"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_largelist_column_with_list_type() {
        let mut writer = Vec::new();
        let error = serialize(
            &nested_type(),
            &mut writer,
            &nested_large_column(),
            &nested_data_type(),
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("expected ListArray"));
    }

    #[test]
    fn test_serialize_nested_sync_rejects_list_column_with_largelist_type() {
        let mut writer = Vec::new();
        let error = serialize(
            &nested_type(),
            &mut writer,
            &nested_column(),
            &nested_large_data_type(),
            &mut SerializerState::default(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("expected LargeListArray"));
    }
}
