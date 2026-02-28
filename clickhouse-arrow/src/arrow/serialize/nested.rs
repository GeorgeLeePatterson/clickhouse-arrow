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
        fields.iter().zip(arrow_fields.iter()).zip(nested_values.into_iter())
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
        fields.iter().zip(arrow_fields.iter()).zip(nested_values.into_iter())
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

    use arrow::array::{Int32Array, ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
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
}
