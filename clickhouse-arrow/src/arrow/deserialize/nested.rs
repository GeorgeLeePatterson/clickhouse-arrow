use std::sync::Arc;

use arrow::array::{ArrayRef, LargeListArray, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use tokio::io::AsyncReadExt;

use super::{ArrowFieldCtx, ClickHouseArrowDeserializer, list};
use crate::arrow::builder::TypedBuilder;
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

#[expect(clippy::too_many_lines)]
#[expect(clippy::cast_possible_truncation)]
pub(super) async fn deserialize<R: ClickHouseRead>(
    fields: &[(String, Type)],
    builder: &mut TypedBuilder,
    data_type: &DataType,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    ctx: &mut ArrowFieldCtx<'_>,
) -> Result<ArrayRef> {
    let DataType::Struct(arrow_fields) = data_type else {
        return Err(Error::ArrowDeserialize(format!(
            "Nested expects Struct datatype, got {data_type:?}"
        )));
    };

    let TypedBuilder::Tuple(builders) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected Nested builder: {}",
            builder.as_ref()
        )));
    };
    let tuple_builder_len = builders.len();

    if fields.len() != arrow_fields.len() || tuple_builder_len != fields.len() {
        return Err(Error::ArrowDeserialize(format!(
            "Nested field count mismatch: type={}, arrow={}, builder={}",
            fields.len(),
            arrow_fields.len(),
            tuple_builder_len
        )));
    }

    let offset_bytes = list::bulk_offsets!(reader, ctx.row_buffer, rows);
    let outer_offsets = bytemuck::cast_slice::<u8, u64>(&ctx.row_buffer[..offset_bytes]).to_vec();
    if outer_offsets.len() != rows + 1 {
        return Err(Error::ArrowDeserialize(format!(
            "Nested offset length mismatch: {} != {}",
            outer_offsets.len(),
            rows + 1
        )));
    }
    let total_values = outer_offsets.last().copied().unwrap_or_default() as usize;

    let list_offsets32 = outer_offsets
        .iter()
        .map(|offset| {
            i32::try_from(*offset).map_err(|_| {
                Error::ArrowDeserialize(format!("Nested offset exceeds Int32 list limit: {offset}"))
            })
        })
        .collect::<Result<ScalarBuffer<i32>>>()?;
    let list_offsets64 = outer_offsets
        .iter()
        .map(|offset| {
            i64::try_from(*offset).map_err(|_| {
                Error::ArrowDeserialize(format!("Nested offset exceeds Int64 list limit: {offset}"))
            })
        })
        .collect::<Result<ScalarBuffer<i64>>>()?;

    let mut arrays = Vec::with_capacity(fields.len());
    for (idx, ((nested_name, inner_type), arrow_field)) in
        fields.iter().zip(arrow_fields.iter()).enumerate()
    {
        if arrow_field.name() != nested_name {
            return Err(Error::ArrowDeserialize(format!(
                "Nested field name mismatch: expected '{nested_name}', got '{}'",
                arrow_field.name()
            )));
        }

        let child_builder = builders.get_mut(idx).ok_or_else(|| {
            Error::ArrowDeserialize(format!(
                "Nested builder missing child at index {idx}; expected {} children",
                fields.len()
            ))
        })?;

        let values: ArrayRef = match arrow_field.data_type() {
            DataType::List(item) => {
                let values = inner_type
                    .deserialize_arrow(
                        child_builder,
                        reader,
                        item.data_type(),
                        total_values,
                        &[],
                        ctx,
                    )
                    .await?;
                if values.len() != total_values {
                    return Err(Error::ArrowDeserialize(format!(
                        "Nested child '{}' length mismatch: {} != {total_values}",
                        fields[idx].0,
                        values.len()
                    )));
                }
                Arc::new(ListArray::new(
                    Arc::clone(item),
                    OffsetBuffer::new(list_offsets32.clone()),
                    values,
                    None,
                ))
            }
            DataType::LargeList(item) => {
                let values = inner_type
                    .deserialize_arrow(
                        child_builder,
                        reader,
                        item.data_type(),
                        total_values,
                        &[],
                        ctx,
                    )
                    .await?;
                if values.len() != total_values {
                    return Err(Error::ArrowDeserialize(format!(
                        "Nested child '{}' length mismatch: {} != {total_values}",
                        fields[idx].0,
                        values.len()
                    )));
                }
                Arc::new(LargeListArray::new(
                    Arc::clone(item),
                    OffsetBuffer::new(list_offsets64.clone()),
                    values,
                    None,
                ))
            }
            other => {
                return Err(Error::ArrowDeserialize(format!(
                    "Nested field '{}' expected List/LargeList datatype, got {other:?}",
                    fields[idx].0
                )));
            }
        };
        arrays.push(values);
    }

    let null_buffer = (!nulls.is_empty())
        .then_some(NullBuffer::from(nulls.iter().map(|value| *value == 0).collect::<Vec<_>>()));
    Ok(Arc::new(StructArray::new(arrow_fields.clone(), arrays, null_buffer)))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, LargeListArray, ListArray, StringArray};
    use arrow::datatypes::{Field, Fields};

    use super::*;

    fn nested_type_fields() -> Vec<(String, Type)> {
        vec![("name".to_string(), Type::String), ("score".to_string(), Type::Int32)]
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

    #[tokio::test]
    async fn test_deserialize_nested_column() {
        // Offsets [0, 2, 2, 3], then tuple payload:
        // names=["alice","bob","carol"], scores=[10,20,30]
        let mut input = Vec::new();
        input.extend_from_slice(&2_u64.to_le_bytes());
        input.extend_from_slice(&2_u64.to_le_bytes());
        input.extend_from_slice(&3_u64.to_le_bytes());
        input.extend_from_slice(&[5_u8, b'a', b'l', b'i', b'c', b'e']);
        input.extend_from_slice(&[3_u8, b'b', b'o', b'b']);
        input.extend_from_slice(&[5_u8, b'c', b'a', b'r', b'o', b'l']);
        input.extend_from_slice(&10_i32.to_le_bytes());
        input.extend_from_slice(&20_i32.to_le_bytes());
        input.extend_from_slice(&30_i32.to_le_bytes());

        let fields = nested_type_fields();
        let data_type = nested_data_type();
        let mut builder = TypedBuilder::try_new(&Type::Nested(fields.clone()), &data_type).unwrap();
        let mut reader = Cursor::new(input);
        let mut row_buffer = Vec::new();
        let array = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            3,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap();

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let name_list = struct_array.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let score_list = struct_array.column(1).as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(name_list.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 2, 3]);
        assert_eq!(score_list.offsets().iter().copied().collect::<Vec<_>>(), vec![0, 2, 2, 3]);

        let names = name_list.values().as_any().downcast_ref::<StringArray>().unwrap();
        let scores = score_list.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(names, &StringArray::from(vec!["alice", "bob", "carol"]));
        assert_eq!(scores, &Int32Array::from(vec![10, 20, 30]));
    }

    #[tokio::test]
    async fn test_deserialize_nested_with_outer_nulls() {
        let mut input = Vec::new();
        input.extend_from_slice(&1_u64.to_le_bytes());
        input.extend_from_slice(&2_u64.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'a']);
        input.extend_from_slice(&[1_u8, b'b']);
        input.extend_from_slice(&10_i32.to_le_bytes());
        input.extend_from_slice(&20_i32.to_le_bytes());

        let fields = nested_type_fields();
        let data_type = nested_data_type();
        let mut builder = TypedBuilder::try_new(&Type::Nested(fields.clone()), &data_type).unwrap();
        let mut reader = Cursor::new(input);
        let mut row_buffer = Vec::new();
        let array = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            2,
            &[0_u8, 1_u8],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap();

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.nulls().unwrap().iter().collect::<Vec<_>>(), vec![true, false]);
    }

    #[tokio::test]
    async fn test_deserialize_nested_rejects_non_struct_data_type() {
        let fields = nested_type_fields();
        let mut builder = TypedBuilder::try_new(&Type::Int32, &DataType::Int32).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();
        let error = deserialize(
            &fields,
            &mut builder,
            &DataType::Int32,
            &mut reader,
            0,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("expects Struct datatype"));
    }

    #[tokio::test]
    async fn test_deserialize_nested_rejects_non_tuple_builder() {
        let fields = nested_type_fields();
        let data_type = nested_data_type();
        let mut builder = TypedBuilder::try_new(&Type::Int32, &DataType::Int32).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();
        let error = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            0,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Unexpected Nested builder"));
    }

    #[tokio::test]
    async fn test_deserialize_nested_rejects_field_count_mismatch() {
        let fields = nested_type_fields();
        let data_type = nested_data_type();
        let mut builder = TypedBuilder::try_new(
            &Type::Tuple(vec![(Some("name".to_string()), Type::String)]),
            &DataType::Struct(Fields::from(vec![Field::new(
                "name",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            )])),
        )
        .unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();
        let error = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            0,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("field count mismatch"));
    }

    #[tokio::test]
    async fn test_deserialize_nested_rejects_offset_length_mismatch() {
        let fields = nested_type_fields();
        let data_type = nested_data_type();
        let mut builder = TypedBuilder::try_new(&Type::Nested(fields.clone()), &data_type).unwrap();
        // rows=2 expects 2 offsets from wire plus implicit 0 => len should be 3.
        let mut reader = Cursor::new(vec![1_u64.to_le_bytes(), 2_u64.to_le_bytes()].concat());
        let mut row_buffer = Vec::new();
        let error = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            2,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        drop(error);
    }

    #[tokio::test]
    async fn test_deserialize_nested_rejects_name_mismatch() {
        let fields =
            vec![("bad_name".to_string(), Type::String), ("score".to_string(), Type::Int32)];
        let type_fields = nested_type_fields();
        let data_type = nested_data_type();
        let mut builder =
            TypedBuilder::try_new(&Type::Nested(type_fields.clone()), &data_type).unwrap();
        let mut reader = Cursor::new(
            vec![0_u64.to_le_bytes(), 0_u64.to_le_bytes(), 0_u64.to_le_bytes()].concat(),
        );
        let mut row_buffer = Vec::new();
        let error = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            3,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("field name mismatch"));
    }

    #[tokio::test]
    async fn test_deserialize_nested_rejects_non_list_child_data_type() {
        let fields = nested_type_fields();
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "score",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
        ]));
        let mut builder = TypedBuilder::try_new(
            &Type::Tuple(vec![
                (Some("name".to_string()), Type::String),
                (Some("score".to_string()), Type::Int32),
            ]),
            &data_type,
        )
        .unwrap();
        let mut reader = Cursor::new(vec![0_u64.to_le_bytes(), 0_u64.to_le_bytes()].concat());
        let mut row_buffer = Vec::new();
        let error = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            2,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("expected List/LargeList datatype"));
    }

    #[tokio::test]
    async fn test_deserialize_nested_large_list() {
        let fields = nested_type_fields();
        let data_type = DataType::Struct(Fields::from(vec![
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
        ]));
        let mut builder = TypedBuilder::try_new(
            &Type::Tuple(vec![
                (Some("name".to_string()), Type::String),
                (Some("score".to_string()), Type::Int32),
            ]),
            &data_type,
        )
        .unwrap();

        let mut input = Vec::new();
        input.extend_from_slice(&1_u64.to_le_bytes());
        input.extend_from_slice(&2_u64.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'a']);
        input.extend_from_slice(&[1_u8, b'b']);
        input.extend_from_slice(&10_i32.to_le_bytes());
        input.extend_from_slice(&20_i32.to_le_bytes());
        let mut reader = Cursor::new(input);
        let mut row_buffer = Vec::new();

        let array = deserialize(
            &fields,
            &mut builder,
            &data_type,
            &mut reader,
            2,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap();

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(struct_array.column(0).as_any().downcast_ref::<LargeListArray>().is_some());
    }
}
