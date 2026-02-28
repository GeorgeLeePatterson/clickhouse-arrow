use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UnionArray, new_empty_array, new_null_array};
use arrow::compute::{concat, filter};
use arrow::datatypes::{DataType, UnionMode};
use tokio::io::AsyncReadExt;

use crate::arrow::builder::TypedBuilder;
use crate::arrow::deserialize::{ArrowFieldCtx, ClickHouseArrowDeserializer};
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

#[expect(clippy::too_many_lines)]
#[expect(clippy::cast_possible_wrap)]
#[expect(clippy::cast_possible_truncation)]
pub(super) async fn deserialize<R: ClickHouseRead>(
    type_hint: &Type,
    builder: &mut TypedBuilder,
    reader: &mut R,
    data_type: &DataType,
    rows: usize,
    nulls: &[u8],
    ctx: &mut ArrowFieldCtx<'_>,
) -> Result<ArrayRef> {
    if !matches!(type_hint.strip_null(), Type::Dynamic { .. }) {
        return Err(Error::ArrowDeserialize(format!(
            "Dynamic deserializer called with non-Dynamic type: {type_hint}"
        )));
    }

    let DataType::Union(union_schema_fields, UnionMode::Dense) = data_type else {
        return Err(Error::ArrowDeserialize(format!(
            "Dynamic strict mode expects Arrow DenseUnion, found {data_type:?}"
        )));
    };

    let TypedBuilder::Union(union_builder) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected builder for Dynamic: {}",
            builder.as_ref()
        )));
    };

    let Some(prefix) = ctx.dynamic_prefix.take() else {
        return Err(Error::ArrowDeserialize(
            "Dynamic strict union deserialization requires prefix metadata".to_string(),
        ));
    };
    if prefix.serialization_version != 3 {
        return Err(Error::ArrowDeserialize(format!(
            "Dynamic strict union only supports flattened serialization version 3, got {}",
            prefix.serialization_version
        )));
    }

    if union_schema_fields.len() != prefix.flattened_types.len()
        || union_builder.children.len() != prefix.flattened_types.len()
    {
        return Err(Error::ArrowDeserialize(format!(
            "Dynamic strict union field count mismatch: schema={}, builder={}, prefix={}",
            union_schema_fields.len(),
            union_builder.children.len(),
            prefix.flattened_types.len()
        )));
    }
    let flattened_types = &prefix.flattened_types;
    if flattened_types.is_empty() {
        return Err(Error::ArrowDeserialize(
            "Dynamic strict union must contain at least one child field".to_string(),
        ));
    }

    let mut null_field_idx = None;
    for (idx, logical_type) in flattened_types.iter().enumerate() {
        if null_field_idx.is_none() && matches!(logical_type.strip_null(), Type::Nothing) {
            null_field_idx = Some(idx);
        }
    }
    let null_field_idx = null_field_idx.unwrap_or(0);

    let total_types = flattened_types.len();
    let null_discriminator = total_types;
    let discriminator_width = if u8::try_from(total_types).is_ok() {
        1
    } else if u16::try_from(total_types).is_ok() {
        2
    } else if u32::try_from(total_types).is_ok() {
        4
    } else {
        8
    };
    let has_outer_nulls = !nulls.is_empty();
    if has_outer_nulls && nulls.len() != rows {
        return Err(Error::ArrowDeserialize(format!(
            "Dynamic outer null mask length mismatch: {} != {rows}",
            nulls.len()
        )));
    }

    let mut source_counts = vec![0_usize; total_types];
    let mut source_keep_mask: Option<Vec<Option<Vec<bool>>>> = None;
    let mut value_offsets = vec![0_usize; total_types];
    let mut null_rows = 0_usize;
    let mut type_ids = Vec::with_capacity(rows);
    let mut offsets = Vec::with_capacity(rows);

    for outer_null in nulls.iter().copied().chain(std::iter::repeat(0_u8)).take(rows) {
        #[expect(clippy::cast_possible_truncation)]
        let discriminator = match discriminator_width {
            1 => reader.read_u8().await? as usize,
            2 => reader.read_u16_le().await? as usize,
            4 => reader.read_u32_le().await? as usize,
            8 => reader.read_u64_le().await? as usize,
            _ => {
                return Err(Error::ArrowDeserialize(format!(
                    "Invalid Dynamic discriminator width: {discriminator_width}"
                )));
            }
        };
        if discriminator > null_discriminator {
            return Err(Error::ArrowDeserialize(format!(
                "Dynamic discriminator {discriminator} is out of range 0..={null_discriminator}"
            )));
        }

        let is_null_disc = discriminator == null_discriminator;
        let forced_null = outer_null != 0;
        if !is_null_disc {
            let source_idx = discriminator;
            let source_row_idx = source_counts[source_idx];
            source_counts[source_idx] += 1;
            if forced_null {
                let masks = source_keep_mask.get_or_insert_with(|| vec![None; total_types]);
                let keep_mask = masks[source_idx].get_or_insert_with(|| vec![true; source_row_idx]);
                keep_mask.push(false);
            } else if let Some(masks) = source_keep_mask.as_mut()
                && let Some(keep_mask) = masks[source_idx].as_mut()
            {
                keep_mask.push(true);
            }
        }

        let is_null_row = forced_null || is_null_disc;
        if is_null_row {
            type_ids.push(union_builder.children[null_field_idx].type_id);
            offsets.push(null_rows as i32);
            null_rows += 1;
        } else {
            let source_idx = discriminator;
            type_ids.push(union_builder.children[source_idx].type_id);
            let offset = value_offsets[source_idx] as i32;
            if source_idx == null_field_idx {
                offsets.push(-1 - offset);
            } else {
                offsets.push(offset);
            }
            value_offsets[source_idx] += 1;
        }
    }
    #[expect(clippy::cast_possible_wrap)]
    let shift = null_rows as i32;
    for offset in &mut offsets {
        if *offset < 0 {
            *offset = shift + (-1 - *offset);
        }
    }

    let mut children = Vec::with_capacity(total_types);

    for (source_idx, logical_type) in flattened_types.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let keep_mask = source_keep_mask.as_ref().and_then(|masks| masks[source_idx].as_deref());
        let source_array = {
            let (child_data_type, child_builder) =
                union_builder.child_parts_mut(source_idx, logical_type)?;
            if source_rows == 0 {
                new_empty_array(child_data_type)
            } else {
                let mut child_ctx = ArrowFieldCtx {
                    row_buffer:     ctx.row_buffer,
                    dynamic_prefix: None,
                    variant_prefix: None,
                };
                let source_array = Box::pin(logical_type.deserialize_arrow(
                    child_builder,
                    reader,
                    child_data_type,
                    source_rows,
                    &[],
                    &mut child_ctx,
                ))
                .await?;

                if let Some(keep_mask) = keep_mask {
                    if keep_mask.len() != source_rows {
                        return Err(Error::ArrowDeserialize(format!(
                            "Dynamic keep-mask mismatch for '{logical_type}': {} != {source_rows}",
                            keep_mask.len()
                        )));
                    }
                    let keep = keep_mask.iter().copied().map(Some).collect::<BooleanArray>();
                    filter(source_array.as_ref(), &keep)?
                } else {
                    source_array
                }
            }
        };

        if source_array.len() != value_offsets[source_idx] {
            return Err(Error::ArrowDeserialize(format!(
                "Dynamic strict union child '{}' value count mismatch: {} != {}",
                union_builder.children[source_idx].name,
                source_array.len(),
                value_offsets[source_idx]
            )));
        }

        if source_idx == null_field_idx && null_rows > 0 {
            let null_data_type = union_builder.data_type(null_field_idx)?;
            let null_child = new_null_array(null_data_type, null_rows);
            let combined = if source_array.is_empty() {
                null_child
            } else {
                concat(&[null_child.as_ref(), source_array.as_ref()])?
            };
            children.push(combined);
        } else {
            children.push(source_array);
        }
    }

    Ok(Arc::new(UnionArray::try_new(
        union_schema_fields.clone(),
        type_ids.into(),
        Some(offsets.into()),
        children,
    )?))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{Int32Array, NullArray, StringArray};
    use arrow::datatypes::{Field, UnionFields};

    use super::*;
    use crate::formats::DynamicPrefixState;

    fn dynamic_union_data_type(
        fields: impl IntoIterator<Item = (&'static str, DataType)>,
    ) -> DataType {
        let fields = fields.into_iter().collect::<Vec<_>>();
        #[expect(clippy::cast_possible_truncation)]
        DataType::Union(
            UnionFields::new(
                (0..fields.len()).map(|i| i as i8),
                fields.into_iter().map(|(name, data_type)| Field::new(name, data_type, false)),
            ),
            UnionMode::Dense,
        )
    }

    fn dynamic_builder(data_type: &DataType) -> TypedBuilder {
        TypedBuilder::try_new(&Type::Dynamic { max_types: 8 }, data_type).unwrap()
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_rejects_non_union_schema() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let mut builder = TypedBuilder::try_new(&Type::Int32, &DataType::Int32).unwrap();
        let mut reader = Cursor::new(vec![]);

        let err = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &DataType::Binary,
            0,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut vec![],
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("expects Arrow DenseUnion"));
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_flattened_union_protocol_happy_path() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type = dynamic_union_data_type([
            ("Int32", DataType::Int32),
            ("String", DataType::Utf8),
            ("Nothing", DataType::Null),
        ]);
        let mut builder = dynamic_builder(&data_type);

        // Discriminators (u8): [Int32, String, NULL, String, Int32]
        // Child payload order on wire: Int32 column, String column, Nothing column.
        let mut input = vec![0_u8, 1, 3, 1, 0];
        input.extend_from_slice(&10_i32.to_le_bytes());
        input.extend_from_slice(&20_i32.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'a', 1_u8, b'b']);
        let mut reader = Cursor::new(input);

        let mut row_buffer = Vec::new();
        let result = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            5,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: Some(DynamicPrefixState {
                    serialization_version: 3,
                    flattened_types:       vec![Type::Int32, Type::String, Type::Nothing],
                }),
                variant_prefix: None,
            },
        )
        .await
        .unwrap();

        let union = result.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!((0..5).map(|i| union.type_id(i)).collect::<Vec<_>>(), vec![0, 1, 2, 1, 0]);
        assert_eq!((0..5).map(|i| union.value_offset(i)).collect::<Vec<_>>(), vec![0, 0, 0, 1, 1]);

        let ints = union.child(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints, &Int32Array::from(vec![10, 20]));

        let strings = union.child(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings, &StringArray::from(vec!["a", "b"]));

        let nulls = union.child(2).as_any().downcast_ref::<NullArray>().unwrap();
        assert_eq!(nulls.len(), 1);
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_keeps_protocol_order_and_filters_outer_nulls() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type =
            dynamic_union_data_type([("Int32", DataType::Int32), ("String", DataType::Utf8)]);
        let mut builder = dynamic_builder(&data_type);

        // Discriminators (u8): [Int32, String, Int32]
        // Outer null mask forces row 0 to null, so child Int32 row 0 is decoded then filtered out.
        let mut input = vec![0_u8, 1, 0];
        input.extend_from_slice(&11_i32.to_le_bytes());
        input.extend_from_slice(&22_i32.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'x']);
        let mut reader = Cursor::new(input);

        let mut row_buffer = Vec::new();
        let result = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            3,
            &[1_u8, 0, 0],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: Some(DynamicPrefixState {
                    serialization_version: 3,
                    flattened_types:       vec![Type::Int32, Type::String],
                }),
                variant_prefix: None,
            },
        )
        .await
        .unwrap();

        let union = result.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!((0..3).map(|i| union.type_id(i)).collect::<Vec<_>>(), vec![0, 1, 0]);
        assert_eq!((0..3).map(|i| union.value_offset(i)).collect::<Vec<_>>(), vec![0, 0, 1]);

        // Nulls were prepended into child 0; filtered non-null value remains after them.
        let ints = union.child(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints, &Int32Array::from(vec![None, Some(22)]));

        let strings = union.child(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings, &StringArray::from(vec!["x"]));
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_outer_null_mask_treats_any_nonzero_as_null() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type =
            dynamic_union_data_type([("Int32", DataType::Int32), ("String", DataType::Utf8)]);
        let mut builder = dynamic_builder(&data_type);

        let mut input = vec![0_u8, 1, 0];
        input.extend_from_slice(&11_i32.to_le_bytes());
        input.extend_from_slice(&22_i32.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'x']);
        let mut reader = Cursor::new(input);

        let mut row_buffer = Vec::new();
        let result = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            3,
            &[2_u8, 0, 0],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: Some(DynamicPrefixState {
                    serialization_version: 3,
                    flattened_types:       vec![Type::Int32, Type::String],
                }),
                variant_prefix: None,
            },
        )
        .await
        .unwrap();

        let union = result.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!((0..3).map(|i| union.type_id(i)).collect::<Vec<_>>(), vec![0, 1, 0]);
        assert_eq!((0..3).map(|i| union.value_offset(i)).collect::<Vec<_>>(), vec![0, 0, 1]);

        let ints = union.child(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints, &Int32Array::from(vec![None, Some(22)]));

        let strings = union.child(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings, &StringArray::from(vec!["x"]));
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_rejects_outer_null_mask_length_mismatch() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type = dynamic_union_data_type([("Int32", DataType::Int32)]);
        let mut builder = dynamic_builder(&data_type);
        let mut reader = Cursor::new(vec![0_u8]);
        let mut row_buffer = Vec::new();

        let err = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[0_u8, 1_u8],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: Some(DynamicPrefixState {
                    serialization_version: 3,
                    flattened_types:       vec![Type::Int32],
                }),
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("outer null mask length mismatch"));
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_rejects_missing_prefix_metadata() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type = dynamic_union_data_type([("Int32", DataType::Int32)]);
        let mut builder = dynamic_builder(&data_type);
        let mut reader = Cursor::new(vec![0_u8]);
        let mut row_buffer = Vec::new();

        let err = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("requires prefix metadata"));
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_rejects_unsupported_version() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type = dynamic_union_data_type([("Int32", DataType::Int32)]);
        let mut builder = dynamic_builder(&data_type);
        let mut reader = Cursor::new(vec![0_u8]);
        let mut row_buffer = Vec::new();

        let err = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: Some(DynamicPrefixState {
                    serialization_version: 2,
                    flattened_types:       vec![Type::Int32],
                }),
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("only supports flattened serialization version 3"));
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_rejects_discriminator_out_of_range() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type = dynamic_union_data_type([("Int32", DataType::Int32)]);
        let mut builder = dynamic_builder(&data_type);
        // total_types = 1 => null_discriminator = 1, so 2 is out-of-range
        let mut reader = Cursor::new(vec![2_u8]);
        let mut row_buffer = Vec::new();

        let err = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: Some(DynamicPrefixState {
                    serialization_version: 3,
                    flattened_types:       vec![Type::Int32],
                }),
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("out of range"));
    }

    #[tokio::test]
    async fn test_deserialize_dynamic_rejects_field_count_mismatch() {
        let type_hint = Type::Dynamic { max_types: 8 };
        let data_type =
            dynamic_union_data_type([("Int32", DataType::Int32), ("String", DataType::Utf8)]);
        let mut builder = dynamic_builder(&data_type);
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let err = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: Some(DynamicPrefixState {
                    serialization_version: 3,
                    flattened_types:       vec![Type::Int32],
                }),
                variant_prefix: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("field count mismatch"));
    }
}
