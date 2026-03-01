use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UnionArray, new_empty_array, new_null_array};
use arrow::compute::filter;
use arrow::datatypes::{DataType, UnionMode};
use tokio::io::AsyncReadExt;

use crate::arrow::builder::TypedBuilder;
use crate::arrow::deserialize::{ArrowFieldCtx, ClickHouseArrowDeserializer};
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

const NULL_DISCRIMINATOR: u8 = u8::MAX;
const VARIANT_DISCRIMINATOR_MODE_BASIC: u8 = 0;
const VARIANT_DISCRIMINATOR_MODE_COMPACT: u8 = 1;
const VARIANT_COMPACT_GRANULE_FORMAT_PLAIN: u8 = 0;
const VARIANT_COMPACT_GRANULE_FORMAT_COMPACT: u8 = 1;

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
    let Type::Variant(variants) = type_hint.strip_null() else {
        return Err(Error::ArrowDeserialize(format!(
            "Variant deserializer called with non-Variant type: {type_hint}"
        )));
    };

    let DataType::Union(union_schema_fields, UnionMode::Dense) = data_type else {
        return Err(Error::ArrowDeserialize(format!(
            "Variant deserialization expects Arrow DenseUnion, found {data_type:?}"
        )));
    };

    if variants.is_empty() {
        return Err(Error::ArrowDeserialize("Variant requires at least one nested type".into()));
    }

    if union_schema_fields.len() != variants.len() + 1 {
        return Err(Error::ArrowDeserialize(format!(
            "Variant union child count mismatch: schema={}, expected={}",
            union_schema_fields.len(),
            variants.len() + 1
        )));
    }

    if !nulls.is_empty() && nulls.len() != rows {
        return Err(Error::ArrowDeserialize(format!(
            "Variant outer null mask length mismatch: {} != {rows}",
            nulls.len()
        )));
    }

    let TypedBuilder::Union(union_builder) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected builder for Variant: {}",
            builder.as_ref()
        )));
    };
    if union_builder.children.len() != union_schema_fields.len() {
        return Err(Error::ArrowDeserialize(format!(
            "Variant union builder/schema child count mismatch: {} != {}",
            union_builder.children.len(),
            union_schema_fields.len()
        )));
    }

    let null_field_idx = variants.len();
    let discriminator_mode = ctx
        .variant_prefix
        .map_or(VARIANT_DISCRIMINATOR_MODE_BASIC, |state| state.discriminator_mode);
    if discriminator_mode != VARIANT_DISCRIMINATOR_MODE_BASIC
        && discriminator_mode != VARIANT_DISCRIMINATOR_MODE_COMPACT
    {
        return Err(Error::deserialize(format!(
            "unsupported Variant discriminator mode {discriminator_mode}; expected 0 or 1 "
        )));
    }

    let mut source_counts = vec![0_usize; variants.len()];
    let mut source_keep_mask: Option<Vec<Option<Vec<bool>>>> = None;
    let mut value_offsets = vec![0_usize; variants.len()];
    let mut null_rows = 0_usize;
    let mut type_ids = Vec::with_capacity(rows);
    let mut offsets = Vec::with_capacity(rows);
    let mut compact_granule_format = VARIANT_COMPACT_GRANULE_FORMAT_PLAIN;
    let mut compact_granule_discriminator = 0_u8;
    let mut compact_granule_remaining_rows = 0_usize;

    for outer_null in nulls.iter().copied().chain(std::iter::repeat(0_u8)).take(rows) {
        let discriminator = if discriminator_mode == VARIANT_DISCRIMINATOR_MODE_BASIC {
            reader.read_u8().await?
        } else {
            if compact_granule_remaining_rows == 0 {
                let granule_rows =
                    usize::try_from(reader.read_var_uint().await?).map_err(|_| {
                        Error::deserialize(
                            "Variant compact discriminator granule row count exceeds usize",
                        )
                    })?;
                if granule_rows == 0 {
                    return Err(Error::deserialize("Variant compact discriminator granule 0 rows"));
                }

                compact_granule_remaining_rows = granule_rows;
                compact_granule_format = reader.read_u8().await?;
                match compact_granule_format {
                    VARIANT_COMPACT_GRANULE_FORMAT_PLAIN => {}
                    VARIANT_COMPACT_GRANULE_FORMAT_COMPACT => {
                        compact_granule_discriminator = reader.read_u8().await?;
                    }
                    other => {
                        return Err(Error::deserialize(format!(
                            "unsupported Variant compact granule format {other}; expected 0 or 1"
                        )));
                    }
                }
            }

            compact_granule_remaining_rows -= 1;
            if compact_granule_format == VARIANT_COMPACT_GRANULE_FORMAT_COMPACT {
                compact_granule_discriminator
            } else {
                reader.read_u8().await?
            }
        };
        let protocol_null = discriminator == NULL_DISCRIMINATOR;

        let source_idx = if protocol_null {
            None
        } else {
            let source_idx = usize::from(discriminator);
            if source_idx >= variants.len() {
                return Err(Error::deserialize(format!(
                    "Variant discriminator {discriminator} is out of bounds for {} nested type(s)",
                    variants.len()
                )));
            }
            Some(source_idx)
        };

        let forced_null = outer_null != 0;
        if let Some(source_idx) = source_idx {
            let source_row_idx = source_counts[source_idx];
            source_counts[source_idx] += 1;

            if forced_null {
                let masks = source_keep_mask.get_or_insert_with(|| vec![None; variants.len()]);
                let keep_mask = masks[source_idx].get_or_insert_with(|| vec![true; source_row_idx]);
                keep_mask.push(false);
            } else if let Some(masks) = source_keep_mask.as_mut()
                && let Some(keep_mask) = masks[source_idx].as_mut()
            {
                keep_mask.push(true);
            }
        }

        if protocol_null || forced_null {
            type_ids.push(union_builder.children[null_field_idx].type_id);
            offsets.push(null_rows as i32);
            null_rows += 1;
        } else if let Some(source_idx) = source_idx {
            type_ids.push(union_builder.children[source_idx].type_id);
            offsets.push(value_offsets[source_idx] as i32);
            value_offsets[source_idx] += 1;
        }
    }

    let mut children = Vec::with_capacity(union_schema_fields.len());
    for (source_idx, source_type) in variants.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let keep_mask = source_keep_mask.as_ref().and_then(|m| m[source_idx].as_deref());
        let source_array = {
            let (child_data_type, child_builder) =
                union_builder.child_parts_mut(source_idx, source_type)?;
            if source_rows == 0 {
                new_empty_array(child_data_type)
            } else {
                let mut child_ctx = ArrowFieldCtx {
                    row_buffer:     ctx.row_buffer,
                    dynamic_prefix: None,
                    variant_prefix: None,
                };
                let source_array = Box::pin(source_type.deserialize_arrow(
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
                            "Variant keep-mask mismatch for '{}': {} != {source_rows}",
                            source_type,
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
                "Variant child '{}' row count mismatch: {} != {}",
                source_type,
                source_array.len(),
                value_offsets[source_idx]
            )));
        }

        children.push(source_array);
    }

    if discriminator_mode == VARIANT_DISCRIMINATOR_MODE_COMPACT
        && compact_granule_remaining_rows != 0
    {
        return Err(Error::ArrowDeserialize(format!(
            "Variant compact discriminator granule declared more rows than available in column: \
             {compact_granule_remaining_rows} trailing row(s)"
        )));
    }

    let null_data_type = union_builder.data_type(null_field_idx)?;
    children.push(new_null_array(null_data_type, null_rows));

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

    use arrow::array::{Array, Int32Array, NullArray, StringArray};
    use arrow::datatypes::{Field, UnionFields};

    use super::*;
    use crate::formats::VariantPrefixState;

    fn variant_data_type() -> DataType {
        DataType::Union(
            UnionFields::new([0_i8, 1_i8, 2_i8], vec![
                Field::new("Int32", DataType::Int32, false),
                Field::new("String", DataType::Utf8, false),
                Field::new("Nothing", DataType::Null, false),
            ]),
            UnionMode::Dense,
        )
    }

    fn variant_type() -> Type { Type::Variant(vec![Type::Int32, Type::String]) }

    fn ctx<'a>(row_buffer: &'a mut Vec<u8>, discriminator_mode: u8) -> ArrowFieldCtx<'a> {
        ArrowFieldCtx {
            row_buffer,
            dynamic_prefix: None,
            variant_prefix: Some(VariantPrefixState { discriminator_mode }),
        }
    }

    #[tokio::test]
    async fn test_deserialize_variant_basic_mode() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();

        let mut input = vec![0_u8, 1_u8, 255_u8, 0_u8];
        input.extend_from_slice(&10_i32.to_le_bytes());
        input.extend_from_slice(&20_i32.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'a']);
        let mut reader = Cursor::new(input);
        let mut row_buffer = Vec::new();

        let array = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            4,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: Some(VariantPrefixState { discriminator_mode: 0 }),
            },
        )
        .await
        .unwrap();

        let union = array.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!((0..4).map(|i| union.type_id(i)).collect::<Vec<_>>(), vec![0, 1, 2, 0]);
        assert_eq!((0..4).map(|i| union.value_offset(i)).collect::<Vec<_>>(), vec![0, 0, 0, 1]);

        let ints = union.child(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints, &Int32Array::from(vec![10, 20]));

        let strings = union.child(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings, &StringArray::from(vec!["a"]));

        let nulls = union.child(2).as_any().downcast_ref::<NullArray>().unwrap();
        assert_eq!(nulls.len(), 1);
    }

    #[tokio::test]
    async fn test_deserialize_variant_compact_mode_plain_granule() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();

        let mut input = vec![
            4_u8, // granule rows
            0_u8, // granule format: plain
            0_u8, 1_u8, 255_u8, 0_u8,
        ];
        input.extend_from_slice(&10_i32.to_le_bytes());
        input.extend_from_slice(&20_i32.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'a']);
        let mut reader = Cursor::new(input);
        let mut row_buffer = Vec::new();

        let array = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            4,
            &[],
            &mut ArrowFieldCtx {
                row_buffer:     &mut row_buffer,
                dynamic_prefix: None,
                variant_prefix: Some(VariantPrefixState { discriminator_mode: 1 }),
            },
        )
        .await
        .unwrap();

        let union = array.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!((0..4).map(|i| union.type_id(i)).collect::<Vec<_>>(), vec![0, 1, 2, 0]);
        assert_eq!((0..4).map(|i| union.value_offset(i)).collect::<Vec<_>>(), vec![0, 0, 0, 1]);
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_non_variant_type() {
        let type_hint = Type::Int32;
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&variant_type(), &data_type).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("non-Variant"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_non_dense_union() {
        let type_hint = variant_type();
        let data_type = DataType::UInt8;
        let mut builder = TypedBuilder::try_new(&Type::UInt8, &DataType::UInt8).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("expects Arrow DenseUnion"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_empty_variants() {
        let type_hint = Type::Variant(vec![]);
        let data_type = DataType::Union(
            UnionFields::new([0_i8], vec![Field::new("Nothing", DataType::Null, false)]),
            UnionMode::Dense,
        );
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("requires at least one nested type"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_union_child_count_mismatch() {
        let type_hint = variant_type();
        let data_type = DataType::Union(
            UnionFields::new([0_i8, 1_i8], vec![
                Field::new("Int32", DataType::Int32, false),
                Field::new("String", DataType::Utf8, false),
            ]),
            UnionMode::Dense,
        );
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("child count mismatch"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_outer_null_mask_length_mismatch() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut reader = Cursor::new(vec![255_u8]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[0_u8, 1_u8],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("outer null mask length mismatch"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_non_union_builder() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&Type::UInt8, &DataType::UInt8).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Unexpected builder"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_invalid_discriminator_mode() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ctx(&mut row_buffer, 9),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("unsupported Variant discriminator mode"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_compact_granule_zero_rows() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut reader = Cursor::new(vec![0_u8]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[],
            &mut ctx(&mut row_buffer, 1),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("granule 0 rows"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_compact_invalid_granule_format() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut reader = Cursor::new(vec![1_u8, 2_u8]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[],
            &mut ctx(&mut row_buffer, 1),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("unsupported Variant compact granule format"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_out_of_bounds_discriminator() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut reader = Cursor::new(vec![3_u8]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("out of bounds"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_trailing_compact_granule_rows() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let mut input = vec![2_u8, 1_u8, 0_u8];
        input.extend_from_slice(&7_i32.to_le_bytes());
        let mut reader = Cursor::new(input);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            1,
            &[],
            &mut ctx(&mut row_buffer, 1),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("trailing row(s)"));
    }

    #[tokio::test]
    async fn test_deserialize_variant_forced_outer_null_filters_child_values() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();

        let mut input = vec![0_u8, 1_u8];
        input.extend_from_slice(&10_i32.to_le_bytes());
        input.extend_from_slice(&[1_u8, b'z']);
        let mut reader = Cursor::new(input);
        let mut row_buffer = Vec::new();

        let array = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            2,
            &[1_u8, 0_u8],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap();

        let union = array.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_id(0), 2);
        assert_eq!(union.type_id(1), 1);

        let ints = union.child(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.len(), 0);
        let strings = union.child(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings, &StringArray::from(vec!["z"]));
    }

    #[tokio::test]
    async fn test_deserialize_variant_rejects_null_builder_schema_mismatch() {
        let type_hint = variant_type();
        let data_type = variant_data_type();
        let builder_data_type = DataType::Union(
            UnionFields::new([0_i8, 1_i8], vec![
                Field::new("Int32", DataType::Int32, false),
                Field::new("String", DataType::Utf8, false),
            ]),
            UnionMode::Dense,
        );
        let mut builder = TypedBuilder::try_new(&type_hint, &builder_data_type).unwrap();
        let mut reader = Cursor::new(vec![]);
        let mut row_buffer = Vec::new();

        let error = deserialize(
            &type_hint,
            &mut builder,
            &mut reader,
            &data_type,
            0,
            &[],
            &mut ctx(&mut row_buffer, 0),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("builder/schema child count mismatch"));
    }
}
