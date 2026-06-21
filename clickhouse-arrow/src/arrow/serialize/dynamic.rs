use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UnionArray};
use arrow::compute::filter;
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use super::ClickHouseArrowSerializer;
use crate::formats::{DynamicPrefixState, SerializerState};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

const DYNAMIC_TYPE_ID_UNSET: u16 = u16::MAX;

#[derive(Debug, Clone)]
struct DynamicUnionField {
    logical_type: Type,
    type_id:      i8,
}

#[derive(Debug, Clone)]
struct DynamicTypeIdLookup {
    by_type_id: [u16; 256],
}

impl DynamicTypeIdLookup {
    fn new() -> Self { Self { by_type_id: [DYNAMIC_TYPE_ID_UNSET; 256] } }

    #[expect(clippy::cast_sign_loss)]
    #[inline]
    fn slot(type_id: i8) -> usize { (i16::from(type_id) + 128) as usize }

    fn insert(&mut self, type_id: i8, field_idx: usize) -> Result<()> {
        let slot = Self::slot(type_id);
        if self.by_type_id[slot] != DYNAMIC_TYPE_ID_UNSET {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic union has duplicate type id {type_id}"
            )));
        }

        let field_idx = u16::try_from(field_idx).map_err(|_| {
            Error::ArrowSerialize(format!(
                "Dynamic union child index {field_idx} exceeds supported lookup range"
            ))
        })?;
        self.by_type_id[slot] = field_idx;
        Ok(())
    }

    fn resolve(&self, type_id: i8) -> Result<usize> {
        let slot = Self::slot(type_id);
        let field_idx = self.by_type_id[slot];
        if field_idx == DYNAMIC_TYPE_ID_UNSET {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic union row references unknown type id {type_id}"
            )));
        }
        Ok(usize::from(field_idx))
    }
}

fn parse_dynamic_union_schema(
    data_type: &DataType,
    dynamic_prefix: DynamicPrefixState,
) -> Result<(Vec<DynamicUnionField>, DynamicTypeIdLookup)> {
    let DataType::Union(fields, _) = data_type else {
        return Err(Error::ArrowSerialize(format!(
            "Dynamic strict mode expects Arrow Union, found {data_type:?}"
        )));
    };

    if dynamic_prefix.flattened_types.len() != fields.len() {
        return Err(Error::ArrowSerialize(format!(
            "Dynamic union child count mismatch between prefix metadata and Arrow schema: {} != {}",
            dynamic_prefix.flattened_types.len(),
            fields.len()
        )));
    }

    let mut field_meta = Vec::with_capacity(fields.len());
    let mut by_type_id = DynamicTypeIdLookup::new();
    for ((type_id, _field), logical_type) in fields.iter().zip(dynamic_prefix.flattened_types) {
        by_type_id.insert(type_id, field_meta.len())?;
        field_meta.push(DynamicUnionField { logical_type, type_id });
    }

    if field_meta.is_empty() {
        return Err(Error::ArrowSerialize(
            "Dynamic strict union must contain at least one child field".to_string(),
        ));
    }

    Ok((field_meta, by_type_id))
}

#[inline]
fn dynamic_index_width(total_types: usize) -> usize {
    // ClickHouse selects the index integer width from the number of non-null Dynamic types.
    // The null discriminator is encoded as `total_types`.
    if u8::try_from(total_types).is_ok() {
        1
    } else if u16::try_from(total_types).is_ok() {
        2
    } else if u32::try_from(total_types).is_ok() {
        4
    } else {
        8
    }
}

#[inline]
#[expect(clippy::cast_possible_truncation)]
async fn write_dynamic_index_async<W: ClickHouseWrite>(
    writer: &mut W,
    width: usize,
    index: usize,
) -> Result<()> {
    match width {
        1 => writer.write_u8(index as u8).await?,
        2 => writer.write_u16_le(index as u16).await?,
        4 => writer.write_u32_le(index as u32).await?,
        8 => writer.write_u64_le(index as u64).await?,
        _ => {
            return Err(Error::ArrowSerialize(format!(
                "invalid Dynamic discriminator width: {width}"
            )));
        }
    }
    Ok(())
}

#[inline]
#[expect(clippy::cast_possible_truncation)]
fn write_dynamic_index_sync<W: ClickHouseBytesWrite>(
    writer: &mut W,
    width: usize,
    index: usize,
) -> Result<()> {
    match width {
        1 => writer.put_u8(index as u8),
        2 => writer.put_u16_le(index as u16),
        4 => writer.put_u32_le(index as u32),
        8 => writer.put_u64_le(index as u64),
        _ => {
            return Err(Error::ArrowSerialize(format!(
                "invalid Dynamic discriminator width: {width}"
            )));
        }
    }
    Ok(())
}

pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    if !matches!(type_.strip_null(), Type::Dynamic { .. }) {
        return Err(Error::ArrowSerialize(format!("Dynamic serialize type unsupported: {type_}")));
    }

    let Some(union) = column.as_any().downcast_ref::<UnionArray>() else {
        return Err(Error::ArrowSerialize("Expected UnionArray for Dynamic serialization".into()));
    };

    let dynamic_prefix = state.take_dynamic_prefix().ok_or_else(|| {
        Error::ArrowSerialize("Dynamic serialization requires prefix metadata".to_string())
    })?;

    let (fields, by_type_id) = parse_dynamic_union_schema(data_type, dynamic_prefix)?;
    let total_types = fields.len();
    let null_discriminator = total_types;
    let discriminator_width = dynamic_index_width(total_types);
    let mut source_counts = vec![0_usize; total_types];
    let mut source_keep_mask: Option<Vec<Option<Vec<bool>>>> = None;

    for row in 0..union.len() {
        let child_type_id = union.type_id(row);
        let source_idx = by_type_id.resolve(child_type_id)?;
        let value_offset = union.value_offset(row);
        let child = union.child(child_type_id);
        if source_counts[source_idx] <= value_offset {
            source_counts[source_idx] = value_offset + 1;
            if let Some(masks) = source_keep_mask.as_mut()
                && let Some(mask) = masks[source_idx].as_mut()
            {
                mask.resize(value_offset + 1, true);
            }
        }

        if child.is_null(value_offset) {
            write_dynamic_index_async(writer, discriminator_width, null_discriminator).await?;
            let masks = source_keep_mask.get_or_insert_with(|| vec![None; total_types]);
            let mask =
                masks[source_idx].get_or_insert_with(|| vec![true; source_counts[source_idx]]);
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = false;
            continue;
        }

        write_dynamic_index_async(writer, discriminator_width, source_idx).await?;
        if let Some(masks) = source_keep_mask.as_mut()
            && let Some(mask) = masks[source_idx].as_mut()
        {
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = true;
        }
    }

    for (source_idx, field) in fields.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let mut values = Arc::clone(union.child(field.type_id));
        if source_rows < values.len() {
            values = values.slice(0, source_rows);
        } else if source_rows > values.len() {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic child '{}' row count mismatch: {} > {}",
                field.logical_type,
                source_rows,
                values.len()
            )));
        }

        if let Some(keep_mask) = source_keep_mask.as_ref().and_then(|m| m[source_idx].as_deref()) {
            if keep_mask.len() != source_rows {
                return Err(Error::ArrowSerialize(format!(
                    "Dynamic keep-mask mismatch for '{}': {} != {source_rows}",
                    field.logical_type,
                    keep_mask.len()
                )));
            }
            let keep = keep_mask.iter().copied().map(Some).collect::<BooleanArray>();
            values = filter(values.as_ref(), &keep)?;
        }

        Box::pin(field.logical_type.serialize_async(writer, &values, values.data_type(), state))
            .await?;
    }

    Ok(())
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_: &Type,
    writer: &mut W,
    column: &ArrayRef,
    data_type: &DataType,
    state: &mut SerializerState,
) -> Result<()> {
    if !matches!(type_.strip_null(), Type::Dynamic { .. }) {
        return Err(Error::ArrowSerialize(format!("Dynamic serialize type unsupported: {type_}")));
    }

    let Some(union) = column.as_any().downcast_ref::<UnionArray>() else {
        return Err(Error::ArrowSerialize("Expected UnionArray for Dynamic serialization".into()));
    };

    let dynamic_prefix = state.take_dynamic_prefix().ok_or_else(|| {
        Error::ArrowSerialize("Dynamic serialization requires prefix metadata".to_string())
    })?;

    let (fields, by_type_id) = parse_dynamic_union_schema(data_type, dynamic_prefix)?;
    let total_types = fields.len();
    let null_discriminator = total_types;
    let discriminator_width = dynamic_index_width(total_types);
    let mut source_counts = vec![0_usize; total_types];
    let mut source_keep_mask: Option<Vec<Option<Vec<bool>>>> = None;

    for row in 0..union.len() {
        let row_type_id = union.type_id(row);
        let source_idx = by_type_id.resolve(row_type_id)?;
        let value_offset = union.value_offset(row);
        let child = union.child(row_type_id);
        if source_counts[source_idx] <= value_offset {
            source_counts[source_idx] = value_offset + 1;
            if let Some(masks) = source_keep_mask.as_mut()
                && let Some(mask) = masks[source_idx].as_mut()
            {
                mask.resize(value_offset + 1, true);
            }
        }

        if child.is_null(value_offset) {
            write_dynamic_index_sync(writer, discriminator_width, null_discriminator)?;
            let masks = source_keep_mask.get_or_insert_with(|| vec![None; total_types]);
            let mask =
                masks[source_idx].get_or_insert_with(|| vec![true; source_counts[source_idx]]);
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = false;
            continue;
        }

        write_dynamic_index_sync(writer, discriminator_width, source_idx)?;
        if let Some(masks) = source_keep_mask.as_mut()
            && let Some(mask) = masks[source_idx].as_mut()
        {
            if mask.len() <= value_offset {
                mask.resize(value_offset + 1, true);
            }
            mask[value_offset] = true;
        }
    }

    for (source_idx, field) in fields.iter().enumerate() {
        let source_rows = source_counts[source_idx];
        let mut values = Arc::clone(union.child(field.type_id));
        if source_rows < values.len() {
            values = values.slice(0, source_rows);
        } else if source_rows > values.len() {
            return Err(Error::ArrowSerialize(format!(
                "Dynamic child '{}' row count mismatch: {} > {}",
                field.logical_type,
                source_rows,
                values.len()
            )));
        }

        if let Some(keep_mask) = source_keep_mask.as_ref().and_then(|m| m[source_idx].as_deref()) {
            if keep_mask.len() != source_rows {
                return Err(Error::ArrowSerialize(format!(
                    "Dynamic keep-mask mismatch for '{}': {} != {source_rows}",
                    field.logical_type,
                    keep_mask.len()
                )));
            }
            let keep = keep_mask.iter().copied().map(Some).collect::<BooleanArray>();
            values = filter(values.as_ref(), &keep)?;
        }

        field.logical_type.serialize(writer, &values, values.data_type(), state)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray, UInt8Array};
    use arrow::datatypes::{Field, UnionFields, UnionMode};

    use super::*;
    use crate::formats::SerializerState;

    fn dynamic_type() -> Type { Type::Dynamic { max_types: 8 } }

    fn dynamic_data_type() -> DataType {
        DataType::Union(
            UnionFields::new([0_i8, 1_i8], vec![
                Field::new("Int32", DataType::Int32, false),
                Field::new("String", DataType::Utf8, true),
            ]),
            UnionMode::Dense,
        )
    }

    fn dynamic_column() -> ArrayRef {
        let DataType::Union(fields, _) = dynamic_data_type() else { unreachable!() };

        Arc::new(
            UnionArray::try_new(
                fields,
                vec![0_i8, 1_i8, 0_i8].into(),
                Some(vec![0_i32, 0_i32, 1_i32].into()),
                vec![
                    Arc::new(Int32Array::from(vec![10_i32, 20])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef,
                ],
            )
            .unwrap(),
        ) as ArrayRef
    }

    fn dynamic_prefix_state() -> DynamicPrefixState {
        DynamicPrefixState {
            serialization_version: 3,
            flattened_types:       vec![Type::Int32, Type::String],
        }
    }

    fn dynamic_sparse_offsets_column() -> ArrayRef {
        let DataType::Union(fields, _) = dynamic_data_type() else {
            unreachable!();
        };

        Arc::new(
            UnionArray::try_new(
                fields,
                vec![0_i8, 0_i8, 0_i8].into(),
                Some(vec![0_i32, 2_i32, 1_i32].into()),
                vec![
                    Arc::new(Int32Array::from(vec![
                        Option::<i32>::None,
                        Some(20_i32),
                        Some(30_i32),
                        Some(40_i32),
                    ])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Option::<&str>::None])) as ArrayRef,
                ],
            )
            .unwrap(),
        ) as ArrayRef
    }

    #[test]
    fn test_dynamic_type_id_lookup_insert_and_resolve() {
        let mut lookup = DynamicTypeIdLookup::new();
        lookup.insert(-1_i8, 3).unwrap();
        lookup.insert(7_i8, 5).unwrap();
        assert_eq!(lookup.resolve(-1_i8).unwrap(), 3);
        assert_eq!(lookup.resolve(7_i8).unwrap(), 5);
    }

    #[test]
    fn test_dynamic_type_id_lookup_rejects_duplicate_type_id() {
        let mut lookup = DynamicTypeIdLookup::new();
        lookup.insert(1_i8, 0).unwrap();
        let error = lookup.insert(1_i8, 1).unwrap_err();
        assert!(error.to_string().contains("duplicate type id"));
    }

    #[test]
    fn test_dynamic_type_id_lookup_rejects_field_index_overflow() {
        let mut lookup = DynamicTypeIdLookup::new();
        let error = lookup.insert(1_i8, usize::from(u16::MAX) + 1).unwrap_err();
        assert!(error.to_string().contains("exceeds supported lookup range"));
    }

    #[test]
    fn test_dynamic_type_id_lookup_rejects_unknown_type_id() {
        let lookup = DynamicTypeIdLookup::new();
        let error = lookup.resolve(4_i8).unwrap_err();
        assert!(error.to_string().contains("unknown type id"));
    }

    #[test]
    fn test_dynamic_index_width_boundaries() {
        assert_eq!(dynamic_index_width(1), 1);
        assert_eq!(dynamic_index_width(255), 1);
        assert_eq!(dynamic_index_width(256), 2);
        assert_eq!(dynamic_index_width(65_535), 2);
        assert_eq!(dynamic_index_width(65_536), 4);
        assert_eq!(dynamic_index_width(usize::MAX), 8);
    }

    #[test]
    fn test_parse_dynamic_union_schema_rejects_non_union_type() {
        let error =
            parse_dynamic_union_schema(&DataType::Int32, dynamic_prefix_state()).unwrap_err();
        assert!(error.to_string().contains("expects Arrow Union"));
    }

    #[tokio::test]
    async fn test_write_dynamic_index_async_widths() {
        let mut writer = Cursor::new(Vec::new());
        write_dynamic_index_async(&mut writer, 1, 1).await.unwrap();
        write_dynamic_index_async(&mut writer, 2, 2).await.unwrap();
        write_dynamic_index_async(&mut writer, 4, 3).await.unwrap();
        write_dynamic_index_async(&mut writer, 8, 4).await.unwrap();

        assert_eq!(writer.into_inner(), vec![
            1_u8, 2_u8, 0_u8, 3_u8, 0_u8, 0_u8, 0_u8, 4_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8,
            0_u8,
        ]);
    }

    #[test]
    fn test_write_dynamic_index_sync_widths() {
        let mut writer = Vec::new();
        write_dynamic_index_sync(&mut writer, 1, 1).unwrap();
        write_dynamic_index_sync(&mut writer, 2, 2).unwrap();
        write_dynamic_index_sync(&mut writer, 4, 3).unwrap();
        write_dynamic_index_sync(&mut writer, 8, 4).unwrap();

        assert_eq!(writer, vec![
            1_u8, 2_u8, 0_u8, 3_u8, 0_u8, 0_u8, 0_u8, 4_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8,
            0_u8,
        ]);
    }

    #[tokio::test]
    async fn test_write_dynamic_index_async_rejects_invalid_width() {
        let mut writer = Cursor::new(Vec::new());
        let error = write_dynamic_index_async(&mut writer, 3, 1).await.unwrap_err();
        assert!(error.to_string().contains("invalid Dynamic discriminator width"));
    }

    #[test]
    fn test_write_dynamic_index_sync_rejects_invalid_width() {
        let mut writer = Vec::new();
        let error = write_dynamic_index_sync(&mut writer, 3, 1).unwrap_err();
        assert!(error.to_string().contains("invalid Dynamic discriminator width"));
    }

    #[test]
    fn test_parse_dynamic_union_schema_rejects_empty_union() {
        let empty_union = DataType::Union(
            UnionFields::new(Vec::<i8>::new(), Vec::<Field>::new()),
            UnionMode::Dense,
        );
        let error = parse_dynamic_union_schema(&empty_union, DynamicPrefixState {
            serialization_version: 3,
            flattened_types:       vec![],
        })
        .unwrap_err();
        assert!(error.to_string().contains("must contain at least one child field"));
    }

    #[tokio::test]
    async fn test_serialize_dynamic_requires_prefix_metadata() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &dynamic_type(),
            &mut writer,
            &dynamic_column(),
            &dynamic_data_type(),
            &mut SerializerState::default(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("requires prefix metadata"));
    }

    #[tokio::test]
    async fn test_serialize_dynamic_with_null_rows_async() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));

        serialize_async(
            &dynamic_type(),
            &mut writer,
            &dynamic_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .await
        .unwrap();

        assert_eq!(writer.into_inner(), vec![
            0_u8, // row 0 => Int32
            2_u8, // row 1 => Dynamic null discriminator
            0_u8, // row 2 => Int32
            10, 0, 0, 0, 20, 0, 0, 0,
        ]);
    }

    #[tokio::test]
    async fn test_serialize_dynamic_async_slices_and_filters_sparse_offsets() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));

        serialize_async(
            &dynamic_type(),
            &mut writer,
            &dynamic_sparse_offsets_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .await
        .unwrap();

        assert_eq!(writer.into_inner(), vec![
            2_u8, // row 0 => null discriminator (child offset 0 is null)
            0_u8, // row 1 => Int32 discriminator
            0_u8, // row 2 => Int32 discriminator
            20, 0, 0, 0, 30, 0, 0, 0, // filtered Int32 child values
        ]);
    }

    #[test]
    fn test_serialize_dynamic_with_null_rows_sync() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));

        serialize(
            &dynamic_type(),
            &mut writer,
            &dynamic_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .unwrap();

        assert_eq!(writer, vec![
            0_u8, // row 0 => Int32
            2_u8, // row 1 => Dynamic null discriminator
            0_u8, // row 2 => Int32
            10, 0, 0, 0, 20, 0, 0, 0,
        ]);
    }

    #[test]
    fn test_serialize_dynamic_sync_slices_and_filters_sparse_offsets() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));

        serialize(
            &dynamic_type(),
            &mut writer,
            &dynamic_sparse_offsets_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .unwrap();

        assert_eq!(writer, vec![
            2_u8, // row 0 => null discriminator (child offset 0 is null)
            0_u8, // row 1 => Int32 discriminator
            0_u8, // row 2 => Int32 discriminator
            20, 0, 0, 0, 30, 0, 0, 0, // filtered Int32 child values
        ]);
    }

    #[tokio::test]
    async fn test_serialize_dynamic_rejects_non_dynamic_type() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));

        let error = serialize_async(
            &Type::Int32,
            &mut writer,
            &dynamic_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("unsupported"));
    }

    #[tokio::test]
    async fn test_serialize_dynamic_rejects_non_union_array() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));
        let column = Arc::new(UInt8Array::from(vec![1_u8, 2, 3])) as ArrayRef;

        let error = serialize_async(
            &dynamic_type(),
            &mut writer,
            &column,
            &dynamic_data_type(),
            &mut state,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Expected UnionArray"));
    }

    #[tokio::test]
    async fn test_serialize_dynamic_rejects_prefix_child_count_mismatch() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(DynamicPrefixState {
            serialization_version: 3,
            flattened_types:       vec![Type::Int32],
        }));

        let error = serialize_async(
            &dynamic_type(),
            &mut writer,
            &dynamic_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("child count mismatch"));
    }

    #[tokio::test]
    async fn test_serialize_dynamic_rejects_unknown_union_type_id_in_row() {
        let mismatched_data_type = DataType::Union(
            UnionFields::new([5_i8, 6_i8], vec![
                Field::new("Int32", DataType::Int32, false),
                Field::new("String", DataType::Utf8, true),
            ]),
            UnionMode::Dense,
        );
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));

        let error = serialize_async(
            &dynamic_type(),
            &mut writer,
            &dynamic_column(),
            &mismatched_data_type,
            &mut state,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("unknown type id"));
    }

    #[test]
    fn test_serialize_dynamic_sync_rejects_non_dynamic_type() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));
        let error = serialize(
            &Type::Int32,
            &mut writer,
            &dynamic_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unsupported"));
    }

    #[test]
    fn test_serialize_dynamic_sync_rejects_non_union_array() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));
        let column = Arc::new(UInt8Array::from(vec![1_u8, 2, 3])) as ArrayRef;
        let error =
            serialize(&dynamic_type(), &mut writer, &column, &dynamic_data_type(), &mut state)
                .unwrap_err();
        assert!(error.to_string().contains("Expected UnionArray"));
    }

    #[test]
    fn test_serialize_dynamic_sync_rejects_prefix_child_count_mismatch() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(DynamicPrefixState {
            serialization_version: 3,
            flattened_types:       vec![Type::Int32],
        }));

        let error = serialize(
            &dynamic_type(),
            &mut writer,
            &dynamic_column(),
            &dynamic_data_type(),
            &mut state,
        )
        .unwrap_err();

        assert!(error.to_string().contains("child count mismatch"));
    }

    #[test]
    fn test_serialize_dynamic_sync_rejects_unknown_union_type_id_in_row() {
        let mismatched_data_type = DataType::Union(
            UnionFields::new([5_i8, 6_i8], vec![
                Field::new("Int32", DataType::Int32, false),
                Field::new("String", DataType::Utf8, true),
            ]),
            UnionMode::Dense,
        );
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        drop(state.replace_dynamic_prefix(dynamic_prefix_state()));

        let error = serialize(
            &dynamic_type(),
            &mut writer,
            &dynamic_column(),
            &mismatched_data_type,
            &mut state,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown type id"));
    }
}
