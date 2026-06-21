use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, new_empty_array};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(feature = "extended-types")]
use super::ArrowSchemaHint;
use super::builder::TypedBuilder;
use super::deserialize::{ArrowDeserializerState, ArrowFieldCtx, ClickHouseArrowDeserializer};
use super::serialize::ClickHouseArrowSerializer;
use super::types::arrow_to_ch_type;
pub use super::types::{
    LIST_ITEM_FIELD_NAME, MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME,
    TUPLE_FIELD_NAME_PREFIX, ch_to_arrow_type,
};
use crate::deserialize::ClickHouseNativeDeserializer;
use crate::flags::debug_arrow;
#[cfg(feature = "extended-types")]
use crate::formats::DynamicPrefixState;
use crate::formats::protocol_data::ProtocolData;
use crate::formats::{CustomPlan, DeserializerState, SerializerState};
use crate::geo::normalize_geo_type;
use crate::io::{ClickHouseBytesWrite, ClickHouseRead, ClickHouseWrite};
use crate::native::block_info::BlockInfo;
use crate::prelude::*;
use crate::serialize::ClickHouseNativeSerializer;
use crate::{ArrowOptions, Error, Result, Type};

/// Implementation of `ProtocolData` for Arrow `RecordBatch`es.
///
/// This implementation serializes a `RecordBatch` to a `ClickHouse` native block, including block
/// info, column count, row count, column metadata (names and types), and serialized column data.
/// It also deserializes a native block into a `RecordBatch`, constructing the schema and arrays
/// based on the block’s metadata and data.
///
/// # Examples
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use arrow::array::Int32Array;
/// use arrow::datatypes::{DataType, Field, Schema};
/// use arrow::record_batch::RecordBatch;
/// use tokio::io::AsyncWriteExt;
///
/// use clickhouse_arrow::CompressionMethod;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]));
/// let batch = RecordBatch::try_new(
///     schema,
///     vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
/// ).unwrap();
/// let mut buffer = Vec::new();
/// batch.write(&mut buffer, None).await.unwrap();
/// ```
impl ProtocolData<RecordBatch, ArrowDeserializerState> for RecordBatch {
    type Options = ArrowOptions;

    #[instrument(level = "trace", name = "clickhouse.serialize.arrow" skip_all)]
    async fn write_async<W: ClickHouseWrite>(
        self,
        writer: &mut W,
        revision: u64,
        header: Option<&[(String, Type)]>,
        options: ArrowOptions,
    ) -> Result<()> {
        let schema = self.schema();

        if revision > 0 {
            BlockInfo::default().write_async(writer).await?;
        }

        // Write number of columns and rows
        let (columns, rows) = (schema.fields().len(), self.num_rows());
        writer.write_var_uint(columns as u64).await?;
        writer.write_var_uint(rows as u64).await?;

        if debug_arrow() {
            trace!(?header, columns, rows, "writing column data");
        }

        let mut state = SerializerState::default().with_arrow_options(options);

        // Convert and write each column
        for (i, field) in schema.fields().iter().enumerate() {
            let column = self.column(i);
            let name = field.name();
            let data_type = field.data_type();
            let nullable = field.is_nullable();
            let maybe_type = header.and_then(|h| h.iter().find(|(n, _)| n == name)).map(|(_, t)| t);
            let type_ = if let Some(t) = maybe_type {
                t
            } else {
                &arrow_to_ch_type(data_type, nullable, Some(options))?
            };
            // Simplify geo types
            let is_geo =
                matches!(type_, Type::Point | Type::Polygon | Type::MultiPolygon | Type::Ring);
            let type_ = if is_geo { &normalize_geo_type(type_).unwrap() } else { type_ };

            if debug_arrow() {
                trace!(name, ?data_type, nullable, ?type_, "serializing column {i}");
            }

            // Write column name
            writer.write_string(name).await?;
            // Write column type
            writer.write_string(type_.to_string()).await?;

            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                writer.write_u8(0).await?;
            }

            #[cfg(feature = "extended-types")]
            if matches!(type_.strip_null(), Type::Dynamic { .. }) {
                drop(state.take_dynamic_prefix());
                drop(
                    DynamicPrefixState::from_field(field, Some(options))?
                        .and_then(|p| state.replace_dynamic_prefix(p)),
                );
            }

            type_.serialize_prefix_async(writer, &mut state).await?;
            if column.is_empty() {
                if debug_arrow() {
                    warn!(name, "column {i} empty");
                }
                continue;
            }
            type_.serialize_async(writer, column, data_type, &mut state).await?;
        }

        Ok(())
    }

    fn write<W: ClickHouseBytesWrite>(
        self,
        writer: &mut W,
        revision: u64,
        header: Option<&[(String, Type)]>,
        options: ArrowOptions,
    ) -> Result<()> {
        let schema = self.schema();

        if revision > 0 {
            BlockInfo::default().write(writer)?;
        }

        // Write number of columns and rows
        let (columns, rows) = (schema.fields().len(), self.num_rows());
        writer.put_var_uint(columns as u64)?;
        writer.put_var_uint(rows as u64)?;

        if debug_arrow() {
            trace!(?header, columns, rows, "writing column data");
        }

        let mut state = SerializerState::default().with_arrow_options(options);

        // Convert and write each column
        for (i, field) in schema.fields().iter().enumerate() {
            let column = self.column(i);
            let name = field.name();
            let data_type = field.data_type();
            let nullable = field.is_nullable();
            let maybe_type = header.and_then(|h| h.iter().find(|(n, _)| n == name)).map(|(_, t)| t);
            let type_ = if let Some(t) = maybe_type {
                t
            } else {
                &arrow_to_ch_type(data_type, nullable, Some(options))?
            };
            // Simplify geo types
            let is_geo =
                matches!(type_, Type::Point | Type::Polygon | Type::MultiPolygon | Type::Ring);
            let type_ = if is_geo { &normalize_geo_type(type_).unwrap() } else { type_ };

            if debug_arrow() {
                trace!(name, ?data_type, nullable, ?type_, "serializing column {i}");
            }

            // Write column name
            writer.put_string(name)?;
            // Write column type
            writer.put_string(type_.to_string())?;

            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                writer.put_u8(0);
            }

            #[cfg(feature = "extended-types")]
            if matches!(type_.strip_null(), Type::Dynamic { .. }) {
                drop(state.take_dynamic_prefix());
                drop(
                    DynamicPrefixState::from_field(field, Some(options))?
                        .and_then(|p| state.replace_dynamic_prefix(p)),
                );
            }

            type_.serialize_prefix(writer, &mut state);
            if column.is_empty() {
                if debug_arrow() {
                    warn!(name, "column {i} empty");
                }
                continue;
            }
            type_.serialize(writer, column, data_type, &mut state)?;
        }

        Ok(())
    }

    #[instrument(level = "trace", name = "clickhouse.deserialize.arrow" skip_all)]
    async fn read<R: ClickHouseRead>(
        reader: &mut R,
        revision: u64,
        options: ArrowOptions,
        state: &mut DeserializerState<ArrowDeserializerState>,
    ) -> Result<RecordBatch> {
        let _ = BlockInfo::read_async(reader).await.inspect_err(|error| {
            error!(?error, "failed to read block info");
        })?;

        #[allow(clippy::cast_possible_truncation)]
        let (columns, rows) =
            (reader.read_var_uint().await? as usize, reader.read_var_uint().await? as usize);

        if columns == 0 && rows == 0 {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        } else if debug_arrow() {
            debug!(columns, rows, "Deserializing arrow");
        }

        let _ = state.format_state().with_capacity(columns, rows);

        for i in 0..columns {
            let name = reader.read_utf8_string().await?;
            let type_name = reader.read_utf8_string().await?;
            let internal_type = Type::from_str(&type_name)?;

            drop(state.take_custom_plan());

            let has_custom = if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                reader.read_u8().await? != 0
            } else {
                false
            };

            if has_custom {
                internal_type.deserialize_custom_serialization_prefix(reader, state).await?;
            } else {
                drop(state.replace_custom_plan(CustomPlan::from_type_structure(&internal_type)));
            }
            if let Some((node_index, node)) = state.custom_plan().and_then(|plan| {
                plan.nodes
                    .iter()
                    .enumerate()
                    .find(|(_, node)| node.stack_type != 0 && !node.is_sparse())
            }) {
                return Err(Error::Protocol(format!(
                    "unsupported SerializationInfo kind stack {} at custom node {node_index} for \
                     column {name}",
                    node.stack_type
                )));
            }

            state.reset_custom_node_to_root();
            internal_type.deserialize_prefix(reader, state).await?;

            // Pull out root-node dynamic metadata (if present) and create ArrowSchemaHint.
            #[cfg(feature = "extended-types")]
            let schema_hints =
                state.custom_plan().and_then(|plan| plan.node(plan.root)).and_then(|node| {
                    node.dynamic_prefix.as_ref().map(|prefix| ArrowSchemaHint {
                        dynamic_types: prefix.flattened_types.clone(),
                    })
                });
            #[cfg(not(feature = "extended-types"))]
            let schema_hints = None;

            let (arrow_type, is_nullable) =
                ch_to_arrow_type(&internal_type, Some(options), schema_hints.as_ref())?;

            // Verify the resulting type against the arrow type, otherwise the builders will fail
            let type_hint =
                super::types::normalize_type(&internal_type, &arrow_type).unwrap_or(internal_type);
            let field = Field::new(name, arrow_type, is_nullable);

            if debug_arrow() {
                trace!(?field, ?type_hint, ?options, "deserializing column {i}");
            }

            let array = if rows > 0 {
                let custom_plan = state.take_custom_plan();
                let deser = state.format_state();
                let dt = field.data_type();

                let builders = &mut deser.builders;
                let builder = if let Some(b) = builders.get_mut(i) {
                    b
                } else {
                    builders.push(TypedBuilder::try_new(&type_hint, dt)?);
                    builders.last_mut().unwrap()
                };

                let mut ctx = ArrowFieldCtx::new(&mut deser.buffer).with_custom_plan(custom_plan);

                type_hint
                    .deserialize_arrow(builder, reader, dt, rows, &[], &mut ctx)
                    .await
                    .inspect_err(|error| error!(?error, ?field, "col {i} deserialize"))?
            } else {
                new_empty_array(field.data_type())
            };

            let deser = state.format_state();
            let _ = deser.push_array(array).push_field(Arc::new(field));
        }

        let (fields, arrays) = state.format_state().take();
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::buffer::OffsetBuffer;
    use arrow::compute::cast;
    use arrow::datatypes::*;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::arrow::types::LIST_ITEM_FIELD_NAME;
    use crate::native::protocol::DBMS_TCP_PROTOCOL_VERSION;

    // Helper to create a simple RecordBatch for testing
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("alice"), None, Some("bob")])),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn test_serialize_record_batch() {
        let batch = create_test_batch();

        let arrow_options = ArrowOptions::default();
        let mut buffer = Vec::new();
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_deserialize_record_batch() {
        // Create a batch and serialize it
        let batch = create_test_batch();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Vec::new();
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        // Deserialize back
        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer);
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        // Verify schema and data
        assert_eq!(deserialized.schema(), batch.schema());
        assert_eq!(deserialized.num_rows(), batch.num_rows());
        assert_eq!(deserialized.num_columns(), batch.num_columns());
        for i in 0..batch.num_columns() {
            assert_eq!(deserialized.column(i).as_ref(), batch.column(i).as_ref());
        }
    }

    #[tokio::test]
    async fn test_serialize_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter(Vec::<i32>::new()))])
                .unwrap();
        let mut buffer = Vec::new();
        batch
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, ArrowOptions::default())
            .await
            .unwrap();
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_deserialize_empty_block() {
        let mut buffer = Vec::new();
        buffer.write_u8(0).await.unwrap(); // Revision
        buffer.write_u8(0).await.unwrap(); // No custom serialization
        buffer.write_var_uint(0).await.unwrap(); // Columns
        buffer.write_var_uint(0).await.unwrap(); // Rows

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer);
        let result = RecordBatch::read(
            &mut reader,
            DBMS_TCP_PROTOCOL_VERSION,
            ArrowOptions::default(),
            &mut state,
        )
        .await
        .unwrap();
        let schema = result.schema();
        assert!(schema.fields.is_empty());
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 0);
    }

    #[tokio::test]
    async fn test_deserialize_invalid_type() {
        let mut buffer = Vec::new();
        BlockInfo::default().write_async(&mut buffer).await.unwrap(); // BlockInfo
        buffer.write_var_uint(1).await.unwrap(); // Columns
        buffer.write_var_uint(3).await.unwrap(); // Rows
        buffer.write_string("id").await.unwrap();
        buffer.write_string("InvalidType").await.unwrap(); // Invalid type name
        buffer.write_u8(0).await.unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer);
        let result = RecordBatch::read(
            &mut reader,
            DBMS_TCP_PROTOCOL_VERSION,
            ArrowOptions::default(),
            &mut state,
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::TypeParse(m))
            if &m == "invalid type name: 'InvalidType'"
        ));
    }

    #[tokio::test]
    async fn test_deserialize_rejects_unsupported_kind() {
        // A column header that declares has_custom=1 and kind=2 (Detached) —
        // a kind the arrow reader does not support. It must be rejected with
        // Error::Protocol, not silently mis-framed.
        let mut buffer = Vec::new();
        BlockInfo::default().write_async(&mut buffer).await.unwrap();
        buffer.write_var_uint(1).await.unwrap(); // Columns
        buffer.write_var_uint(1).await.unwrap(); // Rows
        buffer.write_string("v").await.unwrap();
        buffer.write_string("Int32").await.unwrap();
        buffer.write_u8(1).await.unwrap(); // has_custom = 1
        buffer.write_u8(2).await.unwrap(); // kind = 2 (Detached)

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer);
        let result = RecordBatch::read(
            &mut reader,
            DBMS_TCP_PROTOCOL_VERSION,
            ArrowOptions::default(),
            &mut state,
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::Protocol(m)) if m.contains("unsupported SerializationInfo kind")
        ));
    }

    #[tokio::test]
    async fn test_deserialize_sparse_all_default_column() {
        // A single-row Int32 column serialized Sparse where the only row is
        // the default (0). The SparseOffsets stream is just the terminator
        // (END_OF_GRANULE_FLAG | group_size=1 trailing default), and there are
        // zero non-default values. Exercises the Sparse dispatch + materialize
        // path end-to-end through the block reader.
        const END_OF_GRANULE_FLAG: u64 = 1u64 << 62;

        let mut buffer = Vec::new();
        BlockInfo::default().write_async(&mut buffer).await.unwrap();
        buffer.write_var_uint(1).await.unwrap(); // Columns
        buffer.write_var_uint(1).await.unwrap(); // Rows
        buffer.write_string("v").await.unwrap();
        buffer.write_string("Int32").await.unwrap();
        buffer.write_u8(1).await.unwrap(); // has_custom = 1
        buffer.write_u8(1).await.unwrap(); // kind = 1 (Sparse)
        // SparseOffsets: one flagged terminator = "1 trailing default, no value".
        buffer.write_var_uint(END_OF_GRANULE_FLAG | 1).await.unwrap();
        // SparseElements: zero non-default Int32 values follow.

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer);
        let batch = RecordBatch::read(
            &mut reader,
            DBMS_TCP_PROTOCOL_VERSION,
            ArrowOptions::default(),
            &mut state,
        )
        .await
        .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        let col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.null_count(), 0);
        assert_eq!(col.value(0), 0, "the single sparse-default row materializes as 0");
    }

    #[tokio::test]
    async fn test_deserialize_malformed_input() {
        let mut buffer = Vec::new();
        BlockInfo::default().write_async(&mut buffer).await.unwrap(); // BlockInfo
        buffer.write_var_uint(1).await.unwrap(); // Columns
        // Incomplete: missing row count and metadata
        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer);
        let result = RecordBatch::read(
            &mut reader,
            DBMS_TCP_PROTOCOL_VERSION,
            ArrowOptions::default(),
            &mut state,
        )
        .await;

        assert!(matches!(result, Err(Error::Io(_))));
        let Error::Io(e) = result.err().unwrap() else {
            unreachable!();
        };
        assert!(matches!(e.kind(), std::io::ErrorKind::UnexpectedEof));
    }

    /// Tests round-trip serialization and deserialization of a single-column `Int32` `RecordBatch`.
    #[tokio::test]
    async fn test_round_trip_single_column_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(vec![
                1, 2, 3,
            ]))])
            .unwrap();

        let arrow_options = ArrowOptions::default();
        let mut buffer = Cursor::new(Vec::new());
        batch
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![1, 2, 3])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a multi-column `RecordBatch` with
    /// mixed types.
    #[tokio::test]
    async fn test_round_trip_multi_column_mixed_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "values",
                DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false))),
                false,
            ),
        ]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")]));
        let values = Arc::new(ListArray::new(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)),
            OffsetBuffer::new(vec![0, 2, 3, 5].into()),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            None,
        ));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![id, name, values]).unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![1, 2, 3])).as_ref()
        );
        assert_eq!(
            deserialized.column(1).as_ref(),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])).as_ref()
        );
        let deserialized_values =
            deserialized.column(2).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            deserialized_values.values().as_ref(),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])).as_ref()
        );
        assert_eq!(deserialized_values.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 3, 5
        ]);
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a Map column.
    #[tokio::test]
    async fn test_round_trip_map_column() {
        let key_field = Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false);
        let value_field = Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false);
        let struct_field = Arc::new(Field::new(
            MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![key_field.clone(), value_field.clone()])),
            false,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "map",
            DataType::Map(Arc::clone(&struct_field), false),
            false,
        )]));
        let keys = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let struct_array = StructArray::from(vec![
            (Arc::new(key_field), keys as ArrayRef),
            (Arc::new(value_field), values as ArrayRef),
        ]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let map_array = Arc::new(MapArray::new(struct_field, offsets, struct_array, None, false));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![map_array]).unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_map = deserialized.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array =
            deserialized_map.entries().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            struct_array.column(0).as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a", "b", "c", "d", "e"])
        );
        assert_eq!(
            struct_array.column(1).as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![1, 2, 3, 4, 5])
        );
        assert_eq!(deserialized_map.offsets().iter().copied().collect::<Vec<i32>>(), vec![
            0, 2, 3, 5
        ]);
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with zero rows.
    #[tokio::test]
    async fn test_round_trip_zero_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(
            Vec::<i32>::new(),
        ))])
        .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .inspect_err(|error| eprintln!("Error deserializing RecordBatch: {error:?}"))
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 0);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Int32Array::from(Vec::<i32>::new())).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of an empty `RecordBatch` (zero columns,
    /// zero rows).
    #[tokio::test]
    async fn test_round_trip_empty_block() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        let schema = deserialized.schema();
        assert!(schema.fields().is_empty());
        assert_eq!(deserialized.num_rows(), 0);
        assert_eq!(deserialized.num_columns(), 0);
    }

    /// Tests round-trip serialization and deserialization with header for type disambiguation.
    #[tokio::test]
    async fn test_round_trip_with_header() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(vec![
                1, 2, 3,
            ]))])
            .unwrap();

        let header = vec![("id".to_string(), Type::Int32)];
        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![1, 2, 3])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization with `strings_as_strings=false` (String
    /// as Binary).
    #[tokio::test]
    async fn test_round_trip_strings_as_binary() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Binary, true)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(BinaryArray::from(vec![
                Some(b"a" as &[u8]),
                None,
                Some(b"c" as &[u8]),
            ]))])
            .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(false);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(BinaryArray::from(vec![Some(b"a" as &[u8]), None, Some(b"c" as &[u8])]))
                .as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a Float64 column.
    #[tokio::test]
    async fn test_round_trip_float64() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Float64Array::from(vec![
                1.5, -2.0, 3.1,
            ]))])
            .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Float64Array::from(vec![1.5, -2.0, 3.1])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a
    /// Nullable(DateTime) column.
    #[tokio::test]
    async fn test_round_trip_nullable_datetime() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(
            TimestampSecondArray::from(vec![Some(1000), None, Some(3000)])
                .with_timezone_opt(Some("UTC")),
        )])
        .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(
                TimestampSecondArray::from(vec![Some(1000), None, Some(3000)])
                    .with_timezone_opt(Some("UTC"))
            )
            .as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a Decimal128
    /// column.
    #[tokio::test]
    async fn test_round_trip_decimal128() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("price", DataType::Decimal128(18, 4), false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(
            Decimal128Array::from(vec![10000, 20000, 30000])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        )])
        .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(
                Decimal128Array::from(vec![10000, 20000, 30000])
                    .with_precision_and_scale(18, 4)
                    .unwrap()
            )
            .as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a
    /// LowCardinality(String) column.
    #[tokio::test]
    async fn test_round_trip_low_cardinality_string() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(
                DictionaryArray::<Int32Type>::from_iter(vec!["cat", "dog", "cat"]),
            )])
            .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec!["cat", "dog"])).as_ref()
        );
    }
    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a
    /// Dictionary(Int8, Utf8) column, expecting LowCardinality(String) mapping to
    /// Dictionary(Int32, Utf8) when header is None.
    #[tokio::test]
    async fn test_round_trip_dictionary_int8_no_header() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(
            DictionaryArray::<Int8Type>::from_iter(vec!["active", "inactive", "active"]),
        )])
        .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        assert_eq!(deserialized.schema(), expected_schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec!["active", "inactive"])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a
    /// Dictionary(Int8, Utf8) column, using header to specify Enum8.
    #[tokio::test]
    async fn test_round_trip_dictionary_int8_with_enum8_header() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "enum8_col",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )]));
        let expected = vec![Some("active"), None, Some("inactive"), Some("active"), None];
        let dictionary_values = StringArray::from(vec!["active", "inactive"]);
        let dictionary_array = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1), Some(0), None]),
                Arc::new(dictionary_values.clone()),
            )
            .unwrap(),
        ) as ArrayRef;
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&dictionary_array)]).unwrap();

        let header = vec![(
            "enum8_col".to_string(),
            Type::Enum8(vec![("active".into(), 0), ("inactive".into(), 1)]).into_nullable(),
        )];
        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        // Assert basics
        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 5);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int8Type>>().unwrap();
        assert_eq!(deserialized_array.values().as_ref(), Arc::new(dictionary_values).as_ref());

        // Assert equality
        let deser_values_array = cast(&deserialized_array, &DataType::Utf8).unwrap();
        let deser_values_array = deser_values_array.as_string::<i32>();
        let deser_values = deser_values_array.iter().collect::<Vec<_>>();

        assert_eq!(deser_values, expected);

        let expected_values = dictionary_array
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap()
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        let deser_values = deserialized_array
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(deser_values, expected_values);
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a
    /// Dictionary(Int8, Utf8) column, using header to specify LowCardinality(String).
    #[tokio::test]
    async fn test_round_trip_dictionary_int8_with_low_cardinality_header() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(
            DictionaryArray::<Int8Type>::from_iter(vec!["active", "inactive", "active"]),
        )])
        .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let header = vec![("status".to_string(), Type::LowCardinality(Box::new(Type::String)))];
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        assert_eq!(deserialized.schema(), expected_schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec!["active", "inactive"])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a
    /// Dictionary(Int16, Utf8) column, expecting LowCardinality(String) mapping to
    /// Dictionary(Int32, Utf8) when header is None.
    #[tokio::test]
    async fn test_round_trip_dictionary_int16_no_header() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(
            DictionaryArray::<Int16Type>::from_iter(vec!["active", "inactive", "active"]),
        )])
        .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        assert_eq!(deserialized.schema(), expected_schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec!["active", "inactive"])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a
    /// Dictionary(Int32, Utf8) column, expecting LowCardinality(String) mapping to
    /// Dictionary(Int32, Utf8) when header is None.
    #[tokio::test]
    async fn test_round_trip_dictionary_int32_no_header() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(
                DictionaryArray::<Int32Type>::from_iter(vec!["active", "inactive", "active"]),
            )])
            .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec!["active", "inactive"])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a Tuple(Int32,
    /// String) column.
    #[tokio::test]
    async fn test_round_trip_tuple() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "pair",
            DataType::Struct(Fields::from(vec![
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            ])),
            false,
        )]));
        let tuple_array = StructArray::from(vec![
            (
                Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef,
            ),
        ]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(tuple_array)]).unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_struct =
            deserialized.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            deserialized_struct.column(0).as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![1, 2, 3])
        );
        assert_eq!(
            deserialized_struct.column(1).as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["x", "y", "z"])
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with a large number of
    /// rows.
    #[tokio::test]
    async fn test_round_trip_large_batch() {
        let rows = 1000_i32;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let array = Arc::new((0..rows).collect::<Int32Array>()) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&array)]).unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);

        #[expect(clippy::cast_sign_loss)]
        let num_rows = rows as usize;
        assert_eq!(deserialized.num_rows(), num_rows);
        assert_eq!(deserialized.column(0).as_ref(), array.as_ref());
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with non-UTF-8 Binary
    /// data.
    #[tokio::test]
    async fn test_round_trip_non_utf8_binary() {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Binary, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(BinaryArray::from_vec(vec![
                b"\xFF\xFE" as &[u8], // Non-UTF-8
                b"\x00\x01" as &[u8],
            ]))])
            .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(false);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 2);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(BinaryArray::from_vec(vec![b"\xFF\xFE" as &[u8], b"\x00\x01" as &[u8]]))
                .as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a `RecordBatch` with max/min Int32
    /// values.
    #[tokio::test]
    async fn test_round_trip_max_min_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(vec![
                i32::MIN,
                0,
                i32::MAX,
            ]))])
            .unwrap();

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])).as_ref()
        );
    }

    /// Tests error handling for type mismatch between header and schema.
    #[tokio::test]
    async fn test_header_type_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(vec![
                1, 2, 3,
            ]))])
            .unwrap();
        let header = vec![("id".to_string(), Type::String)]; // Mismatch: Int32 vs String

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        let result = batch
            .clone()
            .write_async(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await;
        assert!(matches!(
            result,
            Err(Error::ArrowSerialize(e))
            if e.contains("Expected one of")
        ));
    }

    /// Test low cardinality nullable string round trip
    #[tokio::test]
    async fn test_low_cardinality_nullable() {
        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "low_cardinality_nullable_string_col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(3), Some(1), None, Some(2)]),
                Arc::new(StringArray::from(vec!["active", "inactive", "pending", "absent"]))
                    as ArrayRef,
            )
            .unwrap(),
        )])
        .expect("Failed to create RecordBatch");

        let mut writer = Cursor::new(Vec::new());
        batch
            .write_async(&mut writer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();
        let output = writer.clone().into_inner();
        let expected = vec![
            1, 2, 2, 255, 255, 255, 255, 0,  // BlockInfo
            1,  // Column len
            5,  // Row len
            35, // Varuint length
            108, 111, 119, 95, 99, 97, 114, 100, 105, 110, 97, 108, 105, 116, 121, 95, 110, 117,
            108, 108, 97, 98, 108, 101, 95, 115, 116, 114, 105, 110, 103, 95, 99, 111,
            108, // <--- Column name
            32,  // Varuint length
            76, 111, 119, 67, 97, 114, 100, 105, 110, 97, 108, 105, 116, 121, 40, 78, 117, 108,
            108, 97, 98, 108, 101, 40, 83, 116, 114, 105, 110, 103, 41, 41,
            0, //  <--- Column type LowCardinality(Nullable(String))
            1, 0, 0, 0, 0, 0, 0, 0, // Prefix
            0, 2, 0, 0, 0, 0, 0, 0, // Flags
            5, 0, 0, 0, 0, 0, 0, 0, // Dict length
            0, // Null value
            6, 97, 99, 116, 105, 118, 101, 8, 105, 110, 97, 99, 116, 105, 118, 101, 7, 112, 101,
            110, 100, 105, 110, 103, 6, 97, 98, 115, 101, 110, 116, // Other dict values
            5, 0, 0, 0, 0, 0, 0, 0, // Key length (# of rows)
            1, 4, 2, 0, 3, // Key indices
        ];
        assert_eq!(output, expected);

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(writer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 5);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();

        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec![
                None,
                Some("active"),
                Some("inactive"),
                Some("pending"),
                Some("absent")
            ]))
            .as_ref()
        );

        // Since clickhouse includes a default value for nulls, we should "expect" that and not the
        // original array
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec![
                None,
                Some("active"),
                Some("inactive"),
                Some("pending"),
                Some("absent")
            ]))
            .as_ref()
        );
    }

    #[tokio::test]
    #[cfg(feature = "extended-types")]
    async fn test_round_trip_variant_with_header() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Union(
                UnionFields::new([0_i8, 1_i8, 2_i8], vec![
                    Field::new("Int32", DataType::Int32, false),
                    Field::new("String", DataType::Utf8, false),
                    Field::new("Nothing", DataType::Null, false),
                ]),
                UnionMode::Dense,
            ),
            false,
        )]));
        let column = Arc::new(
            UnionArray::try_new(
                match schema.field(0).data_type() {
                    DataType::Union(fields, _) => fields.clone(),
                    _ => unreachable!(),
                },
                vec![0_i8, 1_i8, 2_i8, 0_i8].into(),
                Some(vec![0_i32, 0, 0, 1].into()),
                vec![
                    Arc::new(Int32Array::from(vec![10_i32, 20])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    Arc::new(NullArray::new(1)) as ArrayRef,
                ],
            )
            .unwrap(),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let header = vec![("value".to_string(), Type::Variant(vec![Type::Int32, Type::String]))];

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut writer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut writer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(writer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        let union = deserialized.column(0).as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!((0..4).map(|i| union.type_id(i)).collect::<Vec<_>>(), vec![0, 1, 2, 0]);
        assert_eq!((0..4).map(|i| union.value_offset(i)).collect::<Vec<_>>(), vec![0, 0, 0, 1]);
        assert_eq!(
            union.child(0).as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![10, 20])
        );
        assert_eq!(
            union.child(1).as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a"])
        );
    }

    #[tokio::test]
    #[cfg(feature = "extended-types")]
    async fn test_round_trip_dynamic_with_header() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Union(
                UnionFields::new([0_i8, 1_i8], vec![
                    Field::new("Int32", DataType::Int32, false),
                    Field::new("String", DataType::Utf8, false),
                ]),
                UnionMode::Dense,
            ),
            false,
        )]));
        let column = Arc::new(
            UnionArray::try_new(
                match schema.field(0).data_type() {
                    DataType::Union(fields, _) => fields.clone(),
                    _ => unreachable!(),
                },
                vec![0_i8, 1_i8, 0_i8].into(),
                Some(vec![0_i32, 0, 1].into()),
                vec![
                    Arc::new(Int32Array::from(vec![10_i32, 20])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                ],
            )
            .unwrap(),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
        let header = vec![("value".to_string(), Type::Dynamic { max_types: 8 })];

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut writer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut writer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(writer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        let union = deserialized.column(0).as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!((0..3).map(|i| union.type_id(i)).collect::<Vec<_>>(), vec![0, 1, 0]);
        assert_eq!((0..3).map(|i| union.value_offset(i)).collect::<Vec<_>>(), vec![0, 0, 1]);
        assert_eq!(
            union.child(0).as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![10, 20])
        );
        assert_eq!(
            union.child(1).as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["a"])
        );
    }

    #[tokio::test]
    #[cfg(feature = "extended-types")]
    async fn test_round_trip_qbit_and_nested_with_header() {
        let qbit_field = Field::new(
            "qbit_col",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        );
        let nested_field = Field::new(
            "nested_col",
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
            ])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![qbit_field.clone(), nested_field.clone()]));

        let qbit_values =
            Arc::new(Float32Array::from(vec![0.1_f32, 0.2, 0.3, 1.0, 2.0, 3.0])) as ArrayRef;
        let qbit_col = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            3,
            qbit_values,
            None,
        )) as ArrayRef;

        let nested_offsets = OffsetBuffer::new(vec![0, 2, 2].into());
        let nested_name_col = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            nested_offsets.clone(),
            Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            None,
        )) as ArrayRef;
        let nested_score_col = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            nested_offsets,
            Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            None,
        )) as ArrayRef;
        let nested_col = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "name",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                    false,
                )),
                nested_name_col,
            ),
            (
                Arc::new(Field::new(
                    "score",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                    false,
                )),
                nested_score_col,
            ),
        ])) as ArrayRef;

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![qbit_col, nested_col]).unwrap();
        let header = vec![
            ("qbit_col".to_string(), Type::QBit {
                element_type: Box::new(Type::Float32),
                dimension:    3,
            }),
            (
                "nested_col".to_string(),
                Type::Nested(vec![
                    ("name".to_string(), Type::String),
                    ("score".to_string(), Type::Int32),
                ]),
            ),
        ];

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut writer = Cursor::new(Vec::new());
        batch
            .clone()
            .write_async(&mut writer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut state = DeserializerState::default();
        let mut reader = Cursor::new(writer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options, &mut state)
                .await
                .unwrap();

        assert_eq!(deserialized.num_rows(), 2);
        assert_eq!(deserialized.schema(), schema);
    }
}
