use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, new_empty_array};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::deserialize::ClickhouseArrowDeserializer;
use super::serialize::ClickhouseArrowSerializer;
pub use super::types::{
    LIST_ITEM_FIELD_NAME, MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME,
    TUPLE_FIELD_NAME_PREFIX,
};
use crate::flags::debug_arrow;
use crate::formats::protocol_data::ProtocolData;
use crate::formats::{DeserializerState, SerializerState};
use crate::geo::normalize_geo_type;
use crate::io::{ClickhouseRead, ClickhouseWrite};
use crate::native::block_info::BlockInfo;
use crate::native::protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
use crate::prelude::*;
use crate::{ArrowOptions, Result, Type};

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
/// use clickhouse_native::CompressionMethod;
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
impl ProtocolData<RecordBatch> for RecordBatch {
    type Options = ArrowOptions;

    #[instrument(level = "trace", name = "clickhouse.serialize.arrow" skip_all)]
    async fn write<W: ClickhouseWrite>(
        self,
        writer: &mut W,
        revision: u64,
        header: Option<&[(String, Type)]>,
        options: ArrowOptions,
    ) -> Result<()> {
        let schema = self.schema();

        if revision > 0 {
            BlockInfo::default().write(writer).await?;
        }

        // Write number of columns and rows
        let columns = schema.fields().len();
        let rows = self.num_rows();
        writer.write_var_uint(columns as u64).await?;
        writer.write_var_uint(rows as u64).await?;

        let mut state = SerializerState::default().with_arrow_options(options);

        if debug_arrow() {
            trace!(?header, columns, rows, "writing column data");
        }

        // Convert and write each column
        for (i, field) in schema.fields().iter().enumerate() {
            let column = self.column(i);

            let name = field.name();
            let data_type = field.data_type();
            let is_nullable = field.is_nullable();
            let type_ = if let Some(t) =
                header.and_then(|h| h.iter().find(|(n, _)| n == name)).map(|(_, t)| t)
            {
                t
            } else {
                &super::types::arrow_to_ch_type(data_type, is_nullable, Some(options))?
            };

            // Simplify geo types
            let type_ =
                if matches!(type_, Type::Point | Type::Polygon | Type::MultiPolygon | Type::Ring) {
                    &normalize_geo_type(type_).unwrap()
                } else {
                    type_
                };

            if debug_arrow() {
                trace!(name, ?data_type, is_nullable, ?type_, "serializing column {i}");
            }

            // Write column name
            writer.write_string(name).await?;

            // Write column type
            writer.write_string(type_.to_string()).await?;

            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                writer.write_u8(0).await?;
            }

            if column.is_empty() {
                if debug_arrow() {
                    warn!(name, "column {i} empty");
                }
                continue;
            }

            type_.serialize_prefix(writer, &mut state).await?;
            type_.serialize(writer, column, field, &mut state).await?;
        }

        Ok(())
    }

    #[instrument(level = "trace", name = "clickhouse.deserialize.arrow" skip_all)]
    async fn read<R: ClickhouseRead>(
        reader: &mut R,
        revision: u64,
        options: ArrowOptions,
    ) -> Result<RecordBatch> {
        if revision > 0 {
            let _ = BlockInfo::read(reader).await.inspect_err(|error| {
                error!(?error, "failed to read block info");
            })?;
        }

        #[allow(clippy::cast_possible_truncation)]
        let (columns, rows) =
            (reader.read_var_uint().await? as usize, reader.read_var_uint().await? as usize);

        if columns == 0 && rows == 0 {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        } else if debug_arrow() {
            debug!(columns, rows, "Deserializing arrow");
        }

        let mut fields = Vec::with_capacity(columns);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns);

        let mut state = DeserializerState::default().with_arrow_options(options);

        for i in 0..columns {
            let name = reader.read_utf8_string().await?;
            let type_name = reader.read_utf8_string().await?;

            let internal_type = Type::from_str(&type_name)?;
            let (arrow_type, is_nullable) = internal_type.arrow_type(Some(options))?;

            // Verify the resulting type against the arrow type, otherwise the builders will fail
            let internal_type =
                super::types::normalize_type(&internal_type, &arrow_type).unwrap_or(internal_type);
            let arrow_field = Field::new(name, arrow_type, is_nullable);

            if debug_arrow() {
                trace!(
                    name = arrow_field.name(),
                    arrow_type = ?arrow_field.data_type(),
                    is_nullable = arrow_field.is_nullable(),
                    type_name,
                    ?internal_type,
                    arrow_options = ?options,
                    "creating field for column {i}"
                );
            }

            let field = Arc::new(arrow_field);

            // TODO: Ignored - pass this to prefix deserialization
            let _has_custom = if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                reader.read_u8().await? != 0
            } else {
                false
            };

            let array = if rows > 0 {
                let col_span = debug_arrow()
                    .then_some(trace_span!("clickhouse.deserialize.arrow.column"))
                    .unwrap_or(Span::none());

                // Deserialize column data directly into Arrow array builder
                internal_type.deserialize_prefix(reader, &mut state).await?;
                internal_type
                    .deserialize(reader, rows, &[], &mut state)
                    .instrument(col_span)
                    .await
                    .inspect_err(|error| {
                        error!(
                            ?error,
                            type_name,
                            ?internal_type,
                            ?field,
                            arrow_options = ?options,
                            "col deserialize"
                        );
                    })?
            } else {
                new_empty_array(field.data_type())
            };

            arrays.push(array);
            fields.push(field);
        }

        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
    }
}

// TODO: Remove - geo unit tests
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        // Deserialize back
        let mut reader = Cursor::new(buffer);
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, ArrowOptions::default())
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

        let mut reader = Cursor::new(buffer);
        let result =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, ArrowOptions::default())
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
        BlockInfo::default().write(&mut buffer).await.unwrap(); // BlockInfo
        buffer.write_var_uint(1).await.unwrap(); // Columns
        buffer.write_var_uint(3).await.unwrap(); // Rows
        buffer.write_string("id").await.unwrap();
        buffer.write_string("InvalidType").await.unwrap(); // Invalid type name
        buffer.write_u8(0).await.unwrap();

        let mut reader = Cursor::new(buffer);
        let result =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, ArrowOptions::default())
                .await;
        assert!(matches!(
            result,
            Err(Error::TypeParseError(m))
            if &m == "invalid type name: 'InvalidType'"
        ));
    }

    #[tokio::test]
    async fn test_deserialize_malformed_input() {
        let mut buffer = Vec::new();
        BlockInfo::default().write(&mut buffer).await.unwrap(); // BlockInfo
        buffer.write_var_uint(1).await.unwrap(); // Columns
        // Incomplete: missing row count and metadata

        let mut reader = Cursor::new(buffer);
        let result =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, ArrowOptions::default())
                .await;

        assert!(matches!(result, Err(Error::Io(_))));
        let Error::Io(e) = result.err().unwrap() else {
            unreachable!();
        };
        assert!(matches!(e.kind(), std::io::ErrorKind::UnexpectedEof));
    }

    /// Tests round-trip serialization and deserialization of a single-column `Int32` RecordBatch.
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
        batch.write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options).await.unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![1, 2, 3])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a multi-column RecordBatch with mixed
    /// types.
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
        batch.write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options).await.unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Map column.
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with zero rows.
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized = RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options)
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

    /// Tests round-trip serialization and deserialization of an empty RecordBatch (zero columns,
    /// zero rows).
    #[tokio::test]
    async fn test_round_trip_empty_block() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);

        let arrow_options = ArrowOptions::default().with_strings_as_strings(true);
        let mut buffer = Cursor::new(Vec::new());
        batch
            .clone()
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(BinaryArray::from(vec![Some(b"a" as &[u8]), None, Some(b"c" as &[u8])]))
                .as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Float64 column.
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(Float64Array::from(vec![1.5, -2.0, 3.1])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a RecordBatch with a
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Decimal128
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec!["cat", "dog"])).as_ref()
        );
    }
    /// Tests round-trip serialization and deserialization of a RecordBatch with a Dictionary(Int8,
    /// Utf8) column, expecting LowCardinality(String) mapping to Dictionary(Int32, Utf8) when
    /// header is None.
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Dictionary(Int8,
    /// Utf8) column, using header to specify Enum8.
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Dictionary(Int8,
    /// Utf8) column, using header to specify LowCardinality(String).
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Dictionary(Int16,
    /// Utf8) column, expecting LowCardinality(String) mapping to Dictionary(Int32, Utf8) when
    /// header is None.
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Dictionary(Int32,
    /// Utf8) column, expecting LowCardinality(String) mapping to Dictionary(Int32, Utf8) when
    /// header is None.
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 3);
        let deserialized_array =
            deserialized.column(0).as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        assert_eq!(
            deserialized_array.values().as_ref(),
            Arc::new(StringArray::from(vec!["active", "inactive"])).as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a RecordBatch with a Tuple(Int32,
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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

    /// Tests round-trip serialization and deserialization of a RecordBatch with a large number of
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

        assert_eq!(deserialized.schema(), schema);

        #[expect(clippy::cast_sign_loss)]
        let num_rows = rows as usize;
        assert_eq!(deserialized.num_rows(), num_rows);
        assert_eq!(deserialized.column(0).as_ref(), array.as_ref());
    }

    /// Tests round-trip serialization and deserialization of a RecordBatch with non-UTF-8 Binary
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

        assert_eq!(deserialized.schema(), schema);
        assert_eq!(deserialized.num_rows(), 2);
        assert_eq!(
            deserialized.column(0).as_ref(),
            Arc::new(BinaryArray::from_vec(vec![b"\xFF\xFE" as &[u8], b"\x00\x01" as &[u8]]))
                .as_ref()
        );
    }

    /// Tests round-trip serialization and deserialization of a RecordBatch with max/min Int32
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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options)
            .await
            .unwrap();

        let mut reader = Cursor::new(buffer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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
            .write(&mut buffer, DBMS_TCP_PROTOCOL_VERSION, Some(&header), arrow_options)
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
        batch.write(&mut writer, DBMS_TCP_PROTOCOL_VERSION, None, arrow_options).await.unwrap();
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

        let mut reader = Cursor::new(writer.into_inner());
        let deserialized =
            RecordBatch::read(&mut reader, DBMS_TCP_PROTOCOL_VERSION, arrow_options).await.unwrap();

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
}
