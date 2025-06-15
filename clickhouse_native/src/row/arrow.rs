mod binary;
mod builder;
mod dictionary;
mod dynamic;
mod list;
mod nested;
mod primitive;

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes, BytesMut, TryGetError};
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::*;

use self::builder::{TypedBuilderMap, TypedBuilderMapSlice};
use super::protocol::HttpSummary;
use crate::formats::protocol_data::RowData;
use crate::io::ClickhouseBytesRead;
use crate::prelude::SchemaConversions;
use crate::schema::{ColumnDefine, RecordBatchDefinition};
use crate::spawn::SpawnedTask;
use crate::{CompressionMethod, Error, Result};

const DEFAULT_CHUNK_ROWS: usize = 65409; // Seems to be the chunk size used by ClickHouse
const BATCH_CHUNK_SIZE: usize = DEFAULT_CHUNK_ROWS * 2;
const DEFAULT_BATCHES_CAPACITY: usize = 32; // Again, arbitrary-ish but helpful

impl RowData<RecordBatch> for RecordBatch {
    type Row = RecordBatchDefinition;

    async fn read_rows<R>(
        stream: &mut R,
        definition: RecordBatchDefinition,
        overrides: Option<SchemaConversions>,
        summary: HttpSummary,
    ) -> Result<Vec<RecordBatch>>
    where
        R: Stream<Item = Result<Bytes>> + Unpin + Send,
    {
        read_rows_parallel(stream, definition, overrides, summary).await
    }
}

// TODO: Remove - decide whether smaller rows sizes are affected by channel overhead
async fn read_rows_parallel<R>(
    stream: &mut R,
    definition: RecordBatchDefinition,
    overrides: Option<SchemaConversions>,
    summary: HttpSummary,
) -> Result<Vec<RecordBatch>>
where
    R: Stream<Item = Result<Bytes>> + Unpin + Send,
{
    let schema = Arc::clone(&definition.schema);
    let column_definitions = definition
        .runtime_definitions(overrides.as_ref())?
        .or(RecordBatchDefinition::definitions());
    let Some(columns) = column_definitions.filter(|c| !c.is_empty()) else {
        return Err(Error::DDLMalformed("Schema is empty, cannot get rows".into()));
    };

    let batch_cap = if summary.total_rows_to_read > 0 {
        summary.total_rows_to_read.div_ceil(BATCH_CHUNK_SIZE) + 1
    } else {
        DEFAULT_BATCHES_CAPACITY
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<Bytes>>();

    // TODO: Remove
    eprintln!(
        ">> HTTP Summary: read_rows={}, read_bytes={}, total_rows={} / CAPACITY = {batch_cap}",
        summary.read_rows, summary.read_bytes, summary.total_rows_to_read
    );

    let compression = summary.compression;
    let deserializer = SpawnedTask::spawn_blocking(move || {
        if matches!(compression, CompressionMethod::None) {
            deserialize_rows_uncompressed(rx, columns, schema, batch_cap)
        } else {
            deserialize_rows(rx, columns, schema, batch_cap)
        }
    });

    // Send blocks
    let mut sent = 0;
    while let Some(result) = stream.next().await {
        tx.send(result).map_err(|_| Error::ArrowDeserialize("channel closed".into()))?;
        sent += 1;
    }
    drop(tx);

    // TODO: Remove
    debug!("*** Finished sending all blocks: {sent} ***");

    deserializer
        .join_unwind()
        .await
        .map_err(|_| Error::ArrowDeserialize("deser task".into()))?
}

// Compressed path has natural boundaries, no need to track
#[expect(clippy::needless_pass_by_value)]
fn deserialize_rows(
    mut rx: UnboundedReceiver<Result<Bytes>>,
    columns: Vec<(String, crate::Type, Option<String>)>,
    schema: SchemaRef,
    batch_capacity: usize,
) -> Result<Vec<RecordBatch>> {
    let mut rows = 0;
    let mut builder_map = builder::create_typed_builder_map(&columns, &schema)?;
    let mut collector = RecordBatchCollector::new(batch_capacity, builder_map.len(), schema);
    while let Some(result) = rx.blocking_recv() {
        let mut buffer = result?;
        while !buffer.is_empty() {
            for (name, (type_, builder)) in &mut builder_map {
                if builder.append_null(&mut buffer, type_)? {
                    continue;
                }
                builder.append_value(&mut buffer, type_).inspect_err(|error| {
                    error!(?error, "Error deserializing {name}");
                })?;
            }
            rows += 1;
            collector.next(rows, &mut builder_map)?;
        }
    }
    info!("Finished deserializing {rows} rows");

    collector.finish(rows, builder_map)
}

// Uncompressed can run into TryGetRead errors indicating more data needed
#[derive(Debug)]
enum UncompressedStage {
    NeedMoreData(usize, Bytes),
    Continue,
}

#[expect(clippy::needless_pass_by_value)]
fn deserialize_rows_uncompressed(
    mut rx: UnboundedReceiver<Result<Bytes>>,
    columns: Vec<(String, crate::Type, Option<String>)>,
    schema: SchemaRef,
    batch_capacity: usize,
) -> Result<Vec<RecordBatch>> {
    let mut rows = 0;
    let mut map = builder::create_typed_builder_map(&columns, &schema)?;
    let mut collector = RecordBatchCollector::new(batch_capacity, map.len(), schema);
    let mut current_stage = UncompressedStage::Continue;
    while let Some(result) = rx.blocking_recv() {
        let mut buffer = result?;
        current_stage = match current_stage {
            UncompressedStage::Continue => {
                read_columns_uncompressed(&mut buffer, &mut map[..], &mut collector, &mut rows)?
            }
            UncompressedStage::NeedMoreData(o, b) => {
                let map = &mut map[o..];
                read_columns_uncompressed(&mut b.chain(buffer), map, &mut collector, &mut rows)?
            }
        };
    }

    collector.finish(rows, map)
}

fn read_columns_uncompressed<B: ClickhouseBytesRead>(
    mut buffer: B,
    builder_map: &mut TypedBuilderMapSlice<'_>,
    collector: &mut RecordBatchCollector,
    rows: &mut usize,
) -> Result<UncompressedStage> {
    while buffer.remaining() > 0 {
        for (i, (name, (type_, builder))) in builder_map.iter_mut().enumerate() {
            match builder.append_null(&mut buffer, type_) {
                Ok(is_null) if is_null => continue,
                Ok(_) => {}
                Err(Error::RowBinaryRead(TryGetError { available, requested })) => {
                    warn!("Need more bytes (null): requested={requested}, available={available}");
                    return Ok(UncompressedStage::NeedMoreData(i, get_remaining(buffer)));
                }
                Err(error) => return Err(error),
            }

            match builder.append_value(&mut buffer, type_) {
                Ok(()) => {}
                Err(Error::RowBinaryRead(TryGetError { requested, available })) => {
                    warn!("Need more bytes (name={name}): req={requested}, avail={available}");
                    return Ok(UncompressedStage::NeedMoreData(i, get_remaining(buffer)));
                }
                Err(error) => return Err(error),
            }
        }

        *rows += 1;
        collector.next(*rows, builder_map)?;
    }
    Ok(UncompressedStage::Continue)
}

fn get_remaining<B: ClickhouseBytesRead>(mut buffer: B) -> Bytes {
    let remaining_bytes = buffer.remaining();
    if remaining_bytes > 0 {
        let mut bytes_mut = BytesMut::with_capacity(remaining_bytes);
        bytes_mut.resize(remaining_bytes, 0); // This actually sets the length
        buffer.copy_to_slice(&mut bytes_mut);
        bytes_mut.freeze()
    } else {
        Bytes::new()
    }
}

/// Helper struct to collect `RecordBatch`es
struct RecordBatchCollector {
    batches: Vec<RecordBatch>,
    arrays:  Vec<ArrayRef>,
    schema:  SchemaRef,
}

impl RecordBatchCollector {
    fn new(batch_capacity: usize, array_capacity: usize, schema: SchemaRef) -> Self {
        Self {
            batches: Vec::with_capacity(batch_capacity),
            arrays: Vec::with_capacity(array_capacity),
            schema,
        }
    }

    #[inline]
    fn next(&mut self, rows: usize, builder_map: &mut TypedBuilderMapSlice<'_>) -> Result<()> {
        // Cut a record batch
        if rows % BATCH_CHUNK_SIZE == 0 {
            self.arrays.extend(builder_map.iter_mut().map(|(_, (_, b))| b.finish()));
            self.batches.push(
                RecordBatch::try_new(Arc::clone(&self.schema), std::mem::take(&mut self.arrays))
                    .inspect_err(|error| error!(?error, "deserializing record batch"))?,
            );
        }
        Ok(())
    }

    #[inline]
    fn finish(mut self, rows: usize, builder_map: TypedBuilderMap<'_>) -> Result<Vec<RecordBatch>> {
        if rows % BATCH_CHUNK_SIZE != 0 {
            self.arrays.extend(builder_map.into_iter().map(|(_, (_, mut b))| b.finish()));
            let batch = RecordBatch::try_new(self.schema, self.arrays)
                .inspect_err(|error| error!(?error, "deserializing record batch"))?;
            self.batches.push(batch);
        }
        info!("Finished deserializing {rows} rows");

        Ok(self.batches)
    }
}

// // TODO: Remove
// #[allow(unused)]
// async fn read_rows_sync<R>(
//     stream: &mut R,
//     definition: RecordBatchDefinition,
//     overrides: Option<SchemaConversions>,
// ) -> Result<Vec<RecordBatch>>
// where
//     R: Stream<Item = Result<Bytes>> + Unpin + Send,
// {
//     let schema = Arc::clone(&definition.schema);
//     let column_definitions = definition
//         .runtime_definitions(overrides.as_ref())?
//         .or(RecordBatchDefinition::definitions());
//     let Some(definitions) = column_definitions.filter(|c| !c.is_empty()) else {
//         return Err(Error::DDLMalformed("Schema is empty, cannot get rows".into()));
//     };

//     let mut rows = 0;
//     let mut batches = Vec::with_capacity(64);
//     // let mut builder_map = builder::create_builder_map(&definitions, &schema)?;
//     let mut builder_map = builder::create_typed_builder_map(&definitions, &schema)?;
//     let mut arrays = Vec::with_capacity(builder_map.len());

//     while let Some(result) = stream.next().await {
//         let mut buffer = result?;

//         while !buffer.is_empty() {
//             for (name, (type_, builder)) in &mut builder_map {
//                 if !builder.is_nested() && builder.append_null(&mut buffer, type_)? {
//                     continue;
//                 }

//                 builder.append_value(&mut buffer, type_).inspect_err(|error| {
//                     error!(?error, "Error deserializing {name}: rows = {rows}");
//                 })?;
//             }

//             rows += 1;

//             // Cut a record batch
//             if rows % CHUNK_SIZE == 0 {
//                 arrays.extend(builder_map.iter_mut().map(|(_, (_, b))| b.finish()));
//                 let batch = RecordBatch::try_new(Arc::clone(&schema), std::mem::take(&mut
// arrays))                     .inspect_err(|error| error!(?error, "deserializing record batch"))?;
//                 batches.push(batch);
//             }
//         }
//     }

//     info!("Finished deserializing {rows} rows");

//     if rows % CHUNK_SIZE != 0 {
//         arrays.extend(builder_map.into_iter().map(|(_, (_, mut b))| b.finish()));
//         let batch = RecordBatch::try_new(schema, arrays)
//             .inspect_err(|error| error!(?error, "deserializing record batch"))?;
//         batches.push(batch);
//     }

//     Ok(batches)
// }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::*;

    use super::builder::*;
    use crate::Type;

    // Helper to create a test reader from bytes
    // fn create_reader(data: &[u8]) -> impl ClickhouseBytesRead { Bytes::from(data.to_vec()) }

    // ============= RowBinary Column Tests =============

    // #[test]
    // fn test_primitive_types_actual_values() {
    //     // Test Int8
    //     let mut builder = PrimitiveBuilder::<Int8Type>::new();
    //     let mut reader = create_reader(&[0x7F]); // 127
    //     read_column(&mut reader, &Type::Int8, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 127i8);

    //     // Test Int32 little-endian
    //     let mut builder = PrimitiveBuilder::<Int32Type>::new();
    //     let mut reader = create_reader(&[0x2A, 0x00, 0x00, 0x00]); // 42 in LE
    //     read_column(&mut reader, &Type::Int32, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 42i32);

    //     // Test Float32
    //     let mut builder = PrimitiveBuilder::<Float32Type>::new();
    //     let mut reader = create_reader(&[0x00, 0x00, 0x80, 0x3F]); // 1.0 in LE
    //     read_column(&mut reader, &Type::Float32, &mut builder).unwrap();
    //     let array = builder.finish();

    //     assert!((array.value(0) - 1.0f32).abs() < 0.1f32);

    //     // Test UInt64 max value
    //     let mut builder = PrimitiveBuilder::<UInt64Type>::new();
    //     let mut reader = create_reader(&[0xFF; 8]); // u64::MAX
    //     read_column(&mut reader, &Type::UInt64, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), u64::MAX);
    // }

    // #[test]
    // fn test_nullable_primitive_values() {
    //     // Test nullable with null value
    //     let mut builder = PrimitiveBuilder::<Int32Type>::new();
    //     let mut reader = create_reader(&[0x01]); // null flag = 1
    //     read_column(&mut reader, &Type::Nullable(Box::new(Type::Int32)), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert!(array.is_null(0));

    //     // Test nullable with actual value
    //     let mut builder = PrimitiveBuilder::<Int32Type>::new();
    //     let mut reader = create_reader(&[0x00, 0x2A, 0x00, 0x00, 0x00]); // null flag = 0, then
    // 42     read_column(&mut reader, &Type::Nullable(Box::new(Type::Int32)), &mut
    // builder).unwrap();     let array = builder.finish();
    //     assert!(!array.is_null(0));
    //     assert_eq!(array.value(0), 42i32);
    // }

    // #[test]
    // fn test_string_values() {
    //     // Test regular string
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x05, b'h', b'e', b'l', b'l', b'o']); // len=5, "hello"
    //     read_column(&mut reader, &Type::String, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), "hello");

    //     // Test empty string
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x00]); // len=0
    //     read_column(&mut reader, &Type::String, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), "");

    //     // Test Object type fallback (should behave like String)
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x03, b'f', b'o', b'o']);
    //     read_column(&mut reader, &Type::Object, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), "foo");

    //     // Test Object type (should behave like String)
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x05, b'"', b'f', b'o', b'o', b'"']);
    //     read_column(&mut reader, &Type::Object, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), "\"foo\"");

    //     // Test Object type (should behave like String)
    //     let mut builder = StringBuilder::new();
    //     let mut reader =
    //         create_reader(&[0xB, b'{', b'"', b'a', b'"', b':', b'"', b'f', b'o', b'o', b'"',
    // b'}']);     read_column(&mut reader, &Type::Object, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), "{\"a\":\"foo\"}");
    // }

    // #[test]
    // fn test_nullable_string_values() {
    //     // Test nullable string with null
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x01]); // null flag = 1
    //     read_column(&mut reader, &Type::Nullable(Box::new(Type::String)), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert!(array.is_null(0));

    //     // Test nullable string with value
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x00, 0x04, b't', b'e', b's', b't']); // null flag = 0,
    // then "test"     read_column(&mut reader, &Type::Nullable(Box::new(Type::String)), &mut
    // builder).unwrap();     let array = builder.finish();
    //     assert!(!array.is_null(0));
    //     assert_eq!(array.value(0), "test");
    // }

    // #[test]
    // fn test_fixed_size_binary_values() {
    //     // Test UUID (16 bytes)
    //     let uuid_bytes = [
    //         0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
    //         0xDE, 0xF0,
    //     ];
    //     let mut builder = FixedSizeBinaryBuilder::new(16);
    //     let mut reader = create_reader(&uuid_bytes);
    //     read_column(&mut reader, &Type::Uuid, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), &uuid_bytes[..]);

    //     // Test IPv4 (4 bytes)
    //     let ipv4_bytes = [192_u8, 168, 1, 1];
    //     let expected = std::net::Ipv4Addr::from(ipv4_bytes);
    //     let mut builder = FixedSizeBinaryBuilder::new(4);
    //     let mut reader = create_reader(&ipv4_bytes);
    //     read_column(&mut reader, &Type::Ipv4, &mut builder).unwrap();
    //     let array = builder.finish();
    //     let value = array.value(0);
    //     assert_eq!(value.len(), 4);
    //     let value = std::net::Ipv4Addr::from([value[3], value[2], value[1], value[0]]);
    //     assert_eq!(expected, value);

    //     // Test FixedSizedString
    //     let fixed_str_bytes = [b'h', b'e', b'l', b'l', b'o', 0, 0, 0, 0, 0]; // 10 bytes
    //     let mut builder = FixedSizeBinaryBuilder::new(10);
    //     let mut reader = create_reader(&fixed_str_bytes);
    //     read_column(&mut reader, &Type::FixedSizedString(10), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), &fixed_str_bytes[..]);
    // }

    // #[test]
    // fn test_decimal_values() {
    //     // Test Decimal32 - 4 bytes
    //     let mut builder = Decimal128Builder::new().with_precision_and_scale(9, 2).unwrap();
    //     let mut reader = create_reader(&[0x01, 0x00, 0x00, 0x00]); // 1 as i32
    //     read_column(&mut reader, &Type::Decimal32(2), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 1i128);

    //     // Test Decimal64 - 8 bytes
    //     let mut builder = Decimal128Builder::new().with_precision_and_scale(18, 4).unwrap();
    //     let mut reader = create_reader(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // 1
    // as i64     read_column(&mut reader, &Type::Decimal64(4), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 1i128);

    //     // Test Decimal128 - 16 bytes
    //     let mut builder = Decimal128Builder::new().with_precision_and_scale(38, 10).unwrap();
    //     let decimal_bytes = [
    //         0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    //         0x00, 0x00,
    //     ]; // 1 as i128
    //     let mut reader = create_reader(&decimal_bytes);
    //     read_column(&mut reader, &Type::Decimal128(10), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 1i128);
    // }

    // #[test]
    // fn test_date_values() {
    //     // Test Date (days since epoch)
    //     let mut builder = Date32Builder::new();
    //     let mut reader = create_reader(&[0x2A, 0x00, 0x00, 0x00]); // 42 days
    //     read_column(&mut reader, &Type::Date, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 42i32);

    //     // Test Date32
    //     let mut builder = Date32Builder::new();
    //     let arrow_days = 18176i32; // What we want in Arrow (days since 1970)
    //     let days = arrow_days + crate::deserialize::DAYS_1900_TO_1970; // Convert to ClickHouse
    // format     let ch_days = days.to_le_bytes();
    //     let mut reader = create_reader(&ch_days);
    //     read_column(&mut reader, &Type::Date32, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 18176i32);
    // }

    // #[test]
    // fn test_datetime64_precision_ranges() {
    //     let timestamp_bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]; // 8 bytes

    //     // Precision 0 -> TimestampSecondBuilder (reads 8 bytes as i64)
    //     let mut builder = TimestampSecondBuilder::new();
    //     let mut reader = create_reader(&timestamp_bytes);
    //     read_column(&mut reader, &Type::DateTime64(0, Tz::UTC), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 0x0706_0504_0302_0100_i64);

    //     // Precision 1-3 -> TimestampMillisecondBuilder (reads 8 bytes as i64)
    //     let mut builder = TimestampMillisecondBuilder::new();
    //     let mut reader = create_reader(&timestamp_bytes);
    //     read_column(&mut reader, &Type::DateTime64(2, Tz::UTC), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 0x0706_0504_0302_0100_i64);

    //     // Precision 4-6 -> TimestampMicrosecondBuilder (reads 8 bytes as i64)
    //     let mut builder = TimestampMicrosecondBuilder::new();
    //     let mut reader = create_reader(&timestamp_bytes);
    //     read_column(&mut reader, &Type::DateTime64(5, Tz::UTC), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 0x0706_0504_0302_0100_i64);

    //     // Precision 7-9 -> TimestampNanosecondBuilder (reads 8 bytes as i64)
    //     let mut builder = TimestampNanosecondBuilder::new();
    //     let mut reader = create_reader(&timestamp_bytes);
    //     read_column(&mut reader, &Type::DateTime64(8, Tz::UTC), &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), 0x0706_0504_0302_0100_i64);
    // }

    // #[test]
    // fn test_datetime64_invalid_precision() {
    //     let mut builder = TimestampSecondBuilder::new();
    //     let mut reader = create_reader(&[0x00, 0x01, 0x02, 0x03]);

    //     // Test invalid precision (>= 10)
    //     let result = read_column(&mut reader, &Type::DateTime64(10, Tz::UTC), &mut builder);
    //     assert!(result.is_err());
    //     match result.unwrap_err() {
    //         Error::ArrowUnsupportedType(msg) => assert!(msg.contains("DateTime64 > 9: 10")),
    //         _ => panic!("Expected ArrowUnsupportedType error"),
    //     }
    // }

    // #[test]
    // fn test_large_integer_as_binary() {
    //     // Test Int128 (16 bytes as binary)
    //     let int128_bytes = [
    //         0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    //         0x00, 0x00,
    //     ];
    //     let mut builder = FixedSizeBinaryBuilder::new(16);
    //     let mut reader = create_reader(&int128_bytes);
    //     read_column(&mut reader, &Type::Int128, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), &int128_bytes[..]);

    //     // Test UInt256 (32 bytes as binary, reversed)
    //     let uint256_bytes = [0xFF; 32];
    //     let mut builder = FixedSizeBinaryBuilder::new(32);
    //     let mut reader = create_reader(&uint256_bytes);
    //     read_column(&mut reader, &Type::UInt256, &mut builder).unwrap();
    //     let array = builder.finish();
    //     // Note: UInt256 uses FixedRev so bytes should be reversed
    //     let expected: Vec<u8> = uint256_bytes.iter().rev().copied().collect();
    //     assert_eq!(array.value(0), &expected[..]);
    // }

    // #[test]
    // fn test_error_conditions() {
    //     // Test insufficient data for Int32
    //     let mut builder = PrimitiveBuilder::<Int32Type>::new();
    //     let mut reader = create_reader(&[0x00, 0x01]); // Only 2 bytes, need 4
    //     let result = read_column(&mut reader, &Type::Int32, &mut builder);
    //     assert!(result.is_err());

    //     // Test insufficient data for nullable null flag
    //     let mut builder = PrimitiveBuilder::<Int32Type>::new();
    //     let mut reader = create_reader(&[]); // No data at all
    //     let result = read_column(&mut reader, &Type::Nullable(Box::new(Type::Int32)), &mut
    // builder);     assert!(result.is_err());

    //     // Test string with declared length but insufficient data
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x05, b'h', b'i']); // Says 5 bytes, only has 2
    //     let result = read_column(&mut reader, &Type::String, &mut builder);
    //     assert!(result.is_err());
    // }

    // ============= Builder Creation Tests =============

    #[test]
    fn test_create_builder_map_missing_field_error() {
        let definitions = vec![("missing_col".to_string(), Type::Int32, None)];
        let schema =
            Arc::new(Schema::new(vec![Field::new("existing_col", DataType::Int32, false)]));

        let result = create_typed_builder_map(&definitions, &schema);
        assert!(result.is_err());
        // Should be an arrow::ArrowError about field not found
    }

    #[test]
    fn test_traceb_function() {
        // Test that traceb returns the correct builder type
        let original_builder = PrimitiveBuilder::<Int32Type>::new();
        let boxed_builder = traceb(original_builder, &Type::Int32, "Int32", "test_col");

        // Should be able to downcast back to the original type
        assert!(boxed_builder.as_any().is::<PrimitiveBuilder<Int32Type>>());

        // Test with different builder type
        let string_builder = StringBuilder::new();
        let boxed_string_builder = traceb(string_builder, &Type::String, "String", "test_str");
        assert!(boxed_string_builder.as_any().is::<StringBuilder>());
    }

    // #[test]
    // fn test_multiple_values_same_builder() {
    //     // Test that we can deserialize multiple values with the same builder
    //     let mut builder = PrimitiveBuilder::<Int32Type>::new();

    //     // First value
    //     let mut reader = create_reader(&[0x01, 0x00, 0x00, 0x00]); // 1
    //     read_column(&mut reader, &Type::Int32, &mut builder).unwrap();

    //     // Second value
    //     let mut reader = create_reader(&[0x02, 0x00, 0x00, 0x00]); // 2
    //     read_column(&mut reader, &Type::Int32, &mut builder).unwrap();

    //     // Third value
    //     let mut reader = create_reader(&[0x03, 0x00, 0x00, 0x00]); // 3
    //     read_column(&mut reader, &Type::Int32, &mut builder).unwrap();

    //     let array = builder.finish();
    //     assert_eq!(array.len(), 3);
    //     assert_eq!(array.value(0), 1);
    //     assert_eq!(array.value(1), 2);
    //     assert_eq!(array.value(2), 3);
    // }

    // #[test]
    // fn test_edge_case_values() {
    //     // Test edge case numeric values

    //     // Test i8 min/max
    //     let mut builder = PrimitiveBuilder::<Int8Type>::new();
    //     let mut reader = create_reader(&[0x80]); // -128 (i8::MIN)
    //     read_column(&mut reader, &Type::Int8, &mut builder).unwrap();
    //     let mut reader = create_reader(&[0x7F]); // 127 (i8::MAX)
    //     read_column(&mut reader, &Type::Int8, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), i8::MIN);
    //     assert_eq!(array.value(1), i8::MAX);

    //     // Test u64 max
    //     let mut builder = PrimitiveBuilder::<UInt64Type>::new();
    //     let mut reader = create_reader(&[0xFF; 8]); // u64::MAX
    //     read_column(&mut reader, &Type::UInt64, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), u64::MAX);

    //     // Test empty string
    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&[0x00]); // length = 0
    //     read_column(&mut reader, &Type::String, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), "");

    //     // Test very long string
    //     let long_string = "x".repeat(1000);
    //     let mut string_data = vec![0xE8, 0x07]; // 1000 as varint
    //     string_data.extend(long_string.bytes());

    //     let mut builder = StringBuilder::new();
    //     let mut reader = create_reader(&string_data);
    //     read_column(&mut reader, &Type::String, &mut builder).unwrap();
    //     let array = builder.finish();
    //     assert_eq!(array.value(0), long_string);
    // }
}
