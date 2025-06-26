// TODO: Remove
#![cfg_attr(feature = "profile", allow(clippy::cast_precision_loss))]

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes, BytesMut, TryGetError};
use futures_util::{Stream, StreamExt};
use tracing::*;
#[cfg(feature = "profile")]
use tracy_client::{non_continuous_frame, plot, span as tracy_span};

use super::builder::{self, TypedBuilderMap};
use super::protocol::HttpSummary;
use crate::formats::protocol_data::RowData;
use crate::io::ClickHouseBytesRead;
use crate::prelude::SchemaConversions;
use crate::schema::{ColumnDefine, RecordBatchDefinition};
use crate::spawn::SpawnedTask;
use crate::{ColumnDefinition, CompressionMethod, Error, Result};

pub(super) const BATCH_CHUNK_SIZE: usize = DEFAULT_CHUNK_ROWS * 2;
const DEFAULT_CHUNK_ROWS: usize = 65409; // Seems to be the chunk size used by ClickHouse
const LARGE_READ_ROWS_THRESHOLD: usize = 1_000_000;
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
        // TODO: Remove
        #[cfg(feature = "profile")]
        let guard = non_continuous_frame!("read_rows");

        // TODO: Remove
        #[cfg(feature = "profile")]
        let span = tracy_span!("read_rows_definitions");

        let schema = Arc::clone(&definition.schema);
        let column_definitions = definition
            .runtime_definitions(overrides.as_ref())?
            .or(RecordBatchDefinition::definitions());
        let Some(columns) = column_definitions.filter(|c| !c.is_empty()) else {
            return Err(Error::DDLMalformed("Schema is empty, cannot get rows".into()));
        };

        let total_rows = summary.total_rows_to_read;
        let batch_cap = if total_rows > 0 {
            summary.total_rows_to_read.div_ceil(BATCH_CHUNK_SIZE) + 1
        } else {
            DEFAULT_BATCHES_CAPACITY
        };

        // TODO: Remove
        #[cfg(feature = "profile")]
        drop(span);

        let compression = summary.compression;

        // Use different techniques depending on the size of the data, defaulting to parallel
        let result = if total_rows == 0 || total_rows >= LARGE_READ_ROWS_THRESHOLD {
            read_rows_parallel(stream, columns, schema, compression, batch_cap).await
        } else {
            read_rows_sync(stream, columns, schema, compression, batch_cap).await
        };

        // TODO: Remove
        #[cfg(feature = "profile")]
        drop(guard);

        result
    }
}

// Helper macro to split the compressed/uncompressed code paths
macro_rules! read_rows_loop {
    (uncompressed => $coll:expr, $next:expr) => {{
        // Allocate overflow buffer once
        let mut overflow = BytesMut::with_capacity(32);
        let mut stage = UncompressedStage::Continue;
        // TODO: Remove
        #[cfg(feature = "profile")]
        let (mut cont, mut more) = (0, 0);

        while let Some(result) = $next {
            let buffer = result?;

            // TODO: Remove
            #[cfg(feature = "profile")]
            plot!("deserialize_buffer_size", buffer.len() as f64);

            stage = {
                // TODO: Remove or use
                #[cfg(feature = "profile")]
                let _span = tracy_span!("deserialize_uncompressed_chunk");
                deserialize_rows_uncompressed(buffer, $coll, stage, &mut overflow)?
            };

            #[cfg(feature = "profile")]
            match &stage {
                UncompressedStage::Continue => {
                    cont += 1;
                    plot!("stage_continue", f64::from(cont))
                }
                UncompressedStage::NeedMoreData(_, _) => {
                    more += 1;
                    plot!("stage_need_more", f64::from(more))
                }
            }
        }
    }};
    ($coll:expr, $next:expr) => {{
        while let Some(result) = $next {
            let buffer = result?;

            // TODO: Remove
            #[cfg(feature = "profile")]
            plot!("deserialize_buffer_size", buffer.len() as f64);

            {
                // TODO: Remove or use
                #[cfg(feature = "profile")]
                let _span = tracy_span!("deserialize_compressed_chunk");

                deserialize_rows(buffer, $coll)?;
            }
        }
    }};
}

// Directly reads buffers from the stream sequentially. MUCH slower for x-large datasets
async fn read_rows_sync<R>(
    stream: &mut R,
    columns: Vec<ColumnDefinition<String>>,
    schema: SchemaRef,
    compression: CompressionMethod,
    batch_capacity: usize,
) -> Result<Vec<RecordBatch>>
where
    R: Stream<Item = Result<Bytes>> + Unpin + Send,
{
    // TODO: Remove
    #[cfg(feature = "profile")]
    let guard = non_continuous_frame!("read_rows_sync");

    let map = builder::create_typed_builder_map(&columns, &schema)?;
    let mut collector = RecordBatchCollector::new(batch_capacity, map.len(), schema, map);
    if matches!(compression, CompressionMethod::None) {
        read_rows_loop!(uncompressed => &mut collector, stream.next().await);
    } else {
        read_rows_loop!(&mut collector, stream.next().await);
    }

    // TODO: Remove
    #[cfg(feature = "profile")]
    drop(guard);

    collector.finish()
}

// Parallel introduces message passing overhead but is faster for x-large datasets
async fn read_rows_parallel<R>(
    stream: &mut R,
    columns: Vec<ColumnDefinition<String>>,
    schema: SchemaRef,
    compression: CompressionMethod,
    batch_capacity: usize,
) -> Result<Vec<RecordBatch>>
where
    R: Stream<Item = Result<Bytes>> + Unpin + Send,
{
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Result<Bytes>>();
    let deserializer = SpawnedTask::spawn_blocking(move || {
        #[cfg(feature = "profile")]
        let _span = tracy_span!("deserializer_parallel");
        let map = builder::create_typed_builder_map(&columns, &schema)?;
        let mut collector = RecordBatchCollector::new(batch_capacity, map.len(), schema, map);
        if matches!(compression, CompressionMethod::None) {
            read_rows_loop!(uncompressed => &mut collector, rx.blocking_recv());
        } else {
            read_rows_loop!(&mut collector, rx.blocking_recv());
        }
        collector.finish()
    });

    // TODO: Remove
    #[cfg(feature = "profile")]
    let guard = non_continuous_frame!("read_rows_parallel");

    // Send blocks
    while let Some(result) = stream.next().await {
        tx.send(result).map_err(|_| Error::ArrowDeserialize("channel closed".into()))?;
    }
    // Drop the sender to allow the rx to finish, dead lock otherwise
    drop(tx);

    // Collect the results
    let result = deserializer
        .join_unwind()
        .await
        .map_err(|_| Error::ArrowDeserialize("deser task".into()))?;

    // TODO: Remove
    #[cfg(feature = "profile")]
    drop(guard);

    result
}

// ---
// Compressed Path
// ---

// Compressed path has natural boundaries, no need to track
fn deserialize_rows(mut buffer: Bytes, collector: &mut RecordBatchCollector<'_>) -> Result<()> {
    while !buffer.is_empty() {
        for (name, (type_, builder)) in &mut collector.builder_map {
            if builder.append_null(&mut buffer, type_)? {
                continue;
            }
            builder.append_value(&mut buffer, type_).inspect_err(|error| {
                error!(?error, "Error deserializing {name}");
            })?;
        }
        collector.next(0)?;
    }
    Ok(())
}

// ---
// Uncompressed Path
// ---

// Uncompressed can run into TryGetRead errors indicating more data needed
#[derive(Debug)]
enum UncompressedStage {
    NeedMoreData(usize, Bytes),
    Continue,
}

fn deserialize_rows_uncompressed(
    mut buffer: Bytes,
    collector: &mut RecordBatchCollector<'_>,
    current_stage: UncompressedStage,
    overflow_buffer: &mut BytesMut,
) -> Result<UncompressedStage> {
    #[cfg(feature = "profile")]
    let _span = tracy_span!("deserialize_rows_uncompressed");

    match current_stage {
        UncompressedStage::Continue => {
            read_columns_uncompressed(&mut buffer, collector, overflow_buffer, 0)
        }
        UncompressedStage::NeedMoreData(o, b) => {
            read_columns_uncompressed(&mut b.chain(buffer), collector, overflow_buffer, o)
        }
    }
}

fn read_columns_uncompressed<B: ClickHouseBytesRead>(
    mut buffer: B,
    collector: &mut RecordBatchCollector<'_>,
    overflow_buffer: &mut BytesMut,
    column_offset: usize,
) -> Result<UncompressedStage> {
    while buffer.remaining() > 0 {
        let map = &mut collector.builder_map[column_offset..];
        for (i, (name, (type_, builder))) in map.iter_mut().enumerate() {
            let details = match builder.append_null(&mut buffer, type_) {
                Ok(is_null) if is_null => continue,
                Ok(_) => match builder.append_value(&mut buffer, type_) {
                    Ok(()) => continue,
                    Err(Error::BytesRead(details)) => details,
                    Err(error) => return Err(error),
                },
                Err(Error::BytesRead(err)) => err,
                Err(error) => return Err(error),
            };

            let TryGetError { requested, available } = details;
            warn!("Need more bytes (name={name}): req={requested}, avail={available}");
            let remaining = get_remaining(buffer, std::mem::take(overflow_buffer));

            // TODO: Remove
            #[cfg(feature = "profile")]
            plot!("overflow_buffer_size", remaining.len() as f64);

            return Ok(UncompressedStage::NeedMoreData(i, remaining));
        }
        collector.next(column_offset)?;
    }
    Ok(UncompressedStage::Continue)
}

fn get_remaining<B: ClickHouseBytesRead>(mut buffer: B, mut overflow_buffer: BytesMut) -> Bytes {
    let remaining_bytes = buffer.remaining();
    if remaining_bytes > 0 {
        overflow_buffer.resize(remaining_bytes, 0);
        buffer.copy_to_slice(&mut overflow_buffer);
        overflow_buffer.freeze()
    } else {
        Bytes::new()
    }
}

/// Helper struct to collect `RecordBatch`es
struct RecordBatchCollector<'a> {
    builder_map: TypedBuilderMap<'a>,
    batches:     Vec<RecordBatch>,
    arrays:      Vec<ArrayRef>,
    schema:      SchemaRef,
    rows:        usize,
}

impl<'a> RecordBatchCollector<'a> {
    fn new(
        batch_capacity: usize,
        array_capacity: usize,
        schema: SchemaRef,
        builder_map: TypedBuilderMap<'a>,
    ) -> Self {
        Self {
            builder_map,
            batches: Vec::with_capacity(batch_capacity),
            arrays: Vec::with_capacity(array_capacity),
            schema,
            rows: 0,
        }
    }

    #[inline]
    fn next(&mut self, col_offset: usize) -> Result<()> {
        self.rows += 1;

        // Cut a record batch
        if self.rows % BATCH_CHUNK_SIZE == 0 {
            #[cfg(feature = "profile")]
            let _span = tracy_span!("create_record_batch");

            self.arrays
                .extend(self.builder_map[col_offset..].iter_mut().map(|(_, (_, b))| b.finish()));
            self.batches.push(
                RecordBatch::try_new(Arc::clone(&self.schema), std::mem::take(&mut self.arrays))
                    .inspect_err(|error| error!(?error, "deserializing record batch"))?,
            );

            #[cfg(feature = "profile")]
            plot!("batch_rows", self.batches.len() as f64);

            #[cfg(feature = "profile")]
            plot!("cumulative_rows", self.rows as f64);
        }
        Ok(())
    }

    #[inline]
    fn finish(mut self) -> Result<Vec<RecordBatch>> {
        if self.rows % BATCH_CHUNK_SIZE != 0 {
            #[cfg(feature = "profile")]
            let _span = tracy_span!("create_record_batch_final");

            self.arrays.extend(self.builder_map.into_iter().map(|(_, (_, mut b))| b.finish()));
            let batch = RecordBatch::try_new(self.schema, self.arrays)
                .inspect_err(|error| error!(?error, "deserializing record batch"))?;
            self.batches.push(batch);

            #[cfg(feature = "profile")]
            plot!("batch_rows", self.batches.len() as f64);
        }

        #[cfg(feature = "profile")]
        plot!("cumulative_rows", self.rows as f64);

        info!("Finished deserializing {} rows", self.rows);
        Ok(self.batches)
    }
}
