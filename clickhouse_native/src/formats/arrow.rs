use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::protocol_data::{EmptyBlock, ProtocolData};
use crate::Type;
use crate::compression::{decompress_data, write_compressed_data};
use crate::connection::ConnectionMetadata;
use crate::io::{ClickhouseRead, ClickhouseWrite};
use crate::native::protocol::CompressionMethod;
use crate::prelude::*;

/// Marker trait for Arrow format.
///
/// Read native `ClickHouse` blocks into arrow `RecordBatch`es and write arrow `RecordBatch`es into
/// native blocks.
#[derive(Debug, Clone, Copy)]
pub struct ArrowFormat {}

impl ClientFormat for ArrowFormat {
    type Data = RecordBatch;

    const FORMAT: &'static str = "Arrow";
}

impl super::sealed::ClientFormatImpl<RecordBatch> for ArrowFormat {
    type Schema = SchemaRef;

    async fn read<R: ClickhouseRead + 'static>(
        reader: &mut R,
        metadata: ConnectionMetadata,
    ) -> Result<Option<RecordBatch>> {
        let arrow_options = metadata.arrow_options;
        match metadata.compression {
            CompressionMethod::None => {
                RecordBatch::read(reader, metadata.revision, arrow_options).await
            }
            CompressionMethod::LZ4 => {
                let decompressed = decompress_data(reader, metadata.compression, None)
                    .await
                    .inspect_err(|error| error!(?error, "decompressing data"))?;
                RecordBatch::read(&mut decompressed.as_slice(), metadata.revision, arrow_options)
                    .await
            }
        }
        .inspect_err(|error| error!(?error, "deserializing arrow record batch"))
        .map(RecordBatch::into_option)
    }

    async fn write<W: ClickhouseWrite + 'static>(
        writer: &mut W,
        batch: RecordBatch,
        qid: Qid,
        header: Option<&[(String, Type)]>,
        metadata: ConnectionMetadata,
    ) -> Result<()> {
        match metadata.compression {
            CompressionMethod::None => {
                batch
                    .write(writer, metadata.revision, header, metadata.arrow_options)
                    .instrument(trace_span!("serialize_block"))
                    .await
                    .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "serialize"))?;
            }
            CompressionMethod::LZ4 => {
                let mut raw = vec![];
                batch
                    .write(&mut raw, metadata.revision, header, metadata.arrow_options)
                    .instrument(trace_span!("serialize_block"))
                    .await
                    .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "serialize"))?;
                write_compressed_data(writer, raw, metadata.compression)
                    .await
                    .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "compressing"))?;
            }
        }

        Ok(())
    }
}
