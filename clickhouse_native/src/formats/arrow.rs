use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
#[cfg(feature = "row_binary")]
use futures_util::Stream;

use super::protocol_data::{EmptyBlock, ProtocolData};
use crate::Type;
use crate::compression::{DecompressionReader, compress_data};
use crate::connection::ClientMetadata;
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
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<Option<RecordBatch>> {
        let arrow_options = metadata.arrow_options;
        if let CompressionMethod::None = metadata.compression {
            RecordBatch::read(reader, revision, arrow_options).await
        } else {
            let mut streaming_reader =
                DecompressionReader::new(metadata.compression, reader).await?;
            RecordBatch::read(&mut streaming_reader, revision, arrow_options).await
        }
        .inspect_err(|error| error!(?error, "deserializing arrow record batch"))
        .map(RecordBatch::into_option)
    }

    async fn write<W: ClickhouseWrite>(
        writer: &mut W,
        batch: RecordBatch,
        qid: Qid,
        header: Option<&[(String, Type)]>,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<()> {
        if let CompressionMethod::None = metadata.compression {
            batch
                .write(writer, revision, header, metadata.arrow_options)
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "serialize"))?;
        } else {
            let mut raw = vec![];
            batch
                .write(&mut raw, revision, header, metadata.arrow_options)
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "serialize"))?;
            compress_data(writer, raw, metadata.compression)
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "compressing"))?;
        }

        Ok(())
    }

    #[cfg(feature = "row_binary")]
    async fn read_rows<R>(
        reader: &mut R,
        schema: Self::Schema,
        overrides: Option<SchemaConversions>,
        metadata: ClientMetadata,
        summary: crate::row::protocol::HttpSummary,
    ) -> Result<Vec<RecordBatch>>
    where
        R: Stream<Item = Result<bytes::Bytes, Error>> + Unpin + Send,
    {
        use super::protocol_data::RowData;

        let arrow_options = metadata.arrow_options;

        // Create schema definition
        let definition =
            RecordBatchDefinition { arrow_options: Some(arrow_options), schema, defaults: None };

        if let CompressionMethod::None = metadata.compression {
            RecordBatch::read_rows(reader, definition, overrides, summary).await
        } else {
            let mut streaming_reader =
                crate::compression::http::Decompressor::new(reader, metadata.compression);
            RecordBatch::read_rows(&mut streaming_reader, definition, overrides, summary).await
        }
        .inspect_err(|error| error!(?error, "deserializing arrow record batch"))
    }
}
