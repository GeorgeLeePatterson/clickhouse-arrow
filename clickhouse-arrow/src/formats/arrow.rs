use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::BytesMut;

use super::DeserializerState;
use super::protocol_data::{EmptyBlock, ProtocolData};
use crate::Type;
use crate::arrow::ArrowDeserializerState;
use crate::compression::{DecompressionReader, compress_data_sync};
use crate::connection::ClientMetadata;
use crate::io::{ClickHouseRead, ClickHouseWrite};
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
    type Deser = ArrowDeserializerState;
    type Schema = SchemaRef;
    type Ser = ();

    fn finish_deser(state: &mut DeserializerState<Self::Deser>) {
        state.deserializer().builders.clear();
        state.deserializer().buffer.clear();
    }

    async fn write<W: ClickHouseWrite>(
        writer: &mut W,
        batch: RecordBatch,
        qid: Qid,
        header: Option<&[(String, Type)]>,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<()> {
        if let CompressionMethod::None = metadata.compression {
            batch
                .write_async(writer, revision, header, metadata.arrow_options)
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "serialize"))?;
        } else {
            let mut raw = BytesMut::with_capacity(batch.get_array_memory_size());
            batch
                .write(&mut raw, revision, header, metadata.arrow_options)
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "serialize"))?;
            compress_data_sync(writer, raw.freeze(), metadata.compression)
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "compressing"))?;
        }

        Ok(())
    }

    async fn read<R: ClickHouseRead + 'static>(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
        state: &mut DeserializerState<Self::Deser>,
    ) -> Result<Option<RecordBatch>> {
        let arrow_options = metadata.arrow_options;
        if let CompressionMethod::None = metadata.compression {
            RecordBatch::read_async(reader, revision, arrow_options, state).await
        } else {
            let mut decompressor = DecompressionReader::new(metadata.compression, reader).await?;
            RecordBatch::read_async(&mut decompressor, revision, arrow_options, state).await
        }
        .inspect_err(|error| error!(?error, "deserializing arrow record batch"))
        .map(RecordBatch::into_option)
    }
}
