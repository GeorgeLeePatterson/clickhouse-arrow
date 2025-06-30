use bytes::BytesMut;

use super::DeserializerState;
use super::protocol_data::{EmptyBlock, ProtocolData};
use crate::Type;
use crate::client::connection::ClientMetadata;
use crate::compression::{compress_data_sync, decompress_data_async};
use crate::io::{ClickHouseRead, ClickHouseWrite};
use crate::native::block::Block;
use crate::native::protocol::CompressionMethod;
use crate::prelude::*;

/// Marker for Native format.
///
/// Read native `ClickHouse` blocks into this library's `Block` struct and write `Block`s into the
/// provided writer.
#[derive(Debug, Clone, Copy)]
pub struct NativeFormat {}

impl ClientFormat for NativeFormat {
    type Data = Block;

    const FORMAT: &'static str = "Native";
}

impl super::sealed::ClientFormatImpl<Block> for NativeFormat {
    type Deser = ();
    type Schema = Vec<(String, Type)>;
    type Ser = ();

    async fn read<R: ClickHouseRead + 'static>(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
        state: &mut DeserializerState,
    ) -> Result<Option<Block>> {
        Ok(if let CompressionMethod::None = metadata.compression {
            Block::read_async(reader, revision, (), state).await?.into_option()
        } else {
            let mut buffer =
                BytesMut::from_iter(decompress_data_async(reader, metadata.compression).await?);
            Block::read(&mut buffer, revision, (), state)?.into_option()
        })
    }

    async fn write<W: ClickHouseWrite>(
        writer: &mut W,
        data: Block,
        qid: Qid,
        header: Option<&[(String, Type)]>,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<()> {
        if let CompressionMethod::None = metadata.compression {
            data.write_async(writer, revision, header, ())
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "(block:uncompressed)"))
        } else {
            // Hybrid approach: sync serialization + async compression
            let estimated_size = data.estimate_size();
            let mut buffer = BytesMut::with_capacity(estimated_size);

            data.write(&mut buffer, revision, header, ())
                .inspect_err(|error| error!(?error, {ATT_QID} = %qid, "(block:compressed)"))?;

            compress_data_sync(writer, buffer.freeze(), metadata.compression)
                .instrument(trace_span!("compress_block"))
                .await
                .inspect_err(|error| error!(?error, {ATT_QID} = %qid, "compressing"))
        }
    }
}
