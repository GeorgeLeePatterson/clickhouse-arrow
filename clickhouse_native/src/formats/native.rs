use super::protocol_data::{EmptyBlock, ProtocolData};
use crate::Type;
use crate::client::connection::ClientMetadata;
use crate::compression::compress_data;
use crate::io::{ClickhouseRead, ClickhouseWrite};
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
    type Schema = Vec<(String, Type)>;

    async fn read<R: ClickhouseRead + 'static>(
        reader: &mut R,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<Option<Block>> {
        Ok(if let CompressionMethod::None = metadata.compression {
            Block::read(reader, revision, ()).await?.into_option()
        } else {
            let mut reader =
                crate::compression::DecompressionReader::new(metadata.compression, reader).await?;
            Block::read(&mut reader, revision, ()).await?.into_option()
        })
    }

    async fn write<W: ClickhouseWrite>(
        writer: &mut W,
        data: Block,
        qid: Qid,
        header: Option<&[(String, Type)]>,
        revision: u64,
        metadata: ClientMetadata,
    ) -> Result<()> {
        if let CompressionMethod::None = metadata.compression {
            data.write(writer, revision, header, ())
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "(block:uncompressed)"))
        } else {
            let mut raw = vec![];
            data.write(&mut raw, revision, header, ())
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, {ATT_QID} = %qid, "(block:compressed)"))?;
            compress_data(writer, raw, metadata.compression)
                .instrument(trace_span!("compress_block"))
                .await
                .inspect_err(|error| error!(?error, {ATT_QID} = %qid, "compressing"))
        }
    }
}
