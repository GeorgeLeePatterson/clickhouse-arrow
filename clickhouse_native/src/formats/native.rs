use super::protocol_data::{EmptyBlock, ProtocolData};
use crate::Type;
use crate::client::connection::ConnectionMetadata;
use crate::compression::write_compressed_data;
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
        metadata: ConnectionMetadata,
    ) -> Result<Option<Block>> {
        let revision = metadata.revision;
        let compression = metadata.compression;

        Ok(match compression {
            CompressionMethod::None => Block::read(reader, revision, ()).await?.into_option(),
            CompressionMethod::LZ4 => {
                let mut reader = crate::compression::DecompressionReader::new(compression, reader);
                Block::read(&mut reader, revision, ()).await?.into_option()
            }
        })
    }

    async fn write<W: ClickhouseWrite + 'static>(
        writer: &mut W,
        data: Block,
        qid: Qid,
        header: Option<&[(String, Type)]>,
        metadata: ConnectionMetadata,
    ) -> Result<()> {
        let revision = metadata.revision;
        let compression = metadata.compression;
        match compression {
            CompressionMethod::None => data
                .write(writer, revision, header, ())
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "(block:uncompressed)")),
            CompressionMethod::LZ4 => {
                let mut raw = vec![];
                data.write(&mut raw, revision, header, ())
                    .instrument(trace_span!("serialize_block"))
                    .await
                    .inspect_err(|error| error!(?error, {ATT_QID} = %qid, "(block:compressed)"))?;
                write_compressed_data(writer, raw, compression)
                    .instrument(trace_span!("compress_block"))
                    .await
                    .inspect_err(|error| error!(?error, {ATT_QID} = %qid, "compressing"))
            }
        }
    }
}
