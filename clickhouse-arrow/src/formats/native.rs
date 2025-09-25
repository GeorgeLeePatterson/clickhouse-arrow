// use bytes::BytesMut;

use super::DeserializerState;
use super::protocol_data::{EmptyBlock, ProtocolData};
use crate::Type;
use crate::client::connection::ClientMetadata;
use crate::compression::{StreamingCompressor, StreamingDecompressor};
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
        // this reads / parses one block at a time serially, clickhouse client seems to operate in
        // parallel. Even given that it seems like we're 3x faster for single threaded reading?
        // It would still be cool to support parallel reading/deserialization in the future.
        Ok(if let CompressionMethod::None = metadata.compression {
            Block::read_async(reader, revision, (), state).await?.into_option()
        } else {
            // Stream-decompress all chunks for this packet and read block asynchronously
            let mut decompressor = StreamingDecompressor::new(metadata.compression, reader).await?;
            Block::read_async(&mut decompressor, revision, (), state).await?.into_option()
        })
    }

    async fn write<W: ClickHouseWrite>(
        writer: &mut W,
        data: Block,
        qid: Qid,
        header: Option<&[(String, Type)]>,
        revision: u64, // TODO: what is revision - server revision? compression block format
        // revision? Would be good to document.
        metadata: ClientMetadata,
    ) -> Result<()> {
        // No-op: avoid noisy header logs in normal operation
        if let CompressionMethod::None = metadata.compression {
            data.write_async(writer, revision, header, ())
                .instrument(trace_span!("serialize_block"))
                .await
                .inspect_err(|error| error!(?error, { ATT_QID } = %qid, "(block:uncompressed)"))
        } else {
            // Stream-compress while writing the block to avoid buffering the whole block in memory
            use tokio::io::AsyncWriteExt as _;
            let mut sc = StreamingCompressor::new(
                writer,
                metadata.compression,
                1 << 20, // 1 MiB chunks (consider exposing via ClientOptions in the future)
            );
            let res = data
                .write_async(&mut sc, revision, header, ())
                .instrument(trace_span!("serialize_block_streaming"))
                .await
                .inspect_err(
                    |error| error!(?error, {ATT_QID} = %qid, "(block:streaming-compressed)"),
                );
            // Ensure all frames are flushed; do NOT shutdown the underlying socket here.
            if let Err(e) = sc.flush().await {
                error!(?e, {ATT_QID} = %qid, "flush compressor");
            }
            res
        }
    }
}
