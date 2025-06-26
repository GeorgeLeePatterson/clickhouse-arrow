/// Compression and decompression utilities for `ClickHouse`'s native protocol.
///
/// This module provides functions to compress and decompress data using LZ4, as well as a
/// `DecompressionReader` for streaming decompression of `ClickHouse` data blocks. It supports
/// the `CompressionMethod` enum from the protocol module, handling both LZ4 and no
/// compression.
///
/// # Features
/// - Compresses raw data into LZ4 format with `compress_data`.
/// - Decompresses LZ4-compressed data with `decompress_data`, including checksum validation.
/// - Provides `DecompressionReader` for async reading of decompressed data streams.
///
/// # `ClickHouse` Reference
/// See the [ClickHouse Native Protocol Documentation](https://clickhouse.com/docs/en/interfaces/tcp)
/// for details on compression in the native protocol.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use crate::io::{ClickHouseRead, ClickHouseWrite};
use crate::native::protocol::CompressionMethod;
use crate::{Error, Result};

/// Writes compressed data using `ClickHouse` chunk format.
///
/// Compresses the input data using the specified compression method and writes it with:
/// - 16 bytes: `CityHash128` checksum of the compressed block
/// - 1 byte: compression type
/// - 4 bytes: compressed size (including 9-byte header)
/// - 4 bytes: decompressed size
/// - N bytes: compressed payload
///
/// # Arguments
/// * `writer` - The writer to write compressed data to
/// * `raw` - The raw data to compress
/// * `compression` - The compression method to use (LZ4, ZSTD, or None)
///
/// # Returns
/// `Result<()>` indicating success or failure
#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn compress_data<W: ClickHouseWrite>(
    writer: &mut W,
    raw: Vec<u8>,
    compression: CompressionMethod,
) -> Result<()> {
    let decompressed_size = raw.len();
    let mut out = match compression {
        // ZSTD with default compression level (1)
        CompressionMethod::ZSTD => zstd::bulk::compress(&raw, 1)
            .map_err(|e| Error::SerializeError(format!("ZSTD compress error: {e}")))?,
        // LZ4
        CompressionMethod::LZ4 => lz4_flex::compress(&raw),
        // None
        CompressionMethod::None => return Ok(()),
    };

    let mut new_out = Vec::with_capacity(out.len() + 13);
    new_out.push(compression.byte());
    new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
    new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
    new_out.append(&mut out);

    let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
    writer.write_u64_le((hash >> 64) as u64).await?;
    writer.write_u64_le(hash as u64).await?;
    writer.write_all(&new_out[..]).await?;

    Ok(())
}

#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn compress_data_sync<W: ClickHouseWrite>(
    writer: &mut W,
    raw: bytes::Bytes,
    compression: CompressionMethod,
) -> Result<()> {
    let decompressed_size = raw.len();
    let mut out = match compression {
        // ZSTD with default compression level (1)
        CompressionMethod::ZSTD => zstd::bulk::compress(&raw, 1)
            .map_err(|e| Error::SerializeError(format!("ZSTD compress error: {e}")))?,
        // LZ4
        CompressionMethod::LZ4 => lz4_flex::compress(&raw),
        // None
        CompressionMethod::None => return Ok(()),
    };

    let mut new_out = Vec::with_capacity(out.len() + 13);
    new_out.push(compression.byte());
    new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
    new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
    new_out.append(&mut out);

    let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
    writer.write_u64_le((hash >> 64) as u64).await?;
    writer.write_u64_le(hash as u64).await?;
    writer.write_all(&new_out[..]).await?;

    Ok(())
}

/// Reads and decompresses a single compression chunk.
///
/// Reads the chunk header (checksum + compression metadata), validates the checksum,
/// and decompresses the payload using the specified compression method. Each chunk follows the
/// format:
/// - 16 bytes: `CityHash128` checksum
/// - 1 byte: compression type
/// - 4 bytes: compressed size (including 9-byte header)
/// - 4 bytes: decompressed size
/// - N bytes: compressed payload
///
/// # Arguments
/// * `reader` - The reader to read chunk data from
/// * `compression` - The `CompressionMethod` used (LZ4 or ZSTD)
///
/// # Returns
/// Decompressed chunk data as `Vec<u8>`
///
/// # Errors
/// - I/O errors if unable to read expected data
/// - Checksum mismatches indicating data corruption
/// - Decompression failures
/// - Memory safety violations for oversized chunks
pub(crate) async fn decompress_data(
    reader: &mut impl ClickHouseRead,
    compression: CompressionMethod,
) -> Result<Vec<u8>> {
    // Read checksum (16 bytes)
    let checksum_high = reader
        .read_u64_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read checksum high: {e}")))?;
    let checksum_low = reader
        .read_u64_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read checksum low: {e}")))?;
    let checksum = (u128::from(checksum_high) << 64) | u128::from(checksum_low);

    // Read compression header (9 bytes)
    let type_byte = reader
        .read_u8()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read compression type: {e}")))?;
    if type_byte != compression.byte() {
        return Err(Error::Protocol(format!(
            "Unexpected compression algorithm for {compression}: {type_byte:02x}"
        )));
    }

    let compressed_size = reader
        .read_u32_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read compressed size: {e}")))?;
    let decompressed_size = reader
        .read_u32_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read decompressed size: {e}")))?;

    // Sanity checks
    if compressed_size > 100_000_000 || decompressed_size > 1_000_000_000 {
        return Err(Error::Protocol("Chunk size too large".to_string()));
    }

    // Build the complete compressed block for checksum validation
    let mut compressed = vec![0u8; compressed_size as usize];
    let _ = reader
        .read_exact(&mut compressed[9..])
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read compressed payload: {e}")))?;
    compressed[0] = type_byte;
    compressed[1..5].copy_from_slice(&compressed_size.to_le_bytes());
    compressed[5..9].copy_from_slice(&decompressed_size.to_le_bytes());

    // Validate checksum
    let calc_checksum = cityhash_rs::cityhash_102_128(&compressed);
    if calc_checksum != checksum {
        return Err(Error::Protocol(format!(
            "Checksum mismatch: expected {checksum:032x}, got {calc_checksum:032x}"
        )));
    }

    // Decompress based on compression method
    match compression {
        CompressionMethod::LZ4 => {
            lz4_flex::decompress(&compressed[9..], decompressed_size as usize)
                .map_err(|e| Error::DeserializeError(format!("LZ4 decompress error: {e}")))
        }
        CompressionMethod::ZSTD => {
            zstd::bulk::decompress(&compressed[9..], decompressed_size as usize)
                .map_err(|e| Error::DeserializeError(format!("ZSTD decompress error: {e}")))
        }
        CompressionMethod::None => {
            Err(Error::DeserializeError("Attempted to decompress uncompressed data".into()))
        }
    }
}

type BlockReadingFuture<'a, R> =
    Pin<Box<dyn Future<Output = Result<(Vec<u8>, &'a mut R)>> + Send + Sync + 'a>>;

/// An async reader that decompresses `ClickHouse` data blocks on-the-fly.
///
/// Wraps a `ClickHouseRead` reader to provide decompressed data as an `AsyncRead` stream.
/// Supports ZSTD and LZ4, handling block-by-block decompression.
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::compression::{CompressionMethod, DecompressionReader};
/// use tokio::io::AsyncReadExt;
///
/// let mut decompressor = DecompressionReader::new(CompressionMethod::LZ4, reader);
/// let mut buffer = vec![0u8; 1024];
/// let bytes_read = decompressor.read(&mut buffer).await.unwrap();
/// ```
pub(crate) struct DecompressionReader<'a, R: ClickHouseRead + 'static> {
    mode:                 CompressionMethod,
    inner:                Option<&'a mut R>,
    decompressed:         Vec<u8>,
    position:             usize,
    block_reading_future: Option<BlockReadingFuture<'a, R>>,
}

impl<'a, R: ClickHouseRead> DecompressionReader<'a, R> {
    /// Creates a new streaming decompressor by reading all compression chunks.
    ///
    /// Reads and decompresses all compression chunks from the provided reader,
    /// concatenating them into a single decompressed buffer. Each chunk is validated
    /// with its `CityHash128` checksum before decompression.
    ///
    /// # Arguments
    /// * `mode` - The compression method
    /// * `reader` - The `ClickHouse` reader containing compressed chunk data
    ///
    /// # Returns
    /// A `DecompressionReader` ready to serve decompressed data via `AsyncRead`
    ///
    /// # Errors
    /// - Checksum validation failures
    /// - Decompression errors
    /// - I/O errors reading from the underlying stream
    /// - Memory safety violations (chunk sizes exceeding limits)
    pub(crate) async fn new(mode: CompressionMethod, inner: &'a mut R) -> Result<Self> {
        // Decompress intial block
        let decompressed = decompress_data(inner, mode).await.inspect_err(|error| {
            tracing::error!(?error, "Error decompressing data");
        })?;

        Ok(Self { mode, inner: Some(inner), decompressed, position: 0, block_reading_future: None })
    }
}

impl<R: ClickHouseRead> AsyncRead for DecompressionReader<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // Check if we have a pending operation to complete first
        if let Some(block_reading_future) = self.block_reading_future.as_mut() {
            match block_reading_future.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok((value, inner))) => {
                    drop(self.block_reading_future.take());
                    self.decompressed = value;
                    self.position = 0;
                    self.inner = Some(inner);
                    // Fall through to serve data or potentially read more
                }
                Poll::Ready(Err(e)) => {
                    drop(self.block_reading_future.take());
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )));
                }
            }
        }

        // If we have available data, serve what we can
        let available = self.decompressed.len() - self.position;
        if available > 0 {
            let to_serve = available.min(buf.remaining());
            buf.put_slice(&self.decompressed[self.position..self.position + to_serve]);
            self.position += to_serve;
            return Poll::Ready(Ok(()));
        }

        // We have no data available in our buffer
        // Try to read the next chunk if we still have an inner reader
        if let Some(inner) = self.inner.take() {
            let mode = self.mode;
            self.block_reading_future = Some(Box::pin(async move {
                let value = decompress_data(inner, mode).await?;
                Ok((value, inner))
            }));
            // Immediately try to poll the future we just created
            return self.poll_read(cx, buf);
        }

        // No inner reader left AND no data in buffer - this is true EOF
        // Only return EOF if we've actually exhausted all data sources
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "row_binary")]
pub(crate) mod http {
    // TODO: Remove
    #![cfg_attr(feature = "profile", allow(clippy::cast_precision_loss))]

    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use futures_util::{Stream, ready};
    #[cfg(feature = "profile")]
    use tracy_client::{plot, span as tracy_span};

    use crate::prelude::*;

    #[pin_project::pin_project]
    pub(crate) struct Decompressor<S> {
        #[pin]
        stream:          S,
        buffer:          BytesMut,
        compression:     CompressionMethod,
        header:          Option<CompressHeader>,
        checksum_buffer: Vec<u8>,
        header_template: [u8; 9], // Pre-allocated header space
    }

    #[derive(Debug, Clone, Copy)]
    struct CompressHeader {
        checksum:          u128,
        compressed_size:   u32,
        decompressed_size: u32,
    }

    impl<S> Decompressor<S>
    where
        S: Stream<Item = Result<Bytes>> + Unpin + Send,
    {
        pub(crate) fn new(stream: S, compression: CompressionMethod) -> Self {
            Self {
                stream,
                buffer: BytesMut::with_capacity(8 * 1024 * 1024),
                compression,
                header: None,
                checksum_buffer: Vec::with_capacity(1024 * 1024),
                header_template: [0; 9],
            }
        }

        fn try_parse_header_and_advance(&mut self) -> Result<Option<CompressHeader>> {
            if self.buffer.len() < 25 {
                return Ok(None); // Need 16 checksum + 9 header bytes
            }
            // Parse checksum (16 bytes)
            let (high, low) = (self.buffer.get_u64_le(), self.buffer.get_u64_le());
            let checksum = (u128::from(high) << 64) | u128::from(low);
            // Parse header (9 bytes)
            let type_byte = self.buffer.get_u8();
            let compressed_size = self.buffer.get_u32_le();
            let decompressed_size = self.buffer.get_u32_le();
            // Validate type
            if type_byte != self.compression.byte() {
                return Err(Error::Protocol(format!(
                    "Unexpected compression type: expected {}, got {type_byte:02x}",
                    self.compression.byte()
                )));
            }
            // Sanity checks
            if compressed_size > 100_000_000 || decompressed_size > 1_000_000_000 {
                return Err(Error::Protocol("Block size too large".to_string()));
            }
            Ok(Some(CompressHeader { checksum, compressed_size, decompressed_size }))
        }

        fn try_decompress_current_block(
            &mut self,
            block_info: CompressHeader,
        ) -> Result<Option<Bytes>> {
            let compressed_size = block_info.compressed_size;
            let decompressed_size = block_info.decompressed_size;
            // We need compressed_size - 9 bytes (since header was already consumed)
            let payload_size = (compressed_size as usize).saturating_sub(9);
            if self.buffer.len() < payload_size {
                return Ok(None); // Need more data
            }
            // Get a reference to validate checksum
            let payload = &self.buffer[..payload_size];
            // Build header template once
            self.header_template[0] = self.compression.byte();
            self.header_template[1..5].copy_from_slice(&compressed_size.to_le_bytes());
            self.header_template[5..9].copy_from_slice(&decompressed_size.to_le_bytes());
            // Build complete compressed block for checksum validation
            self.checksum_buffer.clear();
            self.checksum_buffer.reserve(compressed_size as usize); // Only need 9 bytes now
            self.checksum_buffer.extend_from_slice(&self.header_template);
            self.checksum_buffer.extend_from_slice(payload);
            // Validate checksum
            let calc_checksum = cityhash_rs::cityhash_102_128(&self.checksum_buffer);
            if calc_checksum != block_info.checksum {
                return Err(Error::Protocol(format!(
                    "Checksum mismatch: expected {:032x}, got {:032x}",
                    block_info.checksum, calc_checksum
                )));
            }
            // Decompress just the payload
            let decompressed = match self.compression {
                CompressionMethod::LZ4 => {
                    lz4_flex::decompress(payload, block_info.decompressed_size as usize).map_err(
                        |e| Error::DeserializeError(format!("LZ4 decompress error: {e}")),
                    )?
                }
                CompressionMethod::ZSTD => {
                    zstd::bulk::decompress(payload, block_info.decompressed_size as usize).map_err(
                        |e| Error::DeserializeError(format!("ZSTD decompress error: {e}")),
                    )?
                }
                CompressionMethod::None => {
                    return Err(Error::DeserializeError(
                        "Attempted to decompress uncompressed data".into(),
                    ));
                }
            };

            self.buffer.advance(payload_size);
            Ok(Some(Bytes::from(decompressed)))
        }
    }

    impl<S> Stream for Decompressor<S>
    where
        S: Stream<Item = Result<Bytes>> + Unpin + Send,
    {
        type Item = Result<Bytes>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Bytes>>> {
            #[cfg(feature = "profile")]
            let _span = tracy_span!("decompressor_poll");

            loop {
                #[cfg(feature = "profile")]
                let _loop_span = tracy_span!("decompressor_poll_loop");

                // If we don't have block info yet, try to parse header
                if self.header.is_none() {
                    #[cfg(feature = "profile")]
                    let _header_span = tracy_span!("decompressor_parse_header");

                    match self.try_parse_header_and_advance() {
                        Ok(Some(block_info)) => {
                            self.header = Some(block_info);
                        }
                        // Need more data for header, fall through to read from stream
                        Ok(None) => {}
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                // If we have block info, try to decompress
                if let Some(block_info) = self.header {
                    #[cfg(feature = "profile")]
                    let _decompress_span = tracy_span!("decompress_block");

                    match self.try_decompress_current_block(block_info) {
                        Ok(Some(decompressed)) => {
                            // Reset for next block
                            self.header = None;
                            return Poll::Ready(Some(Ok(decompressed)));
                        }
                        // Need more data for payload, fall through to read from stream
                        Ok(None) => {}
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                // Need more data from the underlying stream
                match ready!(self.as_mut().project().stream.as_mut().poll_next(cx)) {
                    Some(Ok(chunk)) => {
                        // TODO: Remove
                        #[cfg(feature = "profile")]
                        plot!("decompress_new_chunk", chunk.len() as f64);

                        self.buffer.put(chunk);
                        // Continue loop to try processing again
                    }
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    None => {
                        // Stream ended
                        if self.buffer.is_empty() && self.header.is_none() {
                            return Poll::Ready(None);
                        }
                        return Poll::Ready(Some(Err(Error::Protocol(
                            "Stream ended with incomplete block".to_string(),
                        ))));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn test_write_compressed_data_lz4() {
        let data = b"test data for compression".to_vec();
        let mut buffer = Vec::new();

        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();
        assert!(!buffer.is_empty());
        assert!(buffer.len() >= 25); // 16 checksum + 9 header + payload

        // Verify we can decompress it back
        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data(&mut reader, CompressionMethod::LZ4).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_write_compressed_data_zstd() {
        let data = b"test data for ZSTD compression".to_vec();
        let mut buffer = Vec::new();

        compress_data(&mut buffer, data.clone(), CompressionMethod::ZSTD).await.unwrap();
        assert!(!buffer.is_empty());
        assert!(buffer.len() >= 25); // 16 checksum + 9 header + payload

        // Verify we can decompress it back
        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data(&mut reader, CompressionMethod::ZSTD).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_write_compressed_data_none() {
        let data = b"test data no compression".to_vec();
        let mut buffer = Vec::new();

        compress_data(&mut buffer, data.clone(), CompressionMethod::None).await.unwrap();
        assert!(buffer.is_empty());

        // For None compression, the data should be in the same chunk format
        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data(&mut reader, CompressionMethod::None).await;
        assert!(decompressed.is_err());
    }

    #[tokio::test]
    async fn test_decompress_data_lz4() {
        let data = b"test data for LZ4 decompression".to_vec();

        // First compress the data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();

        // Then decompress it
        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data(&mut reader, CompressionMethod::LZ4).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompress_data_zstd() {
        let data = b"test data for ZSTD decompression".to_vec();

        // First compress the data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::ZSTD).await.unwrap();

        // Then decompress it
        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data(&mut reader, CompressionMethod::ZSTD).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompression_reader_single_chunk() {
        let data = b"test data for single chunk reading".to_vec();
        let expected_len = data.len();

        // Prepare compressed data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();

        // Create decompression reader
        let mut reader = Cursor::new(buffer);
        let mut decompression_reader =
            DecompressionReader::new(CompressionMethod::LZ4, &mut reader).await.unwrap();

        // Read exactly the amount of data we expect (like real ClickHouse usage)
        let mut result = vec![0u8; expected_len];
        let _ = decompression_reader.read_exact(&mut result).await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_round_trip_compression() {
        let original_data = b"This is a longer piece of test data that should compress well with both LZ4 and ZSTD algorithms".to_vec();

        for compression in [CompressionMethod::LZ4, CompressionMethod::ZSTD] {
            // Compress
            let mut compressed_buffer = Vec::new();
            compress_data(&mut compressed_buffer, original_data.clone(), compression)
                .await
                .unwrap();

            // Decompress
            let mut reader = Cursor::new(compressed_buffer);
            let decompressed = decompress_data(&mut reader, compression).await.unwrap();

            assert_eq!(decompressed, original_data, "Round trip failed for {compression:?}");
        }
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let data = b"test data for checksum validation".to_vec();

        // Create properly compressed data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();

        // Corrupt the checksum (first 8 bytes)
        buffer[0] ^= 0xFF;

        // Decompression should fail due to checksum mismatch
        let mut reader = Cursor::new(buffer);
        let result = decompress_data(&mut reader, CompressionMethod::LZ4).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Checksum mismatch"));
    }
}
