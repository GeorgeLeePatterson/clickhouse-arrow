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
use std::io::ErrorKind;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

use crate::io::ClickhouseRead;
use crate::native::protocol::CompressionMethod;
use crate::{ClickhouseNativeError, ClickhouseWrite, Result};

/// Compresses data and writes it to a writer in `ClickHouse`'s native protocol format.
///
/// Writes a complete LZ4 block, including a 16-byte `CityHash128` checksum, a 9-byte header
/// (type byte, compressed size, decompressed size), and the compressed payload.
///
/// # Arguments
/// - `writer`: The writer to serialize the compressed data to.
/// - `raw`: The input data to compress.
/// - `compression`: The compression method (LZ4 or None).
///
/// # Errors
/// Returns an error if writing to the writer fails or if compression is invalid.
#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn write_compressed_data<W: ClickhouseWrite>(
    writer: &mut W,
    raw: Vec<u8>,
    compression: CompressionMethod,
) -> Result<()> {
    let (mut out, decompressed_size) = compress_data(raw, compression);

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

/// Compresses raw data using the specified compression method.
///
/// Returns a tuple of the compressed data and the original raw data length.
///
/// # Arguments
/// - `raw`: The input data to compress.
/// - `compression`: The compression method (LZ4 or None).
fn compress_data(raw: Vec<u8>, compression: CompressionMethod) -> (Vec<u8>, usize) {
    let raw_len = raw.len();
    let compressed = match compression {
        // LZ4
        CompressionMethod::LZ4 => lz4_flex::compress(&raw),
        // None
        CompressionMethod::None => raw,
    };

    (compressed, raw_len)
}

/// Decompresses LZ4-compressed data from a reader.
///
/// Reads a compressed block, validates its checksum, and decompresses it. Returns the
/// decompressed data as a `Vec<u8>`.
///
/// # Arguments
/// - `reader`: The reader providing compressed data.
/// - `compression`: The expected compression method.
/// - `max_capacity`: Optional maximum capacity for decompression (defaults to decompressed size).
///
/// # Errors
/// Returns an error if the compression method is unexpected, checksum mismatches, or
/// decompression fails.
pub(crate) async fn decompress_data(
    reader: &mut impl ClickhouseRead,
    compression: CompressionMethod,
    max_capacity: Option<usize>,
) -> Result<Vec<u8>> {
    // Read LZ4-compressed blob (checksum + payload)
    let checksum =
        (u128::from(reader.read_u64_le().await?) << 64) | u128::from(reader.read_u64_le().await?);
    let type_byte = reader.read_u8().await?;
    if type_byte != compression.byte() {
        return Err(ClickhouseNativeError::ArrowDeserialize(format!(
            "Unexpected compression algorithm: {type_byte:02x}"
        )));
    }
    let compressed_size = reader.read_u32_le().await?;
    let decompressed_size = reader.read_u32_le().await?;
    let mut compressed = vec![0u8; compressed_size as usize];
    let _ = reader.read_exact(&mut compressed[9..]).await?;
    compressed[0] = type_byte;
    compressed[1..5].copy_from_slice(&compressed_size.to_le_bytes()[..]);
    compressed[5..9].copy_from_slice(&decompressed_size.to_le_bytes()[..]);
    let calc_checksum = cityhash_rs::cityhash_102_128(&compressed[..]);
    if calc_checksum != checksum {
        return Err(ClickhouseNativeError::ArrowDeserialize(format!(
            "Checksum mismatch: {calc_checksum:032x} vs {checksum:032x}"
        )));
    }

    lz4_flex::decompress(&compressed[9..], max_capacity.unwrap_or(decompressed_size as usize))
        .map_err(|e| ClickhouseNativeError::DeserializeError(format!("LZ4 decompress error: {e}")))
}

type BlockReadingFuture<R> =
    Pin<Box<dyn Future<Output = Result<(Vec<u8>, &'static mut R)>> + Send + Sync>>;

/// An async reader that decompresses `ClickHouse` data blocks on-the-fly.
///
/// Wraps a `ClickhouseRead` reader to provide decompressed data as an `AsyncRead` stream.
/// Supports LZ4 and no compression, handling block-by-block decompression.
///
/// # Example
/// ```rust,ignore
/// use clickhouse_native::compression::{CompressionMethod, DecompressionReader};
/// use tokio::io::AsyncReadExt;
///
/// async fn example(reader: &mut impl ClickhouseRead) {
///     let mut decompressor = DecompressionReader::new(CompressionMethod::LZ4, reader);
///     let mut buffer = vec![0u8; 1024];
///     let bytes_read = decompressor.read(&mut buffer).await.unwrap();
/// }
/// ```
pub(crate) struct DecompressionReader<'a, R: ClickhouseRead + 'static> {
    mode:                 CompressionMethod,
    inner:                Option<&'a mut R>,
    decompressed:         Vec<u8>,
    position:             usize,
    block_reading_future: Option<BlockReadingFuture<R>>,
}

impl<'a, R: ClickhouseRead + 'static> DecompressionReader<'a, R> {
    pub(crate) fn new(mode: CompressionMethod, inner: &'a mut R) -> Self {
        Self {
            mode,
            inner: Some(inner),
            decompressed: vec![],
            position: 0,
            block_reading_future: None,
        }
    }

    fn run_decompression(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(block_reading_future) = self.block_reading_future.as_mut() {
            match block_reading_future.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok((value, inner))) => {
                    drop(self.block_reading_future.take());
                    self.decompressed = value;
                    assert!(self.inner.is_none());
                    self.inner = Some(inner);
                    self.position = 0;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    drop(self.block_reading_future.take());
                    Poll::Ready(Err(std::io::Error::new(ErrorKind::UnexpectedEof, e)))
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<R: ClickhouseRead + 'static> AsyncRead for DecompressionReader<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.capacity() == 0 {
            return Poll::Ready(Ok(()));
        }
        match self.run_decompression(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(_) => (),
        }
        if self.inner.is_none() {
            return Poll::Ready(Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                "read after EOF",
            )));
        }

        while self.position >= self.decompressed.len() {
            let static_inner: &'static mut R =
                unsafe { std::mem::transmute(self.inner.take().unwrap()) };
            let mode = self.mode;
            self.block_reading_future = Some(Box::pin(async move {
                let value = decompress_data(static_inner, mode, None).await?;
                Ok((value, static_inner))
            }));
            match self.run_decompression(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(_) => (),
            }
        }
        let length = (self.decompressed.len() - self.position).min(buf.remaining());
        buf.put_slice(&self.decompressed[self.position..self.position + length]);
        self.position += length;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_compress_data() {
        let data = b"test data".to_vec();

        // Test LZ4 compression
        let (compressed, raw_len) = compress_data(data.clone(), CompressionMethod::LZ4);
        assert_eq!(raw_len, data.len());
        assert!(!compressed.is_empty());

        // Test no compression
        let (uncompressed, raw_len) = compress_data(data.clone(), CompressionMethod::None);
        assert_eq!(raw_len, data.len());
        assert_eq!(uncompressed, data);
    }

    #[tokio::test]
    async fn test_write_compressed_data() {
        let data = b"test data".to_vec();

        // Test LZ4 compression to writer
        let mut buffer = Vec::new();
        write_compressed_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();
        assert!(!buffer.is_empty());

        // Verify decompression
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data(&mut reader, CompressionMethod::LZ4, None).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompress_data() {
        let data = b"test data".to_vec();

        // Prepare compressed data
        let mut buffer = Vec::new();
        write_compressed_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();
        assert!(!buffer.is_empty(), "Buffer is empty after compression");
        assert!(buffer.len() >= 25, "Buffer too short: {buffer:?}"); // 16 checksum + 9 header + payload

        // Test decompression
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data(&mut reader, CompressionMethod::LZ4, None).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompression_reader() {
        let data = b"test data".to_vec();

        // Prepare compressed data
        let mut buffer = Vec::new();
        write_compressed_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();
        assert!(!buffer.is_empty());

        // Verify decompression
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data(&mut reader, CompressionMethod::LZ4, None).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_write_and_decompress() {
        let data = b"test data".to_vec();

        // Write compressed data to buffer
        let mut buffer = Vec::new();
        write_compressed_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();
        assert!(!buffer.is_empty(), "Buffer is empty after compression");
        assert!(buffer.len() >= 25, "Buffer too short: {buffer:?}"); // 16 checksum + 9 header + payload

        // Verify decompression
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data(&mut reader, CompressionMethod::LZ4, None).await.unwrap();
        assert_eq!(decompressed, data, "Decompressed data does not match original");
    }
}
