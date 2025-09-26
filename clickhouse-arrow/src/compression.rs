/// Compression and decompression utilities for `ClickHouse`'s native protocol.
///
/// This module provides streaming compression and decompression utilities for `ClickHouse`
/// data blocks. It supports the `CompressionMethod` enum from the protocol module, handling
/// LZ4, ZSTD, and uncompressed payloads.
///
/// # Features
/// - Streaming compression via [`StreamingCompressor`]
/// - Streaming decompression via [`StreamingDecompressor`]
///
/// # `ClickHouse` Reference
/// See the [ClickHouse Native Protocol Documentation](https://clickhouse.com/docs/en/interfaces/tcp)
/// for details on compression in the native protocol.
use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::io::ErrorKind;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_util::FutureExt;
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use crate::io::ClickHouseRead;
use crate::native::protocol::CompressionMethod;
use crate::{Error, Result};

/// Number of bytes in the per-frame header: algorithm tag + compressed and decompressed sizes.
const COMPRESSED_FRAME_HEADER_BYTES: usize = 1 + 4 + 4;

/// Reads and decompresses a single compression chunk.
///
/// Reads the chunk header (checksum + compression metadata), validates the checksum,
/// and decompresses the payload using the specified compression method. Each chunk follows the
/// format:
/// - 16 bytes: `CityHash128` checksum
/// - 1 byte: compression type
/// - 4 bytes: compressed size (including the frame header)
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
type BlockReadingFuture<'a, R> =
    Pin<Box<dyn Future<Output = Result<(Option<Vec<u8>>, &'a mut R)>> + Send + Sync + 'a>>;

/// An async reader that decompresses `ClickHouse` data blocks on-the-fly.
///
/// Wraps a `ClickHouseRead` reader to provide decompressed data as an `AsyncRead` stream.
/// Supports ZSTD and LZ4, handling block-by-block decompression.
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::compression::{CompressionMethod, StreamingDecompressor};
/// use tokio::io::AsyncReadExt;
///
/// let mut decompressor = StreamingDecompressor::new(CompressionMethod::LZ4, reader);
/// let mut buffer = vec![0u8; 1024];
/// let bytes_read = decompressor.read(&mut buffer).await.unwrap();
/// ```
pub(crate) struct StreamingDecompressor<'a, R: ClickHouseRead + 'static> {
    mode:                 CompressionMethod,
    inner:                Option<&'a mut R>,
    decompressed:         Vec<u8>,
    position:             usize,
    block_reading_future: Option<BlockReadingFuture<'a, R>>,
}

impl<'a, R: ClickHouseRead> StreamingDecompressor<'a, R> {
    /// Creates a new streaming decompressor and primes it with the first chunk.
    ///
    /// Reads and decompresses the first available compression chunk from the provided
    /// reader, validating the frame checksum before yielding data. Subsequent chunks are
    /// fetched lazily as the [`AsyncRead`] implementation consumes the stream.
    ///
    /// # Arguments
    /// * `mode` - The compression method
    /// * `reader` - The `ClickHouse` reader containing compressed chunk data
    ///
    /// # Returns
    /// A `StreamingDecompressor` ready to serve decompressed data via `AsyncRead`
    ///
    /// # Errors
    /// - Checksum validation failures
    /// - Decompression errors
    /// - I/O errors reading from the underlying stream
    /// - Memory safety violations (chunk sizes exceeding limits)
    async fn read_chunk(inner: &mut R, mode: CompressionMethod) -> Result<Option<Vec<u8>>> {
        let mut checksum_bytes = [0u8; 16];
        checksum_bytes[0] = match inner.read_u8().await {
            Ok(byte) => byte,
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(Error::Protocol(format!("Failed to read checksum high: {e}")));
            }
        };

        let _ = inner
            .read_exact(&mut checksum_bytes[1..])
            .await
            .map_err(|e| Error::Protocol(format!("Failed to read checksum high: {e}")))?;

        let checksum_high = u64::from_le_bytes(checksum_bytes[..8].try_into().unwrap());
        let checksum_low = u64::from_le_bytes(checksum_bytes[8..].try_into().unwrap());
        let checksum = (u128::from(checksum_high) << 64) | u128::from(checksum_low);

        let mut header = [0u8; COMPRESSED_FRAME_HEADER_BYTES];
        let _ = inner
            .read_exact(&mut header)
            .await
            .map_err(|e| Error::Protocol(format!("Failed to read compression header: {e}")))?;

        let type_byte = header[0];
        if type_byte != mode.byte() {
            return Err(Error::Protocol(format!(
                "Unexpected compression algorithm for {mode}: {type_byte:02x}"
            )));
        }

        let compressed_size = u32::from_le_bytes(header[1..5].try_into().unwrap());
        let decompressed_size = u32::from_le_bytes(header[5..9].try_into().unwrap());

        let header_len_u32 =
            u32::try_from(COMPRESSED_FRAME_HEADER_BYTES).expect("frame header fits in u32");

        if compressed_size < header_len_u32 {
            return Err(Error::Protocol(format!("Compressed block too small: {compressed_size}")));
        }

        if compressed_size > 100_000_000 || decompressed_size > 1_000_000_000 {
            return Err(Error::Protocol("Chunk size too large".to_string()));
        }

        let payload_len = usize::try_from(compressed_size).expect("compressed size exceeds usize")
            - COMPRESSED_FRAME_HEADER_BYTES;
        let mut payload = vec![0u8; payload_len];
        let _ = inner
            .read_exact(&mut payload)
            .await
            .map_err(|e| Error::Protocol(format!("Failed to read compressed payload: {e}")))?;

        let mut header_plus_payload =
            Vec::with_capacity(COMPRESSED_FRAME_HEADER_BYTES + payload_len);
        header_plus_payload.extend_from_slice(&header);
        header_plus_payload.extend_from_slice(&payload);

        let calc_checksum = cityhash_rs::cityhash_102_128(&header_plus_payload);
        if calc_checksum != checksum {
            return Err(Error::Protocol(format!(
                "Checksum mismatch: expected {checksum:032x}, got {calc_checksum:032x}"
            )));
        }

        let decompressed = match mode {
            CompressionMethod::LZ4 => lz4_flex::decompress(&payload, decompressed_size as usize)
                .map_err(|e| Error::DeserializeError(format!("LZ4 decompress error: {e}")))?,
            CompressionMethod::ZSTD => zstd::bulk::decompress(&payload, decompressed_size as usize)
                .map_err(|e| Error::DeserializeError(format!("ZSTD decompress error: {e}")))?,
            CompressionMethod::None => {
                return Err(Error::DeserializeError(
                    "Attempted to decompress uncompressed data".into(),
                ));
            }
        };

        Ok(Some(decompressed))
    }

    #[cfg_attr(not(test), allow(unused))]
    pub(crate) async fn new(mode: CompressionMethod, inner: &'a mut R) -> Result<Self> {
        if matches!(mode, CompressionMethod::None) {
            return Err(Error::DeserializeError(
                "Attempted to decompress uncompressed data".into(),
            ));
        }

        let chunk = Self::read_chunk(inner, mode).await.inspect_err(|error| {
            tracing::error!(?error, "Error decompressing data");
        })?;

        let mut inner_opt = Some(inner);
        let decompressed = if let Some(data) = chunk {
            data
        } else {
            inner_opt = None;
            Vec::new()
        };

        Ok(Self { mode, inner: inner_opt, decompressed, position: 0, block_reading_future: None })
    }
}

impl<R: ClickHouseRead> AsyncRead for StreamingDecompressor<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            if let Some(block_reading_future) = self.block_reading_future.as_mut() {
                match block_reading_future.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok((Some(value), inner))) => {
                        drop(self.block_reading_future.take());
                        self.decompressed = value;
                        self.position = 0;
                        self.inner = Some(inner);
                        continue;
                    }
                    Poll::Ready(Ok((None, _inner))) => {
                        drop(self.block_reading_future.take());
                        self.decompressed.clear();
                        self.position = 0;
                        self.inner = None;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        drop(self.block_reading_future.take());
                        return Poll::Ready(Err(std::io::Error::new(ErrorKind::InvalidData, e)));
                    }
                }
            }

            let available = self.decompressed.len() - self.position;
            if available > 0 {
                let to_serve = available.min(buf.remaining());
                buf.put_slice(&self.decompressed[self.position..self.position + to_serve]);
                self.position += to_serve;
                return Poll::Ready(Ok(()));
            }

            if let Some(inner) = self.inner.take() {
                let mode = self.mode;
                self.block_reading_future = Some(Box::pin(async move {
                    let chunk = Self::read_chunk(inner, mode).await?;
                    Ok((chunk, inner))
                }));
                continue;
            }

            return Poll::Ready(Ok(()));
        }
    }
}

/// Async writer that frames and compresses data into `ClickHouse` compression chunks.
/// Each chunk is written as:
/// [16 bytes checksum][1 byte type][4 bytes `compressed_size_with_header`][4 bytes
/// decompressed_size][payload]
#[pin_project]
pub(crate) struct StreamingCompressor<W: AsyncWrite + Unpin> {
    #[pin]
    inner:                  W,
    method:                 CompressionMethod,
    max_uncompressed_chunk: usize,
    in_buf:                 Vec<u8>,
    out_buf:                Vec<u8>,
    out_pos:                usize,
}

impl<W: AsyncWrite + Unpin> StreamingCompressor<W> {
    pub(crate) fn new(inner: W, method: CompressionMethod, max_uncompressed_chunk: usize) -> Self {
        Self {
            inner,
            method,
            max_uncompressed_chunk: max_uncompressed_chunk.max(64 * 1024),
            in_buf: Vec::with_capacity(max_uncompressed_chunk),
            out_buf: Vec::new(),
            out_pos: 0,
        }
    }

    /// Consume the compressor and return the wrapped writer.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn into_inner(self) -> W { self.inner }

    fn build_frame(
        method: CompressionMethod,
        in_buf: &mut Vec<u8>,
        out_buf: &mut Vec<u8>,
        out_pos: &mut usize,
    ) -> Result<()> {
        if in_buf.is_empty() {
            return Ok(());
        }
        let decompressed_size = in_buf.len();
        // Compress
        let mut compressed = match method {
            CompressionMethod::LZ4 => lz4_flex::compress(in_buf),
            CompressionMethod::ZSTD => zstd::bulk::compress(in_buf, 1)
                .map_err(|e| Error::SerializeError(format!("ZSTD compress error: {e}")))?,
            CompressionMethod::None => {
                // Should not be used with None (caller decides), but handle gracefully
                out_buf.clear();
                *out_pos = 0;
                in_buf.clear();
                return Ok(());
            }
        };

        let type_byte = method.byte();
        let header_len_u32 =
            u32::try_from(COMPRESSED_FRAME_HEADER_BYTES).expect("frame header fits in u32");
        let compressed_len = u32::try_from(compressed.len()).map_err(|_| {
            Error::SerializeError("Compressed block larger than u32::MAX".to_string())
        })?;
        let compressed_size_with_header = compressed_len + header_len_u32;
        let decompressed_u32 = u32::try_from(decompressed_size).map_err(|_| {
            Error::SerializeError("Decompressed block larger than u32::MAX".to_string())
        })?;

        // Compose header+payload for checksum
        out_buf.clear();
        *out_pos = 0;
        out_buf.reserve(16 + COMPRESSED_FRAME_HEADER_BYTES + compressed.len());
        out_buf.resize(16, 0); // checksum placeholder
        out_buf.push(type_byte);
        out_buf.extend_from_slice(&compressed_size_with_header.to_le_bytes());
        out_buf.extend_from_slice(&decompressed_u32.to_le_bytes());
        out_buf.append(&mut compressed);

        let checksum = cityhash_rs::cityhash_102_128(&out_buf[16..]);
        let hi = u64::try_from(checksum >> 64).expect("shifted checksum fits in u64");
        let lo = u64::try_from(checksum & u128::from(u64::MAX)).expect("checksum fits in u64");

        out_buf[..8].copy_from_slice(&hi.to_le_bytes());
        out_buf[8..16].copy_from_slice(&lo.to_le_bytes());

        in_buf.clear();
        Ok(())
    }

    fn poll_drain_out_buf(
        mut inner: Pin<&mut W>,
        out_buf: &[u8],
        out_pos: &mut usize,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        while *out_pos < out_buf.len() {
            let start = *out_pos;
            let remaining = &out_buf[start..];
            match inner.as_mut().poll_write(cx, remaining) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        ErrorKind::WriteZero,
                        "write zero",
                    )));
                }
                Poll::Ready(Ok(nw)) => {
                    *out_pos += nw;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for StreamingCompressor<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut this = self.project();
        match Self::poll_drain_out_buf(this.inner.as_mut(), &this.out_buf[..], this.out_pos, cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
        let max_uncompressed_chunk = *this.max_uncompressed_chunk;
        let remaining = max_uncompressed_chunk - this.in_buf.len();
        let take = remaining.min(buf.len());
        if take > 0 {
            this.in_buf.extend_from_slice(&buf[..take]);
        }
        if this.in_buf.len() >= max_uncompressed_chunk {
            let method = *this.method;
            if let Err(e) = Self::build_frame(method, this.in_buf, this.out_buf, this.out_pos) {
                return Poll::Ready(Err(std::io::Error::other(e.to_string())));
            }
            match Self::poll_drain_out_buf(this.inner.as_mut(), &this.out_buf[..], this.out_pos, cx)
            {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
            this.out_buf.clear();
            *this.out_pos = 0;
        }
        Poll::Ready(Ok(take))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        match Self::poll_drain_out_buf(this.inner.as_mut(), &this.out_buf[..], this.out_pos, cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
        if !this.in_buf.is_empty() {
            let method = *this.method;
            if let Err(e) = Self::build_frame(method, this.in_buf, this.out_buf, this.out_pos) {
                return Poll::Ready(Err(std::io::Error::other(e.to_string())));
            }
            match Self::poll_drain_out_buf(this.inner.as_mut(), &this.out_buf[..], this.out_pos, cx)
            {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
            this.out_buf.clear();
            *this.out_pos = 0;
        }
        this.inner.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        ready!(self.as_mut().poll_flush(cx))?;
        let this = self.project();
        this.inner.poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use super::*;

    #[derive(Default)]
    struct CollectingWriter {
        data: Vec<u8>,
    }

    impl CollectingWriter {
        fn into_inner(self) -> Vec<u8> { self.data }
    }

    impl AsyncWrite for CollectingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.data.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn compress_with_streaming(
        data: &[u8],
        method: CompressionMethod,
        chunk_size: usize,
    ) -> Vec<u8> {
        let writer = CollectingWriter::default();
        let mut compressor = StreamingCompressor::new(writer, method, chunk_size);
        compressor.write_all(data).await.unwrap();
        compressor.shutdown().await.unwrap();
        compressor.into_inner().into_inner()
    }

    #[tokio::test]
    async fn test_decompress_data_lz4() {
        let data = b"test data for LZ4 decompression".to_vec();
        let compressed =
            compress_with_streaming(&data, CompressionMethod::LZ4, data.len() + 16).await;

        let mut reader = Cursor::new(compressed);
        let mut decompressor =
            StreamingDecompressor::new(CompressionMethod::LZ4, &mut reader).await.unwrap();
        let mut decompressed = Vec::new();
        let _ = decompressor.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompress_data_zstd() {
        let data = b"test data for ZSTD decompression".to_vec();
        let compressed =
            compress_with_streaming(&data, CompressionMethod::ZSTD, data.len() + 16).await;

        let mut reader = Cursor::new(compressed);
        let mut decompressor =
            StreamingDecompressor::new(CompressionMethod::ZSTD, &mut reader).await.unwrap();
        let mut decompressed = Vec::new();
        let _ = decompressor.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompress_data_none_errors() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let result = StreamingDecompressor::new(CompressionMethod::None, &mut reader).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_streaming_decompressor_single_chunk() {
        let data = b"test data for single chunk reading".to_vec();
        let compressed =
            compress_with_streaming(&data, CompressionMethod::LZ4, data.len() + 16).await;

        let mut reader = Cursor::new(compressed);
        let mut decompressor =
            StreamingDecompressor::new(CompressionMethod::LZ4, &mut reader).await.unwrap();

        let mut result = vec![0u8; data.len()];
        let _ = decompressor.read_exact(&mut result).await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_streaming_decompressor_multiple_chunks() {
        let data = (0..200_000).map(|idx| (idx % 251) as u8).collect::<Vec<_>>();
        let compressed = compress_with_streaming(&data, CompressionMethod::LZ4, 8 * 1024).await;

        let mut reader = Cursor::new(compressed);
        let mut decompressor =
            StreamingDecompressor::new(CompressionMethod::LZ4, &mut reader).await.unwrap();

        let mut output = Vec::new();
        let _ = decompressor.read_to_end(&mut output).await.unwrap();

        assert_eq!(output, data);
    }

    #[tokio::test]
    async fn test_streaming_compressor_emits_data_on_flush() {
        let data = b"flush me".repeat(10);
        let writer = CollectingWriter::default();
        let mut compressor = StreamingCompressor::new(writer, CompressionMethod::LZ4, 64 * 1024);

        compressor.write_all(&data).await.unwrap();
        compressor.flush().await.unwrap();
        let compressed = compressor.into_inner().into_inner();

        assert!(!compressed.is_empty());

        let mut reader = Cursor::new(compressed);
        let mut decompressor =
            StreamingDecompressor::new(CompressionMethod::LZ4, &mut reader).await.unwrap();
        let mut decompressed = Vec::new();
        let _ = decompressor.read_to_end(&mut decompressed).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_streaming_compressor_round_trip() {
        let payload = b"This is a longer piece of test data that should compress well with both LZ4 and ZSTD algorithms"
            .repeat(4);

        for method in [CompressionMethod::LZ4, CompressionMethod::ZSTD] {
            let compressed = compress_with_streaming(&payload, method, 16 * 1024).await;

            let mut reader = Cursor::new(compressed);
            let mut decompressor = StreamingDecompressor::new(method, &mut reader).await.unwrap();
            let mut round_trip = Vec::new();
            let _ = decompressor.read_to_end(&mut round_trip).await.unwrap();

            assert_eq!(round_trip, payload, "round trip failed for {method:?}");
        }
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let data = b"test data for checksum validation".to_vec();
        let mut compressed =
            compress_with_streaming(&data, CompressionMethod::LZ4, data.len() + 16).await;

        // Corrupt the checksum (first byte of the high u64)
        compressed[0] ^= 0xFF;

        let mut reader = Cursor::new(compressed);
        let result = StreamingDecompressor::new(CompressionMethod::LZ4, &mut reader).await;

        let err = result.err().expect("expected checksum mismatch error");
        assert!(err.to_string().contains("Checksum mismatch"));
    }
}
