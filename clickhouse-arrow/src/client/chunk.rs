use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::ready;
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tracing::{debug, trace};

use crate::io::{ClickHouseRead, ClickHouseWrite};

#[pin_project]
#[derive(Clone, Debug)]
pub(super) struct ChunkWriter<W: ClickHouseWrite> {
    buffer: Vec<u8>,
    #[pin]
    inner:  W,
}

impl<W: ClickHouseWrite> ChunkWriter<W> {
    pub(super) fn new(inner: W) -> Self { Self { inner, buffer: Vec::new() } }

    pub(super) async fn finish_chunk(&mut self) -> io::Result<()> {
        let len = self.buffer.len();
        debug!(len, "Sending chunk header, data, and terminator");

        if self.buffer.is_empty() {
            return Ok(());
        }

        // Write complete chunk atomically
        #[expect(clippy::cast_possible_truncation)]
        self.inner.write_u32_le(self.buffer.len() as u32).await?;
        self.inner.write_all(&self.buffer).await?;
        self.inner.write_u32_le(0u32).await?;

        // Clear buffer and flush
        self.buffer.clear();
        self.inner.flush().await?;
        Ok(())
    }
}

impl<W: ClickHouseWrite> AsyncWrite for ChunkWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Buffer all writes until finish_chunk()
        let this = self.project();
        this.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Prevent flushing until finish_chunk() is called
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.project();
        this.inner.poll_shutdown(cx)
    }
}

#[pin_project]
pub(crate) struct ChunkReader<R> {
    #[pin]
    inner:       R,
    state:       ReaderState,
    buffer:      Vec<u8>, // Internal buffer for excess chunk data
    read_buffer: Vec<u8>, // Internal buffer for reading chunk data
    buffer_pos:  usize,   // Current position in the buffer
    chunk_size:  u32,     // Remaining bytes in the current chunk
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum ReaderState {
    Header, // Expecting 4-byte chunk size or terminating header
    Data,   // Reading chunk data
}

impl<R: ClickHouseRead> ChunkReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            state: ReaderState::Header,
            buffer: Vec::with_capacity(64 * 1024),
            // In practice, 1MB chunks is seen often
            read_buffer: Vec::with_capacity(1024 * 1024),
            buffer_pos: 0,
            chunk_size: 0,
        }
    }
}

impl<R: ClickHouseRead> AsyncRead for ChunkReader<R> {
    #[allow(clippy::too_many_lines)]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.project();

        // If we have buffered data, serve it to the caller first
        if *this.buffer_pos < this.buffer.len() {
            let available = &this.buffer[*this.buffer_pos..];
            let to_copy = available.len().min(buf.remaining());
            buf.put_slice(&available[..to_copy]);
            *this.buffer_pos += to_copy;
            // Clear buffer if fully consumed to free memory
            if *this.buffer_pos >= this.buffer.len() {
                this.buffer.clear();
                *this.buffer_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        loop {
            #[expect(clippy::cast_possible_truncation)]
            match this.state {
                ReaderState::Header => {
                    // Read the 4-byte chunk size (little-endian u32)
                    let mut header = [0u8; 4];
                    let mut header_buf = ReadBuf::new(&mut header);
                    ready!(this.inner.as_mut().poll_read(cx, &mut header_buf))?;

                    if header.len() < 4 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Incomplete chunk header",
                        )));
                    }

                    *this.chunk_size = u32::from_le_bytes(header);

                    // Terminating sequence, stay in Header and continue
                    if *this.chunk_size == 0 {
                        trace!("Chunk finished, restarting chunk read");
                        continue;
                    }

                    if *this.chunk_size > 100_000_000 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Chunk size too large",
                        )));
                    }

                    trace!("New chunk started: size = {}", this.chunk_size);
                    *this.state = ReaderState::Data;
                }
                ReaderState::Data => {
                    // Read up to the remaining chunk size or the caller's buffer size
                    let to_read = buf.remaining().min(*this.chunk_size as usize);

                    // This conditional means that this.chunk_size is 0 buf needs more
                    if to_read == 0 && buf.remaining() > 0 {
                        *this.state = ReaderState::Header;
                        continue;
                    }

                    if this.read_buffer.len() < to_read {
                        this.read_buffer.resize(to_read, 0);
                    }

                    let mut read_buf = ReadBuf::new(&mut this.read_buffer[..to_read]);
                    ready!(this.inner.as_mut().poll_read(cx, &mut read_buf))?;

                    let filled = read_buf.filled();
                    if filled.is_empty() && *this.chunk_size > 0 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Unexpected EOF in chunk data",
                        )));
                    }

                    let filled_len = filled.len();
                    let remaining = buf.remaining();
                    let to_copy = filled_len.min(remaining);

                    buf.put_slice(&filled[..to_copy]);

                    // Store excess
                    if to_copy < filled_len {
                        this.buffer.clear();
                        this.buffer.extend_from_slice(&filled[to_copy..]);
                    }

                    *this.chunk_size -= filled_len as u32;

                    if *this.chunk_size == 0 {
                        *this.state = ReaderState::Header;
                    }

                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}
