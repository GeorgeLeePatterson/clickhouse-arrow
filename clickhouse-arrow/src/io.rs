use std::io::IoSlice;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::native::protocol::MAX_STRING_SIZE;
use crate::{Error, Result};

/// An extension trait on [`AsyncRead`] providing `ClickHouse` specific functionality.
pub(crate) trait ClickHouseRead: AsyncRead + Unpin + Send + Sync {
    fn read_var_uint(&mut self) -> impl Future<Output = Result<u64>> + Send + '_;

    fn read_string(&mut self) -> impl Future<Output = Result<Vec<u8>>> + Send + '_;

    fn read_utf8_string(&mut self) -> impl Future<Output = Result<String>> + Send + '_ {
        async { Ok(String::from_utf8(self.read_string().await?)?) }
    }
}

impl<T: AsyncRead + Unpin + Send + Sync> ClickHouseRead for T {
    async fn read_var_uint(&mut self) -> Result<u64> {
        let mut out = 0u64;
        for i in 0..9u64 {
            let mut octet = [0u8];
            let _ = self.read_exact(&mut octet[..]).await?;
            out |= u64::from(octet[0] & 0x7F) << (7 * i);
            if (octet[0] & 0x80) == 0 {
                break;
            }
        }
        Ok(out)
    }

    async fn read_string(&mut self) -> Result<Vec<u8>> {
        #[expect(clippy::cast_possible_truncation)]
        let len = self.read_var_uint().await? as usize;
        if len > MAX_STRING_SIZE {
            return Err(Error::Protocol(format!("string too large: {len} > {MAX_STRING_SIZE}")));
        }
        if len == 0 {
            return Ok(vec![]);
        }
        let mut buf = Vec::with_capacity(len);

        let buf_mut = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), len) };
        let _ = self.read_exact(buf_mut).await?;
        unsafe { buf.set_len(len) };

        Ok(buf)
    }
}

/// An extension trait on [`AsyncWrite`] providing `ClickHouse` specific functionality.
pub(crate) trait ClickHouseWrite: AsyncWrite + Unpin + Send + Sync {
    fn write_var_uint(&mut self, value: u64) -> impl Future<Output = Result<()>> + Send + '_;

    fn write_string<V: AsRef<[u8]> + Send>(
        &mut self,
        value: V,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, Self, V>;

    fn write_vectored_all<'a>(
        &'a mut self,
        bufs: &'a [IoSlice<'a>],
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

impl<T: AsyncWrite + Unpin + Send + Sync> ClickHouseWrite for T {
    async fn write_var_uint(&mut self, mut value: u64) -> Result<()> {
        let mut buf = [0u8; 9]; // Max 9 bytes for u64
        let mut pos = 0;

        #[expect(clippy::cast_possible_truncation)]
        while pos < 9 {
            let mut byte = value & 0x7F;
            value >>= 7;
            if value > 0 {
                byte |= 0x80;
            }
            buf[pos] = byte as u8;
            pos += 1;
            if value == 0 {
                break;
            }
        }
        self.write_all(&buf[..pos]).await?;
        Ok(())
    }

    async fn write_string<V: AsRef<[u8]> + Send>(&mut self, value: V) -> Result<()> {
        let value = value.as_ref();

        // Write length
        self.write_var_uint(value.len() as u64).await?;

        // Write data
        self.write_all(value).await?;

        Ok(())
    }

    async fn write_vectored_all<'a>(&'a mut self, bufs: &'a [IoSlice<'a>]) -> Result<()> {
        let mut buf_index = 0usize;
        let mut buf_offset = 0usize;

        loop {
            while let Some(buf) = bufs.get(buf_index) {
                if buf_offset >= buf.len() {
                    buf_offset -= buf.len();
                    buf_index += 1;
                } else {
                    break;
                }
            }

            if buf_index >= bufs.len() {
                return Ok(());
            }

            let mut views = Vec::with_capacity(bufs.len() - buf_index);
            if let Some(first) = bufs.get(buf_index) {
                views.push(IoSlice::new(&first[buf_offset..]));
            }
            for buf in &bufs[buf_index + 1..] {
                views.push(IoSlice::new(buf));
            }

            match self.write_vectored(&views).await {
                Ok(0) => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "write_vectored returned 0",
                    )));
                }
                Ok(mut written) => {
                    while written > 0 {
                        let current_len = bufs[buf_index].len().saturating_sub(buf_offset);
                        if written < current_len {
                            buf_offset += written;
                            written = 0;
                        } else {
                            written -= current_len;
                            buf_index += 1;
                            buf_offset = 0;
                            if buf_index >= bufs.len() {
                                break;
                            }
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::Io(e)),
            }
        }
    }
}

#[allow(dead_code)] // TODO: remove once synchronous Arrow path is fully retired
pub(crate) trait ClickHouseBytesRead: bytes::Buf {
    fn try_get_var_uint(&mut self) -> Result<u64>;

    fn try_get_string(&mut self) -> Result<bytes::Bytes>;
}

impl<T: bytes::Buf> ClickHouseBytesRead for T {
    #[inline]
    fn try_get_var_uint(&mut self) -> Result<u64> {
        // Unrolled for speed
        if !self.has_remaining() {
            return Err(Error::Protocol("Unexpected EOF reading varint".into()));
        }
        let b = self.get_u8();
        let mut out = u64::from(b & 0x7F);
        if (b & 0x80) == 0 {
            return Ok(out);
        }

        for i in 1..9 {
            if !self.has_remaining() {
                return Err(Error::Protocol("Unexpected EOF reading varint".into()));
            }
            let b = self.get_u8();
            out |= u64::from(b & 0x7F) << (7 * i);
            if (b & 0x80) == 0 {
                return Ok(out);
            }
        }

        Ok(out)
    }

    #[inline]
    fn try_get_string(&mut self) -> Result<bytes::Bytes> {
        #[expect(clippy::cast_possible_truncation)]
        let len = self.try_get_var_uint()? as usize;

        if len > MAX_STRING_SIZE {
            return Err(Error::Protocol(format!("string too large: {len}")));
        }

        if len == 0 {
            return Ok(bytes::Bytes::new());
        }

        if self.remaining() < len {
            return Err(Error::Protocol("Not enough data for string".into()));
        }

        // Zero-copy slice from the Bytes!
        Ok(self.copy_to_bytes(len))
    }
}

pub(crate) trait ClickHouseBytesWrite: bytes::BufMut {
    fn put_var_uint(&mut self, value: u64) -> Result<()>;

    fn put_string<V: AsRef<[u8]>>(&mut self, value: V) -> Result<()>;
}

impl<T: bytes::BufMut> ClickHouseBytesWrite for T {
    fn put_var_uint(&mut self, mut value: u64) -> Result<()> {
        let mut buf = [0u8; 9]; // Max 9 bytes for u64
        let mut pos = 0;

        #[expect(clippy::cast_possible_truncation)]
        while pos < 9 {
            let mut byte = value & 0x7F;
            value >>= 7;
            if value > 0 {
                byte |= 0x80;
            }
            buf[pos] = byte as u8;
            pos += 1;
            if value == 0 {
                break;
            }
        }

        self.put_slice(&buf[..pos]);
        Ok(())
    }

    fn put_string<V: AsRef<[u8]>>(&mut self, value: V) -> Result<()> {
        let value = value.as_ref();
        // Write length as varint
        self.put_var_uint(value.len() as u64)?;
        self.put_slice(value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::AsyncWrite;

    use super::*;

    struct PartialVectoredWriter {
        max_chunk: usize,
        out:       Vec<u8>,
    }

    impl PartialVectoredWriter {
        fn new(max_chunk: usize) -> Self { Self { max_chunk, out: Vec::new() } }
    }

    impl AsyncWrite for PartialVectoredWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let n = self.max_chunk.min(buf.len());
            self.out.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let mut written = 0usize;
            for buf in bufs {
                if written >= self.max_chunk {
                    break;
                }

                let room = self.max_chunk - written;
                let n = room.min(buf.len());
                self.out.extend_from_slice(&buf[..n]);
                written += n;

                if n < buf.len() {
                    break;
                }
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool { true }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn write_vectored_all_handles_partial_vectored_writes() {
        let mut writer = PartialVectoredWriter::new(3);
        let a = IoSlice::new(b"abcd");
        let b = IoSlice::new(b"ef");
        let c = IoSlice::new(b"ghij");
        let bufs = [a, b, c];

        ClickHouseWrite::write_vectored_all(&mut writer, &bufs).await.unwrap();

        assert_eq!(writer.out, b"abcdefghij");
    }

    #[tokio::test]
    async fn write_vectored_all_handles_empty_input() {
        let mut writer = PartialVectoredWriter::new(2);
        let bufs: [IoSlice<'_>; 0] = [];

        ClickHouseWrite::write_vectored_all(&mut writer, &bufs).await.unwrap();

        assert!(writer.out.is_empty());
    }
}
