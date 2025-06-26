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
}

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
