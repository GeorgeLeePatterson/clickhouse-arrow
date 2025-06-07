use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::native::protocol::MAX_STRING_SIZE;
use crate::{Error, Result};

/// An extension trait on [`AsyncRead`] providing `ClickHouse` specific functionality.
pub(crate) trait ClickhouseRead: AsyncRead + Unpin + Send + Sync {
    fn read_var_uint(&mut self) -> impl Future<Output = Result<u64>> + Send + '_;

    fn read_string(&mut self) -> impl Future<Output = Result<Vec<u8>>> + Send + '_;

    fn read_utf8_string(&mut self) -> impl Future<Output = Result<String>> + Send + '_ {
        async { Ok(String::from_utf8(self.read_string().await?)?) }
    }
}

impl<T: AsyncRead + Unpin + Send + Sync> ClickhouseRead for T {
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
pub(crate) trait ClickhouseWrite: AsyncWrite + Unpin + Send + Sync {
    fn write_var_uint(&mut self, value: u64) -> impl Future<Output = Result<()>> + Send + '_;

    fn write_string<V: AsRef<[u8]> + Send>(
        &mut self,
        value: V,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, Self, V>;
}

impl<T: AsyncWrite + Unpin + Send + Sync> ClickhouseWrite for T {
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
