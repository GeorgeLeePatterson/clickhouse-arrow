use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::io::{ClickHouseBytesRead, ClickHouseBytesWrite, ClickHouseRead, ClickHouseWrite};
use crate::{Error, Result};

/// Metadata about a block
#[derive(Debug, Clone, Copy)]
pub struct BlockInfo {
    pub is_overflows: bool,
    pub bucket_num:   i32,
}

impl Default for BlockInfo {
    fn default() -> Self { BlockInfo { is_overflows: false, bucket_num: -1 } }
}

impl BlockInfo {
    pub(crate) async fn read_async<R: ClickHouseRead>(reader: &mut R) -> Result<Self> {
        let mut new = Self::default();
        loop {
            let field_num = reader.read_var_uint().await?;
            match field_num {
                0 => break,
                1 => {
                    new.is_overflows = reader.read_u8().await? != 0;
                }
                2 => {
                    new.bucket_num = reader.read_i32_le().await?;
                }
                field_num => {
                    return Err(Error::Protocol(format!(
                        "unknown block info field number: {field_num}"
                    )));
                }
            }
        }
        Ok(new)
    }

    pub(crate) async fn write_async<W: ClickHouseWrite>(&self, writer: &mut W) -> Result<()> {
        writer.write_var_uint(1).await?; // Block info version
        writer.write_u8(if self.is_overflows { 1 } else { 2 }).await?; // Is overflows
        writer.write_var_uint(2).await?; // Bucket num
        writer.write_i32_le(self.bucket_num).await?; // Bucket num
        writer.write_var_uint(0).await?; // End field
        Ok(())
    }

    #[allow(dead_code)] // TODO: remove once synchronous block path is fully retired
    pub(crate) fn read<R: ClickHouseBytesRead>(reader: &mut R) -> Result<Self> {
        let mut new = Self::default();
        loop {
            let field_num = reader.try_get_var_uint()?;
            match field_num {
                0 => break,
                1 => {
                    new.is_overflows = reader.try_get_u8()? != 0;
                }
                2 => {
                    new.bucket_num = reader.try_get_i32_le()?;
                }
                field_num => {
                    return Err(Error::Protocol(format!(
                        "unknown block info field number: {field_num}"
                    )));
                }
            }
        }
        Ok(new)
    }

    pub(crate) fn write<W: ClickHouseBytesWrite>(self, writer: &mut W) -> Result<()> {
        writer.put_var_uint(1)?; // Block info version
        writer.put_u8(if self.is_overflows { 1 } else { 2 }); // Is overflows
        writer.put_var_uint(2)?; // Bucket num
        writer.put_i32_le(self.bucket_num); // Bucket num
        writer.put_var_uint(0)?; // End field
        Ok(())
    }
}
