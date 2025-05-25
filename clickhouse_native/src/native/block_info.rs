use tokio::io::AsyncReadExt;

use crate::{Error, ClickhouseRead, ClickhouseWrite, Result};

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
    pub(crate) async fn read<R: ClickhouseRead>(reader: &mut R) -> Result<Self> {
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
                    return Err(Error::ProtocolError(format!(
                        "unknown block info field number: {field_num}"
                    )));
                }
            }
        }
        Ok(new)
    }

    pub(crate) async fn write<W: ClickhouseWrite>(&self, writer: &mut W) -> Result<()> {
        writer.write_var_uint(1).await?; // Block info version
        writer.write_u8(if self.is_overflows { 1 } else { 2 }).await?; // Is overflows
        writer.write_var_uint(2).await?; // Bucket num
        writer.write_i32_le(self.bucket_num).await?; // Bucket num
        writer.write_var_uint(0).await?; // End field
        Ok(())
    }
}
