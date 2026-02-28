use tokio::io::AsyncReadExt;

use super::{ClickHouseNativeDeserializer, Deserializer, DeserializerState, Type};
use crate::formats::VariantPrefixState;
use crate::io::ClickHouseRead;
use crate::native::values::Value;
use crate::{Error, Result};

pub(crate) struct VariantDeserializer;

impl Deserializer for VariantDeserializer {
    async fn read_prefix<R: ClickHouseRead, T: Default + Send>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState<T>,
    ) -> Result<()> {
        match type_ {
            Type::Variant(variants) => {
                let mode = reader.read_u64_le().await?;
                if mode > 1 {
                    return Err(Error::deserialize(format!(
                        "unsupported Variant discriminator mode {mode}; only basic mode 0 and \
                         compact mode 1 are supported"
                    )));
                }
                #[expect(clippy::cast_possible_truncation)]
                let _ = state
                    .replace_variant_prefix(VariantPrefixState { discriminator_mode: mode as u8 });

                for variant in variants {
                    variant.deserialize_prefix(reader, state).await?;
                }

                Ok(())
            }
            _ => Err(Error::deserialize("VariantDeserializer called with non-variant type")),
        }
    }

    async fn read<R: ClickHouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        Err(Error::deserialize("VariantDeserializer native value read is not implemented"))
    }

    // TODO: Remove
    // fn read_sync(
    //     _type_: &Type,
    //     _reader: &mut impl ClickHouseBytesRead,
    //     _rows: usize,
    //     _state: &mut DeserializerState,
    // ) -> Result<Vec<Value>> {
    //     Err(Error::DeserializeError(
    //         "VariantDeserializer sync native value read is not implemented".to_string(),
    //     ))
    // }
}
