use tokio::io::AsyncReadExt;

use super::{ClickHouseNativeDeserializer, Deserializer, DeserializerState, Type};
use crate::formats::{CustomPlan, VariantPrefixState};
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
                if state.custom_node().is_none() {
                    drop(state.replace_custom_plan(CustomPlan::from_type_structure(type_)));
                }
                #[expect(clippy::cast_possible_truncation)]
                let _ = state.replace_current_variant_prefix(VariantPrefixState {
                    discriminator_mode: mode as u8,
                });

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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::formats::CustomPlan;

    fn variant_type() -> Type { Type::Variant(vec![Type::UInt8, Type::String]) }

    fn state_with_plan(type_: &Type) -> DeserializerState<()> {
        let mut state = DeserializerState::<()>::default();
        drop(state.replace_custom_plan(CustomPlan::from_type_structure(type_)));
        state
    }

    #[tokio::test]
    async fn read_prefix_rejects_non_variant_type() {
        let mut reader = Cursor::new(0_u64.to_le_bytes().to_vec());
        let mut state = state_with_plan(&Type::UInt8);
        let error = VariantDeserializer::read_prefix(&Type::UInt8, &mut reader, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("non-variant"));
    }

    #[tokio::test]
    async fn read_prefix_rejects_unsupported_discriminator_mode() {
        let mut reader = Cursor::new(2_u64.to_le_bytes().to_vec());
        let mut state = state_with_plan(&variant_type());
        let error = VariantDeserializer::read_prefix(&variant_type(), &mut reader, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unsupported Variant discriminator mode"));
    }

    #[tokio::test]
    async fn read_prefix_stores_variant_prefix_state() {
        let mut reader = Cursor::new(1_u64.to_le_bytes().to_vec());
        let mut state = state_with_plan(&variant_type());
        VariantDeserializer::read_prefix(&variant_type(), &mut reader, &mut state).await.unwrap();
        let prefix = state
            .take_custom_plan()
            .and_then(|plan| plan.node(plan.root).cloned())
            .and_then(|node| node.variant_prefix)
            .unwrap();
        assert_eq!(prefix.discriminator_mode, 1);
    }

    #[tokio::test]
    async fn read_returns_unimplemented_error() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut state = state_with_plan(&variant_type());
        let error = VariantDeserializer::read(&variant_type(), &mut reader, 0, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
