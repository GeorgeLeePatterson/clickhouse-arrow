use std::str::FromStr;

use tokio::io::AsyncReadExt;

use super::{ClickHouseNativeDeserializer, Deserializer, DeserializerState, Type};
use crate::formats::DynamicPrefixState;
use crate::io::ClickHouseRead;
use crate::native::values::Value;
use crate::{Error, Result};

const DYNAMIC_FLATTENED_SERIALIZATION_VERSION: u64 = 3;

#[inline]
fn dynamic_indexes_type(num_flattened_types: usize) -> Type {
    if u8::try_from(num_flattened_types).is_ok() {
        Type::UInt8
    } else if u16::try_from(num_flattened_types).is_ok() {
        Type::UInt16
    } else if u32::try_from(num_flattened_types).is_ok() {
        Type::UInt32
    } else {
        Type::UInt64
    }
}

pub(crate) struct DynamicDeserializer;

impl Deserializer for DynamicDeserializer {
    async fn read_prefix<R: ClickHouseRead, T: Default + Send>(
        type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState<T>,
    ) -> Result<()> {
        if !matches!(type_, Type::Dynamic { .. }) {
            return Err(Error::deserialize("DynamicDeserializer called with non-dynamic type"));
        }

        let serialization_version = reader.read_u64_le().await?;
        if serialization_version != DYNAMIC_FLATTENED_SERIALIZATION_VERSION {
            return Err(Error::deserialize(format!(
                "Dynamic serialization version {serialization_version} is not supported; only \
                 flattened version {DYNAMIC_FLATTENED_SERIALIZATION_VERSION} is supported"
            )));
        }

        #[expect(clippy::cast_possible_truncation)]
        let num_flattened_types = reader.read_var_uint().await? as usize;
        let mut flattened_types: Vec<Type> = Vec::with_capacity(num_flattened_types);
        for _ in 0..num_flattened_types {
            let type_name = reader.read_utf8_string().await?;
            let parsed = Type::from_str(&type_name).map_err(|e| {
                Error::deserialize(format!(
                    "Dynamic flattened type list contains unknown type '{type_name}': {e}"
                ))
            })?;
            flattened_types.push(parsed);
        }

        let indexes_type = dynamic_indexes_type(num_flattened_types);
        indexes_type.deserialize_prefix(reader, state).await?;
        for type_ in &flattened_types {
            type_.deserialize_prefix(reader, state).await?;
        }

        {
            drop(state.replace_dynamic_prefix(DynamicPrefixState {
                serialization_version,
                flattened_types,
            }));
        }
        Ok(())
    }

    async fn read<R: ClickHouseRead>(
        _type_: &Type,
        _reader: &mut R,
        _rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        Err(Error::deserialize("DynamicDeserializer native value read is not implemented"))
    }

    // TODO: Remove
    // fn read_sync(
    //     _type_: &Type,
    //     _reader: &mut impl ClickHouseBytesRead,
    //     _rows: usize,
    //     _state: &mut DeserializerState,
    // ) -> Result<Vec<Value>> {
    //     Err(Error::DeserializeError(
    //         "DynamicDeserializer sync native value read is not implemented".to_string(),
    //     ))
    // }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn push_var_uint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn push_string(out: &mut Vec<u8>, value: &str) {
        push_var_uint(out, value.len() as u64);
        out.extend_from_slice(value.as_bytes());
    }

    fn dynamic_prefix_bytes(types: &[&str], version: u64) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&version.to_le_bytes());
        push_var_uint(&mut out, types.len() as u64);
        for type_name in types {
            push_string(&mut out, type_name);
        }
        out
    }

    #[tokio::test]
    async fn read_prefix_rejects_non_dynamic_type() {
        let mut reader = Cursor::new(dynamic_prefix_bytes(&["UInt8"], 3));
        let mut state = DeserializerState::<()>::default();
        let error = DynamicDeserializer::read_prefix(&Type::UInt8, &mut reader, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("non-dynamic"));
    }

    #[tokio::test]
    async fn read_prefix_rejects_unsupported_version() {
        let mut reader = Cursor::new(dynamic_prefix_bytes(&["UInt8"], 2));
        let mut state = DeserializerState::<()>::default();
        let error = DynamicDeserializer::read_prefix(
            &Type::Dynamic { max_types: 8 },
            &mut reader,
            &mut state,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("not supported"));
    }

    #[tokio::test]
    async fn read_prefix_rejects_unknown_flattened_type() {
        let mut reader = Cursor::new(dynamic_prefix_bytes(&["NopeType"], 3));
        let mut state = DeserializerState::<()>::default();
        let error = DynamicDeserializer::read_prefix(
            &Type::Dynamic { max_types: 8 },
            &mut reader,
            &mut state,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("unknown type"));
    }

    #[tokio::test]
    async fn read_prefix_stores_dynamic_prefix_metadata() {
        let bytes = dynamic_prefix_bytes(&["UInt8", "String"], 3);
        let mut reader = Cursor::new(bytes);
        let mut state = DeserializerState::<()>::default();
        DynamicDeserializer::read_prefix(&Type::Dynamic { max_types: 8 }, &mut reader, &mut state)
            .await
            .unwrap();

        let dynamic = state.take_dynamic_prefix().unwrap();
        assert_eq!(dynamic.serialization_version, 3);
        assert_eq!(dynamic.flattened_types, vec![Type::UInt8, Type::String]);
    }

    #[tokio::test]
    async fn read_returns_unimplemented_error() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut state = DeserializerState::<()>::default();
        let error =
            DynamicDeserializer::read(&Type::Dynamic { max_types: 8 }, &mut reader, 0, &mut state)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
