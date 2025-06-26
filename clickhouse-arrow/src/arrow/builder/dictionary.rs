// TODO: Remove
#![expect(unused)]

use arrow::array::*;
use arrow::datatypes::*;
use strum::AsRefStr;

use super::{TypedBuilder, traceb};
use crate::constants::CLICKHOUSE_DEFAULT_CHUNK_ROWS;
use crate::{Error, Result, Type};

#[derive(Debug)]
pub(crate) enum LowCardinalityKeyBuilder {
    UInt8(PrimitiveBuilder<UInt8Type>),
    UInt16(PrimitiveBuilder<UInt16Type>),
    UInt32(PrimitiveBuilder<UInt32Type>),
    UInt64(PrimitiveBuilder<UInt64Type>),
    Int8(PrimitiveBuilder<Int8Type>),
    Int16(PrimitiveBuilder<Int16Type>),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
}

impl LowCardinalityKeyBuilder {
    pub(crate) fn try_new(data_type: &DataType) -> Result<Self> {
        type Prim<Key> = PrimitiveBuilder<Key>;
        const ROWS: usize = CLICKHOUSE_DEFAULT_CHUNK_ROWS;

        match data_type {
            DataType::UInt8 => Ok(Self::UInt8(Prim::<UInt8Type>::with_capacity(ROWS))),
            DataType::UInt16 => Ok(Self::UInt16(Prim::<UInt16Type>::with_capacity(ROWS))),
            DataType::UInt32 => Ok(Self::UInt32(Prim::<UInt32Type>::with_capacity(ROWS))),
            DataType::UInt64 => Ok(Self::UInt64(Prim::<UInt64Type>::with_capacity(ROWS))),
            DataType::Int8 => Ok(Self::Int8(Prim::<Int8Type>::with_capacity(ROWS))),
            DataType::Int16 => Ok(Self::Int16(Prim::<Int16Type>::with_capacity(ROWS))),
            DataType::Int32 => Ok(Self::Int32(Prim::<Int32Type>::with_capacity(ROWS))),
            DataType::Int64 => Ok(Self::Int64(Prim::<Int64Type>::with_capacity(ROWS))),
            _ => Err(Error::ArrowTypeMismatch {
                expected: "UInt8/UInt16/UInt32/UInt64".into(),
                provided: data_type.to_string(),
            }),
        }
    }
}

pub(crate) struct LowCardinalityBuilder {
    pub(crate) key_builder:   LowCardinalityKeyBuilder,
    pub(crate) value_builder: Box<TypedBuilder>,
}

impl LowCardinalityBuilder {
    pub(crate) fn try_new(type_: &Type, data_type: &DataType) -> Result<Self> {
        let type_ = type_.strip_null();
        let DataType::Dictionary(key_type, value_type) = data_type else {
            return Err(Error::ArrowTypeMismatch {
                expected: "DataType::Dictionary".into(),
                provided: data_type.to_string(),
            });
        };

        let key_builder = LowCardinalityKeyBuilder::try_new(key_type)?;
        let value_builder = Box::new(TypedBuilder::try_new(type_, value_type, "lowcard")?);
        Ok(LowCardinalityBuilder { key_builder, value_builder })
    }
}

impl std::fmt::Debug for LowCardinalityBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LowCardinalityBuilder(key={:?},value={:?})",
            self.key_builder, self.value_builder
        )
    }
}

macro_rules! create_dictionary_builder {
    ($key_type:ty, $data_type:expr, $type_hint:expr) => {{
        use arrow::array::*;
        use arrow::datatypes::*;

        use crate::Type;

        match ($type_hint, $data_type) {
            // Strings/binary
            (Type::String | Type::Binary | Type::Object, DataType::Utf8) => {
                Ok(Box::new(StringDictionaryBuilder::<$key_type>::new()) as Box<dyn ArrayBuilder>)
            }
            (Type::String | Type::Binary | Type::Object, DataType::LargeUtf8) => {
                Ok(Box::new(LargeStringDictionaryBuilder::<$key_type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (Type::String | Type::Binary | Type::Object, DataType::Binary) => {
                Ok(Box::new(BinaryDictionaryBuilder::<$key_type>::new()) as Box<dyn ArrayBuilder>)
            }
            (Type::String | Type::Binary | Type::Object, DataType::LargeBinary) => {
                Ok(Box::new(LargeBinaryDictionaryBuilder::<$key_type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            // Primitive types
            (_, DataType::UInt8) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, UInt8Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::UInt16) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, UInt16Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::UInt32) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, UInt32Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::UInt64) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, UInt64Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::Int8) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, Int8Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::Int16) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, Int16Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::Int32) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, Int32Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::Int64) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, Int64Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::Float16) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, Float16Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::Float32) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, Float32Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            (_, DataType::Float64) => {
                Ok(Box::new(PrimitiveDictionaryBuilder::<$key_type, Float64Type>::new())
                    as Box<dyn ArrayBuilder>)
            }
            _ => Err($crate::Error::ArrowDeserialize(format!(
                "Unexpected types for dictionary: value = {}, type_hint = {}",
                $data_type, $type_hint
            ))),
        }
    }};
}

#[inline]
pub(crate) fn create_dyn_dict_builder(
    data_type: &DataType,
    type_hint: &Type,
    name: &str,
) -> Result<Box<dyn ArrayBuilder>> {
    let base_type = type_hint.strip_null();

    let DataType::Dictionary(idx_type, value_type) = data_type else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected type for dictionary: datatype = {data_type}, type_hint = {type_hint}",
        )));
    };

    Ok(traceb(
        match &**idx_type {
            DataType::Int8 => create_dictionary_builder!(Int8Type, &**value_type, base_type)?,
            DataType::Int16 => create_dictionary_builder!(Int16Type, &**value_type, base_type)?,
            DataType::Int32 => create_dictionary_builder!(Int32Type, &**value_type, base_type)?,
            DataType::Int64 => create_dictionary_builder!(Int64Type, &**value_type, base_type)?,
            DataType::UInt8 => create_dictionary_builder!(UInt8Type, &**value_type, base_type)?,
            DataType::UInt16 => create_dictionary_builder!(UInt16Type, &**value_type, base_type)?,
            DataType::UInt64 => create_dictionary_builder!(UInt64Type, &**value_type, base_type)?,
            // Default
            _ => create_dictionary_builder!(UInt32Type, &**value_type, base_type)?,
        },
        type_hint,
        "Dictionary",
        name,
    ))
}
