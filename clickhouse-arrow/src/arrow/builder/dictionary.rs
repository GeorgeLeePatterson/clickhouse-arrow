use arrow::array::*;
use arrow::datatypes::*;

use super::TypedBuilder;
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
        let value_builder = Box::new(TypedBuilder::try_new(type_, value_type)?);
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
