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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn test_low_cardinality_key_builder_uint8() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::UInt8).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::UInt8(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_uint16() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::UInt16).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::UInt16(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_uint32() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::UInt32).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::UInt32(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_uint64() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::UInt64).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::UInt64(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_int8() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::Int8).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::Int8(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_int16() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::Int16).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::Int16(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_int32() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::Int32).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::Int32(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_int64() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::Int64).unwrap();
        assert!(matches!(builder, LowCardinalityKeyBuilder::Int64(_)));
    }

    #[test]
    fn test_low_cardinality_key_builder_invalid_type() {
        let result = LowCardinalityKeyBuilder::try_new(&DataType::Float32);
        assert!(result.is_err());

        if let Err(Error::ArrowTypeMismatch { expected, provided }) = result {
            assert_eq!(expected, "UInt8/UInt16/UInt32/UInt64");
            assert_eq!(provided, "Float32");
        } else {
            panic!("Expected ArrowTypeMismatch error");
        }
    }

    #[test]
    fn test_low_cardinality_key_builder_debug() {
        let builder = LowCardinalityKeyBuilder::try_new(&DataType::UInt32).unwrap();
        let debug_str = format!("{builder:?}");
        assert!(debug_str.contains("UInt32"));
    }

    #[test]
    fn test_low_cardinality_builder_string() {
        let value_type = Box::new(DataType::Utf8);
        let key_type = Box::new(DataType::UInt32);
        let data_type = DataType::Dictionary(key_type, value_type);
        let type_ = Type::String;

        let builder = LowCardinalityBuilder::try_new(&type_, &data_type).unwrap();
        assert!(matches!(builder.key_builder, LowCardinalityKeyBuilder::UInt32(_)));
    }

    #[test]
    fn test_low_cardinality_builder_nullable() {
        let value_type = Box::new(DataType::Utf8);
        let key_type = Box::new(DataType::UInt8);
        let data_type = DataType::Dictionary(key_type, value_type);
        let type_ = Type::Nullable(Box::new(Type::String));

        let builder = LowCardinalityBuilder::try_new(&type_, &data_type).unwrap();
        assert!(matches!(builder.key_builder, LowCardinalityKeyBuilder::UInt8(_)));
    }

    #[test]
    fn test_low_cardinality_builder_invalid_data_type() {
        let type_ = Type::String;
        let data_type = DataType::Utf8; // Not a Dictionary type

        let result = LowCardinalityBuilder::try_new(&type_, &data_type);
        assert!(result.is_err());

        if let Err(Error::ArrowTypeMismatch { expected, provided }) = result {
            assert_eq!(expected, "DataType::Dictionary");
            assert_eq!(provided, "Utf8");
        } else {
            panic!("Expected ArrowTypeMismatch error");
        }
    }

    #[test]
    fn test_low_cardinality_builder_invalid_key_type() {
        let value_type = Box::new(DataType::Utf8);
        let key_type = Box::new(DataType::Float32); // Invalid key type
        let data_type = DataType::Dictionary(key_type, value_type);
        let type_ = Type::String;

        let result = LowCardinalityBuilder::try_new(&type_, &data_type);
        assert!(result.is_err());
    }

    #[test]
    fn test_low_cardinality_builder_debug() {
        let value_type = Box::new(DataType::Binary);
        let key_type = Box::new(DataType::UInt16);
        let data_type = DataType::Dictionary(key_type, value_type);
        let type_ = Type::Binary;

        let builder = LowCardinalityBuilder::try_new(&type_, &data_type).unwrap();
        let debug_str = format!("{builder:?}");
        assert!(debug_str.contains("LowCardinalityBuilder"));
        assert!(debug_str.contains("key="));
        assert!(debug_str.contains("value="));
    }

    #[test]
    fn test_low_cardinality_builder_different_key_types() {
        let test_cases = vec![
            (DataType::UInt8, Type::String),
            (DataType::UInt16, Type::String),
            (DataType::UInt32, Type::String),
            (DataType::UInt64, Type::String),
            (DataType::Int8, Type::Binary),
            (DataType::Int16, Type::Binary),
            (DataType::Int32, Type::Binary),
            (DataType::Int64, Type::Binary),
        ];

        for (key_data_type, value_type) in test_cases {
            let value_data_type = match &value_type {
                Type::String => Box::new(DataType::Utf8),
                Type::Binary => Box::new(DataType::Binary),
                _ => panic!("Unexpected type"),
            };
            let key_type = Box::new(key_data_type.clone());
            let data_type = DataType::Dictionary(key_type, value_data_type);

            let result = LowCardinalityBuilder::try_new(&value_type, &data_type);
            assert!(result.is_ok(), "Failed for key type: {key_data_type:?}");
        }
    }
}
