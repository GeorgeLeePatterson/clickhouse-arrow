use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

use super::binary::binary;
use super::primitive::primitive;
use crate::io::ClickhouseBytesRead;
use crate::{Error, Result, Type};

pub(super) trait LowCardinalityDictionary {
    fn append_null(&mut self, reader: &mut dyn ClickhouseBytesRead, type_: &Type) -> Result<bool>;

    fn append_value(&mut self, reader: &mut dyn ClickhouseBytesRead, type_: &Type) -> Result<()>;

    fn finish(&mut self) -> ArrayRef;
}

macro_rules! typed_low_card{
    ($type_hint:expr, { $(
        $typ:pat => ($var:ident, $builder:expr $(,)?)
    ),+ $(,)? }) => {
        match $type_hint {
            $(
                $typ => LowCardinalityBuilder::$var($builder),
            )*
            // Malformed
            Type::DateTime64(10.., _) => return Err(Error::ArrowDeserialize(
                "Invalid DateTime64".into()
            )),
            _ => return Err(Error::UnexpectedType($type_hint.clone())),
        }
    };
    ($type_hint:expr, $dt:expr, { $(
          ($typ:pat, $dtyp:pat) => ($var:ident, $builder:expr $(,)?)
      ),+ $(,)? }) => {
          match ($type_hint, $dt) {
              $(
                  ($typ, $dtyp) => LowCardinalityBuilder::$var($builder),
              )*
              _ => return Err(Error::UnexpectedType($type_hint.clone())),
          }
      }
}

pub(super) enum LowCardinalityBuilder<Key: ArrowDictionaryKeyType> {
    // Primitives
    Int8(PrimitiveDictionaryBuilder<Key, Int8Type>),
    Int16(PrimitiveDictionaryBuilder<Key, Int16Type>),
    Int32(PrimitiveDictionaryBuilder<Key, Int32Type>),
    Int64(PrimitiveDictionaryBuilder<Key, Int64Type>),
    UInt8(PrimitiveDictionaryBuilder<Key, UInt8Type>),
    UInt16(PrimitiveDictionaryBuilder<Key, UInt16Type>),
    UInt32(PrimitiveDictionaryBuilder<Key, UInt32Type>),
    UInt64(PrimitiveDictionaryBuilder<Key, UInt64Type>),
    Float32(PrimitiveDictionaryBuilder<Key, Float32Type>),
    Float64(PrimitiveDictionaryBuilder<Key, Float64Type>),
    Date(PrimitiveDictionaryBuilder<Key, Date32Type>),
    Date32(PrimitiveDictionaryBuilder<Key, Date32Type>),
    // Strings/Binary
    String(StringDictionaryBuilder<Key>),
    LargeString(LargeStringDictionaryBuilder<Key>),
    Binary(BinaryDictionaryBuilder<Key>),
    LargeBinary(LargeBinaryDictionaryBuilder<Key>),
    FixedBinary(FixedSizeBinaryDictionaryBuilder<Key>),
    Object(StringDictionaryBuilder<Key>),
    LargeObject(LargeStringDictionaryBuilder<Key>),
}

impl<Key: ArrowDictionaryKeyType> LowCardinalityBuilder<Key> {
    pub(super) fn try_new(type_: &Type, data_type: &DataType) -> Result<Self> {
        type Prim<Key, Val> = PrimitiveDictionaryBuilder<Key, Val>;

        if matches!(type_, Type::String | Type::Binary | Type::Object) {
            return Ok(typed_low_card!(type_, data_type, {
                // String
                (Type::String | Type::Binary , DataType::Utf8) => (
                   String, StringDictionaryBuilder::<Key>::new()
                ),
                (Type::String | Type::Binary , DataType::LargeUtf8) => (
                   LargeString, LargeStringDictionaryBuilder::<Key>::new()
                ),
                // Object
                (Type::Object, DataType::Utf8) => (Object, StringDictionaryBuilder::<Key>::new()),
                (Type::Object, DataType::LargeUtf8) => (
                    LargeObject, LargeStringDictionaryBuilder::<Key>::new()
                ),
                // Binary
                (Type::String | Type::Binary | Type::Object, DataType::Binary) => (
                   Binary, BinaryDictionaryBuilder::<Key>::new()
                ),
                (Type::String | Type::Binary | Type::Object, DataType::LargeBinary) => (
                   LargeBinary, LargeBinaryDictionaryBuilder::<Key>::new()
                ),
                (Type::String | Type::Binary | Type::Object, DataType::FixedSizeBinary(n)) => (
                   FixedBinary, FixedSizeBinaryDictionaryBuilder::<Key>::new(*n)
                )
            }));
        }

        Ok(typed_low_card!(type_, {
            Type::Int8 => (Int8, Prim::<Key, Int8Type>::new()),
            Type::Int16 => (Int16, Prim::<Key, Int16Type>::new()),
            Type::Int32 => (Int32, Prim::<Key, Int32Type>::new()),
            Type::Int64 => (Int64, Prim::<Key, Int64Type>::new()),
            Type::UInt8 => (UInt8, Prim::<Key, UInt8Type>::new()),
            Type::UInt16 => (UInt16, Prim::<Key, UInt16Type>::new()),
            Type::UInt32 => (UInt32, Prim::<Key, UInt32Type>::new()),
            Type::UInt64 => (UInt64, Prim::<Key, UInt64Type>::new()),
            Type::Float32 => (Float32, Prim::<Key, Float32Type>::new()),
            Type::Float64 => (Float64, Prim::<Key, Float64Type>::new()),
            Type::Date => (Date, Prim::<Key, Date32Type>::new()),
            Type::Date32 => (Date32, Prim::<Key, Date32Type>::new()),
            // TODO: support more types
        }))
    }
}
impl<Key: ArrowDictionaryKeyType> LowCardinalityDictionary for LowCardinalityBuilder<Key> {
    fn append_null(&mut self, reader: &mut dyn ClickhouseBytesRead, type_: &Type) -> Result<bool> {
        if !type_.is_nullable() {
            return Ok(false);
        }
        let is_null = reader
            .try_get_u8()
            .map_err(|e| Error::Protocol(format!("Failed to read null flag: {e}")))?
            != 0;
        if is_null {
            match self {
                Self::Int8(b) => b.append_null(),
                Self::Int16(b) => b.append_null(),
                Self::Int32(b) => b.append_null(),
                Self::Int64(b) => b.append_null(),
                Self::UInt8(b) => b.append_null(),
                Self::UInt16(b) => b.append_null(),
                Self::UInt32(b) => b.append_null(),
                Self::UInt64(b) => b.append_null(),
                Self::Float32(b) => b.append_null(),
                Self::Float64(b) => b.append_null(),
                Self::Date(b) | Self::Date32(b) => b.append_null(),
                Self::String(b) | Self::Object(b) => b.append_null(),
                Self::LargeString(b) | Self::LargeObject(b) => b.append_null(),
                Self::Binary(b) => b.append_null(),
                Self::LargeBinary(b) => b.append_null(),
                Self::FixedBinary(b) => b.append_null(),
            }
        }

        Ok(is_null)
    }

    fn append_value(&mut self, reader: &mut dyn ClickhouseBytesRead, type_: &Type) -> Result<()> {
        match self {
            Self::Int8(b) => b.append_value(primitive!(Int8 => reader)),
            Self::Int16(b) => b.append_value(primitive!(Int16 => reader)),
            Self::Int32(b) => b.append_value(primitive!(Int32 => reader)),
            Self::Int64(b) => b.append_value(primitive!(Int64 => reader)),
            Self::UInt8(b) => b.append_value(primitive!(UInt8 => reader)),
            Self::UInt16(b) => b.append_value(primitive!(UInt16 => reader)),
            Self::UInt32(b) => b.append_value(primitive!(UInt32 => reader)),
            Self::UInt64(b) => b.append_value(primitive!(UInt64 => reader)),
            Self::Float32(b) => b.append_value(primitive!(Float32 => reader)),
            Self::Float64(b) => b.append_value(primitive!(Float64 => reader)),
            Self::Date(b) => b.append_value(primitive!(Date => reader)),
            Self::Date32(b) => b.append_value(primitive!(Date32 => reader)),
            Self::String(b) => b.append_value(binary!(String => reader)),
            Self::Object(b) => b.append_value(binary!(Object => reader)),
            Self::LargeObject(b) => b.append_value(binary!(Object => reader)),
            Self::Binary(b) => b.append_value(binary!(Binary=> reader)),
            Self::LargeString(b) => b.append_value(binary!(String => reader)),
            Self::LargeBinary(b) => b.append_value(binary!(Binary => reader)),
            Self::FixedBinary(b) => {
                match type_.strip_null() {
                    Type::FixedSizedString(n) => {
                        b.append_value(binary!(FixedBinary(*n)=> reader));
                    }
                    Type::FixedSizedBinary(n) => {
                        b.append_value(binary!(FixedBinary(*n)=> reader));
                    }
                    Type::Ipv4 => b.append_value(binary!(Ipv4 => reader)),
                    Type::Ipv6 => b.append_value(binary!(Ipv6 => reader)),
                    Type::Uuid => b.append_value(binary!(Fixed(16)=> reader)),
                    // Special numeric types that need to be read as bytes
                    Type::Int128 => b.append_value(binary!(Fixed(16)=> reader)),
                    Type::Int256 => b.append_value(binary!(FixedRev(32)=> reader)),
                    Type::UInt128 => b.append_value(binary!(Fixed(16)=> reader)),
                    Type::UInt256 => b.append_value(binary!(FixedRev(32)=> reader)),
                    _ => return Err(Error::UnexpectedType(type_.clone())),
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            // Primitive
            Self::Int8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt8(b) => Arc::new(b.finish()),
            Self::UInt16(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::UInt64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Date(b) | Self::Date32(b) => Arc::new(b.finish()),
            // Strings/Binary
            Self::String(b) | Self::Object(b) => Arc::new(b.finish()),
            Self::LargeString(b) | Self::LargeObject(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::LargeBinary(b) => Arc::new(b.finish()),
            Self::FixedBinary(b) => Arc::new(b.finish()),
        }
    }
}

pub(super) mod dynamic {
    use arrow::array::*;
    use arrow::datatypes::*;

    use super::*;
    use crate::row::arrow::builder::traceb;
    use crate::row::arrow::dynamic::deserialize;
    use crate::{Error, Result, Type};

    macro_rules! create_dictionary_builder {
        ($key_type:ty, $data_type:expr, $type_hint:expr) => {{
            use arrow::array::*;
            use arrow::datatypes::*;

            use crate::Type;

            match ($type_hint, $data_type) {
                // Strings/binary
                (Type::String | Type::Binary | Type::Object, DataType::Utf8) => {
                    Ok(Box::new(StringDictionaryBuilder::<$key_type>::new())
                        as Box<dyn ArrayBuilder>)
                }
                (Type::String | Type::Binary | Type::Object, DataType::LargeUtf8) => {
                    Ok(Box::new(LargeStringDictionaryBuilder::<$key_type>::new())
                        as Box<dyn ArrayBuilder>)
                }
                (Type::String | Type::Binary | Type::Object, DataType::Binary) => {
                    Ok(Box::new(BinaryDictionaryBuilder::<$key_type>::new())
                        as Box<dyn ArrayBuilder>)
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
    pub(crate) fn create_dict_builder(
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
                _ => create_dictionary_builder!(Int64Type, &**value_type, base_type)?,
            },
            type_hint,
            "Dictionary",
            name,
        ))
    }

    #[inline]
    pub(crate) fn read_column<R: ClickhouseBytesRead, DictKey: ArrowDictionaryKeyType>(
        reader: &mut R,
        type_: &Type,
        builder: &mut dyn ArrayBuilder,
        null_check_only: bool,
    ) -> Result<()> {
        type PrimDict<Key, Val> = PrimitiveDictionaryBuilder<Key, Val>;

        let nil = type_.is_nullable();
        deserialize!(type_.strip_null(), reader, builder, null_check_only => {
            // Numeric, DateTime, Decimal
            Type::Int8 => (PrimDict::<DictKey, Int8Type>, nil, b => { b.append_value(primitive!(Int8 => reader)) }),
            Type::Int16 => (PrimDict::<DictKey, Int16Type>, nil, b => { b.append_value(primitive!(Int16 => reader)) }),
            Type::Int32 => (PrimDict::<DictKey, Int32Type>, nil, b => { b.append_value(primitive!(Int32 => reader)) }),
            Type::Int64 => (PrimDict::<DictKey, Int64Type>, nil, b => { b.append_value(primitive!(Int64 => reader)) }),
            Type::UInt8 => (PrimDict::<DictKey, UInt8Type>, nil, b => { b.append_value(primitive!(UInt8 => reader)) }),
            Type::UInt16 => (PrimDict::<DictKey, UInt16Type>, nil, b => {
                b.append_value(primitive!(UInt16 => reader))
            }),
            Type::UInt32 => (PrimDict::<DictKey, UInt32Type>, nil, b => {
                b.append_value(primitive!(UInt32 => reader))
            }),
            Type::UInt64 => (PrimDict::<DictKey, UInt64Type>, nil, b => {
                b.append_value(primitive!(UInt64 => reader))
            }),
            Type::Float32 => (PrimDict::<DictKey, Float32Type>, nil, b => {
                b.append_value(primitive!(Float32 => reader))
            }),
            Type::Float64 => (PrimDict::<DictKey, Float64Type>, nil, b => {
                b.append_value(primitive!(Float64 => reader))
            }),
            Type::Date => (PrimDict::<DictKey, Date32Type>, nil, b => {
                b.append_value(primitive!(Date => reader))
            }),
            Type::Date32 => (PrimDict::<DictKey, Date32Type>, nil, b => {
                b.append_value(primitive!(Date32 => reader))
            }),
            Type::DateTime(_) => (PrimDict::<DictKey, TimestampSecondType>, nil, b => {
                b.append_value(primitive!(DateTime => reader))
            }),
            Type::DateTime64(0, _) => (PrimDict::<DictKey, TimestampSecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(0) => reader))
            }),
            Type::DateTime64(1..=3, _) => (PrimDict::<DictKey, TimestampMillisecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(3) => reader))
            }),
            Type::DateTime64(4..=6, _) => (PrimDict::<DictKey, TimestampMicrosecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(6) => reader))
            }),
            Type::DateTime64(7..=9, _) => (PrimDict::<DictKey, TimestampNanosecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(9) => reader))
            }),
            Type::Decimal32(_) => (PrimDict::<DictKey, Decimal128Type>, nil, b => {
                b.append_value(primitive!(Decimal32 => reader))
            }),
            Type::Decimal64(_) => (PrimDict::<DictKey, Decimal128Type>, nil, b => {
                b.append_value(primitive!(Decimal64 => reader))
            }),
            Type::Decimal128(_) => (PrimDict::<DictKey, Decimal128Type>, nil, b => {
                b.append_value(primitive!(Decimal128 => reader))
            }),
            Type::Decimal256(_) => (PrimDict::<DictKey, Decimal256Type>, nil, b => {
                b.append_value(primitive!(Decimal256 => reader))
            }),
            // Strings and Binary
            Type::String => (StringDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(String => reader))
            }),
            Type::Object => (StringDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Object => reader))
            }),
            Type::FixedSizedString(n) => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedBinary(*n)=> reader))
            }),
            Type::Binary => (BinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Binary=> reader))
            }),
            Type::FixedSizedBinary(n) => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedBinary(*n)=> reader))
            }),
            Type::Uuid => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Fixed(16)=> reader))
            }),
            Type::Ipv4 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Ipv4 => reader))
            }),
            Type::Ipv6 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Ipv6 => reader))
            }),
            // Special numeric types that need to be read as bytes
            Type::Int128 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Fixed(16)=> reader))
            }),
            Type::Int256 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedRev(32)=> reader))
            }),
            Type::UInt128 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Fixed(16)=> reader))
            }),
            Type::UInt256 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedRev(32)=> reader))
            })
        }
        _ => {
            return Err(Error::ArrowDeserialize(format!(
                "Unsupported type for Dictionary: {type_:?}"
            )))
        });

        Ok(())
    }
}
