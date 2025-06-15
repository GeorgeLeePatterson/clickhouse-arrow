use arrow::array::*;
use arrow::datatypes::*;

use super::binary::binary;
use super::primitive::primitive;
use super::{dictionary, list, nested};
use crate::geo::normalize_geo_type;
use crate::io::ClickhouseBytesRead;
use crate::{Result, Type};

macro_rules! deserialize {
    ($typ:expr, $reader:expr, $builder:expr, $null_check:expr =>
    {
        $( $t:pat => ($builder_ty:ty, $nullable:expr, $b:ident => { $st:expr }) ),+
    }
    _ => {
        $rem:expr
    }) => {
        match $typ {
            $(
                $t => {
                    let builder = $builder
                        .as_any_mut()
                        .downcast_mut::<$builder_ty>()
                        .ok_or_else(|| $crate::Error::Protocol(format!(
                            "Failed to downcast builder for {}", stringify!($builder_ty)
                        )))?;
                    if $nullable {
                        let is_null = $reader
                            .try_get_u8()
                            .map_err(|e| $crate::Error::Protocol(format!(
                                "Failed to read null flag: {}", e
                            )))? != 0;
                        if is_null {
                            builder.append_null();
                            return Ok(());
                        }
                    }
                    if $null_check {
                        return Ok(());
                    }
                    let $b = builder;
                    $st;
                }
            )+
            // Malformed
            Type::DateTime64(p @ 10.., _) => {
                return Err($crate::Error::ArrowUnsupportedType(format!("DateTime64 > 9: {p}")));
            }
            _ => { $rem }
        }
    }
}
pub(super) use deserialize;

pub(super) fn read_column<R: ClickhouseBytesRead>(
    reader: &mut R,
    type_: &Type,
    builder: &mut dyn ArrayBuilder,
    // Whether only a null check is done. Remove this when last of dynamic types are supported.
    null_check_only: bool,
) -> Result<()> {
    type Ty = Type;
    let nil = type_.is_nullable();

    deserialize!(type_.strip_null(), reader, builder, null_check_only => {
        Ty::Int8 => (PrimitiveBuilder::<Int8Type>, nil, b => { b.append_value(primitive!(Int8 => reader)) }),
        Ty::Int16 => (PrimitiveBuilder::<Int16Type>, nil, b => { b.append_value(primitive!(Int16 => reader)) }),
        Ty::Int32 => (PrimitiveBuilder::<Int32Type>, nil, b => { b.append_value(primitive!(Int32 => reader)) }),
        Ty::Int64 => (PrimitiveBuilder::<Int64Type>, nil, b => { b.append_value(primitive!(Int64 => reader)) }),
        Ty::UInt8 => (PrimitiveBuilder::<UInt8Type>, nil, b => { b.append_value(primitive!(UInt8 => reader)) }),
        Ty::UInt16 => (PrimitiveBuilder::<UInt16Type>, nil, b => { b.append_value(primitive!(UInt16 => reader)) }),
        Ty::UInt32 => (PrimitiveBuilder::<UInt32Type>, nil, b => { b.append_value(primitive!(UInt32 => reader)) }),
        Ty::UInt64 => (PrimitiveBuilder::<UInt64Type>, nil, b => { b.append_value(primitive!(UInt64 => reader)) }),
        Ty::Float32 => (PrimitiveBuilder::<Float32Type>, nil, b => { b.append_value(primitive!(Float32 => reader)) }),
        Ty::Float64 => (PrimitiveBuilder::<Float64Type>, nil, b => { b.append_value(primitive!(Float64 => reader)) }),
        Ty::Decimal32(_) => (Decimal128Builder, nil, b => { b.append_value(primitive!(Decimal32 => reader)) }),
        Ty::Decimal64(_) => (Decimal128Builder, nil, b => { b.append_value(primitive!(Decimal64 => reader)) }),
        Ty::Decimal128(_) => (Decimal128Builder, nil, b => { b.append_value(primitive!(Decimal128 => reader)) }),
        Ty::Decimal256(_) => (Decimal256Builder, nil, b => { b.append_value(primitive!(Decimal256 => reader)) }),
        // Dates
        Ty::Date => (Date32Builder, nil, b => { b.append_value(primitive!(Date => reader)) }),
        Ty::Date32 => (Date32Builder, nil, b => { b.append_value(primitive!(Date32 => reader)) }),
        Ty::DateTime(_) => (TimestampSecondBuilder, nil, b => { b.append_value(primitive!(DateTime => reader)) }),
        Ty::DateTime64(0, _) => (TimestampSecondBuilder, nil, b => {
            b.append_value(primitive!(DateTime64(0) => reader))
        }),
        Ty::DateTime64(1..=3, _) => (TimestampMillisecondBuilder, nil, b => {
            b.append_value(primitive!(DateTime64(3) => reader))
        }),
        Ty::DateTime64(4..=6, _) => (TimestampMicrosecondBuilder, nil, b => {
            b.append_value(primitive!(DateTime64(6) => reader))
        }),
        Ty::DateTime64(7..=9, _) => (TimestampNanosecondBuilder, nil, b => {
            b.append_value(primitive!(DateTime64(9) => reader))
        }),
        // Strings and Binary
        Ty::String => (StringBuilder, nil, b => { b.append_value(binary!(String => reader)) }),
        Ty::Object => (StringBuilder, nil, b => { b.append_value(binary!(Object => reader)) }),
        Ty::FixedSizedString(n) => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(FixedBinary(*n)=> reader))?
        }),
        Ty::Binary => (BinaryBuilder, nil, b => { b.append_value(binary!(Binary=> reader)) }),
        Ty::FixedSizedBinary(n) => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(FixedBinary(*n)=> reader))?
        }),
        Ty::Uuid => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(Fixed(16)=> reader))?
        }),
        Ty::Ipv4 => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(Ipv4 => reader))?
        }),
        Ty::Ipv6 => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(Ipv6 => reader))?
        }),
        // Special numeric types that need to be read as bytes
        Ty::Int128 => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(Fixed(16)=> reader))?
        }),
        Ty::Int256 => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(FixedRev(32)=> reader))?
        }),
        Ty::UInt128 => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(Fixed(16)=> reader))?
        }),
        Ty::UInt256 => (FixedSizeBinaryBuilder, nil, b => {
            b.append_value(binary!(FixedRev(32)=> reader))?
        })
    }
    _ => {
        // Nested types
        match type_.strip_null() {
            Ty::LowCardinality(inner) => dictionary::dynamic::read_column::<R, Int32Type>(
                reader, inner, builder, null_check_only
            )?,
            Ty::Array(inner_type) => list::deserialize(reader, inner_type, builder, nil)?,
            // Complex nested types
            Ty::Tuple(_) | Ty::Map(_, _) | Ty::Enum8(_) | Ty::Enum16(_) => {
                nested::deserialize(reader, type_, builder, nil)?;
            }
            // Geo types are aliases of nested structures, delegate to underlying types. This should
            // serve as a fallback only, as the type should be converted before this.
            Ty::Point | Ty::Ring | Ty::Polygon | Ty::MultiPolygon => {
                read_column(reader, &normalize_geo_type(type_).unwrap(), builder, null_check_only)?;
            }
            _ => return Err(crate::Error::UnexpectedType(type_.clone()))
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_dynamic() {
        panic!("Test me!");
    }
}
