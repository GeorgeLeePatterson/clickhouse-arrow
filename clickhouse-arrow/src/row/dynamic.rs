use arrow::array::*;
use arrow::datatypes::*;

use super::deserialize::binary::binary;
use super::deserialize::primitive::primitive;
use super::deserialize::{dictionary, enums, list, map, tuple};
use crate::geo::normalize_geo_type;
use crate::io::ClickHouseBytesRead;
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
pub(crate) use deserialize;

pub(crate) fn read_column<R: ClickHouseBytesRead>(
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
            Ty::LowCardinality(inner) => dictionary::row::read_column::<R, Int32Type>(
                reader, inner, builder, null_check_only
            )?,
            Ty::Array(inner_type) => list::row::deserialize_dynamic(reader, inner_type, builder, nil)?,
            // Complex nested types
            Ty::Map(_, _) => {
                map::row::deserialize_dynamic(reader, type_, builder, nil)?;
            }
            Ty::Tuple(_)  => {
                tuple::row::deserialize_dynamic(reader, type_, builder, nil)?;
            }
            Ty::Enum8(_) | Ty::Enum16(_) => {
                enums::row::deserialize_dynamic(reader, type_, builder, nil)?;
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

pub(crate) mod builder {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::*;

    use super::*;
    use crate::row::builder::dictionary::create_dyn_dict_builder;
    use crate::row::builder::map::create_dyn_map_builder;
    use crate::row::builder::traceb;
    use crate::row::read::BATCH_CHUNK_SIZE;
    use crate::{Error, Result, Type};

    // TODO: Use BATCH_CHUNK_SIZE for capacity of all builders
    #[inline]
    #[expect(clippy::too_many_lines)]
    #[expect(clippy::cast_possible_wrap)]
    #[expect(clippy::cast_possible_truncation)]
    pub(crate) fn create_builder(
        type_: &Type,
        data_type: &DataType,
        name: &str,
    ) -> Result<Box<dyn ArrayBuilder>> {
        Ok(match type_.strip_null() {
            // Numeric
            Type::Int8 => traceb(
                PrimitiveBuilder::<Int8Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "Int8",
                name,
            ),
            Type::Int16 => traceb(
                PrimitiveBuilder::<Int16Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "Int16",
                name,
            ),
            Type::Int32 => traceb(
                PrimitiveBuilder::<Int32Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "Int32",
                name,
            ),
            Type::Int64 => traceb(
                PrimitiveBuilder::<Int64Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "Int64",
                name,
            ),
            Type::UInt8 => traceb(
                PrimitiveBuilder::<UInt8Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "UInt8",
                name,
            ),
            Type::UInt16 => traceb(
                PrimitiveBuilder::<UInt16Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "UInt16",
                name,
            ),
            Type::UInt32 => traceb(
                PrimitiveBuilder::<UInt32Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "UInt32",
                name,
            ),
            Type::UInt64 => traceb(
                PrimitiveBuilder::<UInt64Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "UInt64",
                name,
            ),
            Type::Float32 => traceb(
                PrimitiveBuilder::<Float32Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "Float32",
                name,
            ),
            Type::Float64 => traceb(
                PrimitiveBuilder::<Float64Type>::with_capacity(BATCH_CHUNK_SIZE),
                type_,
                "Float64",
                name,
            ),
            // Decimal
            Type::Decimal32(s) => traceb(
                Decimal128Builder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_precision_and_scale(9, *s as i8)?,
                type_,
                "Decimal32",
                name,
            ),
            Type::Decimal64(s) => traceb(
                Decimal128Builder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_precision_and_scale(18, *s as i8)?,
                type_,
                "Decimal64",
                name,
            ),
            Type::Decimal128(s) => traceb(
                Decimal128Builder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_precision_and_scale(38, *s as i8)?,
                type_,
                "Decimal128",
                name,
            ),
            Type::Decimal256(s) => traceb(
                Decimal256Builder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_precision_and_scale(76, *s as i8)?,
                type_,
                "Decimal256",
                name,
            ),
            // Dates
            Type::Date => {
                traceb(Date32Builder::with_capacity(BATCH_CHUNK_SIZE), type_, "Date", name)
            }
            Type::Date32 => {
                traceb(Date32Builder::with_capacity(BATCH_CHUNK_SIZE), type_, "Date32", name)
            }
            // DateTimes are handled specially
            Type::DateTime(_) | Type::DateTime64(_, _) => {
                create_datetime_builder(type_, data_type, name)?
            }
            // String, Binary, UUID, IPv4, IPv6
            Type::String | Type::Object => traceb(
                StringBuilder::with_capacity(BATCH_CHUNK_SIZE, BATCH_CHUNK_SIZE * 1024),
                type_,
                "String",
                name,
            ),
            Type::FixedSizedString(n) => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, *n as i32),
                type_,
                "FixedSizedString",
                name,
            ),
            Type::Binary => traceb(
                BinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, BATCH_CHUNK_SIZE * 1024),
                type_,
                "Binary",
                name,
            ),
            Type::FixedSizedBinary(n) => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, *n as i32),
                type_,
                "FixedSizedBinary",
                name,
            ),
            Type::Uuid => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16),
                type_,
                "Uuid",
                name,
            ),
            Type::Ipv4 => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 4),
                type_,
                "Ipv4",
                name,
            ),
            Type::Ipv6 => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16),
                type_,
                "Ipv6",
                name,
            ),
            // Special numeric types that need to be read as bytes
            Type::Int128 => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16),
                type_,
                "Int128",
                name,
            ),
            Type::Int256 => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 32),
                type_,
                "Int256",
                name,
            ),
            Type::UInt128 => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16),
                type_,
                "UInt128",
                name,
            ),
            Type::UInt256 => traceb(
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 32),
                type_,
                "UInt256",
                name,
            ),
            // Enums
            Type::Enum8(_) => {
                traceb(StringDictionaryBuilder::<Int8Type>::new(), type_, "Enum8", name)
            }
            Type::Enum16(_) => {
                traceb(StringDictionaryBuilder::<Int16Type>::new(), type_, "Enum16", name)
            }
            // Nested types
            Type::Map(_, _) => {
                let map_builder = create_dyn_map_builder(type_, data_type, name)?;
                traceb(map_builder, type_, "Map", name)
            }
            Type::Array(_) => {
                if !matches!(
                    data_type,
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
                ) {
                    return Err(Error::ArrowDeserialize(format!(
                        "Unexpected array type for array: {data_type:?}"
                    )));
                }
                // TODO: Delegating to make_builder. May re-visit.
                traceb(make_builder(data_type, BATCH_CHUNK_SIZE), type_, "Array", name)
            }
            Type::Tuple(_) => {
                // TODO: Delegating fully to make_builder. May re-visit.
                traceb(make_builder(data_type, BATCH_CHUNK_SIZE), type_, "Tuple", name)
            }
            Type::LowCardinality(inner) => create_dyn_dict_builder(data_type, inner, name)?,
            // Geo types need to be normalized
            Type::Point | Type::Polygon | Type::Ring | Type::MultiPolygon => {
                let normalized = normalize_geo_type(type_).unwrap();
                return create_builder(&normalized, data_type, name);
            }
            // Unwrapped above
            Type::Nullable(_) => unreachable!(),
        })
    }

    /// Helper for aligning datetimes and timezones.
    ///
    /// `ClickHouse` defaults to UTC, but arrow `DataType` might be `None`. This needs to be
    /// resolved and handled with a bit more care.
    ///
    /// See: [Timestamp](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Timestamp)
    #[inline]
    fn create_datetime_builder(
        type_: &Type,
        data_type: &DataType,
        name: &str,
    ) -> Result<Box<dyn ArrayBuilder>> {
        let tz_some = matches!(data_type, DataType::Timestamp(_, tz) if tz.is_some());
        Ok(match type_.strip_null() {
            Type::DateTime(tz) => traceb(
                TimestampSecondBuilder::new()
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name()))),
                type_,
                "DateTime(tz)",
                name,
            ),
            Type::DateTime64(0, tz) => traceb(
                TimestampSecondBuilder::new()
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name()))),
                type_,
                "DateTime64(0)",
                name,
            ),
            Type::DateTime64(1..=3, tz) => traceb(
                TimestampMillisecondBuilder::new()
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name()))),
                type_,
                "DateTime64(3)",
                name,
            ),
            Type::DateTime64(4..=6, tz) => traceb(
                TimestampMicrosecondBuilder::new()
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name()))),
                type_,
                "DateTime(6)",
                name,
            ),
            Type::DateTime64(7..=9, tz) => traceb(
                TimestampNanosecondBuilder::new()
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name()))),
                type_,
                "DateTime(9)",
                name,
            ),
            // Malformed
            _ => return Err(Error::UnexpectedType(type_.clone())),
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_dynamic() {
        panic!("Test me!");
    }
}
