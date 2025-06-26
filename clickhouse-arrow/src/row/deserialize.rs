pub(super) mod binary;
pub(super) mod dictionary;
pub(super) mod enums;
pub(super) mod list;
pub(super) mod map;
pub(super) mod null;
pub(super) mod primitive;
pub(super) mod tuple;

// TODO: Remove
//
// use arrow::datatypes::*;

// use self::binary::binary;
// use self::primitive::primitive;
// use super::builder::TypedBuilder;
// use crate::geo::normalize_geo_type;
// use crate::io::ClickHouseBytesRead;
// use crate::row::dynamic::read_column;
// use crate::{Error, Result, Type};

// /// Append a value to this builder
// #[expect(clippy::too_many_lines)]
// pub(crate) fn append_value_to_builder<R: ClickHouseBytesRead>(
//     builder: &mut TypedBuilder,
//     reader: &mut R,
//     type_: &Type,
// ) -> Result<()> {
//     type B = TypedBuilder;
//     match builder {
//         B::Int8(b) => b.append_value(primitive!(Int8 => reader)),
//         B::Int16(b) => b.append_value(primitive!(Int16 => reader)),
//         B::Int32(b) => b.append_value(primitive!(Int32 => reader)),
//         B::Int64(b) => b.append_value(primitive!(Int64 => reader)),
//         B::UInt8(b) => b.append_value(primitive!(UInt8 => reader)),
//         B::UInt16(b) => b.append_value(primitive!(UInt16 => reader)),
//         B::UInt32(b) => b.append_value(primitive!(UInt32 => reader)),
//         B::UInt64(b) => b.append_value(primitive!(UInt64 => reader)),
//         B::Float32(b) => b.append_value(primitive!(Float32 => reader)),
//         B::Float64(b) => b.append_value(primitive!(Float64 => reader)),
//         B::Decimal32(b) => b.append_value(primitive!(Decimal32 => reader)),
//         B::Decimal64(b) => b.append_value(primitive!(Decimal64 => reader)),
//         B::Decimal128(b) => b.append_value(primitive!(Decimal128 => reader)),
//         B::Decimal256(b) => b.append_value(primitive!(Decimal256 => reader)),
//         B::Date(b) => b.append_value(primitive!(Date => reader)),
//         B::Date32(b) => b.append_value(primitive!(Date32 => reader)),
//         B::DateTime(b) => b.append_value(primitive!(DateTime => reader)),
//         B::DateTimeS(b) => b.append_value(primitive!(DateTime64(0) => reader)),
//         B::DateTimeMs(b) => b.append_value(primitive!(DateTime64(3) => reader)),
//         B::DateTimeMu(b) => b.append_value(primitive!(DateTime64(6) => reader)),
//         B::DateTimeNano(b) => b.append_value(primitive!(DateTime64(9) => reader)),
//         B::String(b) => b.append_value(binary!(String => reader)),
//         B::Object(b) => b.append_value(binary!(Object => reader)),
//         B::Binary(b) => b.append_value(binary!(Binary => reader)),
//         B::FixedSizeBinary(b) => {
//             match type_.strip_null() {
//                 Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => {
//                     b.append_value(binary!(FixedBinary(*n)=> reader))?;
//                 }
//                 Type::Ipv4 => b.append_value(binary!(Ipv4 => reader))?,
//                 Type::Ipv6 => b.append_value(binary!(Ipv6 => reader))?,
//                 Type::Uuid => b.append_value(binary!(Fixed(16)=> reader))?,
//                 // Special numeric types that need to be read as bytes
//                 Type::Int128 | Type::UInt128 => b.append_value(binary!(Fixed(16)=> reader))?,
//                 Type::Int256 | Type::UInt256 => {
//                     b.append_value(binary!(FixedRev(32)=> reader))?;
//                 }
//                 _ => return Err(Error::UnexpectedType(type_.clone())),
//             }
//         }
//         B::Enum8(b) => {
//             let Type::Enum8(enum_values) = type_ else {
//                 return Err(Error::UnexpectedType(type_.clone()));
//             };
//             let key = reader.try_get_i8()?;
//             let _ = b.append(
//                 enum_values
//                     .iter()
//                     .find(|(_, k)| key == *k)
//                     .map_or("<invalid_enum8_value>", |(name, _)| name.as_str()),
//             )?;
//         }
//         B::Enum16(b) => {
//             let Type::Enum16(enum_values) = type_ else {
//                 return Err(Error::UnexpectedType(type_.clone()));
//             };
//             let key = reader.try_get_i16_le()?;
//             let _ = b.append(
//                 enum_values
//                     .iter()
//                     .find(|(_, k)| key == *k)
//                     .map_or("<invalid_enum16_value>", |(name, _)| name.as_str()),
//             )?;
//         }
//         // Lists
//         B::List(b) => b.append_value(reader, type_)?,
//         // LowCardinality
//         B::LowCardinality(b) => b.append_value(reader, type_)?,
//         // Tuple
//         B::Tuple(b) => {
//             let Type::Tuple(inner_types) = type_ else {
//                 return Err(Error::ArrowDeserialize("Expected Tuple type".to_string()));
//             };
//             if inner_types.len() != b.num_fields() {
//                 return Err(Error::ArrowDeserialize(format!(
//                     "Mismatch in number of tuple fields: expected {}, got {}",
//                     inner_types.len(),
//                     b.num_fields()
//                 )));
//             }
//             // Deserialize each tuple element consecutively in order
//             let field_builders = b.field_builders_mut();
//             for (i, element_type) in inner_types.iter().enumerate() {
//                 read_column(reader, element_type, &mut *field_builders[i], false)?;
//             }
//             b.append(true);
//         }
//         B::Map(b) => {
//             let Type::Map(key_type, value_type) = type_.strip_null() else {
//                 return Err(Error::UnexpectedType(type_.clone()));
//             };
//             let size = reader.try_get_var_uint()?;
//             for _ in 0..size {
//                 read_column(reader, key_type, b.keys(), false)?;
//                 read_column(reader, value_type, b.values(), false)?;
//             }
//             b.append(true)?;
//             return Ok(());
//         }
//         // TOOD: Remove - figure this out
//         B::Geo(b) => {
//             // Geo types are aliases of nested structures, delegate to underlying types.
//             // This should serve as a fallback only, as the type should be already be converted.
//             if let Type::Point | Type::Ring | Type::Polygon | Type::MultiPolygon =
//                 type_.strip_null()
//             {
//                 return read_column(
//                     reader,
//                     &normalize_geo_type(type_.strip_null()).unwrap(),
//                     b,
//                     false,
//                 );
//             }
//             return Err(Error::UnexpectedType(type_.clone()));
//         }
//     }
//     Ok(())
// }

// mod column {
//     macro_rules! opt_value {
//         ($b:expr, $row:expr, $read:expr) => {{
//             if nulls.is_empty() || nulls[$row] == 0 {
//                 $b.append_value($read);
//             } else {
//                 let _value = $read;
//                 $b.append_null();
//             }
//         }};
//         (ok => $b:expr, $row:expr, $read:expr) => {{
//             if nulls.is_empty() || nulls[$row] == 0 {
//                 $b.append_value($read)?;
//             } else {
//                 let _value = $read;
//                 $b.append_null();
//             }
//         }};
//     }
//     // pub(crate) use opt_value;
// }
