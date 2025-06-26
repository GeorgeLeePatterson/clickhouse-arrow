pub(super) mod dictionary;
pub(super) mod list;
pub(super) mod map;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use tracing::trace;

use self::dictionary::LowCardinalityBuilder;
use self::list::row::TypedListBuilder;
use super::deserialize::binary::binary;
use super::deserialize::primitive::primitive;
use super::dynamic;
use super::dynamic::read_column;
use super::read::BATCH_CHUNK_SIZE;
use crate::flags::debug_arrow;
use crate::geo::normalize_geo_type;
use crate::io::ClickHouseBytesRead;
use crate::{Error, Result, Type};

// Typed builder map
pub(crate) type TypedBuilderMap<'a> = Vec<(&'a str, (&'a Type, TypedBuilder))>;

pub(crate) fn create_typed_builder_map<'a>(
    definitions: &'a [(String, Type, Option<String>)],
    schema: &SchemaRef,
) -> Result<TypedBuilderMap<'a>> {
    let mut builders = Vec::with_capacity(definitions.len());
    for (name, type_, _) in definitions {
        let field = schema.field_with_name(name)?;
        let builder = TypedBuilder::try_new(type_, field.data_type(), name)?;
        builders.push((name.as_str(), (type_, builder)));
    }

    Ok(builders)
}

macro_rules! typed_arrow_build {
    ($typed:ident, $data_type:expr, { $(
        $typ:pat => ($var:ident, $builder:expr $(,)?)
    ),+ $(,)? }) => {
        match $data_type {
            $(
                $typ => $typed::$var($builder),
            )*
            _ => return Err(Error::ArrowDeserialize(format!("Unexpected type: {}", $data_type))),
        }
    }
}

pub(super) use typed_arrow_build;

#[inline]
pub(super) fn traceb<A: ArrayBuilder>(
    b: A,
    type_: &Type,
    builder: &str,
    name: &str,
) -> Box<dyn ArrayBuilder> {
    if debug_arrow() {
        trace!(?type_, builder, "creating builder for {name}");
    }
    Box::new(b)
}

macro_rules! typed_build {
    ($type_hint:expr, { $(
        $typ:pat => ($var:ident, $builder:expr $(,)?)
    ),+ $(,)? }) => {
        match $type_hint {
            $(
                $typ => TypedBuilder::$var($builder),
            )*
            // Malformed
            Type::DateTime64(10.., _) => return Err(Error::ArrowDeserialize(
                "Invalid DateTime64".into()
            )),
            _ => return Err(Error::UnexpectedType($type_hint.clone())),
        }
    }
}

/// Pre-typed builders eliminating dynamic dispatch
pub(crate) enum TypedBuilder {
    // Primitive numeric types
    Int8(PrimitiveBuilder<Int8Type>),
    Int16(PrimitiveBuilder<Int16Type>),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    UInt8(PrimitiveBuilder<UInt8Type>),
    UInt16(PrimitiveBuilder<UInt16Type>),
    UInt32(PrimitiveBuilder<UInt32Type>),
    UInt64(PrimitiveBuilder<UInt64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),

    // Decimal types (all use Decimal128Builder or Decimal256Builder)
    Decimal32(Decimal128Builder),
    Decimal64(Decimal128Builder),
    Decimal128(Decimal128Builder),
    Decimal256(Decimal256Builder),

    // Date/Time types
    Date(Date32Builder),
    Date32(Date32Builder),
    DateTime(TimestampSecondBuilder),
    DateTimeS(TimestampSecondBuilder),
    DateTimeMs(TimestampMillisecondBuilder),
    DateTimeMu(TimestampMicrosecondBuilder),
    DateTimeNano(TimestampNanosecondBuilder),

    // String and Binary types
    String(StringBuilder),
    Object(StringBuilder),
    Binary(BinaryBuilder),
    FixedSizeBinary(FixedSizeBinaryBuilder),

    // Dictionary types for enums
    Enum8(StringDictionaryBuilder<Int8Type>),
    Enum16(StringDictionaryBuilder<Int16Type>),

    // List types
    List(TypedListBuilder),

    // LowCardinality types
    // TODO: Support more key types without erasing type
    LowCardinality(LowCardinalityBuilder<Int32Type>),

    // Complex types
    Map(MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>),
    Tuple(StructBuilder),
    Geo(Box<dyn ArrayBuilder>),
}

impl TypedBuilder {
    #[expect(clippy::too_many_lines)]
    #[expect(clippy::cast_possible_wrap)]
    #[expect(clippy::cast_possible_truncation)]
    fn try_new(type_: &Type, data_type: &DataType, name: &str) -> Result<Self> {
        let tz_some = matches!(data_type, DataType::Timestamp(_, tz) if tz.is_some());

        // Handle complex nested types
        if matches!(type_, Type::Array(_)) {
            return Ok(Self::List(TypedListBuilder::try_new(type_, data_type, name)?));
        }

        if let Type::LowCardinality(inner) = type_ {
            return Ok(Self::LowCardinality(LowCardinalityBuilder::<Int32Type>::try_new(
                inner, data_type,
            )?));
        }

        if let Type::Tuple(inner) = type_ {
            let DataType::Struct(fields) = data_type else {
                return Err(Error::ArrowDeserialize(format!(
                    "Unexpected datatype for tuple: {data_type:?}",
                )));
            };
            if inner.len() != fields.len() {
                return Err(Error::ArrowDeserialize(format!(
                    "Tuple length mismatch: {inner:?} != {fields:?}",
                )));
            }
            return Ok(Self::Tuple(StructBuilder::from_fields(fields.clone(), BATCH_CHUNK_SIZE)));
        }

        if matches!(type_, Type::Map(_, _)) {
            return Ok(Self::Map(map::create_dyn_map_builder(type_, data_type, name)?));
        }

        // Rest of the types
        Ok(typed_build!(type_.strip_null(), {
            // Nested types
            Type::Point
            | Type::Polygon
            | Type::MultiPolygon
            | Type::Ring => (Geo, dynamic::builder::create_builder(type_, data_type, name)?),
            // LowCardinality
            // Numeric
            Type::Int8 => (Int8, PrimitiveBuilder::<Int8Type>::with_capacity(BATCH_CHUNK_SIZE)),
            Type::Int16 => (Int16, PrimitiveBuilder::<Int16Type>::with_capacity(BATCH_CHUNK_SIZE)),
            Type::Int32 => ( Int32, PrimitiveBuilder::<Int32Type>::with_capacity(BATCH_CHUNK_SIZE) ),
            Type::Int64 => ( Int64, PrimitiveBuilder::<Int64Type>::with_capacity(BATCH_CHUNK_SIZE) ),
            Type::UInt8 => ( UInt8, PrimitiveBuilder::<UInt8Type>::with_capacity(BATCH_CHUNK_SIZE)),
            Type::UInt16 => ( UInt16, PrimitiveBuilder::<UInt16Type>::with_capacity(BATCH_CHUNK_SIZE)),
            Type::UInt32 => ( UInt32, PrimitiveBuilder::<UInt32Type>::with_capacity(BATCH_CHUNK_SIZE)),
            Type::UInt64 => ( UInt64, PrimitiveBuilder::<UInt64Type>::with_capacity(BATCH_CHUNK_SIZE)),
            Type::Float32 => ( Float32, PrimitiveBuilder::<Float32Type>::with_capacity(BATCH_CHUNK_SIZE)),
            Type::Float64 => ( Float64, PrimitiveBuilder::<Float64Type>::with_capacity(BATCH_CHUNK_SIZE)),
            // Decimal
            Type::Decimal32(s) => (
                Decimal32,
                Decimal128Builder::with_capacity(BATCH_CHUNK_SIZE).with_precision_and_scale(9, *s as i8)?
            ),
            Type::Decimal64(s) => (
                Decimal64,
                Decimal128Builder::with_capacity(BATCH_CHUNK_SIZE).with_precision_and_scale(18, *s as i8)?
            ),
            Type::Decimal128(s) => (
                Decimal128,
                Decimal128Builder::with_capacity(BATCH_CHUNK_SIZE).with_precision_and_scale(38, *s as i8)?
            ),
            Type::Decimal256(s) => (
                Decimal256,
                Decimal256Builder::new().with_precision_and_scale(76, *s as i8)?
            ),
            // Dates
            Type::Date => (Date, Date32Builder::with_capacity(BATCH_CHUNK_SIZE)),
            Type::Date32 => (Date32, Date32Builder::with_capacity(BATCH_CHUNK_SIZE)),
            Type::DateTime(tz) => (
                DateTime,
                TimestampSecondBuilder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(0, tz) => (
                DateTimeS,
                TimestampSecondBuilder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(1..=3, tz) => (
                DateTimeMs,
                TimestampMillisecondBuilder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(4..=6, tz) => (
                DateTimeMu,
                TimestampMicrosecondBuilder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(7..=9, tz) => (
                DateTimeNano,
                TimestampNanosecondBuilder::with_capacity(BATCH_CHUNK_SIZE)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            // String, Binary, UUID, IPv4, IPv6
            Type::String => (
                String, StringBuilder::with_capacity(BATCH_CHUNK_SIZE, BATCH_CHUNK_SIZE * 1024)
            ),
            Type::Object => (
                Object, StringBuilder::with_capacity(BATCH_CHUNK_SIZE, BATCH_CHUNK_SIZE * 1024)
            ),
            Type::FixedSizedString(n) => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, *n as i32)
            ),
            Type::Binary => (
                Binary, BinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, BATCH_CHUNK_SIZE * 1024)
            ),
            Type::FixedSizedBinary(n) => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, *n as i32)
            ),
            Type::Ipv4 => (
                FixedSizeBinary, FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 4)
            ),
            Type::Ipv6 => (
                FixedSizeBinary, FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16)
            ),
            Type::Uuid => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16)
            ),
            // Special numeric types that need to be read as bytes
            Type::Int128 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16)
            ),
            Type::Int256 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 32)
            ),
            Type::UInt128 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 16)
            ),
            Type::UInt256 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(BATCH_CHUNK_SIZE, 32)
            ),
            // Enums
            Type::Enum8(_) => (Enum8, StringDictionaryBuilder::<Int8Type>::new()),
            Type::Enum16(_) => ( Enum16, StringDictionaryBuilder::<Int16Type>::new() ),
        }))
    }

    /// Append a null value to this builder
    pub(super) fn append_null<R: ClickHouseBytesRead>(
        &mut self,
        reader: &mut R,
        type_: &Type,
    ) -> Result<bool> {
        if !type_.is_nullable() {
            return Ok(false);
        }
        let is_null = reader
            .try_get_u8()
            .map_err(|e| Error::Protocol(format!("Failed to read null flag: {e}")))?
            != 0;
        if is_null {
            match self {
                TypedBuilder::Int8(b) => b.append_null(),
                TypedBuilder::Int16(b) => b.append_null(),
                TypedBuilder::Int32(b) => b.append_null(),
                TypedBuilder::Int64(b) => b.append_null(),
                TypedBuilder::UInt8(b) => b.append_null(),
                TypedBuilder::UInt16(b) => b.append_null(),
                TypedBuilder::UInt32(b) => b.append_null(),
                TypedBuilder::UInt64(b) => b.append_null(),
                TypedBuilder::Float32(b) => b.append_null(),
                TypedBuilder::Float64(b) => b.append_null(),
                TypedBuilder::Decimal32(b)
                | TypedBuilder::Decimal128(b)
                | TypedBuilder::Decimal64(b) => b.append_null(),
                TypedBuilder::Decimal256(b) => b.append_null(),
                TypedBuilder::Date(b) | TypedBuilder::Date32(b) => b.append_null(),
                TypedBuilder::DateTime(b) | TypedBuilder::DateTimeS(b) => b.append_null(),
                TypedBuilder::DateTimeMs(b) => b.append_null(),
                TypedBuilder::DateTimeMu(b) => b.append_null(),
                TypedBuilder::DateTimeNano(b) => b.append_null(),
                TypedBuilder::String(b) | TypedBuilder::Object(b) => b.append_null(),
                TypedBuilder::Binary(b) => b.append_null(),
                TypedBuilder::FixedSizeBinary(b) => b.append_null(),
                TypedBuilder::Enum8(b) => b.append_null(),
                TypedBuilder::Enum16(b) => b.append_null(),
                // Lists
                TypedBuilder::List(b) => return b.append_null(reader, type_),
                // LowCardinality
                TypedBuilder::LowCardinality(b) => return b.append_null(reader, type_),
                // Tuple
                TypedBuilder::Tuple(b) => b.append(false),
                // Map
                TypedBuilder::Map(b) => b.append(false)?,
                // TOOD: Remove - figure this out
                TypedBuilder::Geo(b) => read_column(reader, type_, b, true)?,
            }
        }
        Ok(is_null)
    }

    /// Append a null value to this builder
    #[expect(clippy::too_many_lines)]
    pub(super) fn append_value<R: ClickHouseBytesRead>(
        &mut self,
        reader: &mut R,
        type_: &Type,
    ) -> Result<()> {
        type B = TypedBuilder;
        match self {
            B::Int8(b) => b.append_value(primitive!(Int8 => reader)),
            B::Int16(b) => b.append_value(primitive!(Int16 => reader)),
            B::Int32(b) => b.append_value(primitive!(Int32 => reader)),
            B::Int64(b) => b.append_value(primitive!(Int64 => reader)),
            B::UInt8(b) => b.append_value(primitive!(UInt8 => reader)),
            B::UInt16(b) => b.append_value(primitive!(UInt16 => reader)),
            B::UInt32(b) => b.append_value(primitive!(UInt32 => reader)),
            B::UInt64(b) => b.append_value(primitive!(UInt64 => reader)),
            B::Float32(b) => b.append_value(primitive!(Float32 => reader)),
            B::Float64(b) => b.append_value(primitive!(Float64 => reader)),
            B::Decimal32(b) => b.append_value(primitive!(Decimal32 => reader)),
            B::Decimal64(b) => b.append_value(primitive!(Decimal64 => reader)),
            B::Decimal128(b) => b.append_value(primitive!(Decimal128 => reader)),
            B::Decimal256(b) => b.append_value(primitive!(Decimal256 => reader)),
            B::Date(b) => b.append_value(primitive!(Date => reader)),
            B::Date32(b) => b.append_value(primitive!(Date32 => reader)),
            B::DateTime(b) => b.append_value(primitive!(DateTime => reader)),
            B::DateTimeS(b) => b.append_value(primitive!(DateTime64(0) => reader)),
            B::DateTimeMs(b) => b.append_value(primitive!(DateTime64(3) => reader)),
            B::DateTimeMu(b) => b.append_value(primitive!(DateTime64(6) => reader)),
            B::DateTimeNano(b) => b.append_value(primitive!(DateTime64(9) => reader)),
            B::String(b) => b.append_value(binary!(String => reader)),
            B::Object(b) => b.append_value(binary!(Object => reader)),
            B::Binary(b) => b.append_value(binary!(Binary => reader)),
            B::FixedSizeBinary(b) => {
                match type_.strip_null() {
                    Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => {
                        b.append_value(binary!(FixedBinary(*n)=> reader))?;
                    }
                    Type::Ipv4 => b.append_value(binary!(Ipv4 => reader))?,
                    Type::Ipv6 => b.append_value(binary!(Ipv6 => reader))?,
                    Type::Uuid => b.append_value(binary!(Fixed(16)=> reader))?,
                    // Special numeric types that need to be read as bytes
                    Type::Int128 | Type::UInt128 => b.append_value(binary!(Fixed(16)=> reader))?,
                    Type::Int256 | Type::UInt256 => {
                        b.append_value(binary!(FixedRev(32)=> reader))?;
                    }
                    _ => return Err(Error::UnexpectedType(type_.clone())),
                }
            }
            B::Enum8(b) => {
                let Type::Enum8(enum_values) = type_ else {
                    return Err(Error::UnexpectedType(type_.clone()));
                };
                let key = reader.try_get_i8()?;
                let _ = b.append(
                    enum_values
                        .iter()
                        .find(|(_, k)| key == *k)
                        .map_or("<invalid_enum8_value>", |(name, _)| name.as_str()),
                )?;
            }
            B::Enum16(b) => {
                let Type::Enum16(enum_values) = type_ else {
                    return Err(Error::UnexpectedType(type_.clone()));
                };
                let key = reader.try_get_i16_le()?;
                let _ = b.append(
                    enum_values
                        .iter()
                        .find(|(_, k)| key == *k)
                        .map_or("<invalid_enum16_value>", |(name, _)| name.as_str()),
                )?;
            }
            // Lists
            B::List(b) => b.append_value(reader, type_)?,
            // LowCardinality
            B::LowCardinality(b) => b.append_value(reader, type_)?,
            // Tuple
            B::Tuple(b) => {
                let Type::Tuple(inner_types) = type_ else {
                    return Err(Error::ArrowDeserialize("Expected Tuple type".to_string()));
                };
                if inner_types.len() != b.num_fields() {
                    return Err(Error::ArrowDeserialize(format!(
                        "Mismatch in number of tuple fields: expected {}, got {}",
                        inner_types.len(),
                        b.num_fields()
                    )));
                }
                // Deserialize each tuple element consecutively in order
                let field_builders = b.field_builders_mut();
                for (i, element_type) in inner_types.iter().enumerate() {
                    read_column(reader, element_type, &mut *field_builders[i], false)?;
                }
                b.append(true);
            }
            B::Map(b) => {
                let Type::Map(key_type, value_type) = type_.strip_null() else {
                    return Err(Error::UnexpectedType(type_.clone()));
                };
                let size = reader.try_get_var_uint()?;
                for _ in 0..size {
                    read_column(reader, key_type, b.keys(), false)?;
                    read_column(reader, value_type, b.values(), false)?;
                }
                b.append(true)?;
                return Ok(());
            }
            // TOOD: Remove - figure this out
            B::Geo(b) => {
                // Geo types are aliases of nested structures, delegate to underlying types.
                // This should serve as a fallback only, as the type should be already be converted.
                if let Type::Point | Type::Ring | Type::Polygon | Type::MultiPolygon =
                    type_.strip_null()
                {
                    return read_column(
                        reader,
                        &normalize_geo_type(type_.strip_null()).unwrap(),
                        b,
                        false,
                    );
                }
                return Err(Error::UnexpectedType(type_.clone()));
            }
        }
        Ok(())
    }

    // TODO: Remove
    // /// Append a null value to this builder
    // #[expect(clippy::too_many_lines)]
    // pub(crate) fn create_columns_array<R: ClickHouseBytesRead>(
    //     &mut self,
    //     type_: &Type,
    //     reader: &mut R,
    //     nulls: &[u8],
    //     rows: usize,
    // ) -> Result<ArrayRef> {
    //     type B = TypedBuilder;

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

    //     // Handle special types first
    //     // match self {
    //     //     B::Enum8(b) => {
    //     //         let Type::Enum8(enum_values) = type_ else {
    //     //             return Err(Error::UnexpectedType(type_.clone()));
    //     //         };
    //     //         let key = reader.try_get_i8()?;
    //     //         let _ = b.append(
    //     //             enum_values
    //     //                 .iter()
    //     //                 .find(|(_, k)| key == *k)
    //     //                 .map_or("<invalid_enum8_value>", |(name, _)| name.as_str()),
    //     //         )?;
    //     //     }
    //     //     B::Enum16(b) => {
    //     //         let Type::Enum16(enum_values) = type_ else {
    //     //             return Err(Error::UnexpectedType(type_.clone()));
    //     //         };
    //     //         let key = reader.try_get_i16_le()?;
    //     //         let _ = b.append(
    //     //             enum_values
    //     //                 .iter()
    //     //                 .find(|(_, k)| key == *k)
    //     //                 .map_or("<invalid_enum16_value>", |(name, _)| name.as_str()),
    //     //         )?;
    //     //     }
    //     //     // Lists
    //     //     B::List(b) => list(reader, type_, b)?,
    //     //     B::LargeList(b) => list_large(reader, type_, b)?,
    //     //     B::FixedList(b) => list_fixed(reader, type_, b)?,
    //     //     // LowCardinality
    //     //     B::LowCardinality(b) => b.append_value(reader, type_)?,
    //     //     // Tuple
    //     //     B::Tuple(b) => {
    //     //         let Type::Tuple(inner_types) = type_ else {
    //     //             return Err(Error::ArrowDeserialize("Expected Tuple type".to_string()));
    //     //         };
    //     //         if inner_types.len() != b.num_fields() {
    //     //             return Err(Error::ArrowDeserialize(format!(
    //     //                 "Mismatch in number of tuple fields: expected {}, got {}",
    //     //                 inner_types.len(),
    //     //                 b.num_fields()
    //     //             )));
    //     //         }
    //     //         // Deserialize each tuple element consecutively in order
    //     //         let field_builders = b.field_builders_mut();
    //     //         for (i, element_type) in inner_types.iter().enumerate() {
    //     //             read_column(reader, element_type, &mut *field_builders[i], false)?;
    //     //         }
    //     //         b.append(true);
    //     //     }
    //     //     B::Map(b) => {
    //     //         let Type::Map(key_type, value_type) = type_.strip_null() else {
    //     //             return Err(Error::UnexpectedType(type_.clone()));
    //     //         };
    //     //         let size = reader.try_get_var_uint()?;
    //     //         for _ in 0..size {
    //     //             read_column(reader, key_type, b.keys(), false)?;
    //     //             read_column(reader, value_type, b.values(), false)?;
    //     //         }
    //     //         b.append(true)?;
    //     //         return Ok(());
    //     //     }
    //     //     // TOOD: Remove - figure this out
    //     //     B::Geo(b) => {
    //     //         // Geo types are aliases of nested structures, delegate to underlying types.
    //     //         // This should serve as a fallback only, as the type should be already be
    //     //         // converted.
    //     //         if let Type::Point | Type::Ring | Type::Polygon | Type::MultiPolygon =
    //     //             type_.strip_null()
    //     //         {
    //     //             return read_column(
    //     //                 reader,
    //     //                 &normalize_geo_type(type_.strip_null()).unwrap(),
    //     //                 b,
    //     //                 false,
    //     //             );
    //     //         }
    //     //         return Err(Error::UnexpectedType(type_.clone()));
    //     //     }
    //     // }

    //     for i in 0..rows {
    //         match self {
    //             B::Int8(b) => opt_value!(b, i, primitive!(Int8 => reader)),
    //             B::Int16(b) => opt_value!(b, i, primitive!(Int16 => reader)),
    //             B::Int32(b) => opt_value!(b, i, primitive!(Int32 => reader)),
    //             B::Int64(b) => opt_value!(b, i, primitive!(Int64 => reader)),
    //             B::UInt8(b) => opt_value!(b, i, primitive!(UInt8 => reader)),
    //             B::UInt16(b) => opt_value!(b, i, primitive!(UInt16 => reader)),
    //             B::UInt32(b) => opt_value!(b, i, primitive!(UInt32 => reader)),
    //             B::UInt64(b) => opt_value!(b, i, primitive!(UInt64 => reader)),
    //             B::Float32(b) => opt_value!(b, i, primitive!(Float32 => reader)),
    //             B::Float64(b) => opt_value!(b, i, primitive!(Float64 => reader)),
    //             B::Decimal32(b) => opt_value!(b, i, primitive!(Decimal32 => reader)),
    //             B::Decimal64(b) => opt_value!(b, i, primitive!(Decimal64 => reader)),
    //             B::Decimal128(b) => opt_value!(b, i, primitive!(Decimal128 => reader)),
    //             B::Decimal256(b) => opt_value!(b, i, primitive!(Decimal256 => reader)),
    //             B::Date(b) => opt_value!(b, i, primitive!(Date => reader)),
    //             B::Date32(b) => opt_value!(b, i, primitive!(Date32 => reader)),
    //             B::DateTime(b) => opt_value!(b, i, primitive!(DateTime => reader)),
    //             B::DateTimeS(b) => opt_value!(b, i, primitive!(DateTime64(0) => reader)),
    //             B::DateTimeMs(b) => {
    //                 opt_value!(b, i, primitive!(DateTime64(3) => reader));
    //             }
    //             B::DateTimeMu(b) => {
    //                 opt_value!(b, i, primitive!(DateTime64(6) => reader));
    //             }
    //             B::DateTimeNano(b) => {
    //                 opt_value!(b, i, primitive!(DateTime64(9) => reader));
    //             }
    //             B::String(b) => opt_value!(b, i, binary!(String => reader)),
    //             B::Object(b) => opt_value!(b, i, binary!(Object => reader)),
    //             B::Binary(b) => opt_value!(b, i, binary!(Binary => reader)),
    //             B::FixedSizeBinary(b) => {
    //                 match type_.strip_null() {
    //                     Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => {
    //                         opt_value!(ok => b, i, binary!(FixedBinary(*n)=> reader));
    //                     }
    //                     Type::Ipv4 => opt_value!(ok => b, i, binary!(Ipv4 => reader)),
    //                     Type::Ipv6 => opt_value!(ok => b, i, binary!(Ipv6 => reader)),
    //                     Type::Uuid => opt_value!(ok => b, i, binary!(Fixed(16)=> reader)),
    //                     // Special numeric types that need to be read as bytes
    //                     Type::Int128 | Type::UInt128 => {
    //                         opt_value!(ok => b, i, binary!(Fixed(16)=> reader));
    //                     }
    //                     Type::Int256 | Type::UInt256 => {
    //                         opt_value!(ok => b, i, binary!(FixedRev(32)=> reader));
    //                     }
    //                     _ => return Err(Error::UnexpectedType(type_.clone())),
    //                 }
    //             }
    //             B::Enum8(b) => {
    //                 let Type::Enum8(enum_values) = type_ else {
    //                     return Err(Error::UnexpectedType(type_.clone()));
    //                 };
    //                 let key = reader.try_get_i8()?;
    //                 let _ = b.append(
    //                     enum_values
    //                         .iter()
    //                         .find(|(_, k)| key == *k)
    //                         .map_or("<invalid_enum8_value>", |(name, _)| name.as_str()),
    //                 )?;
    //             }
    //             B::Enum16(b) => {
    //                 let Type::Enum16(enum_values) = type_ else {
    //                     return Err(Error::UnexpectedType(type_.clone()));
    //                 };
    //                 let key = reader.try_get_i16_le()?;
    //                 let _ = b.append(
    //                     enum_values
    //                         .iter()
    //                         .find(|(_, k)| key == *k)
    //                         .map_or("<invalid_enum16_value>", |(name, _)| name.as_str()),
    //                 )?;
    //             }
    //             // Lists
    //             B::List(b) => deserialize::list::row::list(reader, type_, b)?,
    //             B::LargeList(b) => deserialize::list::row::list_large(reader, type_, b)?,
    //             B::FixedList(b) => deserialize::list::row::list_fixed(reader, type_, b)?,
    //             // LowCardinality
    //             B::LowCardinality(b) => b.append_value(reader, type_)?,
    //             // Tuple
    //             B::Tuple(b) => {
    //                 let Type::Tuple(inner_types) = type_ else {
    //                     return Err(Error::ArrowDeserialize("Expected Tuple type".to_string()));
    //                 };
    //                 if inner_types.len() != b.num_fields() {
    //                     return Err(Error::ArrowDeserialize(format!(
    //                         "Mismatch in number of tuple fields: expected {}, got {}",
    //                         inner_types.len(),
    //                         b.num_fields()
    //                     )));
    //                 }
    //                 // Deserialize each tuple element consecutively in order
    //                 let field_builders = b.field_builders_mut();
    //                 for (i, element_type) in inner_types.iter().enumerate() {
    //                     read_column(reader, element_type, &mut *field_builders[i], false)?;
    //                 }
    //                 b.append(true);
    //             }
    //             B::Map(b) => {
    //                 let Type::Map(key_type, value_type) = type_.strip_null() else {
    //                     return Err(Error::UnexpectedType(type_.clone()));
    //                 };
    //                 let size = reader.try_get_var_uint()?;
    //                 for _ in 0..size {
    //                     read_column(reader, key_type, b.keys(), false)?;
    //                     read_column(reader, value_type, b.values(), false)?;
    //                 }
    //                 b.append(true)?;
    //             }
    //             // TOOD: Remove - figure this out
    //             B::Geo(_) => {
    //                 // TODO: Remove - !!!
    //                 // // Geo types are aliases of nested structures, delegate to underlying
    // types.                 // // This should serve as a fallback only, as the type should be
    // already be                 // // converted.
    //                 // if let Type::Point | Type::Ring | Type::Polygon | Type::MultiPolygon =
    //                 //     type_.strip_null()
    //                 // {
    //                 //     return read_column(
    //                 //         reader,
    //                 //         &normalize_geo_type(type_.strip_null()).unwrap(),
    //                 //         b,
    //                 //         false,
    //                 //     );
    //                 // }
    //                 return Err(Error::UnexpectedType(type_.clone()));
    //             }
    //         }
    //     }
    //     // TODO: Remove - !!!!
    //     Err(Error::UnexpectedType(type_.clone()))
    // }

    /// Finish building and return the array
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            // Primitive
            TypedBuilder::Int8(b) => Arc::new(b.finish()),
            TypedBuilder::Int16(b) => Arc::new(b.finish()),
            TypedBuilder::Int32(b) => Arc::new(b.finish()),
            TypedBuilder::Int64(b) => Arc::new(b.finish()),
            TypedBuilder::UInt8(b) => Arc::new(b.finish()),
            TypedBuilder::UInt16(b) => Arc::new(b.finish()),
            TypedBuilder::UInt32(b) => Arc::new(b.finish()),
            TypedBuilder::UInt64(b) => Arc::new(b.finish()),
            TypedBuilder::Float32(b) => Arc::new(b.finish()),
            TypedBuilder::Float64(b) => Arc::new(b.finish()),
            TypedBuilder::Decimal32(b)
            | TypedBuilder::Decimal64(b)
            | TypedBuilder::Decimal128(b) => Arc::new(b.finish()),
            TypedBuilder::Decimal256(b) => Arc::new(b.finish()),
            TypedBuilder::Date(b) | TypedBuilder::Date32(b) => Arc::new(b.finish()),
            TypedBuilder::DateTime(b) | TypedBuilder::DateTimeS(b) => Arc::new(b.finish()),
            TypedBuilder::DateTimeMs(b) => Arc::new(b.finish()),
            TypedBuilder::DateTimeMu(b) => Arc::new(b.finish()),
            TypedBuilder::DateTimeNano(b) => Arc::new(b.finish()),
            // Strings/Binary
            TypedBuilder::String(b) | TypedBuilder::Object(b) => Arc::new(b.finish()),
            TypedBuilder::Binary(b) => Arc::new(b.finish()),
            TypedBuilder::FixedSizeBinary(b) => Arc::new(b.finish()),
            // Lists
            TypedBuilder::List(b) => b.finish(),
            // LowCardinality
            TypedBuilder::LowCardinality(b) => b.finish(),
            // Tuple
            TypedBuilder::Tuple(b) => Arc::new(b.finish()),
            // Map
            TypedBuilder::Map(b) => Arc::new(b.finish()),
            // Enums
            TypedBuilder::Enum8(b) => Arc::new(b.finish()),
            TypedBuilder::Enum16(b) => Arc::new(b.finish()),
            TypedBuilder::Geo(b) => Arc::new(b.finish()),
        }
    }
}

// TODO: Remove
// pub(crate) mod column {
//     //! Column builders for nested types
//     use super::super::deserialize::list::column::TypedColumnListBuilder;
//     use super::TypedBuilder;

//     pub(crate) enum TypedColumnBuilder {
//         Basic(TypedBuilder),
//         List(TypedColumnListBuilder),
//     }
// }

#[cfg(test)]
mod tests {
    #[test]
    fn test_typed_builder() {
        panic!("Test me!");
    }
}
