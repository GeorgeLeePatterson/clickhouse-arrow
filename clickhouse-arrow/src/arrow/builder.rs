pub(crate) mod dictionary;
pub(crate) mod list;
pub(crate) mod map;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use strum::AsRefStr;

use crate::constants::CLICKHOUSE_DEFAULT_CHUNK_ROWS;
use crate::flags::debug_arrow;
use crate::prelude::*;

// Typed builder map
pub(crate) type TypedBuilderMap<'a> = Vec<(&'a str, (&'a Type, TypedBuilder))>;

#[expect(unused)]
pub(crate) fn create_typed_builder_map(
    definitions: &[(String, Type)],
    get_field: impl Fn(&str) -> Result<&Field>,
) -> Result<TypedBuilderMap<'_>> {
    let mut builders = Vec::with_capacity(definitions.len());
    for (name, type_) in definitions {
        let field = get_field(name)?;
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

use self::dictionary::LowCardinalityBuilder;
use self::list::TypedListBuilder;

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
            // Nested types
            Type::Point
            | Type::Polygon
            | Type::MultiPolygon
            | Type::Ring => {
                // NOTE: This branch should not be hit.
                // Geo types need to be normalized before creating the builder.
                unimplemented!()
            }
            _ => return Err(Error::UnexpectedType($type_hint.clone())),
        }
    }
}

/// Pre-typed builders eliminating dynamic dispatch
#[derive(AsRefStr)]
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
    LowCardinality(LowCardinalityBuilder),

    // Complex types
    Map((Box<TypedBuilder>, Box<TypedBuilder>)),
    Tuple(Vec<TypedBuilder>),
}

impl TypedBuilder {
    #[expect(clippy::too_many_lines)]
    #[expect(clippy::cast_possible_wrap)]
    #[expect(clippy::cast_possible_truncation)]
    pub(crate) fn try_new(type_: &Type, data_type: &DataType, name: &str) -> Result<Self> {
        const ROWS: usize = CLICKHOUSE_DEFAULT_CHUNK_ROWS;

        let tz_some = matches!(data_type, DataType::Timestamp(_, tz) if tz.is_some());

        // Nullability isn't important when creating a builder
        let type_ = type_.strip_null();

        // Handle complex nested types
        if let Type::Array(inner) = type_ {
            return Ok(Self::List(TypedListBuilder::try_new(inner, data_type, name)?));
        }

        if let Type::LowCardinality(inner) = type_ {
            return Ok(Self::LowCardinality(LowCardinalityBuilder::try_new(inner, data_type)?));
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
            return Ok(Self::Tuple(
                inner
                    .iter()
                    .zip(fields.iter())
                    .map(|(t, f)| TypedBuilder::try_new(t, f.data_type(), "tuple"))
                    .collect::<Result<Vec<_>, _>>()?,
            ));
        }

        if let Type::Map(key, value) = type_ {
            let (kfield, vfield) = map::get_map_fields(data_type)?;
            let kbuilder = Box::new(TypedBuilder::try_new(key, kfield.data_type(), "map_key")?);
            let vbuilder = Box::new(TypedBuilder::try_new(value, vfield.data_type(), "map_value")?);
            return Ok(Self::Map((kbuilder, vbuilder)));
        }

        // Rest of the types
        Ok(typed_build!(type_, {
            // Numeric
            Type::Int8 => (Int8, PrimitiveBuilder::<Int8Type>::with_capacity(ROWS)),
            Type::Int16 => (Int16, PrimitiveBuilder::<Int16Type>::with_capacity(ROWS)),
            Type::Int32 => ( Int32, PrimitiveBuilder::<Int32Type>::with_capacity(ROWS) ),
            Type::Int64 => ( Int64, PrimitiveBuilder::<Int64Type>::with_capacity(ROWS) ),
            Type::UInt8 => ( UInt8, PrimitiveBuilder::<UInt8Type>::with_capacity(ROWS)),
            Type::UInt16 => ( UInt16, PrimitiveBuilder::<UInt16Type>::with_capacity(ROWS)),
            Type::UInt32 => ( UInt32, PrimitiveBuilder::<UInt32Type>::with_capacity(ROWS)),
            Type::UInt64 => ( UInt64, PrimitiveBuilder::<UInt64Type>::with_capacity(ROWS)),
            Type::Float32 => ( Float32, PrimitiveBuilder::<Float32Type>::with_capacity(ROWS)),
            Type::Float64 => ( Float64, PrimitiveBuilder::<Float64Type>::with_capacity(ROWS)),
            // Decimal
            Type::Decimal32(s) => (
                Decimal32,
                Decimal128Builder::with_capacity(ROWS).with_precision_and_scale(9, *s as i8)?
            ),
            Type::Decimal64(s) => (
                Decimal64,
                Decimal128Builder::with_capacity(ROWS).with_precision_and_scale(18, *s as i8)?
            ),
            Type::Decimal128(s) => (
                Decimal128,
                Decimal128Builder::with_capacity(ROWS).with_precision_and_scale(38, *s as i8)?
            ),
            Type::Decimal256(s) => (
                Decimal256,
                Decimal256Builder::with_capacity(ROWS).with_precision_and_scale(76, *s as i8)?
            ),
            // Dates
            Type::Date => (Date, Date32Builder::with_capacity(ROWS)),
            Type::Date32 => (Date32, Date32Builder::with_capacity(ROWS)),
            Type::DateTime(tz) => (
                DateTime,
                TimestampSecondBuilder::with_capacity(ROWS)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(0, tz) => (
                DateTimeS,
                TimestampSecondBuilder::with_capacity(ROWS)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(1..=3, tz) => (
                DateTimeMs,
                TimestampMillisecondBuilder::with_capacity(ROWS)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(4..=6, tz) => (
                DateTimeMu,
                TimestampMicrosecondBuilder::with_capacity(ROWS)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            Type::DateTime64(7..=9, tz) => (
                DateTimeNano,
                TimestampNanosecondBuilder::with_capacity(ROWS)
                    .with_timezone_opt(tz_some.then_some(Arc::from(tz.name())))
            ),
            // String, Binary, UUID, IPv4, IPv6
            Type::String => (
                String, StringBuilder::with_capacity(ROWS, ROWS * 64)
            ),
            Type::Object => (
                Object, StringBuilder::with_capacity(ROWS, ROWS * 1024)
            ),
            Type::FixedSizedString(n) => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(ROWS, *n as i32)
            ),
            Type::Binary => (
                Binary, BinaryBuilder::with_capacity(ROWS, ROWS * 64)
            ),
            Type::FixedSizedBinary(n) => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(ROWS, *n as i32)
            ),
            Type::Ipv4 => (
                FixedSizeBinary, FixedSizeBinaryBuilder::with_capacity(ROWS, 4)
            ),
            Type::Ipv6 => (
                FixedSizeBinary, FixedSizeBinaryBuilder::with_capacity(ROWS, 16)
            ),
            Type::Uuid => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(ROWS, 16)
            ),
            // Special numeric types that need to be read as bytes
            Type::Int128 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(ROWS, 16)
            ),
            Type::Int256 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(ROWS, 32)
            ),
            Type::UInt128 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(ROWS, 16)
            ),
            Type::UInt256 => (
                FixedSizeBinary,
                FixedSizeBinaryBuilder::with_capacity(ROWS, 32)
            ),
            // Enums
            Type::Enum8(p) => (
                Enum8,
                StringDictionaryBuilder::<Int8Type>::with_capacity(ROWS, p.len(), ROWS * p.len() * 4)
            ),
            Type::Enum16(p) => (
                Enum16,
                StringDictionaryBuilder::<Int16Type>::with_capacity(ROWS, p.len(), ROWS * p.len() * 4)
            ),
        }))
    }
}

impl std::fmt::Debug for TypedBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::List(l) => write!(f, "TypedBuilder::List({l:?})"),
            Self::LowCardinality(l) => write!(f, "TypedBuilder::LowCardinality({l:?})"),
            Self::Map((k, v)) => write!(f, "TypedBuilder::Map({k:?}, {v:?})"),
            Self::Tuple(t) => write!(f, "TypedBuilder::Tuple({t:?})"),
            Self::String(_) => write!(f, "TypedBuilder::String"),
            b => write!(f, "TypedBuilder::{}", b.as_ref()),
        }
    }
}
