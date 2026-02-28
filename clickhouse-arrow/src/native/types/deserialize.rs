pub(crate) mod array;
#[cfg(feature = "extended-types")]
pub(crate) mod dynamic;
pub(crate) mod geo;
pub(crate) mod low_cardinality;
pub(crate) mod map;
#[cfg(feature = "extended-types")]
pub(crate) mod nested;
pub(crate) mod nullable;
pub(crate) mod object;
pub(crate) mod sized;
pub(crate) mod string;
pub(crate) mod tuple;
#[cfg(feature = "extended-types")]
pub(crate) mod variant;

use tokio::io::AsyncReadExt;

use super::*;
use crate::formats::{CustomSerializationEntry, CustomSerializationState};

const CUSTOM_SERIALIZATION_KIND_STACK_DEFAULT: u8 = 0;
const CUSTOM_SERIALIZATION_KIND_STACK_SPARSE: u8 = 1;
const CUSTOM_SERIALIZATION_KIND_STACK_DETACHED: u8 = 2;
const CUSTOM_SERIALIZATION_KIND_STACK_DETACHED_OVER_SPARSE: u8 = 3;
const CUSTOM_SERIALIZATION_KIND_STACK_REPLICATED: u8 = 4;
const CUSTOM_SERIALIZATION_KIND_STACK_COMBINATION: u8 = 5;

const CUSTOM_SERIALIZATION_KIND_DEFAULT: u8 = 0;
const CUSTOM_SERIALIZATION_KIND_SPARSE: u8 = 1;
const CUSTOM_SERIALIZATION_KIND_DETACHED: u8 = 2;
const CUSTOM_SERIALIZATION_KIND_REPLICATED: u8 = 3;

// Core protocol parsing
pub(crate) trait ClickHouseNativeDeserializer {
    fn deserialize_custom_serialization_prefix<'a, R: ClickHouseRead, T: Default + Send>(
        &'a self,
        reader: &'a mut R,
        state: &'a mut DeserializerState<T>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    fn deserialize_prefix<'a, R: ClickHouseRead, T: Default + Send>(
        &'a self,
        reader: &'a mut R,
        state: &'a mut DeserializerState<T>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}

impl ClickHouseNativeDeserializer for Type {
    fn deserialize_custom_serialization_prefix<'a, R: ClickHouseRead, T: Default + Send>(
        &'a self,
        reader: &'a mut R,
        state: &'a mut DeserializerState<T>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        async move {
            let mut stack = vec![self.strip_null()];
            let mut entries = Vec::with_capacity(1);
            while let Some(next) = stack.pop() {
                let kind_stack_type = reader.read_u8().await?;
                let mut kinds = Vec::new();
                match kind_stack_type {
                    CUSTOM_SERIALIZATION_KIND_STACK_DEFAULT
                    | CUSTOM_SERIALIZATION_KIND_STACK_SPARSE
                    | CUSTOM_SERIALIZATION_KIND_STACK_DETACHED
                    | CUSTOM_SERIALIZATION_KIND_STACK_DETACHED_OVER_SPARSE
                    | CUSTOM_SERIALIZATION_KIND_STACK_REPLICATED => {}
                    CUSTOM_SERIALIZATION_KIND_STACK_COMBINATION => {
                        #[expect(clippy::cast_possible_truncation)]
                        let num_kinds = reader.read_var_uint().await? as usize;
                        kinds.reserve(num_kinds);
                        for _ in 0..num_kinds {
                            let kind = reader.read_u8().await?;
                            if !matches!(
                                kind,
                                CUSTOM_SERIALIZATION_KIND_DEFAULT
                                    | CUSTOM_SERIALIZATION_KIND_SPARSE
                                    | CUSTOM_SERIALIZATION_KIND_DETACHED
                                    | CUSTOM_SERIALIZATION_KIND_REPLICATED
                            ) {
                                return Err(Error::Protocol(format!(
                                    "invalid custom serialization kind: {kind}"
                                )));
                            }
                            kinds.push(kind);
                        }
                    }
                    _ => {
                        return Err(Error::Protocol(format!(
                            "invalid custom serialization kind stack type: {kind_stack_type}"
                        )));
                    }
                }

                if let Type::Tuple(inner) = next {
                    for item in inner.iter().rev() {
                        stack.push(item.1.strip_null());
                    }
                }

                entries.push(CustomSerializationEntry { stack_type: kind_stack_type, kinds });
            }

            drop(state.replace_custom_serialization(CustomSerializationState { entries }));
            Ok(())
        }
        .boxed()
    }

    fn deserialize_prefix<'a, R: ClickHouseRead, T: Default + Send>(
        &'a self,
        reader: &'a mut R,
        state: &'a mut DeserializerState<T>,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        use deserialize::*;
        async move {
            match self {
                Type::Int8
                | Type::Int16
                | Type::Int32
                | Type::Int64
                | Type::Int128
                | Type::Int256
                | Type::UInt8
                | Type::UInt16
                | Type::UInt32
                | Type::UInt64
                | Type::UInt128
                | Type::UInt256
                | Type::Float32
                | Type::Float64
                | Type::Decimal32(_)
                | Type::Decimal64(_)
                | Type::Decimal128(_)
                | Type::Decimal256(_)
                | Type::Uuid
                | Type::Date
                | Type::Date32
                | Type::DateTime(_)
                | Type::DateTime64(_, _)
                | Type::Ipv4
                | Type::Ipv6
                | Type::Enum8(_)
                | Type::Enum16(_) => {
                    sized::SizedDeserializer::read_prefix(self, reader, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::BFloat16 | Type::Time | Type::Time64(_) => {
                    sized::SizedDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Nothing => {}
                #[cfg(feature = "extended-types")]
                Type::QBit { .. } => {}

                Type::String
                | Type::FixedSizedString(_)
                | Type::Binary
                | Type::FixedSizedBinary(_) => {
                    string::StringDeserializer::read_prefix(self, reader, state).await?;
                }

                Type::Array(_) => {
                    array::ArrayDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Tuple(_) => {
                    tuple::TupleDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Point => geo::PointDeserializer::read_prefix(self, reader, state).await?,
                Type::Ring => geo::RingDeserializer::read_prefix(self, reader, state).await?,
                Type::Polygon => geo::PolygonDeserializer::read_prefix(self, reader, state).await?,
                Type::MultiPolygon => {
                    geo::MultiPolygonDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Nullable(_) => {
                    nullable::NullableDeserializer::read_prefix(self, reader, state).await?;
                }
                Type::Map(_, _) => map::MapDeserializer::read_prefix(self, reader, state).await?,
                Type::LowCardinality(_) => {
                    low_cardinality::LowCardinalityDeserializer::read_prefix(self, reader, state)
                        .await?;
                }
                Type::Object => {
                    object::ObjectDeserializer::read_prefix(self, reader, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::Nested(_) => {
                    nested::NestedDeserializer::read_prefix(self, reader, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::SimpleAggregateFunction { types, .. } => {
                    if let Some(inner) = types.first() {
                        inner.deserialize_prefix(reader, state).await?;
                    } else {
                        return Err(Error::Deserialize(
                            "SimpleAggregateFunction has no inner type".to_string(),
                        ));
                    }
                }
                #[cfg(feature = "extended-types")]
                Type::Variant(_) => {
                    variant::VariantDeserializer::read_prefix(self, reader, state).await?;
                }
                #[cfg(feature = "extended-types")]
                Type::AggregateFunction { .. } => {}
                #[cfg(feature = "extended-types")]
                Type::Dynamic { .. } => {
                    dynamic::DynamicDeserializer::read_prefix(self, reader, state).await?;
                }
            }
            Ok(())
        }
        .boxed()
    }
}

// ---
// String => Aggregate Parameter Deserialization
// ---

#[cfg(feature = "extended-types")]
impl AggregateParameter {
    fn parse_quoted_string(input: &str) -> Result<String> {
        if !(input.starts_with('\'') && input.ends_with('\'') && input.len() >= 2) {
            return Err(Error::TypeParse(format!(
                "invalid quoted aggregate parameter literal: {input}"
            )));
        }

        let inner = &input[1..input.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                let Some(escaped) = chars.next() else {
                    return Err(Error::TypeParse(format!(
                        "invalid escape sequence in aggregate parameter literal: {input}"
                    )));
                };
                out.push(match escaped {
                    '\\' => '\\',
                    '\'' => '\'',
                    '"' => '"',
                    '0' => '\0',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    other => other,
                });
                continue;
            }

            if ch == '\'' && chars.peek() == Some(&'\'') {
                let _ = chars.next();
                out.push('\'');
                continue;
            }

            out.push(ch);
        }

        Ok(out)
    }
}

#[cfg(feature = "extended-types")]
impl FromStr for AggregateParameter {
    type Err = Error;

    fn from_str(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.eq_ignore_ascii_case("null") {
            return Ok(Self::Null);
        }
        if input.eq_ignore_ascii_case("true") {
            return Ok(Self::Bool(true));
        }
        if input.eq_ignore_ascii_case("false") {
            return Ok(Self::Bool(false));
        }
        if input.eq_ignore_ascii_case("inf") || input.eq_ignore_ascii_case("+inf") {
            return Ok(Self::Float64(f64::INFINITY.to_bits()));
        }
        if input.eq_ignore_ascii_case("-inf") {
            return Ok(Self::Float64(f64::NEG_INFINITY.to_bits()));
        }

        if input.starts_with('\'') && input.ends_with('\'') {
            return Ok(Self::String(Self::parse_quoted_string(input)?));
        }

        if let Ok(value) = input.parse::<u64>() {
            return Ok(Self::UInt64(value));
        }
        if let Ok(value) = input.parse::<i64>() {
            return Ok(Self::Int64(value));
        }
        if let Ok(value) = input.parse::<f64>() {
            return Ok(Self::Float64(value.to_bits()));
        }

        Err(Error::TypeParse(format!("unsupported aggregate parameter literal '{input}'")))
    }
}

// ---
// String => Type Deserialization
// ---

// For Date32: Days from 1900-01-01 to 1970-01-01
pub(crate) const DAYS_1900_TO_1970: i32 = 25_567;

trait EnumValueType: FromStr + std::fmt::Debug {}
impl EnumValueType for i8 {}
impl EnumValueType for i16 {}

macro_rules! parse_enum_options {
    ($opt_str:expr, $num_type:ty) => {{
        fn inner_parse(input: &str) -> Result<Vec<(String, $num_type)>> {
            if !input.starts_with('(') || !input.ends_with(')') {
                return Err(Error::TypeParse(
                    "Enum arguments must be enclosed in parentheses".to_string(),
                ));
            }

            let input = input[1..input.len() - 1].trim();
            if input.is_empty() {
                return Ok(Vec::new());
            }

            let mut options = Vec::new();
            let mut name = String::new();
            let mut value = String::new();
            let mut state = EnumParseState::ExpectQuote;
            let mut escaped = false;

            for ch in input.chars() {
                match state {
                    EnumParseState::ExpectQuote => {
                        if ch == '\'' {
                            state = EnumParseState::InName;
                        } else if !ch.is_whitespace() {
                            return Err(Error::TypeParse(format!(
                                "Expected single quote at start of variant name, found '{}'",
                                ch
                            )));
                        }
                    }
                    EnumParseState::InName => {
                        if escaped {
                            name.push(ch);
                            escaped = false;
                        } else if ch == '\\' {
                            escaped = true;
                        } else if ch == '\'' {
                            state = EnumParseState::ExpectEqual;
                        } else {
                            name.push(ch);
                        }
                    }
                    EnumParseState::ExpectEqual => {
                        if ch == '=' {
                            state = EnumParseState::InValue;
                        } else if !ch.is_whitespace() {
                            return Err(Error::TypeParse(format!(
                                "Expected '=' after variant name, found '{}'",
                                ch
                            )));
                        }
                    }
                    EnumParseState::InValue => {
                        if ch == ',' {
                            let parsed_value = value.parse::<$num_type>().map_err(|e| {
                                Error::TypeParse(format!("Invalid enum value '{value}': {e}"))
                            })?;
                            options.push((name, parsed_value));
                            name = String::new();
                            value = String::new();
                            state = EnumParseState::ExpectQuote;
                        } else if !ch.is_whitespace() {
                            value.push(ch);
                        }
                    }
                }
            }

            match state {
                EnumParseState::InValue if !value.is_empty() => {
                    let parsed_value = value.parse::<$num_type>().map_err(|e| {
                        Error::TypeParse(format!("Invalid enum value '{value}': {e}"))
                    })?;
                    options.push((name, parsed_value));
                }
                EnumParseState::ExpectQuote if !input.is_empty() => {
                    return Err(Error::TypeParse(
                        "Expected enum variant, found end of input".to_string(),
                    ));
                }
                EnumParseState::InName | EnumParseState::ExpectEqual => {
                    return Err(Error::TypeParse(
                        "Incomplete enum variant at end of input".to_string(),
                    ));
                }
                _ => {}
            }

            if input.ends_with(',') {
                return Err(Error::TypeParse("Trailing comma in enum variants".to_string()));
            }

            Ok(options)
        }

        fn assert_numeric_type<T: EnumValueType>() {}
        assert_numeric_type::<$num_type>();
        inner_parse($opt_str)
    }};
}

#[derive(PartialEq)]
enum EnumParseState {
    ExpectQuote,
    InName,
    ExpectEqual,
    InValue,
}

impl FromStr for Type {
    type Err = Error;

    #[expect(clippy::too_many_lines)]
    fn from_str(s: &str) -> Result<Self> {
        let (ident, following) = eat_identifier(s);

        if ident.is_empty() {
            return Err(Error::TypeParse(format!("invalid empty identifier for type: '{s}'")));
        }

        let following = following.trim();
        if !following.is_empty() {
            let parsed = match ident {
                "Decimal" => {
                    let (args, count) = parse_fixed_args::<2>(following)?;
                    if count != 2 {
                        return Err(Error::TypeParse(format!(
                            "Decimal expects 2 args, got {count}: {args:?}"
                        )));
                    }
                    let p: usize = parse_precision(args[0])?;
                    let s: u8 = parse_scale(args[1])?;
                    if s == 0
                        || (p <= 9 && s > 9)
                        || (p <= 18 && s > 18)
                        || (p <= 38 && s > 38)
                        || (p <= 76 && s > 76)
                    {
                        return Err(Error::TypeParse(format!(
                            "Invalid scale {s} for precision {p}"
                        )));
                    }
                    if p <= 9 {
                        Type::Decimal32(s)
                    } else if p <= 18 {
                        Type::Decimal64(s)
                    } else if p <= 38 {
                        Type::Decimal128(s)
                    } else if p <= 76 {
                        Type::Decimal256(s)
                    } else {
                        return Err(Error::TypeParse(
                            "bad decimal spec, cannot exceed 76 precision".to_string(),
                        ));
                    }
                }
                "Decimal32" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "bad arg count for Decimal32, expected 1 and got {count}: {args:?}"
                        )));
                    }
                    let s: u8 = parse_scale(args[0])?;
                    if s == 0 || s > 9 {
                        return Err(Error::TypeParse(format!(
                            "Invalid scale {s} for Decimal32, must be 1..=9"
                        )));
                    }
                    Type::Decimal32(s)
                }
                "Decimal64" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "bad arg count for Decimal64, expected 1 and got {count}: {args:?}"
                        )));
                    }
                    let s: u8 = parse_scale(args[0])?;
                    if s == 0 || s > 18 {
                        return Err(Error::TypeParse(format!(
                            "Invalid scale {s} for Decimal64, must be 1..=18"
                        )));
                    }
                    Type::Decimal64(s)
                }
                "Decimal128" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "bad arg count for Decimal128, expected 1 and got {count}: {args:?}"
                        )));
                    }
                    let s: u8 = parse_scale(args[0])?;
                    if s == 0 || s > 38 {
                        return Err(Error::TypeParse(format!(
                            "Invalid scale {s} for Decimal128, must be 1..=38"
                        )));
                    }
                    Type::Decimal128(s)
                }
                "Decimal256" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "bad arg count for Decimal256, expected 1 and got {count}: {args:?}"
                        )));
                    }
                    let s: u8 = parse_scale(args[0])?;
                    if s == 0 || s > 76 {
                        return Err(Error::TypeParse(format!(
                            "Invalid scale {s} for Decimal256, must be 1..=76"
                        )));
                    }
                    Type::Decimal256(s)
                }
                "FixedString" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "bad arg count for FixedString, expected 1 and got {count}: {args:?}"
                        )));
                    }
                    let s: usize = parse_precision(args[0])?;
                    if s == 0 {
                        return Err(Error::TypeParse(
                            "FixedString size must be greater than 0".to_string(),
                        ));
                    }
                    Type::FixedSizedString(s)
                }
                "DateTime" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count > 1 {
                        return Err(Error::TypeParse(format!(
                            "DateTime expects 0 or 1 arg: {args:?}"
                        )));
                    }
                    if count == 0 {
                        Type::DateTime(chrono_tz::UTC)
                    } else {
                        let tz_str = args[0];
                        if !tz_str.starts_with('\'') || !tz_str.ends_with('\'') {
                            return Err(Error::TypeParse(format!(
                                "DateTime timezone must be quoted: '{tz_str}'"
                            )));
                        }
                        let tz = tz_str[1..tz_str.len() - 1].parse().map_err(|e| {
                            Error::TypeParse(format!("failed to parse timezone '{tz_str}': {e}"))
                        })?;
                        Type::DateTime(tz)
                    }
                }
                "DateTime64" => {
                    let (args, count) = parse_fixed_args::<2>(following)?;
                    if !(1..=2).contains(&count) {
                        return Err(Error::TypeParse(format!(
                            "DateTime64 expects 1 or 2 args, got {count}: {args:?}"
                        )));
                    }
                    let precision: u8 = args[0]
                        .parse()
                        .map_err(|_| Error::TypeParse("could not parse precision".to_string()))?;
                    let tz = if count == 2 {
                        let tz_str = args[1];
                        if !tz_str.starts_with('\'') || !tz_str.ends_with('\'') {
                            return Err(Error::TypeParse(format!(
                                "DateTime64 timezone must be quoted: '{tz_str}'"
                            )));
                        }
                        tz_str[1..tz_str.len() - 1].parse().map_err(|e| {
                            Error::TypeParse(format!("failed to parse timezone '{tz_str}': {e}"))
                        })?
                    } else {
                        chrono_tz::UTC
                    };
                    Type::DateTime64(precision, tz)
                }
                "Enum8" => Type::Enum8(parse_enum_options!(following, i8)?),
                "Enum16" => Type::Enum16(parse_enum_options!(following, i16)?),
                "LowCardinality" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "LowCardinality expected 1 arg and got {count}: {args:?}"
                        )));
                    }
                    Type::LowCardinality(Box::new(Type::from_str(args[0])?))
                }
                "Array" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "Array expected 1 arg and got {count}: {args:?}"
                        )));
                    }
                    Type::Array(Box::new(Type::from_str(args[0])?))
                }
                "Tuple" => {
                    let args = parse_variable_args(following)?;
                    let inner =
                        args.into_iter().map(parse_tuple_field).collect::<Result<_, _>>()?;
                    Type::Tuple(inner)
                }
                "Nullable" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!("Nullable expects 1 arg: {args:?}")));
                    }
                    Type::Nullable(Box::new(Type::from_str(args[0])?))
                }
                "Map" => {
                    let (args, count) = parse_fixed_args::<2>(following)?;
                    if count != 2 {
                        return Err(Error::TypeParse(format!(
                            "Map expects 2 args, got {count}: {args:?}"
                        )));
                    }
                    Type::Map(
                        Box::new(Type::from_str(args[0])?),
                        Box::new(Type::from_str(args[1])?),
                    )
                }
                #[cfg(feature = "extended-types")]
                "Time64" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count != 1 {
                        return Err(Error::TypeParse(format!(
                            "Time64 expects 1 arg (precision), got {count}: {args:?}"
                        )));
                    }
                    let precision: u8 = args[0]
                        .parse()
                        .map_err(|_| Error::TypeParse("could not parse precision".to_string()))?;
                    Type::Time64(precision)
                }
                #[cfg(feature = "extended-types")]
                "QBit" => {
                    let (args, count) = parse_fixed_args::<2>(following)?;
                    if count != 2 {
                        return Err(Error::TypeParse(format!(
                            "QBit expects 2 args (element_type, dimension), got {count}: {args:?}"
                        )));
                    }
                    let element_type = Type::from_str(args[0])?;
                    let dimension = parse_precision(args[1])?;
                    Type::QBit { element_type: Box::new(element_type), dimension }
                }
                #[cfg(feature = "extended-types")]
                "Variant" => {
                    let args = parse_variable_args(following)?;
                    if args.is_empty() {
                        return Err(Error::TypeParse(
                            "Variant requires at least one inner type".to_string(),
                        ));
                    }
                    Type::Variant(args.into_iter().map(Type::from_str).collect::<Result<Vec<_>>>()?)
                }
                #[cfg(feature = "extended-types")]
                "Dynamic" => {
                    let (args, count) = parse_fixed_args::<1>(following)?;
                    if count == 0 {
                        Type::Dynamic { max_types: 32 }
                    } else {
                        let arg = args[0].trim();
                        let max_types = if let Some(value) = arg.strip_prefix("max_types=") {
                            value.parse::<u8>().map_err(|e| {
                                Error::TypeParse(format!(
                                    "Invalid Dynamic max_types value '{value}': {e}"
                                ))
                            })?
                        } else {
                            arg.parse::<u8>().map_err(|e| {
                                Error::TypeParse(format!(
                                    "Invalid Dynamic argument '{arg}', expected max_types=<n> or \
                                     <n>: {e}"
                                ))
                            })?
                        };
                        if max_types > 254 {
                            return Err(Error::TypeParse(format!(
                                "Invalid Dynamic max_types value '{max_types}', must be in range \
                                 (0..=254)"
                            )));
                        }
                        Type::Dynamic { max_types }
                    }
                }
                #[cfg(feature = "extended-types")]
                "Nested" => {
                    let args = parse_variable_args(following)?;
                    if args.is_empty() {
                        return Err(Error::TypeParse(
                            "Nested requires at least one field".to_string(),
                        ));
                    }

                    let mut fields = Vec::with_capacity(args.len());
                    for arg in args {
                        let arg = arg.trim();
                        let (name, type_spec) = eat_identifier(arg);
                        let name = name.trim();
                        let type_spec = type_spec.trim();
                        if name.is_empty() || type_spec.is_empty() {
                            return Err(Error::TypeParse(format!(
                                "Invalid Nested field '{arg}', expected '<name> <type>'"
                            )));
                        }
                        fields.push((name.to_string(), Type::from_str(type_spec)?));
                    }
                    Type::Nested(fields)
                }
                #[cfg(feature = "extended-types")]
                "AggregateFunction" => {
                    let args = parse_variable_args(following)?;
                    if args.is_empty() {
                        return Err(Error::TypeParse(
                            "AggregateFunction requires at least a function name".to_string(),
                        ));
                    }
                    let signature = args[0].trim();
                    let (name, following) = eat_identifier(signature);
                    if name.is_empty() {
                        return Err(Error::TypeParse(
                            "AggregateFunction requires a non-empty function name".to_string(),
                        ));
                    }
                    let following = following.trim();
                    let parameters = if following.is_empty() {
                        Vec::new()
                    } else {
                        parse_variable_args(following)?
                            .into_iter()
                            .map(AggregateParameter::from_str)
                            .collect::<Result<Vec<_>>>()?
                    };
                    let mut types = Vec::new();
                    for arg in args.into_iter().skip(1) {
                        types.push(Type::from_str(arg)?);
                    }
                    Type::AggregateFunction {
                        name: name.to_string(),
                        parameters,
                        types,
                        version: 0,
                    }
                }
                #[cfg(feature = "extended-types")]
                "SimpleAggregateFunction" => {
                    let args = parse_variable_args(following)?;
                    if args.len() < 2 {
                        return Err(Error::TypeParse(
                            "SimpleAggregateFunction requires function name and type".to_string(),
                        ));
                    }
                    let signature = args[0].trim();
                    let (name, following) = eat_identifier(signature);
                    if name.is_empty() {
                        return Err(Error::TypeParse(
                            "SimpleAggregateFunction requires a non-empty function name"
                                .to_string(),
                        ));
                    }
                    let following = following.trim();
                    let parameters = if following.is_empty() {
                        Vec::new()
                    } else {
                        parse_variable_args(following)?
                            .into_iter()
                            .map(AggregateParameter::from_str)
                            .collect::<Result<Vec<_>>>()?
                    };
                    let types =
                        args.into_iter().skip(1).map(Type::from_str).collect::<Result<Vec<_>>>()?;
                    Type::SimpleAggregateFunction { name: name.to_string(), parameters, types }
                }
                id => {
                    return Err(Error::TypeParse(format!(
                        "invalid type with arguments: '{ident}' (ident = {id})"
                    )));
                }
            };
            return Ok(parsed);
        }
        let parsed = match ident {
            "Int8" => Type::Int8,
            "Int16" => Type::Int16,
            "Int32" => Type::Int32,
            "Int64" => Type::Int64,
            "Int128" => Type::Int128,
            "Int256" => Type::Int256,
            "Bool" | "UInt8" => Type::UInt8,
            "UInt16" => Type::UInt16,
            "UInt32" => Type::UInt32,
            "UInt64" => Type::UInt64,
            "UInt128" => Type::UInt128,
            "UInt256" => Type::UInt256,
            "Float32" => Type::Float32,
            "Float64" => Type::Float64,
            "String" => Type::String,
            "Nothing" => Type::Nothing,
            "UUID" | "Uuid" | "uuid" => Type::Uuid,
            "Date" => Type::Date,
            "Date32" => Type::Date32,
            // TODO: This is duplicated above. Verify if this is needed, for example if ClickHouse
            // ever sends `DateTime` without tz.
            "DateTime" => Type::DateTime(chrono_tz::UTC),
            "IPv4" => Type::Ipv4,
            "IPv6" => Type::Ipv6,
            "Point" => Type::Point,
            "Ring" => Type::Ring,
            "Polygon" => Type::Polygon,
            "MultiPolygon" => Type::MultiPolygon,
            "Object" | "Json" | "OBJECT" | "JSON" => Type::Object,
            #[cfg(feature = "extended-types")]
            "BFloat16" => Type::BFloat16,
            #[cfg(feature = "extended-types")]
            "Time" => Type::Time,
            #[cfg(feature = "extended-types")]
            "Dynamic" => Type::Dynamic { max_types: 32 },
            _ => {
                return Err(Error::TypeParse(format!("invalid type name: '{ident}'")));
            }
        };
        Ok(parsed)
    }
}

// Assumed complete identifier normalization and type resolution from clickhouse
fn eat_identifier(input: &str) -> (&str, &str) {
    for (i, c) in input.char_indices() {
        if c.is_alphabetic() || c == '_' || c == '$' || (i > 0 && c.is_numeric()) {
            continue;
        }
        return (&input[..i], &input[i..]);
    }
    (input, "")
}

fn parse_tuple_field(arg: &str) -> Result<(Option<String>, Type)> {
    let arg = arg.trim();
    let (name, rest) = eat_identifier(arg);
    let name = name.trim();
    let type_spec = rest.trim_start();
    if !name.is_empty()
        && !type_spec.is_empty()
        && type_spec.chars().next().is_some_and(char::is_alphabetic)
    {
        return Ok((Some(name.to_string()), Type::from_str(type_spec)?));
    }
    Ok((None, Type::from_str(arg)?))
}

/// Parse arguments into a fixed-size array for types with a known number of args
fn parse_fixed_args<const N: usize>(input: &str) -> Result<([&str; N], usize)> {
    let mut iter = parse_args_iter(input)?;
    let mut out = [""; N];
    let mut count = 0;

    // Take up to N items
    for (i, arg_result) in iter.by_ref().take(N).enumerate() {
        out[i] = arg_result?;
        count += 1;
    }

    // Check for excess arguments
    if iter.next().is_some() {
        return Err(Error::TypeParse("too many arguments".to_string()));
    }
    Ok((out, count))
}

/// Parse arguments into a Vec for types with variable numbers of args
fn parse_variable_args(input: &str) -> Result<Vec<&str>> { parse_args_iter(input)?.collect() }

fn parse_scale(from: &str) -> Result<u8> {
    from.parse().map_err(|_| Error::TypeParse("couldn't parse scale".to_string()))
}

fn parse_precision(from: &str) -> Result<usize> {
    from.parse().map_err(|_| Error::TypeParse("could not parse precision".to_string()))
}

/// Core iterator for parsing comma-separated arguments within parentheses
fn parse_args_iter(input: &str) -> Result<impl Iterator<Item = Result<&str, Error>>> {
    if !input.starts_with('(') || !input.ends_with(')') {
        return Err(Error::TypeParse("Malformed arguments to type".to_string()));
    }
    let input = input[1..input.len() - 1].trim();
    if input.ends_with(',') {
        return Err(Error::TypeParse("Trailing comma in argument list".to_string()));
    }

    Ok(ArgsIterator { input, last_start: 0, in_parens: 0, in_quotes: false, done: false })
}

struct ArgsIterator<'a> {
    input:      &'a str,
    last_start: usize,
    in_parens:  usize,
    in_quotes:  bool,
    done:       bool,
}

impl<'a> Iterator for ArgsIterator<'a> {
    type Item = Result<&'a str, Error>;

    #[allow(unused_assignments)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let start = self.last_start;
        let mut i = start;
        let chars = self.input[start..].char_indices();
        let mut escaped = false;

        for (offset, c) in chars {
            i = start + offset;
            if self.in_quotes {
                if c == '\\' {
                    escaped = true;
                    continue;
                }
                if c == '\'' && !escaped {
                    self.in_quotes = false;
                }
                escaped = false;
                continue;
            }
            match c {
                '\'' if !escaped => {
                    self.in_quotes = true;
                }
                '(' => self.in_parens += 1,
                ')' => self.in_parens -= 1,
                ',' if self.in_parens == 0 => {
                    let slice = self.input[self.last_start..i].trim();
                    if slice.is_empty() {
                        return Some(Err(Error::TypeParse("Empty argument in list".to_string())));
                    }
                    self.last_start = i + 1;
                    return Some(Ok(slice));
                }
                _ => {}
            }
            escaped = false;
        }

        if self.in_parens != 0 {
            self.done = true;
            return Some(Err(Error::TypeParse("Mismatched parentheses".to_string())));
        }
        if self.last_start <= self.input.len() {
            let slice = self.input[self.last_start..].trim();
            if slice.is_empty() {
                self.done = true;
                return None; // Allow empty input after last comma
            }
            if slice == "," {
                self.done = true;
                return Some(Err(Error::TypeParse("Trailing comma in argument list".to_string())));
            }
            self.done = true;
            return Some(Ok(slice));
        }

        self.done = true;
        None
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    /// Tests `eat_identifier` for splitting type names and arguments.
    #[test]
    fn test_eat_identifier() {
        assert_eq!(eat_identifier("Int8"), ("Int8", ""));
        assert_eq!(eat_identifier("Enum8('a'=1)"), ("Enum8", "('a'=1)"));
        assert_eq!(eat_identifier("DateTime('UTC')"), ("DateTime", "('UTC')"));
        assert_eq!(eat_identifier("Map(String,Int32)"), ("Map", "(String,Int32)"));
        assert_eq!(eat_identifier(""), ("", ""));
        assert_eq!(eat_identifier("Invalid Type"), ("Invalid", " Type"));
    }

    /// Tests `parse_fixed_args` for fixed-size argument lists.
    #[test]
    fn test_parse_fixed_args() {
        let (args, count) = parse_fixed_args::<2>("(UInt32, String)").unwrap();
        assert_eq!(count, 2);
        assert_eq!(args[..count], ["UInt32", "String"]);

        let (args, count) = parse_fixed_args::<1>("(String)").unwrap();
        assert_eq!(count, 1);
        assert_eq!(args[..count], ["String"]);

        let (args, count) = parse_fixed_args::<2>("(3, 'UTC')").unwrap();
        assert_eq!(count, 2);
        assert_eq!(args[..count], ["3", "'UTC'"]);

        assert!(parse_fixed_args::<1>("(UInt32, String)").is_err()); // Too many args
        assert!(parse_fixed_args::<1>("(()").is_err()); // Mismatched parens
        assert!(parse_fixed_args::<1>("(String,)").is_err()); // Trailing comma
    }

    /// Tests `parse_variable_args` for variable-size argument lists.
    #[test]
    fn test_parse_variable_args() {
        let args = parse_variable_args("(Int8, String, Float64)").unwrap();
        assert_eq!(args, vec!["Int8", "String", "Float64"]);

        let args = parse_variable_args("(3, 'UTC', 'extra')").unwrap();
        assert_eq!(args, vec!["3", "'UTC'", "'extra'"]);

        let args = parse_variable_args("(())").unwrap();
        assert_eq!(args, vec!["()"]);

        let args = parse_variable_args("()").unwrap();
        assert_eq!(args, Vec::<&str>::new());

        assert!(parse_variable_args("(()").is_err()); // Mismatched parens
        assert!(parse_variable_args("(String,)").is_err()); // Trailing comma
    }

    /// Tests `Type::from_str` for primitive types.
    #[test]
    fn test_from_str_primitives() {
        assert_eq!(Type::from_str("Int8").unwrap(), Type::Int8);
        assert_eq!(Type::from_str("UInt8").unwrap(), Type::UInt8);
        assert_eq!(Type::from_str("Bool").unwrap(), Type::UInt8); // Bool alias
        assert_eq!(Type::from_str("Float64").unwrap(), Type::Float64);
        assert_eq!(Type::from_str("String").unwrap(), Type::String);
        assert_eq!(Type::from_str("UUID").unwrap(), Type::Uuid);
        assert_eq!(Type::from_str("Date").unwrap(), Type::Date);
        assert_eq!(Type::from_str("IPv4").unwrap(), Type::Ipv4);
        assert_eq!(Type::from_str("IPv6").unwrap(), Type::Ipv6);
    }

    /// Tests `Type::from_str` for decimal types.
    #[test]
    fn test_from_str_decimals() {
        assert_eq!(Type::from_str("Decimal32(2)").unwrap(), Type::Decimal32(2));
        assert_eq!(Type::from_str("Decimal64(4)").unwrap(), Type::Decimal64(4));
        assert_eq!(Type::from_str("Decimal128(6)").unwrap(), Type::Decimal128(6));
        assert_eq!(Type::from_str("Decimal256(8)").unwrap(), Type::Decimal256(8));
        assert_eq!(Type::from_str("Decimal(9, 2)").unwrap(), Type::Decimal32(2));
        assert_eq!(Type::from_str("Decimal(18, 4)").unwrap(), Type::Decimal64(4));
        assert_eq!(Type::from_str("Decimal(38, 6)").unwrap(), Type::Decimal128(6));
        assert_eq!(Type::from_str("Decimal(76, 8)").unwrap(), Type::Decimal256(8));

        assert!(Type::from_str("Decimal32(0)").is_err()); // Invalid scale
        assert!(Type::from_str("Decimal(77, 8)").is_err()); // Precision too large
        assert!(Type::from_str("Decimal(9)").is_err()); // Missing scale
    }

    /// Tests `Type::from_str` for string and binary types.
    #[test]
    fn test_from_str_strings() {
        assert_eq!(Type::from_str("String").unwrap(), Type::String);
        assert_eq!(Type::from_str("Nothing").unwrap(), Type::Nothing);
        assert_eq!(Type::from_str("FixedString(4)").unwrap(), Type::FixedSizedString(4));
        assert!(Type::from_str("FixedString(0)").is_err()); // Invalid size
        assert!(Type::from_str("FixedString(a)").is_err()); // Invalid size
    }

    /// Tests `Type::from_str` for date and time types.
    #[test]
    fn test_from_str_datetime() {
        assert_eq!(Type::from_str("DateTime").unwrap(), Type::DateTime(chrono_tz::UTC));
        assert_eq!(Type::from_str("DateTime('UTC')").unwrap(), Type::DateTime(chrono_tz::UTC));
        assert_eq!(
            Type::from_str("DateTime('America/New_York')").unwrap(),
            Type::DateTime(chrono_tz::America::New_York)
        );
        assert!(Type::from_str("DateTime('UTC', 'extra')").is_err()); // Too many args
        assert!(Type::from_str("DateTime(UTC)").is_err()); // Unquoted timezone

        assert_eq!(Type::from_str("DateTime64(3)").unwrap(), Type::DateTime64(3, chrono_tz::UTC));
        assert_eq!(
            Type::from_str("DateTime64(6, 'UTC')").unwrap(),
            Type::DateTime64(6, chrono_tz::UTC)
        );
        assert_eq!(
            Type::from_str("DateTime64(3, 'America/New_York')").unwrap(),
            Type::DateTime64(3, chrono_tz::America::New_York)
        );
        assert!(Type::from_str("DateTime64()").is_err()); // Too few args
        assert!(Type::from_str("DateTime64(3, 'UTC', 'extra')").is_err()); // Too many args
        assert!(Type::from_str("DateTime64(3, UTC)").is_err()); // Unquoted timezone
    }

    /// Tests `Type::from_str` for Enum8 with explicit indices.
    #[test]
    fn test_from_str_enum8_explicit() {
        let enum8 = Type::from_str("Enum8('active' = 1, 'inactive' = 2)").unwrap();
        assert_eq!(enum8, Type::Enum8(vec![("active".into(), 1), ("inactive".into(), 2)]));

        let single = Type::from_str("Enum8('test' = -1)").unwrap();
        assert_eq!(single, Type::Enum8(vec![("test".into(), -1)]));

        let negative = Type::from_str("Enum8('neg' = -128, 'zero' = 0)").unwrap();
        assert_eq!(negative, Type::Enum8(vec![("neg".into(), -128), ("zero".into(), 0)]));
    }

    /// Tests `Type::from_str` for Enum8 with empty variants.
    #[test]
    fn test_from_str_enum8_empty() {
        let empty = Type::from_str("Enum8()").unwrap();
        assert_eq!(empty, Type::Enum8(vec![]));
    }

    /// Tests `Type::from_str` for Enum16 with explicit indices.
    #[test]
    fn test_from_str_enum16_explicit() {
        let enum16 = Type::from_str("Enum16('high' = 1000, 'low' = -1000)").unwrap();
        assert_eq!(enum16, Type::Enum16(vec![("high".into(), 1000), ("low".into(), -1000)]));

        let single = Type::from_str("Enum16('test' = 0)").unwrap();
        assert_eq!(single, Type::Enum16(vec![("test".into(), 0)]));
    }

    /// Tests `Type::from_str` error cases for Enum8.
    #[test]
    fn test_from_str_enum8_errors() {
        assert!(Type::from_str("Enum8('a' = 1, 2)").is_err()); // Lone value
        assert!(Type::from_str("Enum8('a' = x)").is_err()); // Invalid value
        assert!(Type::from_str("Enum8(a = 1)").is_err()); // Unquoted name
        assert!(Type::from_str("Enum8('a' = 1, )").is_err()); // Trailing comma
        assert!(Type::from_str("Enum8('a' = 1").is_err()); // Unclosed paren
    }

    /// Tests `Type::from_str` error cases for Enum16.
    #[test]
    fn test_from_str_enum16_errors() {
        assert!(Type::from_str("Enum16('a' = 1, 2)").is_err()); // Lone value
        assert!(Type::from_str("Enum16('a' = x)").is_err()); // Invalid value
        assert!(Type::from_str("Enum16(a = 1)").is_err()); // Unquoted name
        assert!(Type::from_str("Enum16('a' = 1, )").is_err()); // Trailing comma
        assert!(Type::from_str("Enum16('a' = 1").is_err()); // Unclosed paren
    }

    /// Tests `Type::from_str` for complex types.
    #[test]
    fn test_from_str_complex_types() {
        assert_eq!(
            Type::from_str("LowCardinality(String)").unwrap(),
            Type::LowCardinality(Box::new(Type::String))
        );
        assert_eq!(Type::from_str("Array(Int32)").unwrap(), Type::Array(Box::new(Type::Int32)));
        assert_eq!(
            Type::from_str("Tuple(Int32, String)").unwrap(),
            Type::tuple_anon(vec![Type::Int32, Type::String])
        );
        assert_eq!(
            Type::from_str("Nullable(Int32)").unwrap(),
            Type::Nullable(Box::new(Type::Int32))
        );
        assert_eq!(
            Type::from_str("Map(String, Int32)").unwrap(),
            Type::Map(Box::new(Type::String), Box::new(Type::Int32))
        );
        assert_eq!(Type::from_str("JSON").unwrap(), Type::Object);
        assert_eq!(Type::from_str("Object").unwrap(), Type::Object);

        assert!(Type::from_str("LowCardinality()").is_err()); // Missing arg
        assert!(Type::from_str("Array(Int32, String)").is_err()); // Too many args
        assert!(Type::from_str("Map(String)").is_err()); // Missing value type
    }

    /// Tests round-trip `to_string` and `from_str` for all types.
    #[test]
    fn test_round_trip_type_strings() {
        let special_types = vec![
            (Type::Binary, Type::String),
            (Type::FixedSizedBinary(8), Type::FixedSizedString(8)),
        ];

        #[allow(unused_mut)]
        let mut types = vec![
            Type::Int8,
            Type::UInt8,
            Type::Float64,
            Type::String,
            Type::FixedSizedString(4),
            Type::Nothing,
            Type::Uuid,
            Type::Date,
            Type::Date32,
            Type::DateTime(Tz::UTC),
            Type::DateTime64(3, Tz::America__New_York),
            Type::Ipv4,
            Type::Ipv6,
            Type::Decimal32(2),
            Type::Enum8(vec![("active".into(), 1), ("inactive".into(), 2)]),
            Type::Enum16(vec![("high".into(), 1000)]),
            Type::LowCardinality(Box::new(Type::String)),
            Type::Array(Box::new(Type::Int32)),
            Type::tuple_anon(vec![Type::Int32, Type::String]),
            Type::Nullable(Box::new(Type::Int32)),
            Type::Map(Box::new(Type::String), Box::new(Type::Int32)),
            Type::Object,
        ];
        #[cfg(feature = "extended-types")]
        types.extend(vec![
            Type::BFloat16,
            Type::Time,
            Type::Time64(6),
            Type::QBit { element_type: Box::new(Type::Float32), dimension: 4 },
            Type::Variant(vec![Type::String, Type::UInt64]),
            Type::Dynamic { max_types: 8 },
            Type::Nested(vec![
                ("name".to_string(), Type::String),
                ("score".to_string(), Type::Float64),
            ]),
            Type::AggregateFunction {
                name:       "sumState".to_string(),
                parameters: vec![],
                types:      vec![Type::UInt64],
                version:    0,
            },
            Type::SimpleAggregateFunction {
                name:       "sum".to_string(),
                parameters: vec![],
                types:      vec![Type::UInt64],
            },
            Type::AggregateFunction {
                name:       "quantilesTDigest".to_string(),
                parameters: vec![
                    AggregateParameter::Float64(0.5_f64.to_bits()),
                    AggregateParameter::Float64(0.9_f64.to_bits()),
                ],
                types:      vec![Type::UInt64],
                version:    0,
            },
            Type::SimpleAggregateFunction {
                name:       "quantile".to_string(),
                parameters: vec![AggregateParameter::Float64(0.9_f64.to_bits())],
                types:      vec![Type::UInt64],
            },
        ]);

        for ty in types {
            let type_str = ty.to_string();
            let parsed = Type::from_str(&type_str)
                .unwrap_or_else(|e| panic!("Failed to parse '{type_str}' for type {ty:?}: {e}"));
            assert_eq!(
                parsed, ty,
                "Round-trip failed for type {ty:?}: expected {ty}, got {parsed}"
            );
        }

        for (ty, mapped_ty) in special_types {
            let type_str = ty.to_string();
            let parsed = Type::from_str(&type_str)
                .unwrap_or_else(|e| panic!("Failed to parse '{type_str}' for type {ty:?}: {e}"));
            assert_eq!(
                parsed, mapped_ty,
                "Round-trip failed for type {ty:?}: expected {mapped_ty}, got {parsed}"
            );
        }
    }

    /// Tests error cases for general type parsing.
    #[test]
    fn test_from_str_general_errors() {
        assert!(Type::from_str("").is_err()); // Empty input
        assert!(Type::from_str("InvalidType").is_err()); // Unknown type
        assert!(Type::from_str("Nested(String)").is_err()); // Invalid Nested field
        assert!(Type::from_str("Int8(").is_err()); // Unclosed paren
        assert!(Type::from_str("Tuple(String,)").is_err()); // Trailing comma
        assert!(Type::from_str("Variant()").is_err()); // Empty Variant
        assert!(Type::from_str("SimpleAggregateFunction(sum)").is_err()); // Missing arg
    }

    #[cfg(feature = "extended-types")]
    #[test]
    fn test_from_str_aggregate_function_with_parameters() {
        let type_ = Type::from_str("AggregateFunction(quantilesTDigest(0.5,0.9),UInt64)").unwrap();
        assert_eq!(type_, Type::AggregateFunction {
            name:       "quantilesTDigest".to_string(),
            parameters: vec![
                AggregateParameter::Float64(0.5_f64.to_bits()),
                AggregateParameter::Float64(0.9_f64.to_bits()),
            ],
            types:      vec![Type::UInt64],
            version:    0,
        });

        let type_ = Type::from_str("SimpleAggregateFunction(quantile(0.9),UInt64)").unwrap();
        assert_eq!(type_, Type::SimpleAggregateFunction {
            name:       "quantile".to_string(),
            parameters: vec![AggregateParameter::Float64(0.9_f64.to_bits())],
            types:      vec![Type::UInt64],
        });
    }

    /// Tests `parse_tuple_field` helper function.
    #[test]
    fn test_parse_tuple_field() {
        assert_eq!(parse_tuple_field("String").unwrap(), (None, Type::String));
        assert_eq!(parse_tuple_field("Int64").unwrap(), (None, Type::Int64));
        assert_eq!(
            parse_tuple_field("Nullable(Int32)").unwrap(),
            (None, Type::Nullable(Box::new(Type::Int32)))
        );
        assert_eq!(parse_tuple_field("s String").unwrap(), (Some("s".to_string()), Type::String));
        assert_eq!(
            parse_tuple_field("my_field Nullable(Int32)").unwrap(),
            (Some("my_field".to_string()), Type::Nullable(Box::new(Type::Int32)))
        );
        assert_eq!(
            parse_tuple_field("my_map Map(String, Int32)").unwrap(),
            (Some("my_map".to_string()), Type::Map(Box::new(Type::String), Box::new(Type::Int32)))
        );
        assert_eq!(
            parse_tuple_field("Map(String, Int32)").unwrap(),
            (None, Type::Map(Box::new(Type::String), Box::new(Type::Int32)))
        );
    }

    /// Tests `Type::from_str` for named tuple fields (issue #85).
    #[test]
    fn test_from_str_named_tuple() {
        // Simple named tuple
        assert_eq!(
            Type::from_str("Tuple(s String, i Int64)").unwrap(),
            Type::Tuple(vec![
                (Some("s".to_string()), Type::String),
                (Some("i".to_string()), Type::Int64),
            ])
        );

        // Named tuple with complex types
        assert_eq!(
            Type::from_str("Tuple(name String, value Nullable(Int32))").unwrap(),
            Type::Tuple(vec![
                (Some("name".to_string()), Type::String),
                (Some("value".to_string()), Type::Nullable(Box::new(Type::Int32))),
            ])
        );

        // Named tuple with nested types
        assert_eq!(
            Type::from_str("Tuple(arr Array(String), map Map(String, Int32))").unwrap(),
            Type::Tuple(vec![
                (Some("arr".to_string()), Type::Array(Box::new(Type::String))),
                (Some("map".to_string()), Type::Map(Box::new(Type::String), Box::new(Type::Int32)),),
            ])
        );

        // Named tuple with Enum
        assert_eq!(
            Type::from_str("Tuple(status Enum8('active' = 1, 'inactive' = 2), count Int64)")
                .unwrap(),
            Type::Tuple(vec![
                (
                    Some("status".to_string()),
                    Type::Enum8(vec![("active".into(), 1), ("inactive".into(), 2)]),
                ),
                (Some("count".to_string()), Type::Int64),
            ])
        );

        // Mixed: some fields named, some not (ClickHouse allows this)
        // Actually, ClickHouse requires all or none to be named, but we handle it gracefully
        assert_eq!(
            Type::from_str("Tuple(String, i Int64)").unwrap(),
            Type::Tuple(vec![(None, Type::String), (Some("i".to_string()), Type::Int64)])
        );

        // Anonymous tuples with types containing internal spaces - regression test for Codex review
        // These must continue to parse correctly (they worked before the named tuple fix)
        assert_eq!(
            Type::from_str("Tuple(Map(String, Int32), Int32)").unwrap(),
            Type::tuple_anon(vec![
                Type::Map(Box::new(Type::String), Box::new(Type::Int32)),
                Type::Int32
            ])
        );
        assert_eq!(
            Type::from_str("Tuple(Array(Nullable(String)), Map(String, Int64))").unwrap(),
            Type::tuple_anon(vec![
                Type::Array(Box::new(Type::Nullable(Box::new(Type::String)))),
                Type::Map(Box::new(Type::String), Box::new(Type::Int64))
            ])
        );
    }
}
