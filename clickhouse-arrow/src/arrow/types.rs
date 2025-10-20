use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::*;

use crate::geo::normalize_geo_type;
use crate::{ArrowOptions, Error, Result, Type};

/// Type alias for schema conversions
pub type SchemaConversions = std::collections::HashMap<String, Type>;

/// Consistent use of list field's inner field name
pub const LIST_ITEM_FIELD_NAME: &str = "item";
/// Consistent use of tuple's field name prefixes
pub const TUPLE_FIELD_NAME_PREFIX: &str = "field_";
/// Consistent use of map's field name
pub const MAP_FIELD_NAME: &str = "entries";
/// Consistent use of struct's key field name
pub const STRUCT_KEY_FIELD_NAME: &str = "key";
/// Consistent use of struct's value field name
pub const STRUCT_VALUE_FIELD_NAME: &str = "value";

// From impl from Arrow's i256 to internal i256
impl From<i256> for crate::i256 {
    fn from(arrow_i256: i256) -> Self {
        // Arrow's i256 provides to_be_bytes() which returns a [u8; 32] in big-endian order
        let bytes = arrow_i256.to_be_bytes();
        crate::i256(bytes)
    }
}

impl From<crate::i256> for i256 {
    fn from(value: crate::i256) -> Self {
        // i256 stores bytes in big-endian order, so use from_be_bytes
        i256::from_be_bytes(value.0)
    }
}

macro_rules! convert_to_enum {
    ($enum_typ:expr, $low_card:expr, $values:expr) => {{
        match $low_card.strip_null() {
            $crate::Type::LowCardinality(inner) => {
                let nullable = inner.is_nullable();
                let inner_raw = inner.strip_null();
                if matches!(inner_raw, $crate::Type::String | $crate::Type::Binary) {
                    let new_inner = $enum_typ($values);
                    if nullable { new_inner.into_nullable() } else { new_inner }
                } else {
                    return Err($crate::Error::TypeConversion(format!(
                        "expected LowCardinality(String), found {}",
                        $low_card
                    )));
                }
            }
            $crate::Type::String | $crate::Type::Binary => {
                let nullable = $low_card.is_nullable();
                let new_inner = $enum_typ($values);
                if nullable { new_inner.into_nullable() } else { new_inner }
            }
            _ => {
                return Err($crate::Error::TypeConversion(format!(
                    "expected LowCardinality(String) or String/Binary, found {}",
                    $low_card
                )));
            }
        }
    }};
}

/// Given an optional `ArrowOptions`, generate strict and conversion arrow options for schema
fn generate_schema_options(options: Option<ArrowOptions>) -> (ArrowOptions, ArrowOptions) {
    // Attempt to create strict arrow options for schema creation
    let strict_options = options.map_or(ArrowOptions::strict(), ArrowOptions::into_strict_ddl);
    // Ensure strict options are off in the case enums are created since the field being
    // configured will not be a LowCardinality, a common source of schema errors.
    let conversion_options =
        options.unwrap_or(ArrowOptions::default().with_nullable_array_default_empty(false));
    (strict_options, conversion_options)
}

pub(crate) fn schema_conversion(
    field: &Field,
    conversions: Option<&SchemaConversions>,
    options: Option<ArrowOptions>,
) -> Result<Type> {
    let name = field.name();
    let data_type = field.data_type();
    let field_nullable = field.is_nullable();

    let (strict_opts, conversion_opts) = generate_schema_options(options);
    // First convert the type to ensure base level compatibility then convert type.
    Ok(match conversions.and_then(|c| c.get(name)).map(Type::strip_null) {
        Some(Type::Enum8(values)) => {
            let type_ = arrow_to_ch_type(data_type, field_nullable, Some(conversion_opts))?;
            convert_to_enum!(Type::Enum8, type_, values.clone())
        }
        Some(Type::Enum16(values)) => {
            let type_ = arrow_to_ch_type(data_type, field_nullable, Some(conversion_opts))?;
            convert_to_enum!(Type::Enum16, type_, values.clone())
        }
        Some(conv @ (Type::Date | Type::Date32)) => {
            let type_ = arrow_to_ch_type(data_type, field_nullable, Some(conversion_opts))?;
            if !matches!(type_, Type::Date | Type::Date32) {
                return Err(Error::TypeConversion(format!(
                    "expected Date or Date32, found {type_}",
                )));
            }
            conv.clone()
        }
        // For schemas, preserve geo types
        Some(conv @ (Type::Ring | Type::Point | Type::Polygon | Type::MultiPolygon)) => {
            conv.clone()
        }
        _ => arrow_to_ch_type(data_type, field_nullable, Some(strict_opts))?,
    })
}

/// Normalizes a `ClickHouse` internal [`Type`] against an Arrow [`DataType`] to ensure
/// compatibility with Arrow array builders and schema creation.
///
/// This function addresses discrepancies between `ClickHouse` types and Arrow types, particularly
/// for string and binary types, which may vary depending on the `strings_as_strings` configuration.
/// For example, a `ClickHouse` `String` type may map to Arrow `DataType::Utf8` or
/// `DataType::Binary`, and this function ensures the internal type aligns with the Arrow type to
/// prevent builder mismatches (e.g., `BinaryBuilder` cannot handle `DataType::FixedSizeBinary`). It
/// also handles nested types like arrays and low cardinality types, preserving nullability.
///
/// # Arguments
/// - `type_`: The `ClickHouse` internal [`Type`] to normalize.
/// - `arrow_type`: The Arrow [`DataType`] to normalize against.
///
/// # Returns
/// - `Some(Type)`: The normalized `ClickHouse` type if normalization is needed (e.g., `String` to
///   `Binary` for `DataType::Binary`).
/// - `None`: If no normalization is needed or the types are incompatible.
#[expect(clippy::cast_sign_loss)]
pub(crate) fn normalize_type(type_: &Type, arrow_type: &DataType) -> Option<Type> {
    let nullable = type_.is_nullable();
    let type_ = match (type_.strip_null(), arrow_type) {
        (Type::String, DataType::Binary | DataType::BinaryView | DataType::LargeBinary) => {
            Some(Type::Binary)
        }
        (Type::String | Type::FixedSizedString(_) | Type::Binary, DataType::FixedSizeBinary(n)) => {
            Some(Type::FixedSizedBinary(*n as usize))
        }
        (Type::Binary, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            Some(Type::String)
        }
        (Type::FixedSizedBinary(n), DataType::Utf8 | DataType::Utf8View) => {
            Some(Type::FixedSizedString(*n))
        }
        (
            Type::Array(inner),
            DataType::List(inner_field)
            | DataType::ListView(inner_field)
            | DataType::LargeList(inner_field)
            | DataType::LargeListView(inner_field),
        ) => normalize_type(inner, inner_field.data_type()).map(Box::new).map(Type::Array),
        (Type::LowCardinality(inner), DataType::Dictionary(_, value_type)) => {
            normalize_type(inner, value_type).map(Box::new).map(Type::LowCardinality)
        }
        (
            Type::LowCardinality(inner),
            t @ (DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)),
        ) => normalize_type(inner, t).map(Box::new).map(Type::LowCardinality),
        (Type::Tuple(inner), DataType::Struct(inner_fields)) => {
            let mut deferred_vec: Option<Vec<Type>> = None;

            for (i, (inner_type, field)) in inner.iter().zip(inner_fields.iter()).enumerate() {
                if let Some(normalized_type) = normalize_type(inner_type, field.data_type()) {
                    // First time we need to normalize, create the vector and copy previous elements
                    if deferred_vec.is_none() {
                        let mut vec = Vec::with_capacity(inner.len());
                        vec.extend(inner[..i].iter().cloned());
                        deferred_vec = Some(vec);
                    }

                    // Add the normalized type
                    deferred_vec.as_mut().unwrap().push(normalized_type);
                } else if let Some(vec) = &mut deferred_vec {
                    // We've already started normalizing, so keep copying
                    vec.push(inner_type.clone());
                }
            }

            deferred_vec.map(Type::Tuple)
        }
        _ => return None,
    };

    if nullable { type_.map(Type::into_nullable) } else { type_ }
}

/// Convert an arrow [`arrow::datatypes::DataType`] to a clickhouse [`Type`].
///
/// NOTE: `ClickHouse` defaults to `UTC` for timezones, hence this function does as well.
#[expect(clippy::cast_sign_loss)]
#[expect(clippy::too_many_lines)]
pub(crate) fn arrow_to_ch_type(
    data_type: &DataType,
    mut is_nullable: bool,
    options: Option<ArrowOptions>,
) -> Result<Type> {
    let tz_map = |tz: Option<&str>| {
        tz.and_then(|s| chrono_tz::Tz::from_str(s).ok()).unwrap_or(chrono_tz::Tz::UTC)
    };

    // Don't use wildcards here to ensure all types are handled explicitly.
    let inner_type = match data_type {
        DataType::Int8 => Type::Int8,
        DataType::Int16 => Type::Int16,
        DataType::Int32 => Type::Int32,
        DataType::Int64 | DataType::Interval(_) => Type::Int64,
        DataType::UInt8 | DataType::Boolean => Type::UInt8,
        DataType::UInt16 => Type::UInt16,
        DataType::UInt32 => Type::UInt32,
        DataType::UInt64 => Type::UInt64,
        DataType::Float32 => Type::Float32,
        DataType::Float64 => Type::Float64,
        DataType::Decimal32(_, s) => Type::Decimal32(*s as usize),
        DataType::Decimal64(p, s) => match *p {
            p if p <= 9 => Type::Decimal32(*s as usize),
            _ => Type::Decimal64(*s as usize),
        },
        DataType::Decimal128(p, s) => match *p {
            p if p <= 9 => Type::Decimal32(*s as usize),
            p if p <= 18 => Type::Decimal64(*s as usize),
            p if p <= 38 => Type::Decimal128(*s as usize),
            _ => Type::Decimal256(*s as usize), // Fallback, though rare
        },
        DataType::Decimal256(_, s) => Type::Decimal256(*s as usize),
        // Whether Date32 maps to Date or Date32
        DataType::Date32 if options.is_some_and(|o| o.use_date32_for_date) => Type::Date32 ,
        DataType::Date32  => Type::Date,
        DataType::Time32(TimeUnit::Second)
        | DataType::Time64(TimeUnit::Second)
        | DataType::Duration(TimeUnit::Second) => Type::DateTime(chrono_tz::Tz::UTC),
        DataType::Date64
        | DataType::Duration(TimeUnit::Millisecond)
        | DataType::Time32(TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Millisecond) => Type::DateTime64(3, chrono_tz::Tz::UTC),
        DataType::Time64(TimeUnit::Microsecond) | DataType::Duration(TimeUnit::Microsecond) => {
            Type::DateTime64(6, chrono_tz::Tz::UTC)
        }
        DataType::Time64(TimeUnit::Nanosecond) | DataType::Duration(TimeUnit::Nanosecond) => {
            Type::DateTime64(9, chrono_tz::Tz::UTC)
        }
        DataType::Timestamp(TimeUnit::Second, tz) => Type::DateTime(tz_map(Some(tz.as_deref().unwrap_or("UTC")))),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            Type::DateTime64(3, tz_map(Some(tz.as_deref().unwrap_or("UTC"))))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            Type::DateTime64(6, tz_map(Some(tz.as_deref().unwrap_or("UTC"))))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => Type::DateTime64(9, tz_map(Some(tz.as_deref().unwrap_or("UTC")))),
        DataType::Time32(TimeUnit::Nanosecond) => Type::DateTime64(9, chrono_tz::Tz::UTC),
        DataType::FixedSizeBinary(s) => Type::FixedSizedBinary(*s as usize),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::String,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => {
            // Reject Nullable(Array(T)) unless configured to ignore
            if is_nullable && options.is_some_and(|o|
                o.strict_schema && !o.nullable_array_default_empty
            ) {
                return Err(Error::TypeConversion(
                    "ClickHouse does not support nullable Lists".to_string(),
                ));
            }

            Type::Array(Box::new(
                arrow_to_ch_type(f.data_type(), f.is_nullable(), options)?
            ))
        }
        DataType::Dictionary(_, value_type) => {
            if is_nullable && options.is_some_and(|o| o.strict_schema) {
                return Err(Error::TypeConversion(
                    "ClickHouse does not support nullable Dictionary".to_string(),
                ));
            }
            // Transpose nullability:
            // Nullable(LowCardinality(String)) -> LowCardinality(Nullable(String))
            let nullable = is_nullable;
            is_nullable = false;
            Type::LowCardinality(Box::new(arrow_to_ch_type(value_type, nullable, options)?))
        }
        DataType::Struct(fields) => {
            let ch_types = fields
                .iter()
                .map(|f| arrow_to_ch_type(f.data_type(), f.is_nullable(), options))
                .collect::<Result<_>>()?;
            Type::Tuple(ch_types)
        }
        DataType::Map(key, _) => {
            let DataType::Struct(inner) = key.data_type() else {
                return Err(Error::ArrowDeserialize(format!(
                    "Unexpected key type for map: {key:?}"
                )));
            };

            let (key_field, value_field) = if inner.len() >= 2 {
                (&inner[0], &inner[1])
            } else {
                return Err(Error::ArrowDeserialize(
                    "Map inner fields malformed".into(),
                ));
            };

            let key_type =
                arrow_to_ch_type(key_field.data_type(), key_field.is_nullable(), options)?;
            let value_type =
                arrow_to_ch_type(value_field.data_type(), value_field.is_nullable(), options)?;

            Type::Map(Box::new(key_type), Box::new(value_type))
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Type::Binary,
        // Fallbacks
        DataType::Time32(TimeUnit::Microsecond) => {
            // Invalid in Arrow; fallback to microsecond precision
            Type::DateTime64(6, chrono_tz::Tz::UTC)
        }
        DataType::Null
        | DataType::Float16
        | DataType::Union(_, _)
        // TODO: Support RunEndEncoded
        | DataType::RunEndEncoded(_, _) => {
            return Err(Error::ArrowUnsupportedType(format!(
                "Arrow data type is not supported: {data_type:?}"
            )));
        }
    };

    // ClickHouse doesn't support Nullable(Array) or Nullable(Map)
    Ok(if is_nullable && !matches!(inner_type, Type::Array(_) | Type::Map(_, _)) {
        Type::Nullable(Box::new(inner_type))
    } else {
        inner_type
    })
}

/// Convert a clickhouse [`Type`] to an arrow [`arrow::datatypes::DataType`].
///
/// This is exposed publicly to help with the conversion of `ClickHouse` types to `Arrow` types, for
/// instance when trying to build an `Arrow` `Schema` that will be used to deserialize data. The
/// internal `Type` representation drives deserialization, so this can be leveraged to align types
/// across the `ClickHouse` `Arrow` boundary.
///
/// # Errors
/// - Returns `Error::ArrowUnsupportedType` if the `ClickHouse` type is not supported by `Arrow`.
/// - Returns `Error::TypeConversion` if the `ClickHouse` type cannot be converted to an `Arrow`
///   type.
///
/// # Panics
/// Should not panic, invariants are checked before conversion, unless arrow API changes.
#[expect(clippy::too_many_lines)]
#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::cast_possible_wrap)]
pub fn ch_to_arrow_type(ch_type: &Type, options: Option<ArrowOptions>) -> Result<(DataType, bool)> {
    let mut is_null = ch_type.is_nullable();
    let inner_type = ch_type.strip_null();

    // Don't use wildcards here to ensure all types are handled explicitly.
    let arrow_type = match inner_type {
        // Primitives
        Type::Int8 => DataType::Int8,
        Type::Int16 => DataType::Int16,
        Type::Int32 => DataType::Int32,
        Type::Int64 => DataType::Int64,
        Type::UInt8 => DataType::UInt8,
        Type::UInt16 => DataType::UInt16,
        Type::UInt32 => DataType::UInt32,
        Type::UInt64 => DataType::UInt64,
        Type::Int128 | Type::UInt128 | Type::Ipv6 | Type::Uuid => DataType::FixedSizeBinary(16),
        Type::Int256 | Type::UInt256 => DataType::FixedSizeBinary(32),
        Type::Float32 => DataType::Float32,
        Type::Float64 => DataType::Float64,
        Type::Decimal32(s) => DataType::Decimal128(9, *s as i8),
        Type::Decimal64(s) => DataType::Decimal128(18, *s as i8),
        Type::Decimal128(s) => DataType::Decimal128(38, *s as i8),
        Type::Decimal256(s) => DataType::Decimal256(76, *s as i8),
        Type::String => {
            if options.is_some_and(|o| o.strings_as_strings) {
                DataType::Utf8
            } else {
                DataType::Binary
            }
        }
        Type::FixedSizedString(len) | Type::FixedSizedBinary(len) => {
            DataType::FixedSizeBinary(*len as i32)
        }
        Type::Binary => DataType::Binary,
        Type::Object => DataType::Utf8,
        Type::Date32 | Type::Date => DataType::Date32,
        Type::DateTime(tz) => DataType::Timestamp(TimeUnit::Second, Some(Arc::from(tz.name()))),
        Type::DateTime64(p, tz) => match p {
            0 => DataType::Timestamp(TimeUnit::Second, Some(Arc::from(tz.name()))),
            1..=3 => DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from(tz.name()))),
            4..=6 => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from(tz.name()))),
            7..=9 => DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(tz.name()))),
            _ => {
                return Err(Error::ArrowUnsupportedType(format!(
                    "DateTime64 precision must be 0-9, received {p}"
                )));
            }
        },
        Type::Ipv4 => DataType::FixedSizeBinary(4),
        Type::Array(inner_type) => {
            if is_null
                && options.is_some_and(|o| o.strict_schema && !o.nullable_array_default_empty)
            {
                return Err(Error::TypeConversion(
                    "ClickHouse does not support nullable Arrays".to_string(),
                ));
            }
            let (inner_arrow_type, is_null) = ch_to_arrow_type(inner_type, options)?;
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, inner_arrow_type, is_null)))
        }
        Type::Tuple(types) => {
            let fields: Vec<Field> = types
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    ch_to_arrow_type(t, options).map(|(arrow_type, is_null)| {
                        Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}{i}"), arrow_type, is_null)
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            DataType::Struct(fields.into())
        }
        Type::Map(key_type, value_type) => {
            let (key_arrow_type, _) = ch_to_arrow_type(key_type, options)?;
            let (value_arrow_type, is_null) = ch_to_arrow_type(value_type, options)?;
            DataType::Map(
                Arc::new(Field::new(
                    MAP_FIELD_NAME,
                    DataType::Struct(
                        vec![
                            Field::new(STRUCT_KEY_FIELD_NAME, key_arrow_type, false),
                            Field::new(STRUCT_VALUE_FIELD_NAME, value_arrow_type, is_null),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        Type::LowCardinality(inner_type) => {
            if is_null && options.is_some_and(|o| o.strict_schema) {
                return Err(Error::TypeConversion(
                    "ClickHouse does not support nullable LowCardinality".to_string(),
                ));
            }

            // LowCardinality itself cannot be nullable, so the nullability applies to the inner.
            is_null = inner_type.is_nullable();

            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ch_to_arrow_type(inner_type, options)?.0),
            )
        }
        Type::Enum8(_) => DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        Type::Enum16(_) => {
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8))
        }
        Type::Point | Type::Ring | Type::Polygon | Type::MultiPolygon => {
            // Normalize Geo types first - Infallible due to type check
            let normalized = normalize_geo_type(ch_type).unwrap();
            return ch_to_arrow_type(&normalized, options);
        }
        // Unwrapped above
        Type::Nullable(_) => unreachable!(),
    };

    Ok((arrow_type, is_null))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, TimeUnit};
    use chrono_tz::Tz;

    use super::*;

    #[test]
    fn test_i256_conversions() {
        // Test round-trip conversion for i256
        let arrow_i256 = i256::from_i128(123_456_789);
        let ch_i256: crate::i256 = arrow_i256.into();
        let back_to_arrow: i256 = ch_i256.into();
        assert_eq!(arrow_i256, back_to_arrow);

        // Test zero
        let arrow_i256 = i256::from_i128(0);
        let ch_i256: crate::i256 = arrow_i256.into();
        let back_to_arrow: i256 = ch_i256.into();
        assert_eq!(arrow_i256, back_to_arrow);

        // Test negative
        let arrow_i256 = i256::from_i128(-987_654_321);
        let ch_i256: crate::i256 = arrow_i256.into();
        let back_to_arrow: i256 = ch_i256.into();
        assert_eq!(arrow_i256, back_to_arrow);
    }

    #[test]
    fn test_normalize_type() {
        // String and binary conversions
        assert_eq!(normalize_type(&Type::String, &DataType::Binary), Some(Type::Binary));
        assert_eq!(normalize_type(&Type::Binary, &DataType::Utf8), Some(Type::String));
        assert_eq!(
            normalize_type(&Type::FixedSizedBinary(4), &DataType::Utf8),
            Some(Type::FixedSizedString(4))
        );
        assert_eq!(
            normalize_type(&Type::String, &DataType::FixedSizeBinary(8)),
            Some(Type::FixedSizedBinary(8))
        );

        // Array with normalized inner type
        let arrow_list =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Binary, false)));
        assert_eq!(
            normalize_type(&Type::Array(Box::new(Type::String)), &arrow_list),
            Some(Type::Array(Box::new(Type::Binary)))
        );

        // LowCardinality with normalized inner type
        let arrow_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(
            normalize_type(&Type::LowCardinality(Box::new(Type::Binary)), &arrow_dict),
            Some(Type::LowCardinality(Box::new(Type::String)))
        );

        let arrow_dict = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        assert_eq!(
            normalize_type(
                &(Type::LowCardinality(Box::new(Type::Binary)).into_nullable()),
                &arrow_dict
            ),
            Some(Type::LowCardinality(Box::new(Type::String)).into_nullable())
        );

        // Nullable with normalized inner type
        assert_eq!(
            normalize_type(&Type::Nullable(Box::new(Type::String)), &DataType::Binary),
            Some(Type::Nullable(Box::new(Type::Binary)))
        );

        // Direct match (no normalization needed)
        assert_eq!(normalize_type(&Type::Int32, &DataType::Int32), None);

        // Incompatible types
        assert_eq!(normalize_type(&Type::Int32, &DataType::Float64), None);
    }

    #[test]
    #[expect(clippy::too_many_lines)]
    fn test_arrow_to_ch_type() {
        // Primitives
        assert_eq!(arrow_to_ch_type(&DataType::Int8, false, None).unwrap(), Type::Int8);
        assert_eq!(arrow_to_ch_type(&DataType::UInt8, false, None).unwrap(), Type::UInt8);
        assert_eq!(arrow_to_ch_type(&DataType::Float64, false, None).unwrap(), Type::Float64);

        // Decimals
        assert_eq!(
            arrow_to_ch_type(&DataType::Decimal128(9, 2), false, None).unwrap(),
            Type::Decimal32(2)
        );
        assert_eq!(
            arrow_to_ch_type(&DataType::Decimal128(18, 4), false, None).unwrap(),
            Type::Decimal64(4)
        );
        assert_eq!(
            arrow_to_ch_type(&DataType::Decimal256(76, 6), false, None).unwrap(),
            Type::Decimal256(6)
        );

        // Dates & Timestamps
        assert_eq!(arrow_to_ch_type(&DataType::Date32, false, None).unwrap(), Type::Date);
        let datetimes = [
            arrow_to_ch_type(&DataType::Time32(TimeUnit::Second), false, None).unwrap(),
            arrow_to_ch_type(&DataType::Time64(TimeUnit::Second), false, None).unwrap(),
            arrow_to_ch_type(&DataType::Duration(TimeUnit::Second), false, None).unwrap(),
        ];
        for dt in datetimes {
            assert_eq!(dt, Type::DateTime(Tz::UTC));
        }

        let datetimes = [
            arrow_to_ch_type(&DataType::Date64, false, None).unwrap(),
            arrow_to_ch_type(&DataType::Duration(TimeUnit::Millisecond), false, None).unwrap(),
            arrow_to_ch_type(&DataType::Time32(TimeUnit::Millisecond), false, None).unwrap(),
            arrow_to_ch_type(&DataType::Time64(TimeUnit::Millisecond), false, None).unwrap(),
        ];
        for dt in datetimes {
            assert_eq!(dt, Type::DateTime64(3, Tz::UTC));
        }
        let datetimes = [
            arrow_to_ch_type(&DataType::Duration(TimeUnit::Microsecond), false, None).unwrap(),
            arrow_to_ch_type(&DataType::Time64(TimeUnit::Microsecond), false, None).unwrap(),
        ];
        for dt in datetimes {
            assert_eq!(dt, Type::DateTime64(6, Tz::UTC));
        }
        let datetimes = [
            arrow_to_ch_type(&DataType::Duration(TimeUnit::Nanosecond), false, None).unwrap(),
            arrow_to_ch_type(&DataType::Time32(TimeUnit::Nanosecond), false, None).unwrap(),
            arrow_to_ch_type(&DataType::Time64(TimeUnit::Nanosecond), false, None).unwrap(),
        ];
        for dt in datetimes {
            assert_eq!(dt, Type::DateTime64(9, Tz::UTC));
        }
        assert_eq!(
            arrow_to_ch_type(
                &DataType::Timestamp(TimeUnit::Second, Some(Arc::from("America/New_York"))),
                false,
                None
            )
            .unwrap(),
            Type::DateTime(Tz::America__New_York)
        );
        assert_eq!(
            arrow_to_ch_type(
                &DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("America/New_York"))),
                false,
                None
            )
            .unwrap(),
            Type::DateTime64(3, Tz::America__New_York)
        );
        assert_eq!(
            arrow_to_ch_type(
                &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("America/New_York"))),
                false,
                None
            )
            .unwrap(),
            Type::DateTime64(6, Tz::America__New_York)
        );
        assert_eq!(
            arrow_to_ch_type(
                &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("America/New_York"))),
                false,
                None
            )
            .unwrap(),
            Type::DateTime64(9, Tz::America__New_York)
        );

        // Strings and binaries
        let strings_types = [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8];
        for s in strings_types {
            assert_eq!(arrow_to_ch_type(&s, false, None).unwrap(), Type::String);
        }

        let binary_types = [DataType::Binary, DataType::BinaryView, DataType::LargeBinary];
        for s in binary_types {
            assert_eq!(arrow_to_ch_type(&s, false, None).unwrap(), Type::Binary);
        }
        assert_eq!(
            arrow_to_ch_type(&DataType::FixedSizeBinary(4), false, None).unwrap(),
            Type::FixedSizedBinary(4)
        );

        // Array/List
        let list_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let list_types = [
            DataType::List(Arc::clone(&list_field)),
            DataType::ListView(Arc::clone(&list_field)),
            DataType::LargeList(list_field),
        ];
        for l in list_types {
            assert_eq!(
                arrow_to_ch_type(&l, false, None).unwrap(),
                Type::Array(Box::new(Type::Int32))
            );
        }

        // LowCardinality
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(
            arrow_to_ch_type(&dict_type, false, None).unwrap(),
            Type::LowCardinality(Box::new(Type::String))
        );

        // Nullable
        assert_eq!(
            arrow_to_ch_type(&DataType::Int32, true, None).unwrap(),
            Type::Nullable(Box::new(Type::Int32))
        );

        // Error cases
        assert!(arrow_to_ch_type(&DataType::Null, false, None).is_err());
        assert!(arrow_to_ch_type(&DataType::Float16, false, None).is_err());
        assert!(
            arrow_to_ch_type(
                &DataType::RunEndEncoded(
                    Field::new("", DataType::Int32, false).into(),
                    Field::new("", DataType::Utf8, false).into()
                ),
                false,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn test_ch_to_arrow_type() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));

        // Primitives
        assert_eq!(ch_to_arrow_type(&Type::Int8, options).unwrap(), (DataType::Int8, false));
        assert_eq!(ch_to_arrow_type(&Type::UInt8, options).unwrap(), (DataType::UInt8, false));
        assert_eq!(ch_to_arrow_type(&Type::Float64, options).unwrap(), (DataType::Float64, false));

        // Decimals
        assert_eq!(
            ch_to_arrow_type(&Type::Decimal32(2), options).unwrap(),
            (DataType::Decimal128(9, 2), false)
        );
        assert_eq!(
            ch_to_arrow_type(&Type::Decimal256(6), options).unwrap(),
            (DataType::Decimal256(76, 6), false)
        );

        // Timestamps
        assert_eq!(
            ch_to_arrow_type(&Type::DateTime(Tz::UTC), options).unwrap(),
            (DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))), false)
        );
        assert_eq!(
            ch_to_arrow_type(&Type::DateTime64(6, Tz::America__New_York), options).unwrap(),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("America/New_York"))),
                false
            )
        );

        // Strings and binaries
        assert_eq!(ch_to_arrow_type(&Type::String, options).unwrap(), (DataType::Utf8, false));
        assert_eq!(
            ch_to_arrow_type(&Type::FixedSizedString(4), options).unwrap(),
            (DataType::FixedSizeBinary(4), false)
        );
        assert_eq!(
            ch_to_arrow_type(&Type::FixedSizedBinary(4), options).unwrap(),
            (DataType::FixedSizeBinary(4), false)
        );

        // Default: Utf8 -> Binary
        assert_eq!(ch_to_arrow_type(&Type::String, None).unwrap(), (DataType::Binary, false));
        // Arrow does not have a fixed sized string
        assert_eq!(
            ch_to_arrow_type(&Type::FixedSizedString(4), None).unwrap(),
            (DataType::FixedSizeBinary(4), false)
        );

        // Array
        assert_eq!(
            ch_to_arrow_type(&Type::Array(Box::new(Type::Int32)), options).unwrap(),
            (
                DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false))),
                false
            )
        );

        // LowCardinality
        assert_eq!(
            ch_to_arrow_type(&Type::LowCardinality(Box::new(Type::String)), None).unwrap(),
            (DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)), false)
        );

        // Tuple
        let tuple_type = Type::Tuple(vec![Type::Int32, Type::String]);
        let expected_struct = DataType::Struct(
            vec![
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, false),
                Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
            ]
            .into(),
        );
        assert_eq!(ch_to_arrow_type(&tuple_type, options).unwrap(), (expected_struct, false));

        // Map
        let map_type = Type::Map(Box::new(Type::String), Box::new(Type::Int32));
        let expected_map = DataType::Map(
            Arc::new(Field::new(
                MAP_FIELD_NAME,
                DataType::Struct(
                    vec![
                        Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false),
                        Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(ch_to_arrow_type(&map_type, options).unwrap(), (expected_map, false));

        // Nullable
        assert_eq!(
            ch_to_arrow_type(&Type::Nullable(Box::new(Type::Int32)), options).unwrap(),
            (DataType::Int32, true)
        );

        // Error case
        assert!(ch_to_arrow_type(&Type::DateTime64(10, Tz::UTC), options).is_err());
    }

    /// Tests `arrow_to_ch_type` for `Map(String, Nullable(Int32))` with outer nullability.
    #[test]
    fn test_arrow_to_ch_type_nullable_map() {
        let options = Some(ArrowOptions::default());
        let struct_field = Arc::new(Field::new(
            MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![
                Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false),
                Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, true),
            ])),
            false,
        ));
        let map_type = DataType::Map(Arc::clone(&struct_field), false);

        let ch_type = arrow_to_ch_type(&map_type, false, options).unwrap();
        assert_eq!(
            ch_type,
            Type::Map(Box::new(Type::String), Box::new(Type::Nullable(Box::new(Type::Int32))))
        );
    }

    /// Tests `ch_to_arrow_type` for `Nullable(Map(String, Int32))` to ensure round-trip
    /// consistency.
    #[test]
    fn test_ch_to_arrow_type_nullable_map() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let ch_type = Type::Map(Box::new(Type::String), Box::new(Type::Int32));
        let (arrow_type, is_nullable) = ch_to_arrow_type(&ch_type, options).unwrap();

        let expected_struct_field = Arc::new(Field::new(
            MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![
                Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false),
                Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false),
            ])),
            false,
        ));
        let expected_arrow_type = DataType::Map(Arc::clone(&expected_struct_field), false);

        assert_eq!(arrow_type, expected_arrow_type);
        assert!(!is_nullable);

        // Test with outer nullability
        let ch_type_nullable = Type::Nullable(Box::new(ch_type));
        let (arrow_type_nullable, is_nullable_nullable) =
            ch_to_arrow_type(&ch_type_nullable, options).unwrap();
        assert_eq!(arrow_type_nullable, expected_arrow_type);
        assert!(is_nullable_nullable);
    }

    /// Tests `arrow_to_ch_type` for `Struct(Nullable(Int32), String)` with outer nullability.
    #[test]
    fn test_roundtrip_struct() {
        // Use strings_as_strings to enable round trip
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let ch_type = Type::Tuple(vec![Type::Nullable(Box::new(Type::Int32)), Type::String]);
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}0"), DataType::Int32, true),
            Field::new(format!("{TUPLE_FIELD_NAME_PREFIX}1"), DataType::Utf8, false),
        ]));

        let (arrow_type, is_nullable) = ch_to_arrow_type(&ch_type, options).unwrap();
        assert_eq!(arrow_type, struct_type.clone());
        assert!(!is_nullable);

        let ch_type_back = arrow_to_ch_type(&struct_type, false, options).unwrap();
        assert_eq!(ch_type_back, ch_type);
    }

    /// Tests `ch_to_arrow_type` for `Nullable(Tuple(Int32, String))` to ensure round-trip
    /// consistency.
    #[test]
    fn test_roundtrip_tuple() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let ch_type = Type::Tuple(vec![Type::Int32, Type::String]);

        let expected_arrow_type = DataType::Struct(Fields::from(vec![
            Field::new("field_0", DataType::Int32, false),
            Field::new("field_1", DataType::Utf8, false),
        ]));
        let (arrow_type, is_nullable) = ch_to_arrow_type(&ch_type, options).unwrap();

        assert_eq!(arrow_type, expected_arrow_type);
        assert!(!is_nullable);

        let ch_type_back = arrow_to_ch_type(&expected_arrow_type, false, options).unwrap();
        assert_eq!(ch_type_back, ch_type);
    }

    /// Tests roundtrip for `Dictionary(Int32, Nullable(String))` to ensure inner
    /// nullability and default behavior for outer nullability.
    #[test]
    fn test_roundtrip_dictionary() {
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let field = Arc::new(Field::new("col", dict_type.clone(), false));
        let nullable_dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        let ch_type = arrow_to_ch_type(&nullable_dict_type, field.is_nullable(), None).unwrap();
        assert_eq!(ch_type, Type::LowCardinality(Box::new(Type::String)));

        // Test that the nullability is pushed down by default
        let ch_type_nullable = arrow_to_ch_type(&nullable_dict_type, true, None).unwrap();
        assert_eq!(
            ch_type_nullable,
            Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String))))
        );

        let ch_type_back = arrow_to_ch_type(&nullable_dict_type, false, None).unwrap();
        assert_eq!(ch_type_back, ch_type);

        let options_err = Some(ArrowOptions::default().with_strict_schema(true));
        assert!(arrow_to_ch_type(&nullable_dict_type, true, options_err).is_err());
    }

    /// Tests `ch_to_arrow_type` for `Array(Nullable(Array(Int32)))` to ensure round-trip
    /// consistency.
    #[test]
    fn test_roundtrip_nested_nullable_array() {
        let ch_type =
            Type::Array(Box::new(Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))))));
        let expected_nullable_list_field = Arc::new(Field::new(
            LIST_ITEM_FIELD_NAME,
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false))),
            true,
        ));
        let expected_arrow_type = DataType::List(Arc::clone(&expected_nullable_list_field));

        let (arrow_type, is_nullable) = ch_to_arrow_type(&ch_type, None).unwrap();
        assert_eq!(arrow_type, expected_arrow_type);
        assert!(!is_nullable);

        // Test with outer nullability
        let ch_type_nullable = Type::Nullable(Box::new(ch_type.clone()));
        let (arrow_type_nullable, is_nullable_nullable) =
            ch_to_arrow_type(&ch_type_nullable, None).unwrap();
        assert_eq!(arrow_type_nullable, expected_arrow_type);
        assert!(is_nullable_nullable);

        // Test roundtrip
        assert!(
            arrow_to_ch_type(
                &expected_arrow_type,
                true,
                Some(
                    ArrowOptions::default()
                        .with_strict_schema(true)
                        .with_nullable_array_default_empty(false)
                )
            )
            .is_err()
        );

        // Test conversion back strips nullable wrapper from arrays due to ClickHouse limitations
        // ClickHouse categorically rejects Nullable(Array(...)) at any level
        let ch_type_back = arrow_to_ch_type(&expected_arrow_type, false, None).unwrap();
        let expected_back = Type::Array(Box::new(Type::Array(Box::new(Type::Int32))));
        assert_eq!(ch_type_back, expected_back);
    }

    /// Tests `Nullable(LowCardinality(Int32))` round trip and failure when option is set.
    #[test]
    fn test_roundtrip_low_cardinality_int32() {
        let options_err = Some(ArrowOptions::default().with_strict_schema(true));
        let ch_type = Type::LowCardinality(Box::new(Type::Int32));
        let expected_arrow_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));

        let (arrow_type, is_nullable) = ch_to_arrow_type(&ch_type, None).unwrap();
        assert_eq!(arrow_type, expected_arrow_type);
        assert!(!is_nullable);

        let ch_type_nullable = Type::Nullable(Box::new(ch_type.clone()));
        assert!(ch_to_arrow_type(&ch_type_nullable, options_err).is_err());

        let ch_type_back = arrow_to_ch_type(&expected_arrow_type, is_nullable, None).unwrap();
        assert_eq!(ch_type_back, ch_type);

        assert!(arrow_to_ch_type(&expected_arrow_type, true, options_err).is_err());
    }

    /// Tests how `Nullable(LowCardinality(String))` is normalized to
    /// `LowCardinality(Nullable(String))` by default
    #[test]
    fn test_round_trip_low_cardinality_nullable() {
        let ch_type = Type::Nullable(Box::new(Type::LowCardinality(Box::new(Type::Nullable(
            Box::new(Type::String),
        )))));
        // ArrowOptions::strings_as_strings is not set, so Binary is expected
        let expected_arrow_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary));

        let (arrow_type, is_nullable) = ch_to_arrow_type(&ch_type, None).unwrap();
        assert_eq!(arrow_type, expected_arrow_type);

        // Nullable is maintained even though ClickHouse doesn't support this
        assert!(is_nullable);

        let ch_type_back = arrow_to_ch_type(&arrow_type, is_nullable, None).unwrap();
        assert_eq!(
            ch_type_back,
            Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::Binary))))
        );
    }

    #[test]
    #[expect(clippy::too_many_lines)]
    fn test_schema_conversion() {
        let arrow_options = Some(
            ArrowOptions::default()
                // Deserialize strings as Utf8, not Binary
                .with_strings_as_strings(true)
                // Deserialize Date as Date32
                .with_use_date32_for_date(true)
                // Ignore fields that ClickHouse doesn't support.
                .with_strict_schema(false),
        );

        // Setup: Create FieldRef instances for the schema
        let fields = [Field::new("string_field", DataType::Utf8, false),
            Field::new("binary_field", DataType::Binary, false),
            Field::new("nullable_string_field", DataType::Utf8, true),
            Field::new(
                "nullable_dict_field",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "nullable_dict_16_field",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("date_field", DataType::Date32, false),
            Field::new("int_field", DataType::Int32, false)];

        // Setup: Define SchemaConversions with enum mappings
        let mut conversions = HashMap::new();
        drop(conversions.insert(
            "string_field".to_string(),
            Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]),
        ));
        drop(conversions.insert(
            "binary_field".to_string(),
            Type::Enum16(vec![("x".to_string(), 1), ("y".to_string(), 2)]),
        ));
        drop(conversions.insert(
            "nullable_string_field".to_string(),
            Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]).into_nullable(),
        ));
        drop(conversions.insert(
            "nullable_dict_field".to_string(),
            Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]).into_nullable(),
        ));
        drop(conversions.insert(
            "nullable_dict_16_field".to_string(),
            Type::Enum16(vec![("x".to_string(), 1), ("y".to_string(), 2)]).into_nullable(),
        ));
        drop(conversions.insert("date_field".to_string(), Type::Date));
        drop(conversions.insert(
            "int_field".to_string(),
            Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]),
        ));

        // Test Case 1: Enum8 conversion from String
        let string_field = &fields[0];
        let result = schema_conversion(string_field, Some(&conversions), arrow_options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]));

        // Test Case 2: Enum16 conversion from Binary
        let binary_field = &fields[1];
        let result = schema_conversion(binary_field, Some(&conversions), arrow_options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Enum16(vec![("x".to_string(), 1), ("y".to_string(), 2)]));

        // Test Case 3: Nullable Enum8 conversion
        let nullable_string_field = &fields[2];
        let result = schema_conversion(nullable_string_field, Some(&conversions), arrow_options);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Type::Nullable(Box::new(Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)])))
        );

        // Test Case 4: Nullable Enum8 Dict conversion
        let nullable_string_dict_field = &fields[3];
        let result =
            schema_conversion(nullable_string_dict_field, Some(&conversions), arrow_options);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Type::Nullable(Box::new(Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)])))
        );

        // Test Case 5: Nullable Enum16 Dict conversion
        let nullable_string_dict_16_field = &fields[4];
        let result =
            schema_conversion(nullable_string_dict_16_field, Some(&conversions), arrow_options);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Type::Nullable(Box::new(Type::Enum16(vec![
                ("x".to_string(), 1),
                ("y".to_string(), 2)
            ])))
        );

        // Test Case 6: Date conversion
        let date_field = &fields[5];
        let result = schema_conversion(date_field, Some(&conversions), arrow_options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Date);

        // Test Case 7: Invalid Enum8 conversion (non-LowCardinality)
        let int_field = &fields[6];
        let result = schema_conversion(int_field, Some(&conversions), arrow_options);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "type conversion failure: expected LowCardinality(String) or String/Binary, found \
             Int32"
        );

        // Test Case 8: Baseline conversion without SchemaConversions
        let result = schema_conversion(string_field, None, arrow_options);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::String);

        // Test Case 9: Date mismatch error
        let mut bad_conversions = HashMap::new();
        drop(bad_conversions.insert("string_field".to_string(), Type::Date));
        let result = schema_conversion(string_field, Some(&bad_conversions), arrow_options);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "type conversion failure: expected Date or Date32, found String"
        );

        // Test Case 10: Strict options with use_date32_for_date
        let conversion_opts_date32 = arrow_options.map(|o| o.with_use_date32_for_date(true));
        let result = schema_conversion(date_field, None, conversion_opts_date32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Date32);
    }
}
