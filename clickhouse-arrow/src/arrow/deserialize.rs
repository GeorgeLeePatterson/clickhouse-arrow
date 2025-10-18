/// Deserialization logic for converting `ClickHouse`’s native format into Arrow arrays.
///
/// This module defines the `ClickHouseArrowDeserializer` trait and implements it for `Type`,
/// enabling the conversion of `ClickHouse`’s native format data into Arrow arrays (e.g.,
/// `Int32Array`, `StringArray`, `ListArray`). It is used by the [`super::ProtocolData`]
/// implementation in `arrow.rs` to deserialize data received from `ClickHouse` into
/// `RecordBatch`es.
///
/// The `deserialize` function dispatches to specialized modules based on the `Type` variant,
/// handling primitive types, strings, nullable types, arrays, low cardinality dictionaries, enums,
/// maps, and tuples. Each module processes the input data from a `ClickHouseRead` reader,
/// respecting nullability and maintaining deserialization state.
mod binary;
mod enums;
mod list;
mod low_cardinality;
mod map;
mod null;
mod primitive;
mod tuple;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

use super::builder::TypedBuilder;
use super::types::ch_to_arrow_type;
use crate::geo::normalize_geo_type;
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::{ArrowOptions, Error, Result, Type};

#[derive(Default)]
pub(crate) struct ArrowDeserializerState {
    pub(crate) builders: Vec<TypedBuilder>,
    pub(crate) buffer:   Vec<u8>,
    fields:              Vec<FieldRef>,
    arrays:              Vec<ArrayRef>,
}

impl ArrowDeserializerState {
    #[inline]
    pub(crate) fn with_capacity(&mut self, field_cap: usize, rows_cap: usize) -> &mut Self {
        if self.builders.capacity() < field_cap {
            self.builders.reserve(field_cap - self.builders.capacity());
        }
        if self.fields.capacity() < field_cap {
            self.fields.reserve(field_cap - self.fields.capacity());
        }
        if self.arrays.capacity() < field_cap {
            self.arrays.reserve(field_cap - self.arrays.capacity());
        }
        // Choose the size of i128 as an upper bound (i128)
        let min_buffer_size = rows_cap * 16;
        if self.buffer.capacity() < min_buffer_size {
            self.buffer.reserve(min_buffer_size - self.buffer.capacity());
        }
        self
    }

    #[inline]
    pub(crate) fn push_array(&mut self, array: ArrayRef) -> &mut Self {
        self.arrays.push(array);
        self
    }

    #[inline]
    pub(crate) fn push_field(&mut self, field: FieldRef) -> &mut Self {
        self.fields.push(field);
        self
    }

    pub(crate) fn take(&mut self) -> (Vec<FieldRef>, Vec<ArrayRef>) {
        (std::mem::take(&mut self.fields), std::mem::take(&mut self.arrays))
    }
}

macro_rules! opt_value {
    ($b:expr, $row:expr, $nulls:expr, $read:expr) => {{
        if $nulls.is_empty() || $nulls[$row] == 0 {
            $b.append_value($read);
        } else {
            let _value = $read;
            $b.append_null();
        }
    }};
    (ok => $b:expr, $row:expr, $nulls:expr, $read:expr) => {{
        if $nulls.is_empty() || $nulls[$row] == 0 {
            $b.append_value($read)?;
        } else {
            let _value = $read;
            $b.append_null();
        }
    }};
}

pub(super) use opt_value;

macro_rules! deser {
    ($b:expr, $rows:expr => {$($t:pat => $i:ident => { $st:expr }),+} $(_ => { $rem:expr })?) => {
        match $b {
        $( $t => {
            for i in 0..$rows {
                let $i = i;
                $st;
            }
        } )+
        $( _ => { $rem } )?
        }
    };
    (() => $b:expr => { $($t:pat => { $st:expr }),+ } $( _ => { $rem:expr } )?) => {
        match $b {
        $( $t => { $st } )+
        $( _ => { $rem } )?
        }
    };
}

pub(super) use deser;

macro_rules! deser_bulk {
    ($builder:expr, $reader:expr, $rows:expr, $nulls:expr, $buf:expr, $type:ty) => {{
        if $rows > 0 {
            let byte_count =
                $crate::arrow::deserialize::primitive::primitive_bulk!($reader, $rows, $buf, $type);
            let values: &[$type] = bytemuck::cast_slice(&$buf[..byte_count]);
            if $nulls.is_empty() {
                $builder.append_slice(values);
            } else {
                for (i, &value) in values.iter().enumerate() {
                    if $nulls[i] == 0 {
                        $builder.append_value(value);
                    } else {
                        $builder.append_null();
                    }
                }
            }
        }
    }};
    (raw; $builder:expr, $reader:expr, $rows:expr, $nulls:expr, $buf:expr, $t1:ty => $t2:ty) => {{
        if $rows > 0 {
            let byte_count =
                $crate::arrow::deserialize::primitive::primitive_bulk!($reader, $rows, $buf, $t1);
            let values: &[$t1] = bytemuck::cast_slice::<u8, $t1>(&$buf[..byte_count]);
            #[allow(clippy::cast_lossless)]
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_possible_wrap)]
            for (i, &value) in values.iter().enumerate() {
                if $nulls.is_empty() || $nulls[i] == 0 {
                    $builder.append_value(value as $t2);
                } else {
                    $builder.append_null();
                }
            }
        }
    }};
}
pub(super) use deser_bulk;

macro_rules! deser_bulk_async {
    ($builder:expr, $reader:expr, $rows:expr, $nulls:expr, $buf:expr, $type:ty) => {{
        use $crate::arrow::deserialize::primitive::primitive_bulk;
        if $rows > 0 {
            let byte_count = primitive_bulk!(tokio; $reader, $rows, $buf, $type);
            let values: &[$type] = bytemuck::cast_slice(&$buf[..byte_count]);
            if $nulls.is_empty() {
                $builder.append_slice(values);
            } else {
                for (i, &value) in values.iter().enumerate() {
                    if $nulls[i] == 0 {
                        $builder.append_value(value);
                    } else {
                        $builder.append_null();
                    }
                }
            }
        }
    }};
    (raw; $builder:expr, $reader:expr, $rows:expr, $nulls:expr, $buf:expr, $t1:ty => $t2:ty) => {{
        use $crate::arrow::deserialize::primitive::primitive_bulk;
        if $rows > 0 {
            let byte_count = primitive_bulk!(tokio; $reader, $rows, $buf, $t1);
            let values: &[$t1] = bytemuck::cast_slice::<u8, $t1>(&$buf[..byte_count]);
            #[allow(clippy::cast_lossless)]
            #[allow(clippy::cast_possible_wrap)]
            for (i, &value) in values.iter().enumerate() {
                if $nulls.is_empty() || $nulls[i] == 0 {
                    $builder.append_value(value as $t2);
                } else {
                    $builder.append_null();
                }
            }
        }
    }};
}
pub(super) use deser_bulk_async;

/// Trait for deserializing `ClickHouse`’s native format into Arrow arrays.
///
/// Implementations convert data from a `ClickHouseRead` reader into an `ArrayRef`, handling
/// nullability and maintaining deserialization state. The trait is used to map `ClickHouse `types
/// to Arrow data types and deserialize rows of data.
///
/// # Methods
/// - `arrow_type`: Maps the `ClickHouse `type to an Arrow `DataType` and nullability flag.
/// - `deserialize`: Reads data from the reader and constructs an `ArrayRef` for the specified
///   number of rows, using nulls and state.
pub(crate) trait ClickHouseArrowDeserializer {
    /// Maps the `ClickHouse `type to an Arrow `DataType` and nullability flag.
    ///
    /// # Arguments
    /// - `strings_as_strings`: If `true`, `ClickHouse ``String` types are mapped to Arrow `Utf8`;
    ///   otherwise, to `Binary`.
    ///
    /// # Returns
    /// A `Result` containing the `(DataType, is_nullable)` tuple or a `Error` if
    /// the type is unsupported.
    fn arrow_type(&self, options: Option<ArrowOptions>) -> Result<(DataType, bool)>;

    /// Deserializes data from a `ClickHouse `reader into an Arrow array.
    ///
    /// # Arguments
    /// - `reader`: The async reader providing the `ClickHouse `native format data.
    /// - `rows`: The number of rows to deserialize.
    /// - `nulls`: A slice indicating null values (`1` for null, `0` for non-null).
    /// - `state`: A mutable `DeserializerState` for maintaining deserialization context.
    ///
    /// # Returns
    /// A `Result` containing the deserialized `ArrayRef` or a `Error` if
    /// deserialization fails.
    async fn deserialize_arrow_async<R: ClickHouseRead>(
        &self,
        builder: &mut TypedBuilder,
        reader: &mut R,
        data_type: &DataType,
        rows: usize,
        nulls: &[u8],
        rbuffer: &mut Vec<u8>,
    ) -> Result<ArrayRef>;

    #[allow(dead_code)] // TODO: remove once synchronous Arrow path is fully retired
    fn deserialize_arrow<R: ClickHouseBytesRead>(
        &self,
        builder: &mut TypedBuilder,
        reader: &mut R,
        data_type: &DataType,
        rows: usize,
        nulls: &[u8],
        // Reusable row buffer
        rbuffer: &mut Vec<u8>,
    ) -> Result<ArrayRef>;
}

/// Deserializes a `ClickHouse ``Type` into an Arrow array.
///
/// This implementation dispatches to specialized modules based on the `Type` variant:
/// - Primitives: `Int8`, `Float64`, `Date`, `IPv4`, `String`, `Uuid`, etc. via
///   `primitive::deserialize`.
/// - Nullable types: `Nullable(inner)` via `null::deserialize`.
/// - Arrays: `Array(inner)` via `array::deserialize`.
/// - Low cardinality: `LowCardinality(inner)` via `low_cardinality::deserialize`.
/// - Enums: `Enum8`, `Enum16` via `enums::deserialize`.
/// - Maps: `Map(key, value)` via `map::deserialize`.
/// - Tuples: `Tuple(inner)` via `tuple::deserialize`.
///
/// # Examples
/// ```rust,ignore
/// use arrow::array::{ArrayRef, StringArray};
/// use clickhouse_arrow::native::types::{Type, DeserializerState};
/// use std::sync::Arc;
/// use tokio::io::Cursor;
///
/// let data = vec![
///     5,                              // Size
///     b'h', b'e', b'l', b'l', b'o',   // "hello"
///     3,                              // Size
///     b'f', b'o', b'o',               // "foo"
/// ];
/// let mut reader = Cursor::new(data);
///
/// let array = Type::String
///     .deserialize(&mut reader, 2, &[])
///     .await
///     .unwrap();
/// let expected = Arc::new(StringArray::from(vec!["hello", "foo"])) as ArrayRef;
/// assert_eq!(array.as_ref(), expected.as_ref());
/// ```
impl ClickHouseArrowDeserializer for Type {
    fn arrow_type(&self, options: Option<ArrowOptions>) -> Result<(DataType, bool)> {
        ch_to_arrow_type(self, options)
    }

    // TODO: Normalize all deserialize functions to take `self`
    //
    // TODO: Consider encoding a new null mask condition, nulls[i] > 1 == Skip entirely. This would
    // be useful in the case where Dictionaries will contain a None value for nullable values since
    // clickhouse prepends dictionary values with a value representing null (the default value).
    async fn deserialize_arrow_async<R: ClickHouseRead>(
        &self,
        builder: &mut TypedBuilder,
        reader: &mut R,
        data_type: &DataType,
        rows: usize,
        nulls: &[u8],
        rbuffer: &mut Vec<u8>,
    ) -> Result<ArrayRef> {
        Ok(match self {
            // Primitive types
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::Float32
            | Type::Float64
            | Type::Date
            | Type::Date32
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_) =>
                primitive::deserialize_async(self, builder, reader, rows, nulls, rbuffer).await?,
            // String/Binary
            Type::String
            | Type::FixedSizedString(_)
            | Type::Binary
            | Type::FixedSizedBinary(_)
            | Type::Object
            // Special Binary types
            | Type::Int128
            | Type::Int256
            | Type::UInt128
            | Type::UInt256
            | Type::Ipv6
            | Type::Uuid
            | Type::Ipv4 => binary::deserialize_async(self, builder, reader, rows, nulls).await?,
            // Nullable
            Type::Nullable(inner) => Box::pin(
                    null::deserialize_async(inner, builder, data_type, reader, rows, rbuffer)
            ).await?,
            // Array
            Type::Array(inner) => Box::pin(
                list::deserialize_async(inner, builder, data_type, reader, rows, nulls, rbuffer)
            ).await?,
            // LowCardinality
            Type::LowCardinality(inner) => Box::pin(low_cardinality::deserialize_async(
                inner,
                builder,
                data_type,
                reader,
                rows,
                nulls,
                rbuffer)
            ).await?,
            // Enum
            Type::Enum8(_) | Type::Enum16(_) =>
                enums::deserialize_async(self, builder, reader, rows, nulls).await?,
            // Map
            Type::Map(key, value) => Box::pin(map::deserialize_async(
                (key, value),
                builder,
                data_type,
                reader,
                rows,
                nulls,
                rbuffer
            )).await?,
            // Tuple
            Type::Tuple(inner) => Box::pin(
                tuple::deserialize_async(inner, builder, data_type, reader, rows, nulls, rbuffer)
            ).await?,
            // Geo types
            Type::Polygon | Type::MultiPolygon | Type::Point | Type::Ring => {
                // Geo types should be converted earlier, this is a fallback
                let normalized = normalize_geo_type(self).unwrap();
                let (normalized_dt, _) = ch_to_arrow_type(&normalized, None)?;
                Box::pin(normalized.deserialize_arrow_async(
                    builder,
                    reader,
                    &normalized_dt,
                    rows,
                    nulls,
                    rbuffer
                )).await?
            }
        })
    }

    #[expect(clippy::too_many_lines)]
    #[allow(dead_code)] // TODO: remove once synchronous Arrow path is fully retired
    fn deserialize_arrow<R: ClickHouseBytesRead>(
        &self,
        builder: &mut TypedBuilder,
        reader: &mut R,
        data_type: &DataType,
        rows: usize,
        nulls: &[u8],
        // Reusable row buffer
        rbuffer: &mut Vec<u8>,
    ) -> Result<ArrayRef> {
        use binary::binary;
        use primitive::primitive;

        type B = TypedBuilder;

        if let Type::Nullable(inner) = self {
            return null::deserialize(inner, builder, reader, data_type, rows, rbuffer);
        }

        // Bulk primitive cases (add these first)
        deser!(() => builder => {
            B::Int8(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i8) },
            B::Int16(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i16) },
            B::Int32(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i32) },
            B::Int64(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i64) },
            B::UInt8(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, u8) },
            B::UInt16(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, u16) },
            B::UInt32(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, u32) },
            B::UInt64(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, u64) },
            B::Float32(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, f32) },
            B::Float64(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, f64) },

            // Decimals (all fixed size)
            B::Decimal32(b) => { deser_bulk!(raw; b, reader, rows, nulls, rbuffer, i32 => i128) },
            B::Decimal64(b) => { deser_bulk!(raw; b, reader, rows, nulls, rbuffer, i64 => i128) },

            // Dates and DateTimes (all fixed size)
            B::Date(b) => { deser_bulk!(raw; b, reader, rows, nulls, rbuffer, u16 => i32) },
            B::DateTime(b) => { deser_bulk!(raw; b, reader, rows, nulls, rbuffer, u32 => i64) },
            B::DateTimeS(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i64) },
            B::DateTimeMs(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i64) },
            B::DateTimeMu(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i64) },
            B::DateTimeNano(b) => { deser_bulk!(b, reader, rows, nulls, rbuffer, i64) }}
            _ => {()});

        // Variable length or special handling
        deser!(builder, rows => {
            B::Date32(b) => i => { opt_value!(b, i, nulls, primitive!(Date32 => reader)) },
            B::Decimal128(b) => i => { opt_value!(b, i, nulls, primitive!(Decimal128 => reader)) },
            B::Decimal256(b) => i => { opt_value!(b, i, nulls, primitive!(Decimal256 => reader)) },
            B::String(b) => i => { opt_value!(b, i, nulls, binary!(String => reader)) },
            B::Object(b) => i => { opt_value!(b, i, nulls, binary!(Object => reader)) },
            B::Binary(b) => i => { opt_value!(b, i, nulls, binary!(Binary => reader)) }
        }
        // Pass through
        _ => {()});

        // Other special types
        deser!(() => builder => {
            // FixedSizeBinary
            B::FixedSizeBinary(b) => {{
                deser!(self.strip_null(), rows => {
                    Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => i => {
                        opt_value!(ok => b, i, nulls, binary!(FixedBinary(*n)=> reader))
                    },
                    Type::Ipv4 => i => { opt_value!(ok => b, i, nulls, binary!(Ipv4 => reader)) },
                    Type::Ipv6 => i => { opt_value!(ok => b, i, nulls, binary!(Ipv6 => reader)) },
                    Type::Uuid => i => {
                        opt_value!(ok => b, i, nulls, binary!(Fixed(16) => reader))
                    },
                    // Special numeric types that need to be read as bytes
                    Type::Int128 | Type::UInt128 => i => {
                        opt_value!(ok => b, i, nulls, binary!(Fixed(16)=> reader))
                    },
                    Type::Int256 | Type::UInt256 => i => {
                        opt_value!(ok => b, i, nulls, binary!(FixedRev(32) => reader))
                    }
                }
                _ => { return Err(Error::UnexpectedType(self.clone())) });
            }},
            // Enum
            B::Enum8(b) => {{
                let Type::Enum8(pairs) = self else {
                    return Err(Error::UnexpectedType(self.clone()));
                };
                for i in 0..rows {
                    let idx = primitive!(Int8 => reader);
                    if nulls.is_empty() || nulls[i] == 0 {
                        // Find index in pairs
                        b.append_value(&pairs.iter().find(|(_, key)| *key == idx).ok_or(
                            Error::ArrowDeserialize(format!(
                                "Invalid Enum16 index: {idx} not found in pairs"
                            ))
                        )?.0);
                    } else {
                        b.append_null();
                    }
                }
            }},
            B::Enum16(b) => {{
                let Type::Enum16(pairs) = self else {
                    return Err(Error::UnexpectedType(self.clone()));
                };
                for i in 0..rows {
                    let idx = primitive!(Int16 => reader);
                    if nulls.is_empty() || nulls[i] == 0 {
                        // Find index in pairs
                        b.append_value(&pairs.iter().find(|(_, key)| *key == idx).ok_or(
                            Error::ArrowDeserialize(format!(
                                "Invalid Enum16 index: {idx} not found in pairs"
                            ))
                        )?.0);
                    } else {
                        b.append_null();
                    }
                }
            }}
        }
        // Pass through
        _ => {()});

        // Then do nested/complicated that handle finish
        deser!(() => (builder, self.strip_null()) => {
            // Lists
            (B::List(b), Type::Array(inner)) => {
                list::deserialize(b, reader, inner, data_type, rows, nulls, rbuffer)
            },
            // LowCardinality
            (B::LowCardinality(b), Type::LowCardinality(inner)) => { low_cardinality::deserialize(
                b,
                reader,
                inner,
                data_type,
                rows,
                nulls,
                rbuffer
            )},
            // Tuple
            (B::Tuple(bds), Type::Tuple(inner)) => {
                tuple::deserialize(bds, reader, inner, data_type, rows, nulls, rbuffer)
            },
            // Map
            (TypedBuilder::Map((kb, vb)), Type::Map(key, value)) => { map::deserialize(
                (kb, vb),
                reader,
                (key, value),
                data_type,
                rows,
                nulls,
                rbuffer
            )},
            (builder, _) => {
                // Finish the builder and return an ArrayRef
                Ok(deser!(() => builder => {
                    // Primitives
                    B::Int8(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Int16(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Int32(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Int64(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::UInt8(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::UInt16(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::UInt32(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::UInt64(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Float32(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Float64(b) => { Arc::new(b.finish()) as ArrayRef },
                    // Decimals
                    B::Decimal32(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Decimal64(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Decimal128(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Decimal256(b) => { Arc::new(b.finish()) as ArrayRef },
                    // Dates
                    B::Date(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Date32(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::DateTime(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::DateTimeS(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::DateTimeMs(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::DateTimeMu(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::DateTimeNano(b) => { Arc::new(b.finish()) as ArrayRef }   ,
                    // String/Binary
                    B::String(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Object(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Binary(b) => { Arc::new(b.finish()) as ArrayRef },
                    // Fixed sized binary, Int256, UInt256, UUID, Ipv4, etc
                    B::FixedSizeBinary(b) => { Arc::new(b.finish()) as ArrayRef },
                    // Enums
                    B::Enum8(b) => { Arc::new(b.finish()) as ArrayRef },
                    B::Enum16(b) => { Arc::new(b.finish()) as ArrayRef }
                    // Rest are handled above
                }
                // Should not be possible. TODO: Somehow enforce exhaustive
                _ => { return Err(Error::ArrowDeserialize("Unexpected builder type".into())) }))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::DataType;

    use super::*;
    use crate::arrow::block::{LIST_ITEM_FIELD_NAME, MAP_FIELD_NAME};
    use crate::native::types::Type;

    /// Tests `arrow_type` for `Int32` (non-nullable).
    #[test]
    fn test_arrow_type_int32() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (data_type, is_nullable) = Type::Int32.arrow_type(options).unwrap();
        assert_eq!(data_type, DataType::Int32);
        assert!(!is_nullable);
    }

    /// Tests `arrow_type` for `Nullable(Int32)`.
    #[test]
    fn test_arrow_type_nullable_int32() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (data_type, is_nullable) =
            Type::Nullable(Box::new(Type::Int32)).arrow_type(options).unwrap();
        assert_eq!(data_type, DataType::Int32);
        assert!(is_nullable);
    }

    /// Tests `arrow_type` for `String` with `strings_as_strings=true`.
    #[test]
    fn test_arrow_type_string_utf8() {
        let options = Some(ArrowOptions::default().with_strings_as_strings(true));
        let (data_type, is_nullable) = Type::String.arrow_type(options).unwrap();
        assert_eq!(data_type, DataType::Utf8);
        assert!(!is_nullable);
    }

    /// Tests `arrow_type` for `String` with `strings_as_strings=false`.
    #[test]
    fn test_arrow_type_string_binary() {
        let (data_type, is_nullable) = Type::String.arrow_type(None).unwrap();
        assert_eq!(data_type, DataType::Binary);
        assert!(!is_nullable);
    }

    /// Tests deserialization of `Int32` array.
    #[tokio::test]
    async fn test_deserialize_int32() {
        let input = vec![
            // Values: [1, 2, 3]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Int32;
        let data_type = DataType::Int32;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .await
            .unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Nullable(Int32)` array with nulls.
    #[tokio::test]
    async fn test_deserialize_nullable_int32() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: [1, 0, 3]
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Nullable(Box::new(Type::Int32));
        let data_type = DataType::Int32;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .await
            .unwrap();
        let expected = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `String` array.
    #[tokio::test]
    async fn test_deserialize_string() {
        let input = vec![
            // Values: ["hello", "", "world"]
            5, b'h', b'e', b'l', b'l', b'o', // "hello"
            0,    // ""
            5, b'w', b'o', b'r', b'l', b'd', // "world"
        ];
        let mut reader = Cursor::new(input);
        let type_ = Type::String;
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .await
            .unwrap();
        let expected = Arc::new(StringArray::from(vec!["hello", "", "world"])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Nullable(String)` array with nulls.
    #[tokio::test]
    async fn test_deserialize_nullable_string() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: ["a", "", "c"]
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Nullable(Box::new(Type::String));
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .await
            .unwrap();
        let expected = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Array(Int32)` with non-nullable inner values.
    #[tokio::test]
    async fn test_deserialize_array_int32() {
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);

        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let type_ = Type::Array(Box::new(Type::Int32));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .await
            .unwrap();
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Array(Int32))` with null arrays.
    #[tokio::test]
    async fn test_deserialize_nullable_array_int32() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Offsets: [2, 2, 5] (skipping first 0, null array repeats offset)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (null)
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);

        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true)));
        let type_ = Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .await
            .unwrap();
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 2, 5]);
        assert_eq!(list_array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![
            true, false, true
        ]);
    }

    /// Tests deserialization of `Map(String, Int32)` with non-nullable key-value pairs.
    #[tokio::test]
    async fn test_deserialize_map_string_int32() {
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys: ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let data_type = DataType::Map(
            Arc::new(Field::new(
                MAP_FIELD_NAME,
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Int32, true),
                ])),
                false,
            )),
            true,
        );
        let type_ = Type::Map(Box::new(Type::String), Box::new(Type::Int32));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .await
            .unwrap();
        let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(map_array.len(), 3);
        assert_eq!(keys, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(map_array.nulls(), None);
    }

    /// Tests deserialization of `Int32` array with zero rows.
    #[tokio::test]
    async fn test_deserialize_int32_zero_rows() {
        let input = vec![];
        let mut reader = Cursor::new(input);
        let type_ = Type::Int32;
        let data_type = DataType::Int32;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 0, &[], &mut vec![])
            .await
            .unwrap();
        let expected = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    #[tokio::test]
    async fn test_deserialize_list_zero_rows() {
        let input = vec![];
        let mut reader = Cursor::new(input);
        let data_type = DataType::List(Arc::new(Field::new("", DataType::Int32, false)));
        let type_ = Type::Array(Box::new(Type::Int32));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 0, &[], &mut vec![])
            .await
            .unwrap();
        let list_array = array.as_any().downcast_ref::<ListArray>();
        assert!(list_array.is_some());
        let list_array = list_array.unwrap();
        assert!(list_array.is_empty());
    }

    #[tokio::test]
    async fn test_deserialize_lowcard_zero_rows() {
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            0, 0, 0, 0, 0, 0, 0, 0, // Dict size: 0
            0, 0, 0, 0, 0, 0, 0, 0, // Key count: 0
        ];
        let mut reader = Cursor::new(input);
        let data_type = DataType::Dictionary(DataType::Int32.into(), DataType::Binary.into());
        let type_ = Type::LowCardinality(Box::new(Type::Binary));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow_async(&mut builder, &mut reader, &data_type, 0, &[], &mut vec![])
            .await
            .unwrap();
        let array = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>();
        assert!(array.is_some());
        let array = array.unwrap();
        assert!(array.is_empty());
    }
}

#[cfg(test)]
mod tests_sync {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::DataType;

    use super::*;
    use crate::arrow::block::{LIST_ITEM_FIELD_NAME, MAP_FIELD_NAME};
    use crate::native::types::Type;

    /// Tests deserialization of `Int32` array.
    #[test]
    fn test_deserialize_int32() {
        let input = vec![
            // Values: [1, 2, 3]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);

        let data_type = DataType::Int32;
        let type_ = Type::Int32;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .unwrap();
        let expected = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Nullable(Int32)` array with nulls.
    #[test]
    fn test_deserialize_nullable_int32() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: [1, 0, 3]
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Nullable(Box::new(Type::Int32));
        let data_type = DataType::Int32;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .unwrap();
        let expected = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `String` array.
    #[test]
    fn test_deserialize_string() {
        let input = vec![
            // Values: ["hello", "", "world"]
            5, b'h', b'e', b'l', b'l', b'o', // "hello"
            0,    // ""
            5, b'w', b'o', b'r', b'l', b'd', // "world"
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::String;
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .unwrap();
        let expected = Arc::new(StringArray::from(vec!["hello", "", "world"])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Nullable(String)` array with nulls.
    #[test]
    fn test_deserialize_nullable_string() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: ["a", "", "c"]
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Nullable(Type::String.into());
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .unwrap();
        let expected = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    /// Tests deserialization of `Array(Int32)` with non-nullable inner values.
    #[test]
    fn test_deserialize_array_int32() {
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Array(Box::new(Type::Int32));
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .unwrap();
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(list_array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Array(Int32))` with null arrays.
    #[test]
    fn test_deserialize_nullable_array_int32() {
        let input = vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Offsets: [2, 2, 5] (skipping first 0, null array repeats offset)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (null)
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))));
        let data_type =
            DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, true)));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .unwrap();
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list_array.values().as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(list_array.len(), 3);
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(list_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 2, 5]);
        assert_eq!(list_array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![
            true, false, true
        ]);
    }

    /// Tests deserialization of `Map(String, Int32)` with non-nullable key-value pairs.
    #[test]
    fn test_deserialize_map_string_int32() {
        let input = vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys: ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ];
        let mut reader = Cursor::new(input);
        let data_type = DataType::Map(
            Arc::new(Field::new(
                MAP_FIELD_NAME,
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Int32, true),
                ])),
                false,
            )),
            true,
        );
        let type_ = Type::Map(Box::new(Type::String), Box::new(Type::Int32));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 3, &[], &mut vec![])
            .unwrap();
        let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
        let struct_array = map_array.entries().as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_array.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let values = struct_array.column(1).as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(map_array.len(), 3);
        assert_eq!(keys, &StringArray::from(vec!["a", "b", "c", "d", "e"]));
        assert_eq!(values, &Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_eq!(map_array.offsets().iter().copied().collect::<Vec<i32>>(), vec![0, 2, 3, 5]);
        assert_eq!(map_array.nulls(), None);
    }

    /// Tests deserialization of `Int32` array with zero rows.
    #[test]
    fn test_deserialize_int32_zero_rows() {
        let mut reader = Cursor::new(Vec::new());
        let type_ = Type::Int32;
        let data_type = DataType::Int32;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 0, &[], &mut vec![])
            .unwrap();
        let expected = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_deserialize_list_zero_rows() {
        let input = vec![];
        let mut reader = Cursor::new(input);
        let data_type = DataType::List(Arc::new(Field::new("", DataType::Int32, false)));
        let type_ = Type::Array(Box::new(Type::Int32));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 0, &[], &mut vec![])
            .unwrap();
        let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let expected = Arc::new(ListArray::new_null(field, 0)) as ArrayRef;
        assert_eq!(array.as_ref(), expected.as_ref());
    }

    #[test]
    fn test_deserialize_lowcard_zero_rows() {
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            0, 0, 0, 0, 0, 0, 0, 0, // Dict size: 0
            0, 0, 0, 0, 0, 0, 0, 0, // Key count: 0
        ];
        let mut reader = Cursor::new(input);
        let data_type = DataType::Dictionary(DataType::Int32.into(), DataType::Binary.into());
        let type_ = Type::LowCardinality(Box::new(Type::Binary));
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let array = type_
            .deserialize_arrow(&mut builder, &mut reader, &data_type, 0, &[], &mut vec![])
            .unwrap();
        let array = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>();
        assert!(array.is_some());
        let array = array.unwrap();
        assert!(array.is_empty());
    }
}
