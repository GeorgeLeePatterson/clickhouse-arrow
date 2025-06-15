#![expect(clippy::cast_possible_truncation)]
/// Deserialization logic for `ClickHouse` primitive types into Arrow arrays.
///
/// This module provides a function to deserialize `ClickHouse`’s native format for primitive
/// types into Arrow arrays, such as `Int8Array` for `Int8`, `Float64Array` for `Float64`,
/// `Date32Array` for `Date`, `TimestampSecondArray` for `DateTime`, and `Decimal128Array` or
/// `Decimal256Array` for decimal types. It is used by the `ClickhouseArrowDeserializer`
/// implementation in `deserialize.rs` to handle numeric, date/time, and decimal types,
/// supporting both nullable and non-nullable variants.
///
/// The `deserialize` function dispatches to specialized logic based on the `Type` variant,
/// reading fixed-size values (e.g., `i8`, `u32`, `f64`) or byte arrays (for decimals) from the
/// input stream. It respects the `ClickHouse` null mask convention (`1`=null, `0`=non-null)
/// and includes default values for nulls (e.g., `0` for numeric types, zeroed buffers for
/// decimals). The implementation aligns with ClickHouse’s native format, using little-endian
/// for most numeric types and big-endian with byte reversal for `Decimal256`.
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

use crate::deserialize::DAYS_1900_TO_1970;
use crate::formats::DeserializerState;
use crate::io::ClickhouseRead;
use crate::{Error, Result, Type};

/// Macro to deserialize primitive types into Arrow arrays.
///
/// Generates code to read values (e.g., `i8`, `u32`, `f64`) for each row, building an Arrow array
/// using the specified builder type (e.g., `PrimitiveBuilder<Int32Type>`,
/// `Decimal128Builder`). Handles nullability via the null mask (`1`=null, `0`=non-null),
/// appending nulls for null values, assuming default values (e.g., `0`) are included in the
/// input stream.
///
/// # Arguments
/// - `$builder_type`: The Arrow builder type (e.g., `PrimitiveBuilder<Int32Type>`).
/// - `$rows`: The number of rows to deserialize.
/// - `$null_mask`: The null mask slice (`1`=null, `0`=non-null).
/// - `$st`: The statement to read and convert the value (e.g., `read_i32_le`).
/// - `builder => $builder`: Alternative syntax for builders requiring initialization (e.g.,
///   `TimestampSecondBuilder` with timezone).
macro_rules! deserialize_primitive {
    ($builder:expr, $rows:expr, $null_mask:expr, { $st:expr }) => {{
        #[allow(unused_imports)]
        use ::tokio::io::AsyncReadExt as _;

        for i in 0..$rows {
            let value = $st;
            if $null_mask.is_empty() || $null_mask[i] == 0 {
                $builder.append_value(value);
            } else {
                $builder.append_null();
            }
        }
        Ok(::std::sync::Arc::new($builder.finish()) as ArrayRef)
    }}; /* (builder => $builder:expr, $rows:expr, $null_mask:expr, { $st:expr }) => {{
         *     #[allow(unused_imports)]
         *     use ::tokio::io::AsyncReadExt as _; */

        /*     for i in 0..$rows {
         *         let value = $st;
         *         if $null_mask.is_empty() || $null_mask[i] == 0 {
         *             $builder.append_value(value);
         *         } else {
         *             $builder.append_null();
         *         }
         *     }
         *     Ok(::std::sync::Arc::new($builder.finish()) as ArrayRef)
         * }}; */
}

/// Deserializes a `ClickHouse` primitive type into an Arrow array.
///
/// Reads fixed-size values (e.g., `i8`, `u32`, `f64`) or byte arrays (for decimals) from the
/// input stream, constructing an Arrow array based on the `Type` variant. Supports numeric
/// types (`Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`,
/// `Float64`), date/time types (`Date`, `DateTime`, `DateTime64`), and decimal types
/// (`Decimal32`, `Decimal64`, `Decimal128`, `Decimal256`). Handles nullability via the provided
/// null mask (`1`=null, `0`=non-null), producing default values (e.g., `0` for numeric types,
/// zeroed buffers for decimals) for nulls. Aligns with ClickHouse’s native format, using
/// little-endian for most numeric types and big-endian with byte reversal for `Decimal256`.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` type to deserialize (e.g., `Int32`, `DateTime`).
/// - `reader`: The async reader providing the `ClickHouse` native format data.
/// - `rows`: The number of rows to deserialize.
/// - `null_mask`: A slice indicating null values (`1` for null, `0` for non-null).
/// - `_state`: A mutable `DeserializerState` for deserialization context (unused).
///
/// # Returns
/// A `Result` containing the deserialized `ArrayRef` or a `Error` if
/// deserialization fails.
///
/// # Errors
/// - Returns `Io` if reading from the reader fails (e.g., EOF).
/// - Returns `ArrowDeserialize` if the `type_hint` is unsupported or data is malformed.
///
/// # Example
/// ```rust,ignore
/// use arrow::array::{ArrayRef, Int32Array};
/// use clickhouse_native::types::{Type, DeserializerState};
/// use std::io::Cursor;
/// use std::sync::Arc;
///
/// let data = vec![
///     // Int32: [1, 2, 3]
///     1, 0, 0, 0, // 1
///     2, 0, 0, 0, // 2
///     3, 0, 0, 0, // 3
/// ];
/// let mut reader = Cursor::new(data);
/// let mut state = DeserializerState::default();
/// let array = crate::arrow::deserialize::primitive::deserialize(
///     &Type::Int32,
///     &mut reader,
///     3,
///     &[],
///     &mut state,
/// )
/// .await
/// .unwrap();
/// let expected = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
/// assert_eq!(array.as_ref(), expected.as_ref());
/// ```
#[expect(clippy::too_many_lines)]
pub(crate) async fn deserialize<R: ClickhouseRead>(
    type_hint: &Type,
    reader: &mut R,
    rows: usize,
    null_mask: &[u8],
    _state: &mut DeserializerState,
) -> Result<ArrayRef> {
    match type_hint.strip_null() {
        // Numeric, DateTime, Decimal
        Type::Int8 => {
            let mut builder = PrimitiveBuilder::<Int8Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { { reader.read_i8().await? } })
        }
        Type::Int16 => {
            let mut builder = PrimitiveBuilder::<Int16Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_i16_le().await? })
        }
        Type::Int32 => {
            let mut builder = PrimitiveBuilder::<Int32Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_i32_le().await? })
        }
        Type::Int64 => {
            let mut builder = PrimitiveBuilder::<Int64Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_i64_le().await? })
        }
        Type::UInt8 => {
            let mut builder = PrimitiveBuilder::<UInt8Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_u8().await? })
        }
        Type::UInt16 => {
            let mut builder = PrimitiveBuilder::<UInt16Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_u16_le().await? })
        }
        Type::UInt32 => {
            let mut builder = PrimitiveBuilder::<UInt32Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_u32_le().await? })
        }
        Type::UInt64 => {
            let mut builder = PrimitiveBuilder::<UInt64Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_u64_le().await? })
        }
        Type::Float32 => {
            let mut builder = PrimitiveBuilder::<Float32Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_f32_le().await? })
        }
        Type::Float64 => {
            let mut builder = PrimitiveBuilder::<Float64Type>::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, { reader.read_f64_le().await? })
        }
        Type::Date => {
            let mut builder = Date32Builder::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, {
                {
                    reader.read_u16_le().await.map(i32::from)? // Cast to i32 for Arrow Date32
                }
            })
        }
        Type::Date32 => {
            let mut builder = Date32Builder::with_capacity(rows);
            deserialize_primitive!(&mut builder, rows, null_mask, {
                {
                    let days = reader.read_i32_le().await?;
                    days - DAYS_1900_TO_1970 // Adjust to days since 1970-01-01
                }
            })
        }
        Type::DateTime(tz) => {
            let mut b =
                TimestampSecondBuilder::with_capacity(rows).with_timezone(Arc::from(tz.name()));
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    reader.read_u32_le().await.map(i64::from)? // Cast to i64 for Arrow TimestampSecond
                }
            })
        }
        Type::DateTime64(0, tz) => {
            let mut b = TimestampMillisecondBuilder::with_capacity(rows)
                .with_timezone(Arc::from(tz.name()));
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    reader.read_i64_le().await? // Cast to i64 for Arrow TimestampMillisecond
                }
            })
        }
        Type::DateTime64(1..=3, tz) => {
            let mut b = TimestampMillisecondBuilder::with_capacity(rows)
                .with_timezone(Arc::from(tz.name()));
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    reader.read_i64_le().await? // Cast to i64 for Arrow TimestampMillisecond
                }
            })
        }
        Type::DateTime64(4..=6, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows)
                .with_timezone(Arc::from(tz.name()));
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    reader.read_i64_le().await? // Cast to i64 for Arrow TimestampMicrosecond
                }
            })
        }
        Type::DateTime64(7..=9, tz) => {
            let mut b =
                TimestampNanosecondBuilder::with_capacity(rows).with_timezone(Arc::from(tz.name()));
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    reader.read_i64_le().await? // Cast to i64 for Arrow TimestampNanosecond
                }
            })
        }
        Type::Decimal32(s) => {
            let mut b =
                Decimal128Builder::with_capacity(rows).with_precision_and_scale(9, *s as i8)?;
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    reader.read_i32_le().await.map(i128::from)? // Decimal128
                }
            })
        }
        Type::Decimal64(s) => {
            let mut b =
                Decimal128Builder::with_capacity(rows).with_precision_and_scale(18, *s as i8)?;
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    reader.read_i64_le().await.map(i128::from)? // Decimal128
                }
            })
        }
        Type::Decimal128(s) => {
            let mut b =
                Decimal128Builder::with_capacity(rows).with_precision_and_scale(38, *s as i8)?;
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    let mut buf = [0u8; 16];
                    let _ = reader.read_exact(&mut buf).await?;
                    i128::from_le_bytes(buf)
                }
            })
        }
        Type::Decimal256(s) => {
            let mut b =
                Decimal256Builder::with_capacity(rows).with_precision_and_scale(76, *s as i8)?;
            deserialize_primitive!(&mut b, rows, null_mask, {
                {
                    let mut buf = [0u8; 32];
                    let _ = reader.read_exact(&mut buf).await?;
                    buf.reverse();
                    i256::from_le_bytes(buf)
                }
            })
        }
        _ => Err(Error::ArrowDeserialize(format!("Expected primitive, got {type_hint:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::*;
    use arrow::datatypes::*;
    use chrono_tz::Tz;

    use super::*;
    use crate::native::types::Type;

    /// Tests deserialization of `Int8` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_int8() {
        let type_hint = Type::Int8;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![1, 2, 3]; // Int8: [1, 2, 3]
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Int8");
        let array = result.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(array, &Int8Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Int8)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int8() {
        let type_hint = Type::Nullable(Box::new(Type::Int8));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![1, 0, 3]; // Int8: [1, 0, 3]
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int8)");
        let array = result.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(array, &Int8Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Int16` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_int16() {
        let type_hint = Type::Int16;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Int16: [1, 2, 3] (little-endian)
            1, 0, 2, 0, 3, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Int16");
        let array = result.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(array, &Int16Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Int16)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int16() {
        let type_hint = Type::Nullable(Box::new(Type::Int16));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Int16: [1, 0, 3] (little-endian)
            1, 0, 0, 0, 3, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int16)");
        let array = result.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(array, &Int16Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Int32` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_int32() {
        let type_hint = Type::Int32;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Int32: [1, 2, 3] (little-endian)
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Int32");
        let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array, &Int32Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Int32)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int32() {
        let type_hint = Type::Nullable(Box::new(Type::Int32));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Int32: [1, 0, 3] (little-endian)
            1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int32)");
        let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array, &Int32Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Int64` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_int64() {
        let type_hint = Type::Int64;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Int64: [1, 2, 3] (little-endian)
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Int64");
        let array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(array, &Int64Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Int64)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int64() {
        let type_hint = Type::Nullable(Box::new(Type::Int64));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Int64: [1, 0, 3] (little-endian)
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int64)");
        let array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(array, &Int64Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `UInt8` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_uint8() {
        let type_hint = Type::UInt8;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![1, 2, 3]; // UInt8: [1, 2, 3]
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize UInt8");
        let array = result.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(array, &UInt8Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(UInt8)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_uint8() {
        let type_hint = Type::Nullable(Box::new(Type::UInt8));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![1, 0, 3]; // UInt8: [1, 0, 3]
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(UInt8)");
        let array = result.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(array, &UInt8Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `UInt16` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_uint16() {
        let type_hint = Type::UInt16;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // UInt16: [1, 2, 3] (little-endian)
            1, 0, 2, 0, 3, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize UInt16");
        let array = result.as_any().downcast_ref::<UInt16Array>().unwrap();
        assert_eq!(array, &UInt16Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(UInt16)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_uint16() {
        let type_hint = Type::Nullable(Box::new(Type::UInt16));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // UInt16: [1, 0, 3] (little-endian)
            1, 0, 0, 0, 3, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(UInt16)");
        let array = result.as_any().downcast_ref::<UInt16Array>().unwrap();
        assert_eq!(array, &UInt16Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `UInt32` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_uint32() {
        let type_hint = Type::UInt32;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // UInt32: [1, 2, 3] (little-endian)
            1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize UInt32");
        let array = result.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array, &UInt32Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(UInt32)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_uint32() {
        let type_hint = Type::Nullable(Box::new(Type::UInt32));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // UInt32: [1, 0, 3] (little-endian)
            1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(UInt32)");
        let array = result.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array, &UInt32Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `UInt64` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_uint64() {
        let type_hint = Type::UInt64;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // UInt64: [1, 2, 3] (little-endian)
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize UInt64");
        let array = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array, &UInt64Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(UInt64)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_uint64() {
        let type_hint = Type::Nullable(Box::new(Type::UInt64));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // UInt64: [1, 0, 3] (little-endian)
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(UInt64)");
        let array = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array, &UInt64Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Float32` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_float32() {
        let type_hint = Type::Float32;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Float32: [1.0, 2.0, 3.0] (little-endian)
            0, 0, 128, 63, // 1.0
            0, 0, 0, 64, // 2.0
            0, 0, 64, 64, // 3.0
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Float32");
        let array = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(array, &Float32Array::from(vec![1.0, 2.0, 3.0]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Float32)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_float32() {
        let type_hint = Type::Nullable(Box::new(Type::Float32));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Float32: [1.0, 0.0, 3.0] (little-endian)
            0, 0, 128, 63, // 1.0
            0, 0, 0, 0, // 0.0 (null)
            0, 0, 64, 64, // 3.0
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Float32)");
        let array = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(array, &Float32Array::from(vec![Some(1.0), None, Some(3.0)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Float64` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_float64() {
        let type_hint = Type::Float64;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Float64: [1.0, 2.0, 3.0] (little-endian)
            0, 0, 0, 0, 0, 0, 240, 63, // 1.0
            0, 0, 0, 0, 0, 0, 0, 64, // 2.0
            0, 0, 0, 0, 0, 0, 8, 64, // 3.0
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Float64");
        let array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array, &Float64Array::from(vec![1.0, 2.0, 3.0]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Float64)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_float64() {
        let type_hint = Type::Nullable(Box::new(Type::Float64));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Float64: [1.0, 0.0, 3.0] (little-endian)
            0, 0, 0, 0, 0, 0, 240, 63, // 1.0
            0, 0, 0, 0, 0, 0, 0, 0, // 0.0 (null)
            0, 0, 0, 0, 0, 0, 8, 64, // 3.0
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Float64)");
        let array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array, &Float64Array::from(vec![Some(1.0), None, Some(3.0)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Date` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_date() {
        let type_hint = Type::Date;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Date: [1, 2, 3] (days since 1970-01-01, little-endian u16)
            1, 0, 2, 0, 3, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Date");
        let array = result.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(array, &Date32Array::from(vec![1, 2, 3]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Date)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_date() {
        let type_hint = Type::Nullable(Box::new(Type::Date));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Date: [1, 0, 3] (little-endian u16)
            1, 0, 0, 0, 3, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Date)");
        let array = result.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(array, &Date32Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }
    /// Tests deserialization of `DateTime` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_datetime() {
        let type_hint = Type::DateTime(Tz::UTC);
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // DateTime: [1000, 2000, 3000] (seconds since 1970-01-01, little-endian u32)
            232, 3, 0, 0, // 1000
            208, 7, 0, 0, // 2000
            184, 11, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize DateTime");
        let array = result.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
        let expected =
            TimestampSecondArray::from(vec![1000, 2000, 3000]).with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(DateTime)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_datetime() {
        let type_hint = Type::Nullable(Box::new(Type::DateTime(Tz::UTC)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // DateTime: [1000, 0, 3000] (little-endian u32)
            232, 3, 0, 0, // 1000
            0, 0, 0, 0, // 0 (null)
            184, 11, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(DateTime)");
        let array = result.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
        let expected = TimestampSecondArray::from(vec![Some(1000), None, Some(3000)])
            .with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `DateTime64(3)` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_datetime64_3() {
        let type_hint = Type::DateTime64(3, Tz::UTC);
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // DateTime64(3): [1000, 2000, 3000] (milliseconds since 1970-01-01, little-endian i64)
            232, 3, 0, 0, 0, 0, 0, 0, // 1000
            208, 7, 0, 0, 0, 0, 0, 0, // 2000
            184, 11, 0, 0, 0, 0, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize DateTime64(3)");
        let array = result.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        let expected =
            TimestampMillisecondArray::from(vec![1000, 2000, 3000]).with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(DateTime64(3))` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_datetime64_3() {
        let type_hint = Type::Nullable(Box::new(Type::DateTime64(3, Tz::UTC)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // DateTime64(3): [1000, 0, 3000] (little-endian i64)
            232, 3, 0, 0, 0, 0, 0, 0, // 1000
            0, 0, 0, 0, 0, 0, 0, 0, // 0 (null)
            184, 11, 0, 0, 0, 0, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(DateTime64(3))");
        let array = result.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        let expected = TimestampMillisecondArray::from(vec![Some(1000), None, Some(3000)])
            .with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `DateTime64(6)` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_datetime64_6() {
        let type_hint = Type::DateTime64(6, Tz::UTC);
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // DateTime64(6): [1000, 2000, 3000] (microseconds since 1970-01-01, little-endian i64)
            232, 3, 0, 0, 0, 0, 0, 0, // 1000
            208, 7, 0, 0, 0, 0, 0, 0, // 2000
            184, 11, 0, 0, 0, 0, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize DateTime64(6)");
        let array = result.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let expected =
            TimestampMicrosecondArray::from(vec![1000, 2000, 3000]).with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(DateTime64(6))` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_datetime64_6() {
        let type_hint = Type::Nullable(Box::new(Type::DateTime64(6, Tz::UTC)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // DateTime64(6): [1000, 0, 3000] (little-endian i64)
            232, 3, 0, 0, 0, 0, 0, 0, // 1000
            0, 0, 0, 0, 0, 0, 0, 0, // 0 (null)
            184, 11, 0, 0, 0, 0, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(DateTime64(6))");
        let array = result.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let expected = TimestampMicrosecondArray::from(vec![Some(1000), None, Some(3000)])
            .with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `DateTime64(9)` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_datetime64_9() {
        let type_hint = Type::DateTime64(9, Tz::UTC);
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // DateTime64(9): [1000, 2000, 3000] (nanoseconds since 1970-01-01, little-endian i64)
            232, 3, 0, 0, 0, 0, 0, 0, // 1000
            208, 7, 0, 0, 0, 0, 0, 0, // 2000
            184, 11, 0, 0, 0, 0, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize DateTime64(9)");
        let array = result.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        let expected =
            TimestampNanosecondArray::from(vec![1000, 2000, 3000]).with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(DateTime64(9))` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_datetime64_9() {
        let type_hint = Type::Nullable(Box::new(Type::DateTime64(9, Tz::UTC)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // DateTime64(9): [1000, 0, 3000] (little-endian i64)
            232, 3, 0, 0, 0, 0, 0, 0, // 1000
            0, 0, 0, 0, 0, 0, 0, 0, // 0 (null)
            184, 11, 0, 0, 0, 0, 0, 0, // 3000
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(DateTime64(9))");
        let array = result.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        let expected = TimestampNanosecondArray::from(vec![Some(1000), None, Some(3000)])
            .with_timezone_opt(Some("UTC"));
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }
    /// Tests deserialization of `Decimal32` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_decimal32() {
        let type_hint = Type::Decimal32(2); // Scale 2 (within precision 9)
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Decimal32: [100, 200, 300] (little-endian i32, scale=2)
            100, 0, 0, 0, // 100
            200, 0, 0, 0, // 200
            44, 1, 0, 0, // 300
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Decimal32");
        let array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let expected =
            Decimal128Array::from(vec![100, 200, 300]).with_precision_and_scale(9, 2).unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Decimal32)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_decimal32() {
        let type_hint = Type::Nullable(Box::new(Type::Decimal32(2)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Decimal32: [100, 0, 300] (little-endian i32, scale=2)
            100, 0, 0, 0, // 100
            0, 0, 0, 0, // 0 (null)
            44, 1, 0, 0, // 300
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Decimal32)");
        let array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let expected = Decimal128Array::from(vec![Some(100), None, Some(300)])
            .with_precision_and_scale(9, 2)
            .unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Decimal64` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_decimal64() {
        let type_hint = Type::Decimal64(4); // Scale 4 (within precision 18)
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Decimal64: [100, 200, 300] (little-endian i64, scale=4)
            100, 0, 0, 0, 0, 0, 0, 0, // 100
            200, 0, 0, 0, 0, 0, 0, 0, // 200
            44, 1, 0, 0, 0, 0, 0, 0, // 300
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Decimal64");
        let array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let expected =
            Decimal128Array::from(vec![100, 200, 300]).with_precision_and_scale(18, 4).unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Decimal64)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_decimal64() {
        let type_hint = Type::Nullable(Box::new(Type::Decimal64(4)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Decimal64: [100, 0, 300] (little-endian i64, scale=4)
            100, 0, 0, 0, 0, 0, 0, 0, // 100
            0, 0, 0, 0, 0, 0, 0, 0, // 0 (null)
            44, 1, 0, 0, 0, 0, 0, 0, // 300
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Decimal64)");
        let array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let expected = Decimal128Array::from(vec![Some(100), None, Some(300)])
            .with_precision_and_scale(18, 4)
            .unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Decimal128` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_decimal128() {
        let type_hint = Type::Decimal128(8); // Scale 8 (within precision 38)
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Decimal128: [100, 200, 300] (little-endian i128)
            100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 100
            200, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 200
            44, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 300
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Decimal128");
        let array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let expected =
            Decimal128Array::from(vec![100, 200, 300]).with_precision_and_scale(38, 8).unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Decimal128)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_decimal128() {
        let type_hint = Type::Nullable(Box::new(Type::Decimal128(8)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Decimal128: [100, 0, 300] (little-endian i128)
            100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 100
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0 (null)
            44, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 300
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Decimal128)");
        let array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let expected = Decimal128Array::from(vec![Some(100), None, Some(300)])
            .with_precision_and_scale(38, 8)
            .unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Decimal256` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_decimal256() {
        let type_hint = Type::Decimal256(10); // Scale 10 (within precision 76)
        let rows = 2;
        let null_mask = vec![];
        let input = vec![
            // Decimal256: [100, 200] (big-endian, reversed to little-endian)
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 100, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 200, //
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Decimal256");
        let array = result.as_any().downcast_ref::<Decimal256Array>().unwrap();
        let expected = Decimal256Array::from(vec![i256::from(100), i256::from(200)])
            .with_precision_and_scale(76, 10)
            .unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Decimal256)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_decimal256() {
        let type_hint = Type::Nullable(Box::new(Type::Decimal256(10)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Decimal256: [100, 200] (big-endian, reversed to little-endian)
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 100, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 200, //
            // Decimal256: [100, 0, 300] (big-endian, reversed to little-endian)
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 100, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 44, //
            1,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Decimal256)");
        let array = result.as_any().downcast_ref::<Decimal256Array>().unwrap();
        let expected =
            Decimal256Array::from(vec![Some(i256::from(100)), None, Some(i256::from(100))])
                .with_precision_and_scale(76, 10)
                .unwrap();
        assert_eq!(array, &expected);
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Int32` with zero rows.
    #[tokio::test]
    async fn test_deserialize_int32_zero_rows() {
        let type_hint = Type::Int32;
        let rows = 0;
        let null_mask = vec![];
        let input = vec![];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Int32 with zero rows");
        let array = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array.len(), 0);
        assert_eq!(array, &Int32Array::from(Vec::<i32>::new()));
        assert_eq!(array.nulls(), None);
    }
}
