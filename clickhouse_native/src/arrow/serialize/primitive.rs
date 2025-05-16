#![expect(clippy::cast_possible_truncation)]
#![expect(clippy::cast_sign_loss)]
/// Serialization logic for `ClickHouse` primitive types from Arrow arrays.
///
/// This module provides functions to serialize Arrow arrays into ClickHouse’s native format
/// for primitive types, including integers (`Int8` to `UInt256`), floats (`Float32`,
/// `Float64`), decimals (`Decimal32` to `Decimal256`), dates (`Date`, `DateTime`,
/// `DateTime64`), and IP addresses (`IPv4`, `IPv6`, `Uuid`). It is used by the
/// `ClickhouseArrowSerializer` implementation in `types.rs` to handle scalar data types.
///
/// The main `serialize` function dispatches to specialized serialization functions based on
/// the `Type` variant, supporting various Arrow array types via downcasting. Serialization is
/// performed using macros (`write_primitive_values!`, `write_float_values!`) to handle
/// different types and input arrays efficiently. Special handling is included for large
/// integers (endian swapping), decimals (truncation), and `Uuid` (high/low bits).
///
/// # Examples
/// ```
/// use arrow::array::Int32Array;
/// use arrow::datatypes::{DataType, Field};
/// use clickhouse_native::types::{Type, primitive::serialize};
/// use std::sync::Arc;
/// use tokio::io::AsyncWriteExt;
///
/// #[tokio::test]
/// async fn test_serialize_int32() {
///     let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
///     let field = Field::new("int", DataType::Int32, false);
///     let mut buffer = Vec::new();
///     serialize(&Type::Int32, &column, &field, &mut buffer)
///         .await
///         .unwrap();
/// }
/// ```
use arrow::array::*;
use arrow::datatypes::{DataType, Field, i256};

use crate::io::ClickhouseWrite;
use crate::{ClickhouseNativeError, Result, Type};

/// Serializes an Arrow array to ClickHouse’s native format for primitive types.
///
/// Dispatches to specialized serialization functions based on the `Type` variant, handling:
/// - Integers: `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`, `UInt8`, `UInt16`, `UInt32`,
///   `UInt64`, `UInt128`, `UInt256`.
/// - Floats: `Float32`, `Float64`.
/// - Decimals: `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256`.
/// - Dates: `Date`, `DateTime`, `DateTime64` (with precision 0, 1-3, 4-6, 7-9).
/// - IP addresses: `IPv4`, `IPv6`, `Uuid`.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` `Type` indicating the target type.
/// - `values`: The Arrow array containing the data to serialize.
/// - `field`: The Arrow `Field` describing the column’s metadata.
/// - `writer`: The async writer to serialize to (e.g., a TCP stream).
///
/// # Returns
/// A `Result` indicating success or a `ClickhouseNativeError` if serialization fails.
///
/// # Errors
/// - Returns `ArrowSerialize` if the `type_hint` is unsupported, the Arrow array type is
///   incompatible, or binary data has incorrect length (e.g., for `Uuid`, `IPv4`).
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn serialize<W: ClickhouseWrite>(
    type_hint: &Type,
    values: &ArrayRef,
    field: &Field,
    writer: &mut W,
) -> Result<()> {
    match type_hint.strip_null() {
        Type::Int8 => write_i8_values(values, writer).await?,
        Type::Int16 => write_i16_values(values, writer).await?,
        Type::Int32 => write_i32_values(values, writer).await?,
        Type::Int64 => write_i64_values(values, writer).await?,
        Type::Int128 => write_i128_values(values, writer).await?,
        Type::Int256 => write_i256_values(values, writer).await?,
        Type::UInt8 => {
            if matches!(field.data_type(), DataType::Boolean) {
                write_bool_values(values, writer).await?;
            } else {
                write_u8_values(values, writer).await?;
            }
        }
        Type::UInt16 => write_u16_values(values, writer).await?,
        Type::UInt32 => write_u32_values(values, writer).await?,
        Type::UInt64 => write_u64_values(values, writer).await?,
        Type::UInt128 => write_u128_values(values, writer).await?,
        Type::UInt256 => write_u256_values(values, writer).await?,
        Type::Float32 => write_f32_values(values, writer).await?,
        Type::Float64 => write_f64_values(values, writer).await?,
        Type::Decimal32(_) => write_decimal32_values(values, writer).await?,
        Type::Decimal64(_) => write_decimal64_values(values, writer).await?,
        Type::Decimal128(_) => write_decimal128_values(values, writer).await?,
        Type::Decimal256(_) => write_decimal256_values(values, writer).await?,
        Type::Date => write_date_values(values, writer).await?,
        Type::Date32 => write_date32_values(values, writer).await?,
        Type::DateTime(_) => write_datetime_values(values, writer).await?,
        Type::DateTime64(p, _) => match p {
            0 => write_datetime64_unknown_values(values, writer).await?,
            1..=3 => write_datetime64_3_values(values, writer).await?,
            4..=6 => write_datetime64_6_values(values, writer).await?,
            7..=9 => write_datetime64_9_values(values, writer).await?,
            _ => {
                return Err(ClickhouseNativeError::ArrowSerialize(format!(
                    "Unsupported precision for DateTime64: {p}"
                )));
            }
        },
        Type::Ipv4 => write_ipv4_values(values, writer).await?,
        Type::Ipv6 => write_ipv6_values(values, writer).await?,
        Type::Uuid => {
            let array = values.as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or(
                ClickhouseNativeError::ArrowSerialize(
                    "Expected FixedSizeBinaryArray for Uuid".into(),
                ),
            )?;

            for i in 0..array.len() {
                let value = if array.is_null(i) { &[0u8; 16] } else { array.value(i) };
                if value.len() != 16 {
                    return Err(ClickhouseNativeError::ArrowSerialize(
                        "UUID must be 16 bytes".into(),
                    ));
                }
                let bytes: [u8; 16] = value.try_into().map_err(|_| {
                    ClickhouseNativeError::ArrowSerialize("UUID must be 16 bytes".into())
                })?;
                let low = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                let high = u64::from_le_bytes(bytes[8..].try_into().unwrap());
                writer.write_u64_le(high).await?; // High bits first
                writer.write_u64_le(low).await?; // Low bits second
            }
        }
        _ => {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "Unsupported data type: {type_hint:?}"
            )));
        }
    }

    Ok(())
}

/// Macro to generate serialization functions for primitive types.
///
/// Supports three forms:
/// - Simple numerics: Single array type (e.g., `Int8Array` for `Int8`) with direct casting.
/// - Multi-type scalars: Multiple array types (e.g., `Int64Array`, `BinaryArray` for `Int128`) with
///   coercion.
/// - Multi-type arrays: Array types (e.g., `[u8; 32]` for `Int256`) with coercion and `write_all`.
macro_rules! write_primitive_values {
    // Simple Numerics: Int8, UInt8, etc.
    ($name:ident, $at:ty, $pt:ty, $write_fn:ident) => {
        /// Serializes an Arrow array to ClickHouse’s native format for a primitive type.
        ///
        /// Writes values as the specified primitive type, mapping nulls to the type’s default value.
        ///
        /// # Arguments
        /// - `column`: The Arrow array containing the data.
        /// - `writer`: The async writer to serialize to.
        ///
        /// # Returns
        /// A `Result` indicating success or a `ClickhouseNativeError` if the array type is unsupported.
        #[allow(clippy::cast_sign_loss)]
        #[allow(clippy::cast_lossless)]
        #[allow(clippy::cast_possible_truncation)]
        #[allow(trivial_numeric_casts)]
        async fn $name<W: ClickhouseWrite>(
            column: &::arrow::array::ArrayRef,
            writer: &mut W,
        ) -> Result<()> {
            let array = column.as_any().downcast_ref::<$at>().ok_or_else(|| {
                $crate::ClickhouseNativeError::ArrowSerialize(
                    concat!("Expected ", stringify!($at)).into(),
                )
            })?;
            for i in 0..array.len() {
                let value = if array.is_null(i) { <$pt>::default() } else { array.value(i) as $pt };
                writer.$write_fn(value).await?;

            }
            Ok(())
        }
    };
    // Multi-type case with coercion
    ($name:ident, scalar $pt:expr, $write_fn:ident, [$(($at:ty, $coerce:expr)),* $(,)?]) => {
        /// Serializes an Arrow array to ClickHouse’s native format for a primitive type.
        ///
        /// Supports multiple Arrow array types with coercion to the target type. Maps nulls to the
        /// type’s default value.
        ///
        /// # Arguments
        /// - `column`: The Arrow array containing the data.
        /// - `writer`: The async writer to serialize to.
        ///
        /// # Returns
        /// A `Result` indicating success or a `ClickhouseNativeError` if the array type is unsupported.
        async fn $name<W: ClickhouseWrite>(
            column: &::arrow::array::ArrayRef,
            writer: &mut W,
        ) -> Result<()> {
            $(
                if let Some(array) = column.as_any().downcast_ref::<$at>() {
                    for i in 0..array.len() {
                        let value = if array.is_null(i) {
                            $pt
                        } else {
                            $coerce(array.value(i))?
                        };
                        writer.$write_fn(value).await?;
                    }
                    return Ok(());
                }
            )*
            Err($crate::ClickhouseNativeError::ArrowSerialize(
                concat!("Expected one of: ", $(stringify!($at), " "),*).into()
            ))
        }
    };
    // Array types (e.g., [u8; 32]) with multi-type coercion - borrow for write_all
    ($name:ident, array $pt:ty, $write_fn:ident, [$(($at:ty, $coerce:expr)),* $(,)?]) => {
        /// Serializes an Arrow array to ClickHouse’s native format for a primitive array type.
        ///
        /// Supports multiple Arrow array types with coercion to the target array type (e.g., `[u8; 32]`).
        /// Maps nulls to the type’s default value.
        ///
        /// # Arguments
        /// - `column`: The Arrow array containing the data.
        /// - `writer`: The async writer to serialize to.
        ///
        /// # Returns
        /// A `Result` indicating success or a `ClickhouseNativeError` if the array type is unsupported.
        async fn $name<W: ClickhouseWrite>(
            column: &::arrow::array::ArrayRef,
            writer: &mut W,
        ) -> Result<()> {
            $(
                if let Some(array) = column.as_any().downcast_ref::<$at>() {
                    for i in 0..array.len() {
                        let value = if array.is_null(i) {
                            <$pt>::default()
                        } else {
                            $coerce(array.value(i))?
                        };
                        writer.$write_fn(&value).await?;
                    }
                    return Ok(());
                }
            )*
            Err($crate::ClickhouseNativeError::ArrowSerialize(
                concat!("Expected one of: ", $(stringify!($at), " "),*).into()
            ))
        }
    };
}

// Primitives
write_primitive_values!(write_i8_values, Int8Array, i8, write_i8);
write_primitive_values!(write_i16_values, Int16Array, i16, write_i16_le);
write_primitive_values!(write_i32_values, Int32Array, i32, write_i32_le);
write_primitive_values!(write_i64_values, Int64Array, i64, write_i64_le);
write_primitive_values!(write_u8_values, UInt8Array, u8, write_u8);
write_primitive_values!(write_bool_values, BooleanArray, u8, write_u8);
write_primitive_values!(write_u16_values, UInt16Array, u16, write_u16_le);
write_primitive_values!(write_u32_values, UInt32Array, u32, write_u32_le);
write_primitive_values!(write_u64_values, UInt64Array, u64, write_u64_le);

// Large primitives
write_primitive_values!(write_i128_values, scalar i128::default(), write_i128_le, [
    (Int64Array, |v: i64| Ok::<_, ClickhouseNativeError>(i128::from(v))), // Cast i64 to i128
    (BinaryArray, |v: &[u8]| Ok::<_, ClickhouseNativeError>(i128::from_le_bytes(
        v.try_into().map_err(|_| {
            ClickhouseNativeError::ArrowSerialize("Binary must be 16 bytes for Int128".into())
        })?
    ))),
    (FixedSizeBinaryArray, |v: &[u8]| {
        if v.len() != 16 {
            return Err(ClickhouseNativeError::ArrowSerialize(
                "FixedSizeBinary must be 16 bytes for Int128".into(),
            ));
        }
        Ok(i128::from_le_bytes(v.try_into().map_err(|_| {
            ClickhouseNativeError::ArrowSerialize("Binary must be 16 bytes for Int128".into())
        })?))
    })
]);

write_primitive_values!(write_u128_values, scalar u128::default(), write_u128_le, [
    (UInt64Array, |v: u64| Ok::<_, ClickhouseNativeError>(u128::from(v))), // Cast u64 to u128
    (BinaryArray, |v: &[u8]| Ok::<_, ClickhouseNativeError>(u128::from_le_bytes(
        v.try_into().map_err(|_| {
            ClickhouseNativeError::ArrowSerialize("Binary must be 16 bytes for UInt128".into())
        })?
    ))),
    (FixedSizeBinaryArray, |v: &[u8]| {
        if v.len() != 16 {
            return Err(ClickhouseNativeError::ArrowSerialize(
                "FixedSizeBinary must be 16 bytes for UInt128".into(),
            ));
        }
        Ok(u128::from_le_bytes(v.try_into().map_err(|_| {
            ClickhouseNativeError::ArrowSerialize("Binary must be 16 bytes for UInt128".into())
        })?))
    })
]);

write_primitive_values!(write_i256_values, array [u8; 32], write_all, [
    (Int64Array, |v: i64| Ok::<_, ClickhouseNativeError>({
        let mut bytes = [0u8; 32];
        let i128_bytes = i128::from(v).to_le_bytes(); // 16 bytes
        bytes[..16].copy_from_slice(&i128_bytes);
        if v < 0 {
            bytes[16..].fill(0xFF);
        } // Sign-extend
        swap_endian_256(bytes)
    })),
    (BinaryArray, |v: &[u8]| Ok::<_, ClickhouseNativeError>({
        let bytes: [u8; 32] = v
            .try_into()
            .map_err(|_| {
                ClickhouseNativeError::ArrowSerialize("Binary must be 32 bytes for Int256".into())
            })?;
        swap_endian_256(bytes)
    })),
    (FixedSizeBinaryArray, |v: &[u8]| {
        if v.len() != 32 {
            return Err(ClickhouseNativeError::ArrowSerialize(
                "FixedSizeBinary must be 32 bytes for Int256".into(),
            ));
        }
        let bytes: [u8; 32] = v
            .try_into()
            .map_err(|_| {
                ClickhouseNativeError::ArrowSerialize("Binary must be 32 bytes for Int256".into())
            })?;
        Ok(swap_endian_256(bytes))
    })
]);

write_primitive_values!(write_u256_values, array [u8; 32], write_all, [
    (UInt64Array, |v: u64| Ok::<_, ClickhouseNativeError>({
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&v.to_le_bytes()); // Lower 8 bytes
        // Upper 24 bytes remain 0
        swap_endian_256(bytes)
    })),
    (BinaryArray, |v: &[u8]| Ok::<_, ClickhouseNativeError>({
        let bytes: [u8; 32] = v
            .try_into()
            .map_err(|_| {
                ClickhouseNativeError::ArrowSerialize("Binary must be 32 bytes for UInt256".into())
            })?;
        swap_endian_256(bytes)
    })),
    (FixedSizeBinaryArray, |v: &[u8]| {
        if v.len() != 32 {
            return Err(ClickhouseNativeError::ArrowSerialize(
                "FixedSizeBinary must be 32 bytes for UInt256".into(),
            ));
        }
        let bytes: [u8; 32] = v
            .try_into()
            .map_err(|_| {
                ClickhouseNativeError::ArrowSerialize("Binary must be 32 bytes for UInt256".into())
            })?;
        Ok(swap_endian_256(bytes))
    })
]);

// Decimals
write_primitive_values!(write_decimal32_values, scalar i32::default(), write_i32_le, [
    (Decimal128Array, |v: i128| {
        if !(-999_999_999..=999_999_999).contains(&v) {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "Decimal32 out of range of (max 9 digits): {v}"
            )));
        }
        Ok::<_, ClickhouseNativeError>(v as i32) // Truncate to 9 digits
    })
]);
write_primitive_values!(write_decimal64_values, scalar i64::default(), write_i64_le, [
    (Decimal128Array, |v: i128| {
        if !(-999_999_999_999_999_999..=999_999_999_999_999_999).contains(&v) {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "Decimal64 out of range of (max 18 digits): {v}"
            )));
        }
        Ok::<_, ClickhouseNativeError>(v as i64) // Truncate to 18 digits
    })
]);
write_primitive_values!(write_decimal128_values, scalar i128::default(), write_i128_le, [
    (Decimal128Array, |v: i128| Ok::<_, ClickhouseNativeError>(v)) // Up to 38 digits
]);

write_primitive_values!(write_decimal256_values, array [u8; 32], write_all, [
    (Decimal256Array, |v: i256| Ok::<_, ClickhouseNativeError>({
        let bytes = v.to_le_bytes(); // i256 provides 32 bytes in little-endian
        swap_endian_256(bytes) // Convert to ClickHouse's big-endian
    })),
    (Decimal128Array, |v: i128| Ok::<_, ClickhouseNativeError>({
        let mut bytes = [0u8; 32];
        let i128_bytes = v.to_le_bytes(); // 16 bytes
        bytes[..16].copy_from_slice(&i128_bytes);
        if v < 0 {
            bytes[16..].fill(0xFF);
        } // Sign-extend
        swap_endian_256(bytes)
    }))
]);

// Dates
write_primitive_values!(write_date_values, scalar u16::default(), write_u16_le, [
    (Date32Array, |v: i32| {
        if v < 0 || v > i32::from(u16::MAX) {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "Date out of range for Date32 (ClickHouse uses u16): {v}"
            )));
        }
        Ok::<_, ClickhouseNativeError>(v as u16) // Days since epoch
    })
]);
write_primitive_values!(write_date32_values, scalar i32::default(), write_i32_le, [
    (Date32Array, |v: i32| {
        const DAYS_1900_TO_1970: i32 = 25_567; // Days from 1900-01-01 to 1970-01-01
        let adjusted = v + DAYS_1900_TO_1970;
        Ok::<_, ClickhouseNativeError>(adjusted) // Days since 1900-01-01
    })
]);
write_primitive_values!(write_datetime_values, scalar u32::default(), write_u32_le, [
    (TimestampSecondArray, |v: i64| {
        #[expect(clippy::cast_lossless)]
        if v > u32::MAX as i64 {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "DateTime out of range for TimestampSecond (ClickHouse uses u32): {v}"
            )));
        }
        Ok::<_, ClickhouseNativeError>(v as u32)
    }), // Seconds since epoch
]);

write_primitive_values!(write_datetime64_3_values, scalar u64::default(), write_u64_le, [
    (TimestampMillisecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64)), // Milliseconds
    (TimestampMicrosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 / 1000)), // Convert to ms
    (TimestampNanosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 / 1_000_000)), // Convert to ms
    (TimestampSecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 * 1000)) // Convert to ms
]);
write_primitive_values!(write_datetime64_6_values, scalar u64::default(), write_u64_le, [
    (TimestampMicrosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64)), // Microseconds
    (TimestampMillisecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 * 1000)), // Convert to us
    (TimestampNanosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 / 1000)), // Convert to us
    (TimestampSecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 * 1_000_000)) // Convert to us
]);
write_primitive_values!(write_datetime64_9_values, scalar u64::default(), write_u64_le, [
    (TimestampNanosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64)), // Nanoseconds
    (TimestampMillisecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 * 1_000_000)), // Convert to ns
    (TimestampMicrosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 * 1000)), // Convert to ns
    (TimestampSecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 * 1_000_000_000)) // Convert to ns
]);
write_primitive_values!(write_datetime64_unknown_values, scalar u64::default(), write_u64_le, [
    (TimestampSecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64)), // Seconds
    (TimestampMillisecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 / 1000)), // Convert to s
    (TimestampMicrosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 / 1_000_000)), // Convert to s
    (TimestampNanosecondArray, |v: i64| Ok::<_, ClickhouseNativeError>(v as u64 / 1_000_000_000)) // Convert to s
]);

// IPs
write_primitive_values!(write_ipv4_values, scalar u32::default(), write_u32_le, [
    (FixedSizeBinaryArray, |v: &[u8]| {
        if v.len() != 4 {
            return Err(ClickhouseNativeError::ArrowSerialize(
                "IPv4 must be 4 bytes".into(),
            ));
        }
        Ok(u32::from_le_bytes(v.try_into().map_err(|_| {
            ClickhouseNativeError::ArrowSerialize("IPv4 must be 4 bytes".into())
        })?))
    })
]);
write_primitive_values!(write_ipv6_values, array [u8; 16], write_all, [
    (FixedSizeBinaryArray, |v: &[u8]| {
        if v.len() != 16 {
            return Err(ClickhouseNativeError::ArrowSerialize(
                "IPv6 must be 16 bytes".into(),
            ));
        }
        v.try_into().map_err(|_| {
            ClickhouseNativeError::ArrowSerialize("IPv6 must be 16 bytes".into())
        })
    })
]);

// Floats
macro_rules! write_float_values {
    ($name:ident, $pt:ty, $write_fn:ident, [$($at:ty),* $(,)?]) => {
        /// Serializes an Arrow array to ClickHouse’s native format for a floating-point type.
        ///
        /// Supports multiple Arrow array types, converting to the target float type. Maps nulls to
        /// the type’s default value. Writes the raw bits of the float value.
        ///
        /// # Arguments
        /// - `column`: The Arrow array containing the data.
        /// - `writer`: The async writer to serialize to.
        ///
        /// # Returns
        /// A `Result` indicating success or a `ClickhouseNativeError` if the array type is unsupported.
        async fn $name<W: ClickhouseWrite>(
            column: &::arrow::array::ArrayRef,
            writer: &mut W,
        ) -> Result<()> {
            $(
                if let Some(array) = column.as_any().downcast_ref::<$at>() {
                    for i in 0..array.len() {
                        let value = if array.is_null(i) { <$pt>::default() } else { <$pt>::from(array.value(i)) };
                        writer.$write_fn(value.to_bits()).await?;
                    }
                    return Ok(());
                }
            )*
            Err($crate::ClickhouseNativeError::ArrowSerialize(
                concat!("Expected one of: ", $(stringify!($at), " "),*).into()
            ))
        }
    };
}

write_float_values!(write_f32_values, f32, write_u32_le, [Float32Array, Float16Array]);
write_float_values!(write_f64_values, f64, write_u64_le, [
    Float64Array,
    Float32Array,
    Float16Array
]);

/// Swaps the endianness of a 256-bit (32-byte) array.
///
/// Converts between little-endian and big-endian for `Int256`, `UInt256`, and `Decimal256`.
fn swap_endian_256(mut input: [u8; 32]) -> [u8; 32] {
    input.reverse();
    input
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::*;
    use chrono_tz::Tz;

    use super::*;

    type MockWriter = Vec<u8>;

    #[tokio::test]
    async fn test_serialize_int8() {
        let column = Arc::new(Int8Array::from(vec![1, -2, 0])) as ArrayRef;
        let field = Field::new("int", DataType::Int8, false);
        let mut writer = MockWriter::new();
        serialize(&Type::Int8, &column, &field, &mut writer).await.unwrap();
        let expected = vec![1, 254, 0]; // -2 = 254 in u8
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_int8_min_max() {
        let column = Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX, 0])) as ArrayRef;
        let field = Field::new("int", DataType::Int8, false);
        let mut writer = MockWriter::new();
        serialize(&Type::Int8, &column, &field, &mut writer).await.unwrap();
        let expected = vec![128, 127, 0]; // i8::MIN = -128, i8::MAX = 127
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_uint8_bool() {
        let column = Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef;
        let field = Field::new("bool", DataType::Boolean, false);
        let mut writer = MockWriter::new();
        serialize(&Type::UInt8, &column, &field, &mut writer).await.unwrap();
        let expected = vec![1, 0, 1];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_uint8() {
        let column = Arc::new(UInt8Array::from(vec![0, u8::MAX, 42])) as ArrayRef;
        let field = Field::new("uint", DataType::UInt8, false);
        let mut writer = MockWriter::new();
        serialize(&Type::UInt8, &column, &field, &mut writer).await.unwrap();
        let expected = vec![0, 255, 42];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_int32() {
        let column = Arc::new(Int32Array::from(vec![1, -2, 0])) as ArrayRef;
        let field = Field::new("int", DataType::Int32, false);
        let mut writer = MockWriter::new();
        serialize(&Type::Int32, &column, &field, &mut writer).await.unwrap();
        let expected = vec![1, 0, 0, 0, 254, 255, 255, 255, 0, 0, 0, 0]; // -2 = 0xFFFF_FFFE
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_int128_binary() {
        let column = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                vec![
                    i128::from(123).to_le_bytes().as_ref(),
                    i128::from(-456).to_le_bytes().as_ref(),
                ]
                .into_iter(),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new("int", DataType::FixedSizeBinary(16), false);
        let mut writer = MockWriter::new();
        serialize(&Type::Int128, &column, &field, &mut writer).await.unwrap();
        let expected = vec![
            123, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 123
            56, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            255, // -456
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_int256() {
        let column = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                vec![i256::from(123).to_le_bytes().as_ref()].into_iter(),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new("int", DataType::FixedSizeBinary(32), false);
        let mut writer = MockWriter::new();
        serialize(&Type::Int256, &column, &field, &mut writer).await.unwrap();
        let expected = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // Upper 16 bytes (0)
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 123, // Lower 16 bytes (123)
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_float32() {
        let column = Arc::new(Float32Array::from(vec![1.5, -2.0, 0.0])) as ArrayRef;
        let field = Field::new("float", DataType::Float32, false);
        let mut writer = MockWriter::new();
        serialize(&Type::Float32, &column, &field, &mut writer).await.unwrap();
        let expected = vec![
            0, 0, 192, 63, // 1.5 (0x3FC00000)
            0, 0, 0, 192, // -2.0 (0xC0000000)
            0, 0, 0, 0, // 0.0 (0x00000000)
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_float64() {
        let column = Arc::new(Float64Array::from(vec![1.5, -2.0, 0.0])) as ArrayRef;
        let field = Field::new("float", DataType::Float64, false);
        let mut writer = MockWriter::new();
        serialize(&Type::Float64, &column, &field, &mut writer).await.unwrap();
        let expected = vec![
            0, 0, 0, 0, 0, 0, 248, 63, // 1.5 (0x3FF8000000000000)
            0, 0, 0, 0, 0, 0, 0, 192, // -2.0 (0xC000000000000000)
            0, 0, 0, 0, 0, 0, 0, 0, // 0.0 (0x0000000000000000)
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_decimal128() {
        let column = Arc::new(Decimal128Array::from(vec![0, 1])) as ArrayRef;
        let field = Field::new("decimal", DataType::Decimal128(38, 0), false);
        let mut writer = MockWriter::new();
        serialize(&Type::Decimal128(0), &column, &field, &mut writer).await.unwrap();
        let expected = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 1
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_date() {
        let column = Arc::new(Date32Array::from(vec![0, 1])) as ArrayRef; // 1970-01-01, 1970-01-02
        let field = Field::new("date", DataType::Date32, false);
        let mut writer = MockWriter::new();
        serialize(&Type::Date, &column, &field, &mut writer).await.unwrap();
        let expected = vec![0, 0, 1, 0]; // 0, 1 (u16 LE)
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_datetime64_3() {
        let column = Arc::new(TimestampMillisecondArray::from(vec![0, 1000])) as ArrayRef; // 1970-01-01 00:00:00, 00:00:01
        let field = Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false);
        let mut writer = MockWriter::new();
        serialize(&Type::DateTime64(3, Tz::UTC), &column, &field, &mut writer).await.unwrap();
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0]; // 0, 1000 (u64 LE)
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_ipv4() {
        let column = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                vec![[192, 168, 1, 1].as_ref(), [10, 0, 0, 1].as_ref()].into_iter(),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new("ip", DataType::FixedSizeBinary(4), false);
        let mut writer = MockWriter::new();
        serialize(&Type::Ipv4, &column, &field, &mut writer).await.unwrap();
        let expected = vec![192, 168, 1, 1, 10, 0, 0, 1]; // 192.168.1.1, 10.0.0.1 (big-endian)
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_uuid() {
        let column = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                vec![
                    [
                        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78,
                        0x9a, 0xbc, 0xde, 0xf0,
                    ]
                    .as_ref(),
                ]
                .into_iter(),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new("uuid", DataType::FixedSizeBinary(16), false);
        let mut writer = MockWriter::new();
        serialize(&Type::Uuid, &column, &field, &mut writer).await.unwrap();
        let expected = vec![
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, // High bits
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, // Low bits
        ];
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_empty_int32() {
        let column = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let field = Field::new("int", DataType::Int32, false);
        let mut writer = MockWriter::new();
        serialize(&Type::Int32, &column, &field, &mut writer).await.unwrap();
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_serialize_nullable_int32() {
        let column = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let field = Field::new("int", DataType::Int32, true);
        let mut writer = MockWriter::new();
        serialize(&Type::Nullable(Box::new(Type::Int32)), &column, &field, &mut writer)
            .await
            .unwrap();
        let expected = vec![1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0]; // 1, 0 (null), 3
        assert_eq!(writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_null_only_int32() {
        let column = Arc::new(Int32Array::from(vec![None, None])) as ArrayRef;
        let field = Field::new("int", DataType::Int32, true);
        let mut writer = MockWriter::new();
        serialize(&Type::Nullable(Box::new(Type::Int32)), &column, &field, &mut writer)
            .await
            .unwrap();
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0]; // Two nulls (0)
        assert_eq!(writer, expected);
    }

    // Test invalid values

    #[tokio::test]
    async fn test_invalid_datetimes() -> Result<()> {
        let cases = [(
            Type::Date,
            Arc::new(Date32Array::from(vec![Some(-1)])) as ArrayRef,
            Field::new("date", DataType::Date32, true),
            "Date out of range",
        )];

        for (type_, array, field, expected) in cases {
            let mut writer = MockWriter::new();
            let result = serialize(&type_, &array, &field, &mut writer).await;
            assert!(matches!(
                result,
                Err(ClickhouseNativeError::ArrowSerialize(msg))
                if msg.contains(expected)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_decimal() -> Result<()> {
        let cases = [
            (
                Type::Decimal32(0),
                Arc::new(Decimal128Array::from(vec![Some(1_000_000_000)])) as ArrayRef,
                Field::new("decimal", DataType::Decimal128(9, 0), true),
                "Decimal32 out of range",
            ),
            (
                Type::Decimal64(0),
                Arc::new(Decimal128Array::from(vec![Some(1_000_000_000_000_000_000)])) as ArrayRef,
                Field::new("decimal", DataType::Decimal128(18, 0), true),
                "Decimal64 out of range",
            ),
        ];
        for (type_, array, field, expected) in cases {
            let mut writer = MockWriter::new();
            let result = serialize(&type_, &array, &field, &mut writer).await;
            assert!(matches!(
                result,
                Err(ClickhouseNativeError::ArrowSerialize(msg))
                if msg.contains(expected)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_serialize_invalid_type() {
        let column = Arc::new(StringArray::from(vec!["a"])) as ArrayRef;
        let field = Field::new("str", DataType::Utf8, false);
        let mut writer = MockWriter::new();
        let result = serialize(&Type::Int32, &column, &field, &mut writer).await;
        assert!(matches!(
            result,
            Err(ClickhouseNativeError::ArrowSerialize(msg)) if msg.contains("Expected Int32Array")
        ));
    }

    #[tokio::test]
    async fn test_serialize_invalid_uuid_length() {
        let column = Arc::new(
            FixedSizeBinaryArray::try_from_iter(vec![[0x12, 0x34].as_ref()].into_iter()).unwrap(),
        ) as ArrayRef;
        let field = Field::new("uuid", DataType::FixedSizeBinary(2), false);
        let mut writer = MockWriter::new();
        let result = serialize(&Type::Uuid, &column, &field, &mut writer).await;
        assert!(matches!(
            result,
            Err(ClickhouseNativeError::ArrowSerialize(msg)) if msg.contains("UUID must be 16 bytes")
        ));
    }
}
