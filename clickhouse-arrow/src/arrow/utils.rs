use std::sync::Arc;

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use crate::{Date, Date32, DateTime, DynDateTime64, Error, Ipv4, Ipv6, Result, Type, Value};

/// Internal target-width tag for Decimal128 emission.
///
/// `array_to_values` collapses an incoming Arrow Decimal128 column into
/// the narrowest `Value::Decimal{32,64,128}` variant the data fits,
/// either from the `Type` hint when present, or by inspecting the Arrow
/// type's precision.
#[derive(Copy, Clone)]
enum DecimalNarrow {
    D32,
    D64,
    D128,
}

/// Convert an Arrow Decimal scale (`i8`) to the canonical `usize` scale
/// used in `Value::Decimal*` variants. Negative scales are clamped to 0
/// — they're unrepresentable in our `Value` shape and CH rejects them
/// at table-creation time, so the clamp is conservative defence.
fn arrow_scale_to_usize(scale: i8) -> usize { usize::try_from(scale).unwrap_or(0) }

/// Splits a `RecordBatch` into multiple `RecordBatch`es, each containing at most `max` rows.
///
/// # Arguments
///
/// * `batch` - A reference to the input `RecordBatch` to split.
/// * `max` - The maximum number of rows per output `RecordBatch`. Must be non-zero to avoid an
///   empty result.
///
/// # Returns
///
/// A `Result` containing:
/// * `Vec<RecordBatch>` - A vector of `RecordBatch`es, each with at most `max_rows` rows.
///
/// # Edge Cases
///
/// * If `max` is 0, returns an empty `Vec`.
/// * If the input `batch` has 0 rows, returns an the original `RecordBatch`.
/// * If the number of rows is not evenly divisible by `max`, the last `RecordBatch` will contain
///   the remaining rows.
///
/// # Performance Notes
///
/// * **Zero-copy**: Uses `RecordBatch::slice` for zero-copy access to the underlying data buffers,
///   avoiding deep copies of row data.
/// * **Single allocation**: Allocates a single `Vec` with pre-computed capacity to store the output
///   `RecordBatch`es, avoiding reallocations.
///
/// # Example
///
/// ```rust,ignore
/// use arrow::record_batch::RecordBatch;
/// use arrow::error::ArrowError;
///
/// let max_rows = 3;
/// let chunks = split_record_batch_by_rows(batch, max_rows)?;
/// for (i, chunk) in chunks.iter().enumerate() {
///     println!("Chunk {}: {} rows", i, chunk.num_rows());
/// }
/// ```
pub fn split_record_batch(batch: RecordBatch, max: usize) -> Vec<RecordBatch> {
    if max == 0 {
        return vec![];
    }

    let rows = batch.num_rows();
    if rows == 0 || rows < max {
        return vec![batch];
    }

    // Calculate number of chunks using ceiling division
    let mut chunks = Vec::with_capacity(rows.div_ceil(max));
    let mut offset = 0;
    while offset < rows {
        let remaining_rows = rows - offset;
        let chunk_rows = remaining_rows.min(max);
        chunks.push(batch.slice(offset, chunk_rows));
        offset += chunk_rows;
    }

    chunks
}

/// Converts a [`RecordBatch`] to an iterator of rows, where each row is a Vec of Values.
///
/// # Arguments
/// - `batch`: The [`RecordBatch`] to convert.
/// - `type_hints`: Optional mapping of column names to `ClickHouse` types for disambiguation.
///
/// # Returns
/// A Result containing an iterator of rows, where each row is a [`Vec<Value>`].
///
/// # Errors
/// Returns an error if downcasting fails or the arrow data type is not supported
pub fn batch_to_rows(
    batch: &RecordBatch,
    type_hints: Option<&[(String, Type)]>,
) -> Result<impl Iterator<Item = Result<Vec<Value>, Error>> + use<>> {
    let row_len = batch.num_rows();
    let col_len = batch.num_columns();
    let columns = batch.columns();
    let schema = batch.schema();

    // Convert columns to Vec<Vec<Value>> once
    let values = columns
        .iter()
        .enumerate()
        .map(|(i, column)| {
            let name = schema.field(i).name();
            let type_hint =
                type_hints.as_ref().and_then(|hints| hints.iter().find(|(n, _)| n == name));
            array_to_values(column, column.data_type(), type_hint.map(|(_, t)| t))
        })
        .collect::<Result<Vec<_>>>()?;

    let row_iter = (0..row_len).map(move |i| {
        let row = (0..col_len).map(|j| values[j][i].clone()).collect::<Vec<_>>();
        Ok(row)
    });

    Ok(row_iter)
}

/// Converts a [`ArrayRef`]s to clickhouse values
///
/// # Errors
///
/// Returns an error is downcasting fails or the arrow data type is not supported
#[expect(clippy::too_many_lines)]
pub fn array_to_values(
    column: &dyn Array,
    data_type: &DataType,
    type_hint: Option<&Type>,
) -> Result<Vec<Value>> {
    fn map_or_null<T>(
        iter: impl Iterator<Item = Option<T>>,
        conv: impl Fn(T) -> Value,
    ) -> Vec<Value> {
        iter.map(|v| v.map_or(Value::Null, &conv)).collect::<Vec<Value>>()
    }

    Ok(match data_type {
        // Integer types
        DataType::Int8 => map_or_null(array_to_i8_iter(column)?, Value::Int8),
        DataType::Int16 => map_or_null(array_to_i16_iter(column)?, Value::Int16),
        DataType::Int32 => map_or_null(array_to_i32_iter(column)?, Value::Int32),
        DataType::Int64 => map_or_null(array_to_i64_iter(column)?, Value::Int64),

        // Unsigned integer types
        DataType::UInt8 => map_or_null(array_to_u8_iter(column)?, Value::UInt8),
        DataType::UInt16 => map_or_null(array_to_u16_iter(column)?, Value::UInt16),
        DataType::UInt32 => map_or_null(array_to_u32_iter(column)?, Value::UInt32),
        DataType::UInt64 => map_or_null(array_to_u64_iter(column)?, Value::UInt64),

        // Floating point types
        DataType::Float32 => map_or_null(array_to_f32_iter(column)?, Value::Float32),
        DataType::Float64 => map_or_null(array_to_f64_iter(column)?, Value::Float64),

        // Binary-like types (converted to String).
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            map_or_null(array_to_binary_iter(column)?, Value::String)
        }

        // UUID — the wire-to-Arrow deserializer normalises CH's two-LE-u64
        // wire form to canonical RFC 4122 byte order; here the Arrow bytes
        // are already in the form Uuid::from_bytes expects.
        DataType::FixedSizeBinary(16) if matches!(type_hint, Some(Type::Uuid)) => {
            let iter = array_to_binary_iter(column)?.map(|opt| {
                opt.and_then(|bytes| {
                    (bytes.len() == 16).then(|| {
                        let mut uuid_bytes = [0u8; 16];
                        uuid_bytes.copy_from_slice(&bytes);
                        uuid::Uuid::from_bytes(uuid_bytes)
                    })
                })
            });
            map_or_null(iter, Value::Uuid)
        }

        // IPv4 — the deserializer already converts the CH u32-LE wire form
        // into octet order (a.b.c.d), so the FixedSizeBinary(4) bytes here
        // are exactly what Ipv4Addr::from([u8;4]) expects.
        DataType::FixedSizeBinary(4) if matches!(type_hint, Some(Type::Ipv4)) => {
            let iter = array_to_binary_iter(column)?.map(|opt| {
                opt.and_then(|bytes| {
                    (bytes.len() == 4).then(|| {
                        let mut buf = [0u8; 4];
                        buf.copy_from_slice(&bytes);
                        Ipv4(std::net::Ipv4Addr::from(buf))
                    })
                })
            });
            map_or_null(iter, Value::Ipv4)
        }

        // IPv6 — wire is the 16 address bytes in network byte order, which
        // is what Ipv6Addr::from([u8;16]) takes directly.
        DataType::FixedSizeBinary(16) if matches!(type_hint, Some(Type::Ipv6)) => {
            let iter = array_to_binary_iter(column)?.map(|opt| {
                opt.and_then(|bytes| {
                    (bytes.len() == 16).then(|| {
                        let mut buf = [0u8; 16];
                        buf.copy_from_slice(&bytes);
                        Ipv6(std::net::Ipv6Addr::from(buf))
                    })
                })
            });
            map_or_null(iter, Value::Ipv6)
        }

        // Int128 / UInt128 — CH writes the 16 bytes in little-endian.
        DataType::FixedSizeBinary(16) if matches!(type_hint, Some(Type::Int128)) => {
            let iter = array_to_binary_iter(column)?.map(|opt| {
                opt.and_then(|bytes| {
                    (bytes.len() == 16).then(|| {
                        let mut buf = [0u8; 16];
                        buf.copy_from_slice(&bytes);
                        i128::from_le_bytes(buf)
                    })
                })
            });
            map_or_null(iter, Value::Int128)
        }
        DataType::FixedSizeBinary(16) if matches!(type_hint, Some(Type::UInt128)) => {
            let iter = array_to_binary_iter(column)?.map(|opt| {
                opt.and_then(|bytes| {
                    (bytes.len() == 16).then(|| {
                        let mut buf = [0u8; 16];
                        buf.copy_from_slice(&bytes);
                        u128::from_le_bytes(buf)
                    })
                })
            });
            map_or_null(iter, Value::UInt128)
        }

        // Int256 / UInt256 — crate::i256/u256 are byte-array wrappers; the
        // 32-byte wire payload is stored verbatim. Use full crate paths
        // here because arrow::datatypes::i256 is a different type that
        // is also in scope.
        DataType::FixedSizeBinary(32) if matches!(type_hint, Some(Type::Int256)) => {
            let iter = array_to_binary_iter(column)?.map(|opt| {
                opt.and_then(|bytes| {
                    (bytes.len() == 32).then(|| {
                        let mut buf = [0u8; 32];
                        buf.copy_from_slice(&bytes);
                        crate::i256(buf)
                    })
                })
            });
            map_or_null(iter, Value::Int256)
        }
        DataType::FixedSizeBinary(32) if matches!(type_hint, Some(Type::UInt256)) => {
            let iter = array_to_binary_iter(column)?.map(|opt| {
                opt.and_then(|bytes| {
                    (bytes.len() == 32).then(|| {
                        let mut buf = [0u8; 32];
                        buf.copy_from_slice(&bytes);
                        crate::u256(buf)
                    })
                })
            });
            map_or_null(iter, Value::UInt256)
        }

        // Catch-all for FixedSizeBinary with no type_hint match — caller
        // wants the raw bytes wrapped as a String value.
        DataType::FixedSizeBinary(_) => map_or_null(array_to_binary_iter(column)?, Value::String),

        // String-like types (converted to String)
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let iter = array_to_string_iter(column)?.map(|opt| opt.map(String::into_bytes));
            map_or_null(iter, Value::String)
        }

        // Boolean (convert to UInt8)
        DataType::Boolean => {
            let iter = array_to_bool_iter(column)?.map(|opt| opt.map(u8::from));
            map_or_null(iter, Value::UInt8)
        }

        // Decimal types
        //
        // CH Decimal{32,64,128,256}(scale) all arrive at the Arrow boundary
        // as Decimal128 or Decimal256, with the CH width encoded in the
        // Arrow precision (9 → Decimal32, 18 → Decimal64, 38 → Decimal128,
        // 76 → Decimal256). The scale is the *second* arg of the Arrow type
        // and lives on the type only — the wire payload is just the raw
        // signed mantissa.
        //
        // Prefer the CH-side scale from type_hint when available (already
        // the canonical usize); fall back to the Arrow type's scale
        // otherwise. Emit the narrowest Value variant the data fits.
        DataType::Decimal128(precision, scale) => {
            let arr = column
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| Error::ArrowDeserialize("Expected Decimal128Array".to_string()))?;
            let s: usize = match type_hint {
                Some(Type::Decimal32(s) | Type::Decimal64(s) | Type::Decimal128(s)) => {
                    usize::from(*s)
                }
                _ => arrow_scale_to_usize(*scale),
            };
            let narrow = match type_hint {
                Some(Type::Decimal32(_)) => DecimalNarrow::D32,
                Some(Type::Decimal64(_)) => DecimalNarrow::D64,
                Some(Type::Decimal128(_)) => DecimalNarrow::D128,
                _ => match *precision {
                    p if p <= 9 => DecimalNarrow::D32,
                    p if p <= 18 => DecimalNarrow::D64,
                    _ => DecimalNarrow::D128,
                },
            };
            map_or_null(
                (0..arr.len()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }),
                move |v| match narrow {
                    DecimalNarrow::D32 =>
                    {
                        #[expect(clippy::cast_possible_truncation)]
                        Value::Decimal32(s, v as i32)
                    }
                    DecimalNarrow::D64 =>
                    {
                        #[expect(clippy::cast_possible_truncation)]
                        Value::Decimal64(s, v as i64)
                    }
                    DecimalNarrow::D128 => Value::Decimal128(s, v),
                },
            )
        }
        DataType::Decimal256(_precision, scale) => {
            let arr = column
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| Error::ArrowDeserialize("Expected Decimal256Array".to_string()))?;
            let s: usize = match type_hint {
                Some(Type::Decimal256(s)) => usize::from(*s),
                _ => arrow_scale_to_usize(*scale),
            };
            map_or_null(
                (0..arr.len())
                    .map(|i| if arr.is_null(i) { None } else { Some((s, arr.value(i).into())) }),
                |(s, v)| Value::Decimal256(s, v),
            )
        }

        // Date types
        //
        // Arrow Date32 is i32 days-since-1970, matching CH's Date32. CH's
        // Date is u16 days-since-1970 (range 1970-01-01..=2149-06-06) and
        // also round-trips through Arrow Date32. Pick the Value variant
        // from type_hint so a pre-1970 or post-2149 Date32 value doesn't
        // panic the u16-bounded Date::from_days path.
        DataType::Date32 if matches!(type_hint, Some(Type::Date32)) => {
            map_or_null(array_to_i32_iter(column)?, |d| Value::Date32(Date32(d)))
        }
        DataType::Date32 => {
            map_or_null(array_to_i32_iter(column)?, |d| Value::Date(Date::from_days(d)))
        }
        DataType::Date64 => {
            let tz = type_hint.and_then(|t| match t {
                Type::DateTime64(_, tz) => Some(Arc::from(tz.clone().to_string().as_str())),
                Type::Date | Type::Date32 => Some(Arc::from("UTC")),
                _ => None,
            });
            map_or_null(array_to_i64_iter(column)?, |ms| {
                Value::DateTime64(DynDateTime64::from_millis(ms, tz.clone()))
            })
        }

        // Timestamp/DateTime types
        DataType::Timestamp(unit, tz) => match unit {
            TimeUnit::Second => map_or_null(
                array_to_i64_iter(column)?
                    .map(|opt| opt.map(|s| DateTime::from_seconds(s, tz.clone()))),
                Value::DateTime,
            ),
            TimeUnit::Millisecond => map_or_null(
                array_to_i64_iter(column)?
                    .map(|opt| opt.map(|ms| DynDateTime64::from_millis(ms, tz.clone()))),
                Value::DateTime64,
            ),
            TimeUnit::Microsecond => map_or_null(
                array_to_i64_iter(column)?
                    .map(|opt| opt.map(|us| DynDateTime64::from_micros(us, tz.clone()))),
                Value::DateTime64,
            ),
            TimeUnit::Nanosecond => map_or_null(
                array_to_i64_iter(column)?
                    .map(|opt| opt.map(|ns| DynDateTime64::from_nanos(ns, tz.clone()))),
                Value::DateTime64,
            ),
        },

        // List type
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            let data_type = f.data_type();
            let inner_type_hint = type_hint.and_then(|t| match t {
                Type::Array(inner) => Some(&(**inner)),
                _ => None,
            });
            let mut caster = |a: Option<ArrayRef>| {
                a.map_or(Ok(Value::Null), |arr| {
                    array_to_values(&arr, data_type, inner_type_hint).map(Value::Array)
                })
            };
            array_to_list_vec(column, &mut caster)?
        }

        // Struct type (map to Tuple)
        DataType::Struct(fields) => {
            let struct_array = column.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::ArrowDeserialize("Could not downcast struct array".to_string())
            })?;
            (0..struct_array.len())
                .map(|i| {
                    if struct_array.is_null(i) {
                        Ok(Value::Null)
                    } else {
                        let field_values = fields
                            .iter()
                            .enumerate()
                            .map(|(j, field)| {
                                let field_array = struct_array.column(j);
                                let single_value = array_to_values(
                                    &field_array.slice(i, 1),
                                    field.data_type(),
                                    None,
                                )?;
                                Ok(single_value[0].clone())
                            })
                            .collect::<Result<Vec<Value>>>()?;
                        Ok(Value::Tuple(field_values))
                    }
                })
                .collect::<Result<Vec<Value>>>()?
        }

        // Map type
        DataType::Map(_, _) => {
            let map_array = column.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                Error::ArrowDeserialize("Could not downcast map array".to_string())
            })?;
            (0..map_array.len())
                .map(|i| {
                    if map_array.is_null(i) {
                        Ok(Value::Null)
                    } else {
                        let entry = map_array.value(i);
                        let keys_type = map_array.keys().data_type();
                        let values_type = map_array.values().data_type();
                        Ok(Value::Map(
                            array_to_values(&entry.column(0), keys_type, None)?,
                            array_to_values(&entry.column(1), values_type, None)?,
                        ))
                    }
                })
                .collect::<Result<Vec<Value>>>()?
        }

        // Dictionary type - need to unpack it first
        DataType::Dictionary(key_type, value_type) => {
            match (key_type.as_ref(), type_hint) {
                (DataType::Int8, Some(Type::Enum8(pairs))) => {
                    return Ok(array_to_string_iter(column)?
                        .map(|v| {
                            if let Some(v) = v {
                                pairs
                                    .iter()
                                    .find(|(value, _)| &v == value)
                                    .map_or(Value::Null, |(_, i)| Value::Enum8(v, *i))
                            } else {
                                Value::Null
                            }
                        })
                        .collect::<Vec<_>>());
                }
                (DataType::Int16, Some(Type::Enum16(pairs))) => {
                    return Ok(array_to_string_iter(column)?
                        .map(|v| {
                            if let Some(v) = v {
                                pairs
                                    .iter()
                                    .find(|(value, _)| &v == value)
                                    .map_or(Value::Null, |(_, i)| Value::Enum16(v, *i))
                            } else {
                                Value::Null
                            }
                        })
                        .collect::<Vec<_>>());
                }
                _ => {}
            }

            let unpacked = cast(column, value_type).map_err(Error::Arrow)?;
            array_to_values(&unpacked, value_type, type_hint)?
        }

        // Null type
        DataType::Null => vec![Value::Null; column.len()],

        // For all other types, return an error
        _ => {
            return Err(Error::ArrowUnsupportedType(format!(
                "Unsupported Arrow data type: {data_type:?}"
            )));
        }
    })
}

/// Modify the items for list-like arrays
///
/// # Errors
/// Errors if the array cannot be downcast
pub fn array_to_list_vec<T>(
    array: &dyn Array,
    caster: &mut impl FnMut(Option<ArrayRef>) -> Result<T>,
) -> Result<Vec<T>> {
    match array.data_type() {
        DataType::List(_) => {
            let array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                Error::ArrowDeserialize("Failed to downcast to ListArray".to_string())
            })?;
            Ok(array.iter().map(caster).collect::<Result<Vec<_>>>()?)
        }
        DataType::LargeList(_) => {
            let array = array.as_any().downcast_ref::<LargeListArray>().ok_or_else(|| {
                Error::ArrowDeserialize("Failed to downcast to LargeListArray".to_string())
            })?;
            Ok(array.iter().map(caster).collect::<Result<Vec<_>>>()?)
        }
        DataType::ListView(_) => {
            let array = array.as_any().downcast_ref::<ListViewArray>().ok_or_else(|| {
                Error::ArrowDeserialize("Failed to downcast to ListView".to_string())
            })?;
            Ok(array.iter().map(caster).collect::<Result<Vec<_>>>()?)
        }
        DataType::FixedSizeList(..) => {
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| {
                Error::ArrowDeserialize("Failed to downcast to FixedSizeListArray".to_string())
            })?;
            Ok(array.iter().map(caster).collect::<Result<Vec<_>>>()?)
        }
        _ => Err(Error::ArrowUnsupportedType(format!(
            "Could not cast array to list type: {:?}",
            array.data_type()
        ))),
    }
}

/// Converts any array that can be cast to a string array into an iterator of [`Option<String>`]
///
/// # Errors
/// Returns an error if the array cannot be cast to a string array.
pub fn array_to_string_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<String>>> {
    // First, cast the array to Utf8 (String) type
    let string_array = if let Ok(array) = cast(array, &DataType::Utf8) {
        array

    // Then try Binary
    } else {
        let binary_array = cast(array, &DataType::Binary).map_err(Error::Arrow)?;
        cast(&binary_array, &DataType::Utf8).map_err(Error::Arrow)?
    }
    .as_string_opt::<i32>()
    .ok_or(Error::ArrowUnsupportedType(format!(
        "Unable to downcast array to string: type hint={:?}",
        array.data_type(),
    )))?
    .clone();

    // Return an iterator that yields Option<String> for each element
    let iter = (0..string_array.len()).map(move |i| {
        if string_array.is_null(i) { None } else { Some(string_array.value(i).to_string()) }
    });

    Ok(iter)
}

/// Converts any array that can be cast to a binary array into an iterator of [`Option<Vec<u8>>`]
///
/// # Errors
/// Returns an error if the array cannot be cast to a binary array.
pub fn array_to_binary_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<Vec<u8>>>> {
    // First, cast the array to Binary type
    let binary_array = cast(array, &DataType::Binary)
        .map_err(Error::Arrow)?
        .as_binary_opt::<i32>()
        .ok_or(Error::ArrowUnsupportedType(format!(
            "Unable to downcast array to binary: type hint={:?}",
            array.data_type(),
        )))?
        .clone();

    // Return an iterator that yields Option<String> for each element
    let iter = (0..binary_array.len()).map(move |i| {
        if binary_array.is_null(i) { None } else { Some(binary_array.value(i).to_vec()) }
    });

    Ok(iter)
}

/// Converts any array that can be cast to a boolean array into an iterator of [`Option<bool>`]
///
/// # Errors
/// Returns an error if the array cannot be cast to a bool array.
pub fn array_to_bool_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<bool>>> {
    // First, cast the array to Boolean type
    let bool_array = cast(array, &DataType::Boolean)
        .map_err(Error::Arrow)?
        .as_boolean_opt()
        .ok_or(Error::ArrowUnsupportedType(format!(
            "Unable to downcast array boolean: type hint={:?}",
            array.data_type(),
        )))?
        .clone();

    // Return an iterator that yields Option<bool> for each element
    let iter = (0..bool_array.len())
        .map(move |i| if bool_array.is_null(i) { None } else { Some(bool_array.value(i)) });

    Ok(iter)
}

/// Directly converts an arrow [`PrimitiveArray`] to a rust primitive type
///
/// # Errors
/// Returns an error if the array cannot be cast to the target arrow type.
pub fn array_to_native_iter<A, T>(array: &dyn Array) -> Result<impl Iterator<Item = Option<T>>>
where
    A: ArrowPrimitiveType,
    A::Native: Into<T>,
    T: Clone,
{
    let cast_array = cast(array, &A::DATA_TYPE).map_err(Error::Arrow)?;
    let primitive_array = cast_array
        .as_primitive_opt::<A>()
        .ok_or(Error::ArrowUnsupportedType(format!(
            "Unable to downcast array {}: type hint={:?}",
            A::DATA_TYPE,
            array.data_type(),
        )))?
        .clone();

    let iter = (0..primitive_array.len()).map(move |i| {
        if primitive_array.is_null(i) { None } else { Some(primitive_array.value(i).into()) }
    });

    Ok(iter)
}

/// Converts any array to an iterator of [`Option<i8>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_i8_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<i8>>> {
    array_to_native_iter::<Int8Type, _>(array)
}

/// Converts any array to an iterator of [`Option<i16>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_i16_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<i16>>> {
    array_to_native_iter::<Int16Type, _>(array)
}

/// Converts any array to an iterator of [`Option<i32>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_i32_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<i32>>> {
    array_to_native_iter::<Int32Type, _>(array)
}

/// Converts any array to an iterator of [`Option<i64>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_i64_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<i64>>> {
    array_to_native_iter::<Int64Type, _>(array)
}

/// Converts any array to an iterator of [`Option<u8>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_u8_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<u8>>> {
    array_to_native_iter::<UInt8Type, _>(array)
}

/// Converts any array to an iterator of [`Option<u16>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_u16_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<u16>>> {
    array_to_native_iter::<UInt16Type, _>(array)
}

/// Converts any array to an iterator of [`Option<u32>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_u32_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<u32>>> {
    array_to_native_iter::<UInt32Type, _>(array)
}

/// Converts any array to an iterator of [`Option<u64>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_u64_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<u64>>> {
    array_to_native_iter::<UInt64Type, _>(array)
}

/// Converts any array to an iterator of [`Option<f32>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_f32_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<f32>>> {
    array_to_native_iter::<Float32Type, _>(array)
}

/// Converts any array to an iterator of [`Option<f64>`]
///
/// # Errors
/// Returns an error if the array cannot be cast
pub fn array_to_f64_iter(array: &dyn Array) -> Result<impl Iterator<Item = Option<f64>>> {
    array_to_native_iter::<Float64Type, _>(array)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::buffer::{Buffer, OffsetBuffer};
    use arrow::datatypes::*;
    use chrono_tz::Tz;

    use super::*;
    use crate::arrow::types::{
        LIST_ITEM_FIELD_NAME, MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME,
    };

    // Helper function to collect iterator to a Vec for easier testing
    fn collect_string_iter<I: Iterator<Item = Option<String>>>(iter: I) -> Vec<Option<String>> {
        iter.collect()
    }

    // Helper function to collect binary iterator to a Vec for easier testing
    fn collect_binary_iter<I: Iterator<Item = Option<Vec<u8>>>>(iter: I) -> Vec<Option<Vec<u8>>> {
        iter.collect()
    }

    // Helper function to collect boolean iterator to a Vec for easier testing
    fn collect_bool_iter<I: Iterator<Item = Option<bool>>>(iter: I) -> Vec<Option<bool>> {
        iter.collect()
    }

    #[test]
    fn test_string_array() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("hello".to_string()), None, Some("world".to_string())]);
    }

    #[test]
    fn test_large_string_array() {
        let array = LargeStringArray::from(vec![Some("hello"), None, Some("world")]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("hello".to_string()), None, Some("world".to_string())]);
    }

    #[test]
    fn test_string_view_array() {
        let array = StringViewArray::from(vec![Some("hello"), None, Some("world")]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("hello".to_string()), None, Some("world".to_string())]);
    }

    #[test]
    fn test_binary_array() {
        let array =
            BinaryArray::from(vec![Some("hello".as_bytes()), None, Some("world".as_bytes())]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("hello".to_string()), None, Some("world".to_string())]);
    }

    #[test]
    fn test_large_binary_array() {
        let array =
            LargeBinaryArray::from(vec![Some("hello".as_bytes()), None, Some("world".as_bytes())]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("hello".to_string()), None, Some("world".to_string())]);
    }

    #[test]
    fn test_binary_view_array() {
        let array =
            BinaryViewArray::from(vec![Some("hello".as_bytes()), None, Some("world".as_bytes())]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("hello".to_string()), None, Some("world".to_string())]);
    }

    #[test]
    fn test_fixed_size_binary_array() {
        // Create a fixed size binary array with 5-byte elements
        let array = FixedSizeBinaryArray::try_from_iter(
            vec!["hello".as_bytes(), "world".as_bytes()].into_iter(),
        )
        .unwrap();
        let result = array_to_string_iter(&array).unwrap().collect::<Vec<_>>();
        assert_eq!(result, vec![Some("hello".to_string()), Some("world".to_string())]);
    }

    #[test]
    fn test_dictionary_array() {
        // Create a dictionary array with string values
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        let _ = builder.append("hello").unwrap();
        builder.append_null();
        let _ = builder.append("world").unwrap();
        let array = builder.finish();

        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("hello".to_string()), None, Some("world".to_string())]);
    }

    #[test]
    fn test_boolean_to_string() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("true".to_string()), None, Some("false".to_string())]);
    }

    #[test]
    fn test_numeric_to_string() {
        let array = Int32Array::from(vec![Some(42), None, Some(-123)]);
        let result = collect_string_iter(array_to_string_iter(&array).unwrap());
        assert_eq!(result, vec![Some("42".to_string()), None, Some("-123".to_string())]);
    }

    // Tests for the numeric conversion functions
    #[test]
    fn test_i32_array() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_i32_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_i64_array() {
        let array = Int64Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_i64_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_f64_array() {
        let array = Float64Array::from(vec![Some(1.5), None, Some(3.7)]);
        let result: Vec<_> = array_to_f64_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1.5), None, Some(3.7)]);
    }

    #[test]
    fn test_i8_array() {
        let array = Int8Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_i8_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_i16_array() {
        let array = Int16Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_i16_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_u8_array() {
        let array = UInt8Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_u8_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_u16_array() {
        let array = UInt16Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_u16_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_u32_array() {
        let array = UInt32Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_u32_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_u64_array() {
        let array = UInt64Array::from(vec![Some(1), None, Some(3)]);
        let result: Vec<_> = array_to_u64_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_f32_array() {
        let array = Float32Array::from(vec![Some(1.5), None, Some(3.7)]);
        let result: Vec<_> = array_to_f32_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1.5), None, Some(3.7)]);
    }

    #[test]
    fn test_int32_array() {
        let array = Int32Array::from(vec![Some(42), None, Some(-123)]);
        let result = array_to_values(&array, &DataType::Int32, None).unwrap();
        assert_eq!(result, vec![Value::Int32(42), Value::Null, Value::Int32(-123)]);
    }

    #[test]
    fn test_float64_array() {
        let array = Float64Array::from(vec![Some(3.15), None, Some(-2.719)]);
        let result = array_to_values(&array, &DataType::Float64, None).unwrap();
        assert_eq!(result, vec![Value::Float64(3.15), Value::Null, Value::Float64(-2.719)]);
    }

    #[test]
    fn test_utf8_array() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let result = array_to_values(&array, &DataType::Utf8, None).unwrap();
        assert_eq!(result, vec![
            Value::String(b"hello".to_vec()),
            Value::Null,
            Value::String(b"world".to_vec()),
        ]);
    }

    #[test]
    fn test_binary_array_values() {
        let array = BinaryArray::from(vec![Some(b"abc".as_ref()), None, Some(b"def".as_ref())]);
        let result = array_to_values(&array, &DataType::Binary, None).unwrap();
        assert_eq!(result, vec![
            Value::String(b"abc".to_vec()),
            Value::Null,
            Value::String(b"def".to_vec()),
        ]);
    }

    #[test]
    fn test_binary_array_direct() {
        let array = BinaryArray::from(vec![Some(b"abc".as_ref()), None, Some(b"def".as_ref())]);
        let result = collect_binary_iter(array_to_binary_iter(&array).unwrap());
        assert_eq!(result, vec![Some(b"abc".to_vec()), None, Some(b"def".to_vec()),]);
    }

    #[test]
    fn test_bool_array_direct() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let result = collect_bool_iter(array_to_bool_iter(&array).unwrap());
        assert_eq!(result, vec![Some(true), None, Some(false)]);
    }

    #[test]
    fn test_large_list_array() {
        let values = Int32Array::from(vec![1, 2, 3, 4]);
        let offsets_buffer = Buffer::from_vec(vec![0_i64, 2_i64, 4_i64]); // Explicit i64
        let offsets = OffsetBuffer::new(offsets_buffer.into());
        let large_list_array = LargeListArray::new(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        let result = array_to_values(
            &large_list_array,
            &DataType::LargeList(Arc::new(Field::new(
                LIST_ITEM_FIELD_NAME,
                DataType::Int32,
                false,
            ))),
            None,
        )
        .unwrap();
        assert_eq!(result, vec![
            Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
            Value::Array(vec![Value::Int32(3), Value::Int32(4)]),
        ]);
    }

    #[test]
    fn test_fixed_size_list_array() {
        let values = Int32Array::from(vec![1, 2, 3, 4]);
        let fixed_size_list_array = FixedSizeListArray::new(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)),
            2,
            Arc::new(values),
            None,
        );
        let result = array_to_values(
            &fixed_size_list_array,
            &DataType::FixedSizeList(
                Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)),
                2,
            ),
            None,
        )
        .unwrap();
        assert_eq!(result, vec![
            Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
            Value::Array(vec![Value::Int32(3), Value::Int32(4)]),
        ]);
    }

    #[test]
    fn test_empty_list_array() {
        let values = Int32Array::from(Vec::<i32>::new());
        let offsets_buffer = Buffer::from_vec(vec![0, 0, 0]);
        let offsets = OffsetBuffer::new(offsets_buffer.into());
        let list_array = ListArray::new(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        let result = array_to_values(
            &list_array,
            &DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false))),
            None,
        )
        .unwrap();
        assert_eq!(result, vec![Value::Array(vec![]), Value::Array(vec![])]);
    }

    #[test]
    fn test_enum8_dictionary() {
        let pairs = vec![("a".to_string(), 1_i8), ("b".to_string(), 2_i8)];
        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        let _ = builder.append("a").unwrap();
        builder.append_null();
        let _ = builder.append("b").unwrap();
        let array = builder.finish();
        let result = array_to_values(
            &array,
            &DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            Some(&Type::Enum8(pairs.clone())),
        )
        .unwrap();
        assert_eq!(result, vec![
            Value::Enum8("a".to_string(), 1),
            Value::Null,
            Value::Enum8("b".to_string(), 2),
        ]);
    }

    #[test]
    fn test_enum16_dictionary() {
        let pairs = vec![("x".to_string(), 10_i16), ("y".to_string(), 20_i16)];
        let mut builder = StringDictionaryBuilder::<Int16Type>::new();
        let _ = builder.append("x").unwrap();
        builder.append_null();
        let _ = builder.append("y").unwrap();
        let array = builder.finish();
        let result = array_to_values(
            &array,
            &DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            Some(&Type::Enum16(pairs.clone())),
        )
        .unwrap();
        assert_eq!(result, vec![
            Value::Enum16("x".to_string(), 10),
            Value::Null,
            Value::Enum16("y".to_string(), 20),
        ]);
    }

    #[test]
    fn test_nested_struct_array() {
        let inner_field = Arc::new(Field::new("inner", DataType::Int32, false));
        let inner_struct_array = StructArray::from(vec![(
            Arc::clone(&inner_field),
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )]);
        let outer_field =
            Arc::new(Field::new("outer", inner_struct_array.data_type().clone(), false));
        let outer_struct_array = StructArray::from(vec![(
            Arc::clone(&outer_field),
            Arc::new(inner_struct_array) as ArrayRef,
        )]);
        let fields = Fields::from_iter(vec![outer_field]);
        let result = array_to_values(&outer_struct_array, &DataType::Struct(fields), None).unwrap();
        assert_eq!(result, vec![
            Value::Tuple(vec![Value::Tuple(vec![Value::Int32(1)])]),
            Value::Tuple(vec![Value::Tuple(vec![Value::Int32(2)])]),
        ]);
    }

    #[test]
    fn test_timestamp_non_utc() {
        let tz: Arc<str> = Arc::from("America/New_York");
        let array =
            TimestampSecondArray::from(vec![Some(1_625_097_600), None, Some(1_625_184_000)]);
        let result =
            array_to_values(&array, &DataType::Timestamp(TimeUnit::Second, Some(tz)), None)
                .unwrap();
        assert_eq!(result, vec![
            Value::DateTime(DateTime(Tz::America__New_York, 1_625_097_600)),
            Value::Null,
            Value::DateTime(DateTime(Tz::America__New_York, 1_625_184_000)),
        ]);
    }

    // Cross-type conversion tests
    #[test]
    fn test_string_to_i32() {
        let array = StringArray::from(vec![Some("42"), None, Some("-123")]);
        let result: Vec<_> = array_to_i32_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(42), None, Some(-123)]);
    }

    #[test]
    fn test_i32_to_f64() {
        let array = Int32Array::from(vec![Some(42), None, Some(-123)]);
        let result: Vec<_> = array_to_f64_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(42.0), None, Some(-123.0)]);
    }

    #[test]
    fn test_bool_to_i32() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let result: Vec<_> = array_to_i32_iter(&array).unwrap().collect();
        assert_eq!(result, vec![Some(1), None, Some(0)]);
    }

    #[test]
    fn test_fixed_size_binary_as_string() {
        let array = FixedSizeBinaryArray::try_from_iter(
            vec![b"abcde".as_ref(), b"fghij".as_ref()].into_iter(),
        )
        .unwrap();
        let result = array_to_values(&array, &DataType::FixedSizeBinary(5), None).unwrap();
        assert_eq!(result, vec![
            Value::String(b"abcde".to_vec()),
            Value::String(b"fghij".to_vec()),
        ]);
    }

    #[test]
    fn test_fixed_size_binary_as_uuid() {
        let uuid1 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let array = FixedSizeBinaryArray::try_from_iter(
            vec![uuid1.as_bytes(), uuid2.as_bytes()].into_iter(),
        )
        .unwrap();
        let result =
            array_to_values(&array, &DataType::FixedSizeBinary(16), Some(&Type::Uuid)).unwrap();
        assert_eq!(result, vec![Value::Uuid(uuid1), Value::Uuid(uuid2)]);
    }

    #[test]
    fn test_boolean_array() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let result = array_to_values(&array, &DataType::Boolean, None).unwrap();
        assert_eq!(result, vec![Value::UInt8(1), Value::Null, Value::UInt8(0)]);
    }

    #[test]
    fn test_decimal128_array() {
        // Arrow Decimal128(10, 2) is the storage CH uses for Decimal64(2).
        // Without a type hint we infer the CH width from precision: 10 <= 18
        // → CH Decimal64. The first tuple member is the CH-side scale.
        let array = Decimal128Array::from_iter_values([12345, -67890])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let result = array_to_values(&array, &DataType::Decimal128(10, 2), None).unwrap();
        assert_eq!(result.len(), 2);
        match &result[0] {
            Value::Decimal64(s, v) => {
                assert_eq!(*s, 2);
                assert_eq!(*v, 12345); // Raw mantissa for 123.45 with scale 2
            }
            other => panic!("Expected Decimal64, got {other:?}"),
        }
        match &result[1] {
            Value::Decimal64(s, v) => {
                assert_eq!(*s, 2);
                assert_eq!(*v, -67890); // Raw mantissa for -678.90 with scale 2
            }
            other => panic!("Expected Decimal64, got {other:?}"),
        }
    }

    #[test]
    fn test_decimal128_array_wide() {
        // Arrow Decimal128(38, 6) maps to CH Decimal128(6).
        let array = Decimal128Array::from_iter_values([1_234_567_890_i128, -987_654_321_i128])
            .with_precision_and_scale(38, 6)
            .unwrap();
        let result = array_to_values(&array, &DataType::Decimal128(38, 6), None).unwrap();
        match &result[0] {
            Value::Decimal128(s, v) => {
                assert_eq!(*s, 6);
                assert_eq!(*v, 1_234_567_890_i128);
            }
            other => panic!("Expected Decimal128, got {other:?}"),
        }
    }

    #[test]
    fn test_decimal128_array_with_type_hint() {
        // type_hint Type::Decimal32(2) forces narrow even with a wider
        // Arrow precision (which a user might pass when reconstructing
        // from a Decimal32 source).
        let array = Decimal128Array::from_iter_values([12345, -67890])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let result =
            array_to_values(&array, &DataType::Decimal128(10, 2), Some(&Type::Decimal32(2)))
                .unwrap();
        match &result[0] {
            Value::Decimal32(s, v) => {
                assert_eq!(*s, 2);
                assert_eq!(*v, 12345);
            }
            other => panic!("Expected Decimal32, got {other:?}"),
        }
    }

    #[test]
    fn test_decimal128_array_error() {
        let array = StringArray::from(vec![""]);
        let result = array_to_values(&array, &DataType::Decimal128(10, 2), None);
        assert!(matches!(
            result.unwrap_err(),
            Error::ArrowDeserialize(err)
            if err.clone().contains("Expected Decimal128Array")
        ));
    }

    #[test]
    fn test_decimal256_array() {
        // CH Decimal256 always emits Value::Decimal256 with the CH-side scale.
        let array =
            Decimal256Array::from_iter_values([i256::from_i128(12345), i256::from_i128(-67890)])
                .with_precision_and_scale(20, 2)
                .unwrap();
        let result = array_to_values(&array, &DataType::Decimal256(20, 2), None).unwrap();
        assert_eq!(result.len(), 2);
        match &result[0] {
            Value::Decimal256(s, v) => {
                assert_eq!(*s, 2);
                assert_eq!(*v, crate::i256(i256::from_i128(12345).to_be_bytes()));
            }
            _ => panic!("Expected Decimal256"),
        }
        match &result[1] {
            Value::Decimal256(s, v) => {
                assert_eq!(*s, 2);
                assert_eq!(*v, crate::i256(i256::from_i128(-67890).to_be_bytes()));
            }
            _ => panic!("Expected Decimal256"),
        }
    }

    // --- array_to_values type_hint branches: IPv4/6 + wide ints -----------
    //
    // These FixedSizeBinary branches only fire when the matching `type_hint`
    // is supplied; the deserializer has already normalised the bytes to the
    // form each `Value` variant expects.

    #[test]
    fn test_array_to_values_ipv4() {
        let a = std::net::Ipv4Addr::LOCALHOST.octets();
        let b = std::net::Ipv4Addr::new(192, 168, 1, 42).octets();
        let array =
            FixedSizeBinaryArray::try_from_iter([a.as_slice(), b.as_slice()].into_iter()).unwrap();
        let result =
            array_to_values(&array, &DataType::FixedSizeBinary(4), Some(&Type::Ipv4)).unwrap();
        assert_eq!(result, vec![
            Value::Ipv4(Ipv4(std::net::Ipv4Addr::LOCALHOST)),
            Value::Ipv4(Ipv4(std::net::Ipv4Addr::new(192, 168, 1, 42))),
        ]);
    }

    #[test]
    fn test_array_to_values_ipv6() {
        let addr = std::net::Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
        let array =
            FixedSizeBinaryArray::try_from_iter([addr.octets().as_slice()].into_iter()).unwrap();
        let result =
            array_to_values(&array, &DataType::FixedSizeBinary(16), Some(&Type::Ipv6)).unwrap();
        assert_eq!(result, vec![Value::Ipv6(Ipv6(addr))]);
    }

    #[test]
    fn test_array_to_values_int128() {
        let vals = [i128::MIN, 0, i128::MAX];
        let bytes: Vec<[u8; 16]> = vals.iter().map(|v| v.to_le_bytes()).collect();
        let array =
            FixedSizeBinaryArray::try_from_iter(bytes.iter().map(<[u8; 16]>::as_slice)).unwrap();
        let result =
            array_to_values(&array, &DataType::FixedSizeBinary(16), Some(&Type::Int128)).unwrap();
        assert_eq!(result, vec![
            Value::Int128(i128::MIN),
            Value::Int128(0),
            Value::Int128(i128::MAX),
        ]);
    }

    #[test]
    fn test_array_to_values_uint128() {
        let vals = [0_u128, u128::MAX];
        let bytes: Vec<[u8; 16]> = vals.iter().map(|v| v.to_le_bytes()).collect();
        let array =
            FixedSizeBinaryArray::try_from_iter(bytes.iter().map(<[u8; 16]>::as_slice)).unwrap();
        let result =
            array_to_values(&array, &DataType::FixedSizeBinary(16), Some(&Type::UInt128)).unwrap();
        assert_eq!(result, vec![Value::UInt128(0), Value::UInt128(u128::MAX)]);
    }

    #[test]
    fn test_array_to_values_int256() {
        // The 32 wire bytes are stored verbatim into crate::i256.
        let raw = [7_u8; 32];
        let array = FixedSizeBinaryArray::try_from_iter([raw.as_slice()].into_iter()).unwrap();
        let result =
            array_to_values(&array, &DataType::FixedSizeBinary(32), Some(&Type::Int256)).unwrap();
        assert_eq!(result, vec![Value::Int256(crate::i256(raw))]);
    }

    #[test]
    fn test_array_to_values_uint256() {
        let raw = [9_u8; 32];
        let array = FixedSizeBinaryArray::try_from_iter([raw.as_slice()].into_iter()).unwrap();
        let result =
            array_to_values(&array, &DataType::FixedSizeBinary(32), Some(&Type::UInt256)).unwrap();
        assert_eq!(result, vec![Value::UInt256(crate::u256(raw))]);
    }

    #[test]
    fn test_array_to_values_fixed_size_binary_no_hint_is_string() {
        // No matching type_hint -> the catch-all wraps raw bytes as String.
        let array = FixedSizeBinaryArray::try_from_iter([b"abcd".as_slice()].into_iter()).unwrap();
        let result = array_to_values(&array, &DataType::FixedSizeBinary(4), None).unwrap();
        assert_eq!(result, vec![Value::String(b"abcd".to_vec())]);
    }

    #[test]
    fn test_decimal256_array_error() {
        let array = StringArray::from(vec![""]);
        let result = array_to_values(&array, &DataType::Decimal256(20, 2), None);
        assert!(matches!(
            result.unwrap_err(),
            Error::ArrowDeserialize(err)
            if err.clone().contains("Expected Decimal256Array")
        ));
    }

    #[test]
    fn test_date32_array() {
        let array = Date32Array::from(vec![Some(0), None, Some(1)]);
        let result = array_to_values(&array, &DataType::Date32, None).unwrap();
        assert_eq!(result, vec![Value::Date(Date(0)), Value::Null, Value::Date(Date(1))]);
    }

    #[test]
    fn test_date64_array() {
        let array = Date64Array::from(vec![Some(0), None, Some(1)]);
        let result = array_to_values(&array, &DataType::Date64, None).unwrap();
        assert_eq!(result, vec![
            Value::DateTime64(DynDateTime64(Tz::UTC, 0, 3)),
            Value::Null,
            Value::DateTime64(DynDateTime64(Tz::UTC, 1, 3)),
        ]);

        // With timezone
        let typ = Type::DateTime64(3, Tz::America__New_York);
        let result = array_to_values(&array, &DataType::Date64, Some(&typ)).unwrap();
        assert_eq!(result, vec![
            Value::DateTime64(DynDateTime64(Tz::America__New_York, 0, 3)),
            Value::Null,
            Value::DateTime64(DynDateTime64(Tz::America__New_York, 1, 3)),
        ]);

        // With timezone default
        let typ = Type::Date;
        let result = array_to_values(&array, &DataType::Date64, Some(&typ)).unwrap();
        assert_eq!(result, vec![
            Value::DateTime64(DynDateTime64(Tz::UTC, 0, 3)),
            Value::Null,
            Value::DateTime64(DynDateTime64(Tz::UTC, 1, 3)),
        ]);
    }

    #[test]
    fn test_timestamp_second_array() {
        let array =
            TimestampSecondArray::from(vec![Some(1_625_097_600), None, Some(1_625_184_000)]);
        let result =
            array_to_values(&array, &DataType::Timestamp(TimeUnit::Second, None), None).unwrap();
        assert_eq!(result, vec![
            Value::DateTime(DateTime(chrono_tz::UTC, 1_625_097_600)),
            Value::Null,
            Value::DateTime(DateTime(chrono_tz::UTC, 1_625_184_000)),
        ]);
    }

    #[test]
    fn test_list_array() {
        let values = Int32Array::from(vec![1, 2, 3, 4]);
        let offsets_buffer = Buffer::from_vec(vec![0, 2, 4]);
        let offsets = OffsetBuffer::new(offsets_buffer.into());
        let list_array = ListArray::new(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        let result = array_to_values(
            &list_array,
            &DataType::List(Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false))),
            None,
        )
        .unwrap();
        assert_eq!(result, vec![
            Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
            Value::Array(vec![Value::Int32(3), Value::Int32(4)]),
        ]);
    }

    #[test]
    fn test_struct_array() {
        let int_field = Arc::new(Field::new("a", DataType::Int32, false));
        let str_field = Arc::new(Field::new("b", DataType::Utf8, false));
        let struct_array = StructArray::from(vec![
            (Arc::clone(&int_field), Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            (Arc::clone(&str_field), Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef),
        ]);
        let fields = Fields::from_iter(vec![int_field, str_field]);
        let result = array_to_values(&struct_array, &DataType::Struct(fields), None).unwrap();
        assert_eq!(result, vec![
            Value::Tuple(vec![Value::Int32(1), Value::String(b"x".to_vec())]),
            Value::Tuple(vec![Value::Int32(2), Value::String(b"y".to_vec())]),
        ]);
    }

    #[test]
    fn test_struct_array_with_nulls() {
        let int_field = Arc::new(Field::new("a", DataType::Int32, false));
        let str_field = Arc::new(Field::new("b", DataType::Utf8, true));
        let struct_array = StructArray::from(vec![
            (Arc::clone(&int_field), Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            (
                Arc::clone(&str_field),
                Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
            ),
        ]);
        let fields = Fields::from_iter(vec![int_field, str_field]);
        let result = array_to_values(&struct_array, &DataType::Struct(fields), None).unwrap();
        assert_eq!(result, vec![
            Value::Tuple(vec![Value::Int32(1), Value::String(b"x".to_vec())]),
            Value::Tuple(vec![Value::Int32(2), Value::Null]),
        ]);
    }

    #[test]
    fn test_struct_array_err() {
        let int_field = Arc::new(Field::new("a", DataType::Int32, false));
        let str_field = Arc::new(Field::new("b", DataType::Utf8, false));
        let string_array = StringArray::from(vec![""]);
        let fields = Fields::from_iter(vec![int_field, str_field]);
        let result = array_to_values(&string_array, &DataType::Struct(fields), None);
        assert!(matches!(
            result,
            Err(Error::ArrowDeserialize(e))
            if e.clone().contains("Could not downcast struct array")
        ));
    }

    #[test]
    fn test_map_array() {
        let keys = Arc::new(StringArray::from(vec!["k1", "k2"])) as ArrayRef;
        let values = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false)), keys),
            (Arc::new(Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false)), values),
        ]);
        let map_array = MapArray::new(
            Arc::new(Field::new(MAP_FIELD_NAME, struct_array.data_type().clone(), false)),
            OffsetBuffer::new(Buffer::from_vec(vec![0, 1, 2]).into()),
            struct_array,
            None,
            false,
        );
        let result = array_to_values(
            &map_array,
            &DataType::Map(
                Arc::new(Field::new(
                    MAP_FIELD_NAME,
                    DataType::Struct(Fields::from_iter(vec![
                        Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false),
                        Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false),
                    ])),
                    false,
                )),
                false,
            ),
            None,
        )
        .unwrap();
        assert_eq!(result, vec![
            Value::Map(vec![Value::String(b"k1".to_vec())], vec![Value::Int32(10)]),
            Value::Map(vec![Value::String(b"k2".to_vec())], vec![Value::Int32(20)]),
        ]);
    }

    #[test]
    fn test_dictionary_array_values() {
        use arrow::array::StringDictionaryBuilder;
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        let _ = builder.append("hello").unwrap();
        builder.append_null();
        let _ = builder.append("world").unwrap();
        let array = builder.finish();
        let result = array_to_values(
            &array,
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            None,
        )
        .unwrap();
        assert_eq!(result, vec![
            Value::String(b"hello".to_vec()),
            Value::Null,
            Value::String(b"world".to_vec()),
        ]);
    }

    #[test]
    fn test_null_array() {
        let array = NullArray::new(3);
        let result = array_to_values(&array, &DataType::Null, None).unwrap();
        assert_eq!(result, vec![Value::Null, Value::Null, Value::Null]);
    }

    #[test]
    fn test_unhandled_array() {
        let array = StringArray::from(vec![""]);
        let result = array_to_values(&array, &DataType::Float16, None);
        assert!(matches!(result, Err(Error::ArrowUnsupportedType(_))));
    }

    #[test]
    fn test_batch_to_rows_simple() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v_0", DataType::Int8, false),
            Field::new("v_1", DataType::Int16, false),
            Field::new("v_2", DataType::Int32, false),
            Field::new("v_3", DataType::Int64, false),
            Field::new("v_4", DataType::UInt8, false),
            Field::new("v_5", DataType::UInt16, false),
            Field::new("v_6", DataType::UInt32, false),
            Field::new("v_7", DataType::UInt64, false),
            Field::new("v_8", DataType::Float32, false),
            Field::new("v_9", DataType::Float64, false),
            Field::new("v_10", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("v_11", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("v_12", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("v_13", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("v_14", DataType::Utf8, false),
        ]));
        let str_vals = vec!["a", "b", "c"];
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int8Array::from(vec![1, 2, 3])),
            Arc::new(Int16Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(UInt8Array::from(vec![1, 2, 3])),
            Arc::new(UInt16Array::from(vec![1, 2, 3])),
            Arc::new(UInt32Array::from(vec![1, 2, 3])),
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(Float32Array::from(vec![1.0_f32, 2.0, 3.0])),
            Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0])),
            Arc::new(TimestampSecondArray::from(vec![1, 2, 3])),
            Arc::new(TimestampMillisecondArray::from(vec![1000, 2 * 1000, 3 * 1000])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_000_000,
                2 * 1_000_000,
                3 * 1_000_000,
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1_000_000_000,
                2 * 1_000_000_000,
                3 * 1_000_000_000,
            ])),
            Arc::new(StringArray::from(str_vals.clone())),
        ])
        .unwrap();

        let result = batch_to_rows(&batch, None).unwrap().collect::<Vec<_>>();
        assert_eq!(result.len(), 3);

        #[expect(clippy::cast_precision_loss)]
        #[expect(clippy::cast_possible_truncation)]
        #[expect(clippy::cast_possible_wrap)]
        for (i, row) in result.into_iter().enumerate() {
            let row = row.unwrap();
            let seed = i + 1;
            assert_eq!(row, vec![
                Value::Int8(seed as i8),
                Value::Int16(seed as i16),
                Value::Int32(seed as i32),
                Value::Int64(seed as i64),
                Value::UInt8(seed as u8),
                Value::UInt16(seed as u16),
                Value::UInt32(seed as u32),
                Value::UInt64(seed as u64),
                Value::Float32(seed as f32),
                Value::Float64(seed as f64),
                Value::DateTime(DateTime(Tz::UTC, seed as u32)),
                Value::DateTime64(DynDateTime64(Tz::UTC, seed as u64 * 1000, 3)),
                Value::DateTime64(DynDateTime64(Tz::UTC, seed as u64 * 1_000_000, 6)),
                Value::DateTime64(DynDateTime64(Tz::UTC, seed as u64 * 1_000_000_000, 9)),
                Value::String(str_vals[i].as_bytes().to_vec())
            ]);
        }
    }

    #[test]
    fn test_batch_to_rows_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ])
        .unwrap();

        let mut result = batch_to_rows(&batch, None).unwrap().collect::<Vec<_>>();
        assert_eq!(result.len(), 3);
        assert_eq!(result.pop().unwrap().unwrap(), vec![Value::Int32(3), Value::Null]);
        assert_eq!(result.pop().unwrap().unwrap(), vec![Value::Null, Value::String(b"b".to_vec())]);
        assert_eq!(result.pop().unwrap().unwrap(), vec![
            Value::Int32(1),
            Value::String(b"a".to_vec())
        ]);
    }

    #[test]
    fn test_batch_to_rows_with_type_hints() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("uuid", DataType::FixedSizeBinary(16), false)]));
        let uuid1 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let uuid2 = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                vec![uuid1.as_bytes(), uuid2.as_bytes()].into_iter(),
            )
            .unwrap(),
        )])
        .unwrap();

        let type_hints = vec![("uuid".to_string(), Type::Uuid)];
        let mut result = batch_to_rows(&batch, Some(&type_hints)).unwrap().collect::<Vec<_>>();
        assert_eq!(result.len(), 2);
        assert_eq!(result.pop().unwrap().unwrap(), vec![Value::Uuid(uuid2)]);
        assert_eq!(result.pop().unwrap().unwrap(), vec![Value::Uuid(uuid1)]);
    }

    #[test]
    fn test_batch_to_rows_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))])
                .unwrap();

        let result = batch_to_rows(&batch, None).unwrap().collect::<Vec<_>>();
        assert_eq!(result.len(), 0);
    }

    // Failure tests
    #[test]
    fn test_invalid_string_conversion() {
        // Create an array with invalid UTF-8 data
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD]; // Invalid UTF-8 bytes
        let array = BinaryArray::from_iter_values(vec![&invalid_utf8]);

        // This should still technically work but the strings might be corrupted
        // as the system will substitute replacement characters
        let result = array_to_string_iter(&array);
        assert!(result.is_ok(), "Should convert even with replacement chars");
    }

    #[test]
    fn test_out_of_range_conversion() {
        // Try to convert a number that's too large for i8
        let array = Int32Array::from(vec![Some(1000)]); // Too large for i8
        let result = array_to_i8_iter(&array);

        // The conversion should succeed, but the value will be None because it's out of range
        let collected: Vec<_> = result.unwrap().collect();
        assert_eq!(collected, vec![None]);
    }
}
