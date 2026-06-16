//! Sparse column deserialization.
//!
//! `ClickHouse` uses sparse serialization for columns whose values are
//! mostly equal to their type's default. The wire format follows the
//! per-column kind-stack byte and consists of two streams:
//!
//! 1. **`SparseOffsets`** — a sequence of varints, matching CH's `deserializeOffsets`. Each
//!    varint's low 62 bits are a "group size": the count of default rows in this group. Bit 62
//!    (`END_OF_GRANULE_FLAG`) marks the terminating entry of the stream. A *non-terminating* entry
//!    means "`group_size` defaults, then exactly one non-default value"; the *terminating*
//!    (flagged) entry means "`group_size` trailing defaults, and no value follows". CH always
//!    writes a final flagged entry, so the loop is driven by the flag bit — not by a row count.
//!    Stopping early on a row-count match leaves the terminator unread and desyncs the next column
//!    body.
//!
//! 2. **`SparseElements`** — the nested type's body, holding exactly N values (where N is the
//!    number of non-default rows discovered in the offsets stream).
//!
//! On read we:
//!  - decode the offsets into a `Vec<u64>` of absolute non-default row positions in `0..rows`
//!  - deserialize the N nested values into a temporary Arrow array via the standard dense path
//!  - **materialize** a full-row Arrow array by walking row-by-row, appending the type's default at
//!    default rows and copying from the nested array at non-default rows.
//!
//! Sparse for `Nullable(T)` is the same shape, but the null map is
//! reconstructed implicitly from the offsets: default rows are null,
//! non-default rows take the nested value (which may itself be null
//! per the nested null map).

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

use super::ClickHouseArrowDeserializer;
use crate::arrow::builder::TypedBuilder;
use crate::deserialize::ClickHouseNativeDeserializer;
use crate::formats::DeserializerState;
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

/// High bit on a sparse-offset varint marks the last entry of a CH
/// serialization granule. We don't model granules at the protocol-block
/// layer, but the byte is still set on the last varint so we mask it
/// off before treating the value as a count.
const END_OF_GRANULE_FLAG: u64 = 1u64 << 62;

/// Read the `SparseOffsets` stream, returning the sorted absolute row
/// indices of the non-default values.
///
/// Mirrors CH's `deserializeOffsets`: read varints until the flagged
/// (`END_OF_GRANULE_FLAG`) terminator is consumed. A non-flagged entry is
/// `group_size` defaults then one non-default value at the resulting
/// position; the flagged entry is `group_size` trailing defaults with no
/// value after it. The terminator is always present on the wire, so the
/// loop must run until it is read — leaving it unread desyncs the next
/// column body (manifesting as a bogus compression-block header).
///
/// `rows` is used only as a sanity bound; the flag, not the row count,
/// terminates the stream.
async fn read_offsets<R: ClickHouseRead>(reader: &mut R, rows: usize) -> Result<Vec<u64>> {
    let mut offsets = Vec::new();
    let mut position: u64 = 0;
    let total = rows as u64;

    loop {
        let raw = reader.read_var_uint().await?;
        let end_of_granule = raw & END_OF_GRANULE_FLAG != 0;
        let group_size = raw & !END_OF_GRANULE_FLAG;

        position = position
            .checked_add(group_size)
            .ok_or_else(|| Error::Protocol("sparse offsets overflow".into()))?;

        if end_of_granule {
            // Terminating entry: `group_size` trailing defaults, no value.
            break;
        }

        // Non-terminating entry: a non-default value sits at `position`.
        if position >= total {
            return Err(Error::Protocol(format!(
                "sparse offset points past row count: position={position} total={total}"
            )));
        }
        offsets.push(position);
        position += 1;
    }

    if position > total {
        return Err(Error::Protocol(format!(
            "sparse offsets overshot row count: position={position} total={total}"
        )));
    }

    Ok(offsets)
}

/// Deserialize a sparse column to a full-row dense Arrow array.
///
/// Reads the offsets stream, then reads N non-default values into a
/// temporary array via the standard dense path, then materializes a
/// full-row column by interleaving defaults and non-default values.
///
/// Note there is no `builder` parameter (unlike the dense
/// `deserialize_arrow_async` path): the result is built entirely from the
/// interleave output, and the nested values are read into a fresh
/// builder sized for the non-default count. The caller uses the returned
/// `ArrayRef` directly.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn deserialize_async<R: ClickHouseRead>(
    type_hint: &Type,
    reader: &mut R,
    data_type: &DataType,
    is_nullable: bool,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
    prefix_state: &mut DeserializerState,
) -> Result<ArrayRef> {
    if rows == 0 {
        return Ok(new_empty_array(data_type));
    }

    let offsets = read_offsets(reader, rows).await?;
    let num_nondefault = offsets.len();

    // Read the dense nested values into a fresh builder sized for the
    // non-default count (the caller's full-`rows` builder is not used on
    // this path; see the function doc).
    let mut nested_builder = TypedBuilder::try_new(type_hint, data_type)?;
    // Sparse values stream still carries the type's prefix once.
    type_hint.deserialize_prefix_async(reader, prefix_state).await?;
    let nested_array = type_hint
        .deserialize_arrow_async(
            &mut nested_builder,
            reader,
            data_type,
            num_nondefault,
            nulls,
            rbuffer,
        )
        .await?;

    // Walk row-by-row, scattering nested values at non-default positions
    // and filling everything else with the type's default.
    materialize(data_type, is_nullable, rows, &offsets, &nested_array)
}

/// Build the full-row array by interleaving the type's default with
/// values from `nested` at the rows listed in `offsets`.
fn materialize(
    data_type: &DataType,
    is_nullable: bool,
    rows: usize,
    offsets: &[u64],
    nested: &ArrayRef,
) -> Result<ArrayRef> {
    // Strategy: use arrow::compute::interleave to splice together a
    // single-row "default" array and the nested values. This sidesteps
    // per-builder-variant scatter code and works uniformly for every
    // Arrow type (incl. Strings, FixedSizeBinary, Lists, etc.).
    use arrow::compute::interleave;

    let default_array = default_array_of(data_type, is_nullable)?;
    let arrays: [&dyn Array; 2] = [default_array.as_ref(), nested.as_ref()];

    // Build the (source_idx, row_idx) plan: at each output row, either
    // index 0 of `default_array` or the n-th value of `nested`.
    let mut indices: Vec<(usize, usize)> = Vec::with_capacity(rows);
    let mut nondefault_iter = offsets.iter().copied().peekable();
    let mut next_nested: usize = 0;
    for row in 0..(rows as u64) {
        if nondefault_iter.peek() == Some(&row) {
            let _ = nondefault_iter.next();
            indices.push((1, next_nested));
            next_nested += 1;
        } else {
            indices.push((0, 0));
        }
    }

    let materialized = interleave(&arrays, &indices)
        .map_err(|e| Error::ArrowDeserialize(format!("sparse interleave failed: {e}")))?;

    Ok(materialized)
}

/// One-row array carrying the row value used at the omitted (default)
/// positions of a sparse column.
///
/// - When the column is **nullable**, the omitted rows are the `Nullable` default, which is NULL.
///   `new_null_array` produces that uniformly for every Arrow type, so we short-circuit there.
/// - When the column is **non-nullable**, the omitted rows are the underlying type's *zero*
///   (numeric 0, empty string/binary, epoch timestamp, zero-scaled decimal, …). We build that
///   explicitly per type; emitting a NULL here would inject nulls into a non-nullable column and
///   `RecordBatch::try_new` would reject the batch.
///
/// The returned array's `DataType` matches `data_type` exactly (including
/// decimal precision/scale and timestamp unit/timezone) so the caller's
/// `interleave` against the nested values array does not hit a
/// type-mismatch.
///
/// A genuinely unsupported non-nullable type is a hard `Error::Protocol`
/// rather than a silent NULL fill — consistent with the dialect note in
/// [`crate::native::protocol`]: we don't fabricate values we can't
/// represent correctly.
fn default_array_of(data_type: &DataType, is_nullable: bool) -> Result<ArrayRef> {
    use arrow::array::new_null_array;

    // Nullable column: the default cell is null, uniformly across types.
    if is_nullable {
        return Ok(new_null_array(data_type, 1));
    }

    // Non-nullable column: build the type's zero. interleave demands the
    // default array carry the same DataType as the nested values, so the
    // decimal/timestamp arms re-apply precision/scale and unit/timezone.
    Ok(match data_type {
        DataType::Int8 => Arc::new(Int8Array::from(vec![0_i8])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![0_i16])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![0_i32])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![0_i64])),
        DataType::UInt8 => Arc::new(UInt8Array::from(vec![0_u8])),
        DataType::UInt16 => Arc::new(UInt16Array::from(vec![0_u16])),
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![0_u32])),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![0_u64])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![0.0_f32])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![0.0_f64])),
        DataType::Boolean => Arc::new(BooleanArray::from(vec![false])),
        DataType::Date32 => Arc::new(Date32Array::from(vec![0_i32])),
        DataType::Date64 => Arc::new(Date64Array::from(vec![0_i64])),
        DataType::Utf8 => Arc::new(StringArray::from(vec![""])),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![""])),
        DataType::Binary => Arc::new(BinaryArray::from(vec![&b""[..]])),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![&b""[..]])),
        DataType::FixedSizeBinary(n) => {
            let zeros = vec![0u8; usize::try_from(*n).unwrap_or(0)];
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(std::iter::once(zeros.as_slice()))
                    .map_err(|e| Error::ArrowDeserialize(format!("fixed bin default: {e}")))?,
            )
        }
        // Decimal: zero mantissa, preserving precision/scale so interleave
        // sees a matching DataType.
        DataType::Decimal128(p, s) => Arc::new(
            Decimal128Array::from(vec![0_i128])
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::ArrowDeserialize(format!("decimal128 default: {e}")))?,
        ),
        DataType::Decimal256(p, s) => Arc::new(
            Decimal256Array::from(vec![i256::ZERO])
                .with_precision_and_scale(*p, *s)
                .map_err(|e| Error::ArrowDeserialize(format!("decimal256 default: {e}")))?,
        ),
        // Timestamp: epoch (0), preserving unit + timezone metadata.
        DataType::Timestamp(unit, tz) => {
            let arr: ArrayRef = match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(vec![0_i64])),
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(vec![0_i64])),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(vec![0_i64])),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(vec![0_i64])),
            };
            // Re-attach the timezone via cast so the DataType matches the
            // nested values exactly.
            if tz.is_some() {
                arrow::compute::cast(&arr, data_type)
                    .map_err(|e| Error::ArrowDeserialize(format!("timestamp tz default: {e}")))?
            } else {
                arr
            }
        }
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(vec![0_i32])),
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(vec![0_i32]))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(vec![0_i64]))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from(vec![0_i64]))
        }
        DataType::Duration(TimeUnit::Second) => Arc::new(DurationSecondArray::from(vec![0_i64])),
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(vec![0_i64]))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(vec![0_i64]))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::from(vec![0_i64]))
        }
        // We do not fabricate a zero we can't represent faithfully for a
        // non-nullable column; surface it loudly instead of corrupting.
        other => {
            return Err(Error::Protocol(format!(
                "sparse default materialization not implemented for non-nullable type {other:?}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    /// LEB128-encode a varint the same way CH's `write_var_uint` does, so
    /// the fixtures `read_offsets` consumes are byte-identical to the wire.
    fn push_varint(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value > 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    /// Encode a `SparseOffsets` stream from `(group_size, end_of_granule)`
    /// entries. The caller spells out the exact wire entries — including
    /// the mandatory final flagged terminator — so the test pins the
    /// format, not our own helper's idea of it.
    fn encode_offsets(entries: &[(u64, bool)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for &(group, end) in entries {
            let raw = if end { group | END_OF_GRANULE_FLAG } else { group };
            push_varint(&mut buf, raw);
        }
        buf
    }

    async fn read_offsets_from(entries: &[(u64, bool)], rows: usize) -> Result<Vec<u64>> {
        let bytes = encode_offsets(entries);
        let mut reader = Cursor::new(bytes);
        read_offsets(&mut reader, rows).await
    }

    #[tokio::test]
    async fn offsets_all_default() {
        // 5 rows, all default: a single flagged terminator with the full
        // trailing-default run and no values.
        let offsets = read_offsets_from(&[(5, true)], 5).await.unwrap();
        assert!(offsets.is_empty());
    }

    #[tokio::test]
    async fn offsets_ends_with_default() {
        // 8 rows: value at index 2, value at index 4, then defaults to the
        // end. Groups: 2 defaults+value (idx 2), 1 default+value (idx 4),
        // terminator with 3 trailing defaults.
        let offsets = read_offsets_from(&[(2, false), (1, false), (3, true)], 8).await.unwrap();
        assert_eq!(offsets, vec![2, 4]);
    }

    #[tokio::test]
    async fn offsets_ends_with_nondefault() {
        // 5 rows: value at index 4 is the last row. CH still writes a
        // flagged terminator, here with group_size 0 (no trailing
        // defaults). Dropping it would desync the next column.
        let offsets = read_offsets_from(&[(4, false), (0, true)], 5).await.unwrap();
        assert_eq!(offsets, vec![4]);
    }

    #[tokio::test]
    async fn offsets_dense_all_nondefault() {
        // Every row non-default (sparse is still legal here): a value at
        // each index 0..3, then the zero-trailing terminator.
        let offsets =
            read_offsets_from(&[(0, false), (0, false), (0, false), (0, true)], 3).await.unwrap();
        assert_eq!(offsets, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn offsets_value_past_rows_is_error() {
        // A non-terminating entry whose value would land at/after the row
        // count is a malformed stream, not a silent truncation.
        let err = read_offsets_from(&[(5, false), (0, true)], 5).await.unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn default_nullable_is_null_for_any_type() {
        // The nullable branch must short-circuit to a null cell even for a
        // type that has no explicit zero arm (e.g. Decimal with odd scale).
        let dt = DataType::Decimal128(38, 10);
        let arr = default_array_of(&dt, true).unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.null_count(), 1);
        assert_eq!(arr.data_type(), &dt);
    }

    #[test]
    fn default_non_nullable_decimal_is_zero() {
        let dt = DataType::Decimal128(18, 4);
        let arr = default_array_of(&dt, false).unwrap();
        assert_eq!(arr.null_count(), 0);
        assert_eq!(arr.data_type(), &dt, "precision/scale must survive for interleave");
        let dec = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec.value(0), 0_i128);
    }

    #[test]
    fn default_non_nullable_timestamp_preserves_unit_and_tz() {
        let dt = DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")));
        let arr = default_array_of(&dt, false).unwrap();
        assert_eq!(arr.null_count(), 0);
        assert_eq!(arr.data_type(), &dt, "unit + tz must match the nested values' DataType");
        let ts = arr.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(ts.value(0), 0_i64);
    }

    #[test]
    fn default_non_nullable_unsupported_type_errors() {
        // A non-nullable type with no zero arm must fail loud rather than
        // inject a null into a non-nullable column.
        let dt = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let err = default_array_of(&dt, false).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn materialize_scatters_values_and_fills_zero_defaults() {
        // Non-nullable Int32, 5 rows, non-default values 10 and 20 at
        // positions 1 and 3. Default rows must be 0, not null.
        let dt = DataType::Int32;
        let nested: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let out = materialize(&dt, false, 5, &[1, 3], &nested).unwrap();
        assert_eq!(out.null_count(), 0);
        let ints = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ints.values(), &[0, 10, 0, 20, 0]);
    }

    #[test]
    fn materialize_nullable_defaults_are_null() {
        // Nullable Int32: default rows are null, non-default rows carry the
        // nested value.
        let dt = DataType::Int32;
        let nested: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let out = materialize(&dt, true, 5, &[1, 3], &nested).unwrap();
        let ints = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(ints.is_null(0) && ints.is_null(2) && ints.is_null(4));
        assert_eq!(ints.value(1), 10);
        assert_eq!(ints.value(3), 20);
    }
}
