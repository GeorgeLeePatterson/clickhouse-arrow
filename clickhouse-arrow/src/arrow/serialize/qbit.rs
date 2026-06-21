use arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, UInt16Array};
use tokio::io::AsyncWriteExt;

use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

#[inline]
pub(super) fn f32_to_bfloat16_bits(value: f32) -> u16 {
    let bits = value.to_bits();
    let lsb = (bits >> 16) & 1;
    let rounded = bits.wrapping_add(0x7FFF + lsb);
    (rounded >> 16) as u16
}

fn parse_qbit_input<'a>(
    type_hint: &'a Type,
    column: &'a ArrayRef,
) -> Result<(&'a Type, usize, &'a FixedSizeListArray)> {
    let Type::QBit { element_type, dimension } = type_hint.strip_null() else {
        return Err(Error::ArrowSerialize(format!(
            "QBit serializer called with non-QBit type: {type_hint}"
        )));
    };

    let list = column.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| {
        Error::ArrowSerialize("Expected FixedSizeListArray for QBit serialization".to_string())
    })?;

    #[expect(clippy::cast_sign_loss)]
    if list.value_length() as usize != *dimension {
        return Err(Error::ArrowSerialize(format!(
            "QBit dimension mismatch: type={dimension}, array={}",
            list.value_length()
        )));
    }

    Ok((element_type.strip_null(), *dimension, list))
}

#[inline]
fn encode_qbit_plane_bits(
    output: &mut [u8],
    element_bits: usize,
    rows: usize,
    bytes_per_fixed_string: usize,
    row: usize,
    element_idx: usize,
    mut word: u64,
) {
    if word == 0 {
        return;
    }

    let total_bits = bytes_per_fixed_string * 8;
    let bit_index = (total_bits - 1) - (element_idx ^ 7);
    let byte_pos = bit_index / 8;
    let bit_mask = 1_u8 << (bit_index % 8);
    let row_byte_offset = row * bytes_per_fixed_string + byte_pos;
    let plane_stride = rows * bytes_per_fixed_string;

    while word != 0 {
        let trailing_zeros = word.trailing_zeros() as usize;
        let plane_idx = element_bits - 1 - trailing_zeros;
        output[plane_idx * plane_stride + row_byte_offset] |= bit_mask;
        word &= word - 1;
    }
}

#[inline]
fn encode_qbit_planes_f32(
    list: &FixedSizeListArray,
    values: &Float32Array,
    rows: usize,
    dim: usize,
    bytes_per_fixed_string: usize,
) -> Vec<u8> {
    let mut output = vec![0_u8; 32 * rows * bytes_per_fixed_string];
    for row in 0..rows {
        let row_is_null = list.is_null(row);
        let row_start = row * dim;
        for element_idx in 0..dim {
            let idx = row_start + element_idx;
            let word = if row_is_null || values.is_null(idx) {
                0
            } else {
                u64::from(values.value(idx).to_bits())
            };
            encode_qbit_plane_bits(
                &mut output,
                32,
                rows,
                bytes_per_fixed_string,
                row,
                element_idx,
                word,
            );
        }
    }
    output
}

#[inline]
fn encode_qbit_planes_f64(
    list: &FixedSizeListArray,
    values: &Float64Array,
    rows: usize,
    dim: usize,
    bytes_per_fixed_string: usize,
) -> Vec<u8> {
    let mut output = vec![0_u8; 64 * rows * bytes_per_fixed_string];
    for row in 0..rows {
        let row_is_null = list.is_null(row);
        let row_start = row * dim;
        for element_idx in 0..dim {
            let idx = row_start + element_idx;
            let word =
                if row_is_null || values.is_null(idx) { 0 } else { values.value(idx).to_bits() };
            encode_qbit_plane_bits(
                &mut output,
                64,
                rows,
                bytes_per_fixed_string,
                row,
                element_idx,
                word,
            );
        }
    }
    output
}

#[inline]
fn encode_qbit_planes_bfloat16_from_f32(
    list: &FixedSizeListArray,
    values: &Float32Array,
    rows: usize,
    dim: usize,
    bytes_per_fixed_string: usize,
) -> Vec<u8> {
    let mut output = vec![0_u8; 16 * rows * bytes_per_fixed_string];
    for row in 0..rows {
        let row_is_null = list.is_null(row);
        let row_start = row * dim;
        for element_idx in 0..dim {
            let idx = row_start + element_idx;
            let word = if row_is_null || values.is_null(idx) {
                0
            } else {
                u64::from(f32_to_bfloat16_bits(values.value(idx)))
            };
            encode_qbit_plane_bits(
                &mut output,
                16,
                rows,
                bytes_per_fixed_string,
                row,
                element_idx,
                word,
            );
        }
    }
    output
}

#[inline]
fn encode_qbit_planes_bfloat16_from_u16(
    list: &FixedSizeListArray,
    values: &UInt16Array,
    rows: usize,
    dim: usize,
    bytes_per_fixed_string: usize,
) -> Vec<u8> {
    let mut output = vec![0_u8; 16 * rows * bytes_per_fixed_string];
    for row in 0..rows {
        let row_is_null = list.is_null(row);
        let row_start = row * dim;
        for element_idx in 0..dim {
            let idx = row_start + element_idx;
            let word =
                if row_is_null || values.is_null(idx) { 0 } else { u64::from(values.value(idx)) };
            encode_qbit_plane_bits(
                &mut output,
                16,
                rows,
                bytes_per_fixed_string,
                row,
                element_idx,
                word,
            );
        }
    }
    output
}

pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
) -> Result<()> {
    let (element_type, dim, list) = parse_qbit_input(type_hint, column)?;
    let values = list.values();
    let rows = list.len();
    let bytes_per_fixed_string = dim.div_ceil(8);

    let encoded = match element_type {
        Type::Float32 => {
            let float_values = values.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                Error::ArrowSerialize(
                    "Expected Float32Array for QBit(Float32, ...) serialization".to_string(),
                )
            })?;
            encode_qbit_planes_f32(list, float_values, rows, dim, bytes_per_fixed_string)
        }
        Type::Float64 => {
            let float_values = values.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                Error::ArrowSerialize(
                    "Expected Float64Array for QBit(Float64, ...) serialization".to_string(),
                )
            })?;
            encode_qbit_planes_f64(list, float_values, rows, dim, bytes_per_fixed_string)
        }
        Type::BFloat16 => {
            if let Some(float_values) = values.as_any().downcast_ref::<Float32Array>() {
                encode_qbit_planes_bfloat16_from_f32(
                    list,
                    float_values,
                    rows,
                    dim,
                    bytes_per_fixed_string,
                )
            } else {
                let bf_values = values.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                    Error::ArrowSerialize(
                        "Expected Float32Array or UInt16Array for QBit(BFloat16, ...) \
                         serialization"
                            .to_string(),
                    )
                })?;
                encode_qbit_planes_bfloat16_from_u16(
                    list,
                    bf_values,
                    rows,
                    dim,
                    bytes_per_fixed_string,
                )
            }
        }
        other => {
            return Err(Error::ArrowSerialize(format!(
                "QBit element type must be BFloat16, Float32, or Float64, got {other}"
            )));
        }
    };

    writer.write_all(&encoded).await?;
    Ok(())
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
) -> Result<()> {
    let (element_type, dim, list) = parse_qbit_input(type_hint, column)?;
    let values = list.values();
    let rows = list.len();
    let bytes_per_fixed_string = dim.div_ceil(8);

    let encoded = match element_type {
        Type::Float32 => {
            let float_values = values.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                Error::ArrowSerialize(
                    "Expected Float32Array for QBit(Float32, ...) serialization".to_string(),
                )
            })?;
            encode_qbit_planes_f32(list, float_values, rows, dim, bytes_per_fixed_string)
        }
        Type::Float64 => {
            let float_values = values.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                Error::ArrowSerialize(
                    "Expected Float64Array for QBit(Float64, ...) serialization".to_string(),
                )
            })?;
            encode_qbit_planes_f64(list, float_values, rows, dim, bytes_per_fixed_string)
        }
        Type::BFloat16 => {
            if let Some(float_values) = values.as_any().downcast_ref::<Float32Array>() {
                encode_qbit_planes_bfloat16_from_f32(
                    list,
                    float_values,
                    rows,
                    dim,
                    bytes_per_fixed_string,
                )
            } else {
                let bf_values = values.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                    Error::ArrowSerialize(
                        "Expected Float32Array or UInt16Array for QBit(BFloat16, ...) \
                         serialization"
                            .to_string(),
                    )
                })?;
                encode_qbit_planes_bfloat16_from_u16(
                    list,
                    bf_values,
                    rows,
                    dim,
                    bytes_per_fixed_string,
                )
            }
        }
        other => {
            return Err(Error::ArrowSerialize(format!(
                "QBit element type must be BFloat16, Float32, or Float64, got {other}"
            )));
        }
    };

    writer.put_slice(&encoded);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{FixedSizeListArray, Float32Array, Float64Array, Int32Array, UInt16Array};
    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn qbit_array_f32() -> ArrayRef {
        let values =
            Arc::new(Float32Array::from(vec![0.1_f32, 0.2, 0.3, 1.0, 2.0, 3.0])) as ArrayRef;
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            3,
            values,
            None,
        )) as ArrayRef
    }

    fn qbit_array_f64() -> ArrayRef {
        let values =
            Arc::new(Float64Array::from(vec![0.1_f64, 0.2, 0.3, 1.0, 2.0, 3.0])) as ArrayRef;
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float64, false)),
            3,
            values,
            None,
        )) as ArrayRef
    }

    fn qbit_array_bfloat16_from_f32() -> ArrayRef {
        let values =
            Arc::new(Float32Array::from(vec![0.25_f32, -0.5, 1.5, 2.0, 3.0, 4.0])) as ArrayRef;
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            3,
            values,
            None,
        )) as ArrayRef
    }

    fn qbit_array_bfloat16_from_u16() -> ArrayRef {
        let values =
            Arc::new(UInt16Array::from(vec![0x3F80_u16, 0x4000, 0x4040, 0x3F00, 0x3E80, 0x3E00]))
                as ArrayRef;
        Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::UInt16, false)),
            3,
            values,
            None,
        )) as ArrayRef
    }

    #[tokio::test]
    async fn test_serialize_qbit_async_matches_sync() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Float32), dimension: 3 };
        let column = qbit_array_f32();

        let mut async_writer = Cursor::new(Vec::new());
        serialize_async(&type_hint, &mut async_writer, &column).await.unwrap();

        let mut sync_writer = Vec::new();
        serialize(&type_hint, &mut sync_writer, &column).unwrap();

        assert_eq!(async_writer.into_inner(), sync_writer);
    }

    #[tokio::test]
    async fn test_serialize_qbit_dimension_mismatch() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Float32), dimension: 4 };
        let column = qbit_array_f32();
        let mut writer = Cursor::new(Vec::new());

        let error = serialize_async(&type_hint, &mut writer, &column).await.unwrap_err();
        assert!(error.to_string().contains("QBit dimension mismatch"));
    }

    #[tokio::test]
    async fn test_serialize_qbit_float64_async_matches_sync() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Float64), dimension: 3 };
        let column = qbit_array_f64();

        let mut async_writer = Cursor::new(Vec::new());
        serialize_async(&type_hint, &mut async_writer, &column).await.unwrap();

        let mut sync_writer = Vec::new();
        serialize(&type_hint, &mut sync_writer, &column).unwrap();

        assert_eq!(async_writer.into_inner(), sync_writer);
        assert!(!sync_writer.is_empty());
    }

    #[tokio::test]
    async fn test_serialize_qbit_bfloat16_from_float32_async_matches_sync() {
        let type_hint = Type::QBit { element_type: Box::new(Type::BFloat16), dimension: 3 };
        let column = qbit_array_bfloat16_from_f32();

        let mut async_writer = Cursor::new(Vec::new());
        serialize_async(&type_hint, &mut async_writer, &column).await.unwrap();

        let mut sync_writer = Vec::new();
        serialize(&type_hint, &mut sync_writer, &column).unwrap();

        assert_eq!(async_writer.into_inner(), sync_writer);
        assert!(!sync_writer.is_empty());
    }

    #[tokio::test]
    async fn test_serialize_qbit_bfloat16_from_u16_async_matches_sync() {
        let type_hint = Type::QBit { element_type: Box::new(Type::BFloat16), dimension: 3 };
        let column = qbit_array_bfloat16_from_u16();

        let mut async_writer = Cursor::new(Vec::new());
        serialize_async(&type_hint, &mut async_writer, &column).await.unwrap();

        let mut sync_writer = Vec::new();
        serialize(&type_hint, &mut sync_writer, &column).unwrap();

        assert_eq!(async_writer.into_inner(), sync_writer);
        assert!(!sync_writer.is_empty());
    }

    #[tokio::test]
    async fn test_serialize_qbit_rejects_non_qbit_type() {
        let mut writer = Cursor::new(Vec::new());
        let error =
            serialize_async(&Type::Int32, &mut writer, &qbit_array_f32()).await.unwrap_err();
        assert!(error.to_string().contains("non-QBit"));
    }

    #[tokio::test]
    async fn test_serialize_qbit_rejects_non_fixed_size_list_column() {
        let mut writer = Cursor::new(Vec::new());
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let error = serialize_async(
            &Type::QBit { element_type: Box::new(Type::Float32), dimension: 3 },
            &mut writer,
            &column,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("Expected FixedSizeListArray"));
    }

    #[tokio::test]
    async fn test_serialize_qbit_rejects_unsupported_element_type() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &Type::QBit { element_type: Box::new(Type::Int32), dimension: 3 },
            &mut writer,
            &qbit_array_f32(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("must be BFloat16, Float32, or Float64"));
    }

    #[tokio::test]
    async fn test_serialize_qbit_rejects_wrong_value_array_type_for_float64() {
        let mut writer = Cursor::new(Vec::new());
        let error = serialize_async(
            &Type::QBit { element_type: Box::new(Type::Float64), dimension: 3 },
            &mut writer,
            &qbit_array_f32(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("Expected Float64Array"));
    }

    #[test]
    fn test_serialize_qbit_sync_rejects_non_qbit_type() {
        let mut writer = Vec::new();
        let error = serialize(&Type::Int32, &mut writer, &qbit_array_f32()).unwrap_err();
        assert!(error.to_string().contains("non-QBit"));
    }

    #[test]
    fn test_serialize_qbit_sync_rejects_non_fixed_size_list_column() {
        let mut writer = Vec::new();
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let error = serialize(
            &Type::QBit { element_type: Box::new(Type::Float32), dimension: 3 },
            &mut writer,
            &column,
        )
        .unwrap_err();
        assert!(error.to_string().contains("Expected FixedSizeListArray"));
    }
}
