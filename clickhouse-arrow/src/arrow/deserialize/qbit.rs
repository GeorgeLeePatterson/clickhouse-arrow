use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Float64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field};
use tokio::io::AsyncReadExt;

use crate::arrow::builder::TypedBuilder;
use crate::arrow::types::LIST_ITEM_FIELD_NAME;
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

#[inline]
fn decode_qbit_word(
    planes: &[u8],
    element_bits: usize,
    rows: usize,
    bytes_per_fixed_string: usize,
    row: usize,
    element_idx: usize,
) -> u64 {
    let total_bits = bytes_per_fixed_string * 8;
    let bit_index = (total_bits - 1) - (element_idx ^ 7);
    let byte_pos = bit_index / 8;
    let bit_mask = 1_u8 << (bit_index % 8);
    let row_byte_offset = row * bytes_per_fixed_string + byte_pos;
    let plane_stride = rows * bytes_per_fixed_string;

    let mut word = 0_u64;
    for plane_idx in 0..element_bits {
        if planes[plane_idx * plane_stride + row_byte_offset] & bit_mask != 0 {
            word |= 1_u64 << (element_bits - 1 - plane_idx);
        }
    }
    word
}

#[inline]
fn decode_qbit_values_f32(
    planes: &[u8],
    rows: usize,
    dim: usize,
    bytes_per_fixed_string: usize,
    nulls: &[u8],
) -> Vec<f32> {
    let mut values = Vec::with_capacity(rows * dim);
    for row in 0..rows {
        let row_is_null = !nulls.is_empty() && nulls[row] == 1;
        for element_idx in 0..dim {
            let value = if row_is_null {
                0.0
            } else {
                let bits =
                    decode_qbit_word(planes, 32, rows, bytes_per_fixed_string, row, element_idx)
                        as u32;
                f32::from_bits(bits)
            };
            values.push(value);
        }
    }
    values
}

#[inline]
fn decode_qbit_values_f64(
    planes: &[u8],
    rows: usize,
    dim: usize,
    bytes_per_fixed_string: usize,
    nulls: &[u8],
) -> Vec<f64> {
    let mut values = Vec::with_capacity(rows * dim);
    for row in 0..rows {
        let row_is_null = !nulls.is_empty() && nulls[row] == 1;
        for element_idx in 0..dim {
            let value = if row_is_null {
                0.0
            } else {
                let bits =
                    decode_qbit_word(planes, 64, rows, bytes_per_fixed_string, row, element_idx);
                f64::from_bits(bits)
            };
            values.push(value);
        }
    }
    values
}

#[inline]
fn decode_qbit_values_bfloat16(
    planes: &[u8],
    rows: usize,
    dim: usize,
    bytes_per_fixed_string: usize,
    nulls: &[u8],
) -> Vec<f32> {
    let mut values = Vec::with_capacity(rows * dim);
    for row in 0..rows {
        let row_is_null = !nulls.is_empty() && nulls[row] == 1;
        for element_idx in 0..dim {
            let value = if row_is_null {
                0.0
            } else {
                let bits =
                    decode_qbit_word(planes, 16, rows, bytes_per_fixed_string, row, element_idx)
                        as u16;
                f32::from_bits(u32::from(bits) << 16)
            };
            values.push(value);
        }
    }
    values
}

pub(super) async fn deserialize_async<R: ClickHouseRead>(
    type_hint: &Type,
    _builder: &mut TypedBuilder,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    _rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    let Type::QBit { element_type, dimension } = type_hint.strip_null() else {
        return Err(Error::ArrowDeserialize(format!(
            "QBit deserializer called with non-QBit type: {type_hint}"
        )));
    };

    let dim = *dimension;
    let bytes_per_fixed_string = dim.div_ceil(8);
    let null_buffer = (!nulls.is_empty())
        .then_some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<_>>()));

    let list = match element_type.strip_null() {
        Type::Float32 => {
            let mut planes = vec![0_u8; 32 * rows * bytes_per_fixed_string];
            let _ = reader.read_exact(&mut planes).await?;
            let values = decode_qbit_values_f32(&planes, rows, dim, bytes_per_fixed_string, nulls);
            let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float32, false));
            FixedSizeListArray::new(
                field,
                dim as i32,
                Arc::new(Float32Array::from(values)),
                null_buffer,
            )
        }
        Type::Float64 => {
            let mut planes = vec![0_u8; 64 * rows * bytes_per_fixed_string];
            let _ = reader.read_exact(&mut planes).await?;
            let values = decode_qbit_values_f64(&planes, rows, dim, bytes_per_fixed_string, nulls);
            let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float64, false));
            FixedSizeListArray::new(
                field,
                dim as i32,
                Arc::new(Float64Array::from(values)),
                null_buffer,
            )
        }
        Type::BFloat16 => {
            let mut planes = vec![0_u8; 16 * rows * bytes_per_fixed_string];
            let _ = reader.read_exact(&mut planes).await?;
            let values =
                decode_qbit_values_bfloat16(&planes, rows, dim, bytes_per_fixed_string, nulls);
            let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float32, false));
            FixedSizeListArray::new(
                field,
                dim as i32,
                Arc::new(Float32Array::from(values)),
                null_buffer,
            )
        }
        other => {
            return Err(Error::ArrowDeserialize(format!(
                "QBit element type must be BFloat16, Float32, or Float64, got {other}"
            )));
        }
    };

    Ok(Arc::new(list))
}
