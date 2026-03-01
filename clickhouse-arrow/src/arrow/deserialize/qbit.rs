use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeListArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::Field;
use tokio::io::AsyncReadExt;

use crate::arrow::builder::TypedBuilder;
use crate::arrow::builder::list::TypedListBuilder;
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

macro_rules! decode_qbit_values {
    ($fn_name:ident, $out_ty:ty, $element_bits:expr, $zero:expr, $decode:expr) => {
        #[inline]
        fn $fn_name(
            planes: &[u8],
            rows: usize,
            dim: usize,
            bytes_per_fixed_string: usize,
            nulls: &[u8],
        ) -> Vec<$out_ty> {
            let mut values = Vec::with_capacity(rows * dim);
            for row in 0..rows {
                let row_is_null = !nulls.is_empty() && nulls[row] != 0;
                for element_idx in 0..dim {
                    let value = if row_is_null {
                        $zero
                    } else {
                        let bits = decode_qbit_word(
                            planes,
                            $element_bits,
                            rows,
                            bytes_per_fixed_string,
                            row,
                            element_idx,
                        );
                        ($decode)(bits)
                    };
                    values.push(value);
                }
            }
            values
        }
    };
}

decode_qbit_values!(decode_qbit_values_f32, f32, 32, 0.0_f32, |bits: u64| {
    #[expect(clippy::cast_possible_truncation)]
    f32::from_bits(bits as u32)
});
decode_qbit_values!(decode_qbit_values_f64, f64, 64, 0.0_f64, |bits: u64| f64::from_bits(bits));
decode_qbit_values!(decode_qbit_values_bfloat16, f32, 16, 0.0_f32, |bits: u64| {
    #[expect(clippy::cast_possible_truncation)]
    f32::from_bits((bits as u32) << 16)
});

async fn read_qbit_planes_async<R: ClickHouseRead>(
    reader: &mut R,
    rbuffer: &mut Vec<u8>,
    bytes: usize,
) -> Result<()> {
    rbuffer.clear();
    if rbuffer.capacity() < bytes {
        rbuffer.reserve(bytes - rbuffer.capacity());
    }
    rbuffer.resize(bytes, 0);
    let _ = reader.read_exact(rbuffer).await?;
    Ok(())
}

pub(super) async fn deserialize<R: ClickHouseRead>(
    type_hint: &Type,
    builder: &mut TypedBuilder,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    let Type::QBit { element_type, dimension } = type_hint.strip_null() else {
        return Err(Error::ArrowDeserialize(format!("QBit deser w/ non-QBit type: {type_hint}")));
    };

    let TypedBuilder::List(list_builder) = builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected QBit builder type: {}",
            builder.as_ref()
        )));
    };
    let TypedListBuilder::FixedList((size, inner_builder)) = list_builder else {
        return Err(Error::ArrowDeserialize(format!(
            "Unexpected QBit list builder shape: {list_builder:?}"
        )));
    };

    let dim = *dimension;
    #[expect(clippy::cast_possible_wrap)]
    #[expect(clippy::cast_possible_truncation)]
    if *size != dim as i32 {
        return Err(Error::ArrowDeserialize(format!(
            "QBit dimension mismatch: type={dim}, builder={size}"
        )));
    }

    let bytes_per_fixed_string = dim.div_ceil(8);
    let null_buffer = (!nulls.is_empty())
        .then_some(NullBuffer::from(nulls.iter().map(|&n| n == 0).collect::<Vec<_>>()));

    let values_array: ArrayRef = match element_type.strip_null() {
        Type::Float32 => {
            read_qbit_planes_async(reader, rbuffer, 32 * rows * bytes_per_fixed_string).await?;
            let values = decode_qbit_values_f32(rbuffer, rows, dim, bytes_per_fixed_string, nulls);
            let TypedBuilder::Float32(float_builder) = inner_builder.as_mut() else {
                return Err(Error::ArrowDeserialize(format!(
                    "QBit(Float32) expected Float32 builder, got {:?}",
                    inner_builder.as_ref()
                )));
            };
            float_builder.append_slice(&values);
            Arc::new(float_builder.finish())
        }
        Type::Float64 => {
            read_qbit_planes_async(reader, rbuffer, 64 * rows * bytes_per_fixed_string).await?;
            let values = decode_qbit_values_f64(rbuffer, rows, dim, bytes_per_fixed_string, nulls);
            let TypedBuilder::Float64(float_builder) = inner_builder.as_mut() else {
                return Err(Error::ArrowDeserialize(format!(
                    "QBit(Float64) expected Float64 builder, got {:?}",
                    inner_builder.as_ref()
                )));
            };
            float_builder.append_slice(&values);
            Arc::new(float_builder.finish())
        }
        Type::BFloat16 => {
            read_qbit_planes_async(reader, rbuffer, 16 * rows * bytes_per_fixed_string).await?;
            let values =
                decode_qbit_values_bfloat16(rbuffer, rows, dim, bytes_per_fixed_string, nulls);
            let TypedBuilder::Float32(float_builder) = inner_builder.as_mut() else {
                return Err(Error::ArrowDeserialize(format!(
                    "QBit(BFloat16) expected Float32 builder, got {:?}",
                    inner_builder.as_ref()
                )));
            };
            float_builder.append_slice(&values);
            Arc::new(float_builder.finish())
        }
        other => {
            return Err(Error::ArrowDeserialize(format!(
                "QBit element type must be BFloat16, Float32, or Float64, got {other}"
            )));
        }
    };

    let field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, values_array.data_type().clone(), false));
    Ok(Arc::new(FixedSizeListArray::new(field, *size, values_array, null_buffer)))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{Array, FixedSizeListArray, Float32Array, Float64Array};
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[tokio::test]
    async fn test_deserialize_qbit_zero_payload_float32() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Float32), dimension: 3 };
        let rows = 2_usize;
        let bytes_per_fixed_string = 3_usize.div_ceil(8);
        let mut reader = Cursor::new(vec![0_u8; 32 * rows * bytes_per_fixed_string]);
        let qbit_data_type = DataType::FixedSizeList(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float32, false)),
            3,
        );
        let mut builder = TypedBuilder::try_new(&type_hint, &qbit_data_type).unwrap();

        let array = deserialize(&type_hint, &mut builder, &mut reader, rows, &[], &mut Vec::new())
            .await
            .unwrap();
        let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(list.len(), rows);
        assert_eq!(list.value_length(), 3);
        let values = list.values().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(values, &Float32Array::from(vec![0.0_f32; rows * 3]));
    }

    #[tokio::test]
    async fn test_deserialize_qbit_rejects_invalid_element_type() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Int32), dimension: 3 };
        let mut reader = Cursor::new(vec![]);
        let qbit_data_type = DataType::FixedSizeList(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false)),
            3,
        );
        let mut builder = TypedBuilder::try_new(&type_hint, &qbit_data_type).unwrap();

        let error = deserialize(&type_hint, &mut builder, &mut reader, 0, &[], &mut Vec::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("QBit element type must be"));
    }

    #[tokio::test]
    async fn test_deserialize_qbit_zero_payload_float64() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Float64), dimension: 2 };
        let rows = 3_usize;
        let bytes_per_fixed_string = 2_usize.div_ceil(8);
        let mut reader = Cursor::new(vec![0_u8; 64 * rows * bytes_per_fixed_string]);
        let qbit_data_type = DataType::FixedSizeList(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float64, false)),
            2,
        );
        let mut builder = TypedBuilder::try_new(&type_hint, &qbit_data_type).unwrap();

        let array = deserialize(&type_hint, &mut builder, &mut reader, rows, &[], &mut Vec::new())
            .await
            .unwrap();
        let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values, &Float64Array::from(vec![0.0_f64; rows * 2]));
    }

    #[tokio::test]
    async fn test_deserialize_qbit_zero_payload_bfloat16() {
        let type_hint = Type::QBit { element_type: Box::new(Type::BFloat16), dimension: 3 };
        let rows = 2_usize;
        let bytes_per_fixed_string = 3_usize.div_ceil(8);
        let mut reader = Cursor::new(vec![0_u8; 16 * rows * bytes_per_fixed_string]);
        let qbit_data_type = DataType::FixedSizeList(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float32, false)),
            3,
        );
        let mut builder = TypedBuilder::try_new(&type_hint, &qbit_data_type).unwrap();

        let array = deserialize(&type_hint, &mut builder, &mut reader, rows, &[], &mut Vec::new())
            .await
            .unwrap();
        let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(values, &Float32Array::from(vec![0.0_f32; rows * 3]));
    }

    #[tokio::test]
    async fn test_deserialize_qbit_rejects_non_qbit_type() {
        let type_hint = Type::Int32;
        let qbit_data_type = DataType::FixedSizeList(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float32, false)),
            3,
        );
        let mut builder = TypedBuilder::try_new(
            &Type::QBit { element_type: Box::new(Type::Float32), dimension: 3 },
            &qbit_data_type,
        )
        .unwrap();
        let mut reader = Cursor::new(vec![]);

        let error = deserialize(&type_hint, &mut builder, &mut reader, 0, &[], &mut Vec::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("non-QBit"));
    }

    #[test]
    fn test_deserialize_qbit_rejects_dimension_mismatch() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Float32), dimension: 4 };
        let qbit_data_type = DataType::FixedSizeList(
            Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Float32, false)),
            3,
        );
        let error = TypedBuilder::try_new(&type_hint, &qbit_data_type).unwrap_err();
        assert!(error.to_string().contains("dimension mismatch"));
    }

    #[tokio::test]
    async fn test_deserialize_qbit_rejects_non_list_builder() {
        let type_hint = Type::QBit { element_type: Box::new(Type::Float32), dimension: 3 };
        let mut builder = TypedBuilder::try_new(&Type::UInt8, &DataType::UInt8).unwrap();
        let mut reader = Cursor::new(vec![]);

        let error = deserialize(&type_hint, &mut builder, &mut reader, 0, &[], &mut Vec::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Unexpected QBit builder type"));
    }
}
