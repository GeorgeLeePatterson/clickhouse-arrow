// use arrow::array::ArrayRef;

// use crate::formats::DeserializerState;
// use crate::io::ClickHouseBytesRead;
// use crate::row::builder::TypedBuilder;
// use crate::{Error, Result, Type};

// pub(crate) fn deserialize<R: ClickHouseBytesRead>(
//     inner: &Type,
//     builder: &mut TypedBuilder,
//     reader: &mut R,
//     rows: usize,
//     _state: &mut DeserializerState,
// ) -> Result<ArrayRef> {
//     let mut mask = vec![0u8; rows];
//     let _ = reader.try_copy_to_slice(&mut mask)?;
//     if mask.len() != rows {
//         return Err(Error::DeserializeError(format!(
//             "Mask length {} does not match rows {rows}",
//             mask.len()
//         )));
//     }
//     // builder.create_columns_array(inner, reader, &mask, rows)
// }
