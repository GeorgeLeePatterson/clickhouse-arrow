use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use tokio::io::AsyncReadExt;

use super::{ClickHouseArrowDeserializer, deser_bulk_async};
use crate::arrow::builder::dictionary::{LowCardinalityBuilder, LowCardinalityKeyBuilder};
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::native::types::low_cardinality::*;
use crate::{Error, Result, Type};

/// Deserializes a `ClickHouse` `LowCardinality` column into an Arrow `DictionaryArray<Int32Type>`.
///
/// The `LowCardinality` type in `ClickHouse` is a dictionary-encoded column that stores a
/// dictionary of unique values and indices referencing those values, optimizing storage for columns
/// with low cardinality. This function reads the binary format, which includes flags, dictionary
/// data, chunked row counts, and indices, and constructs a `DictionaryArray` with `Int32` indices
/// and values of the inner type (e.g., `String`, `Int32`, `Array`).
///
/// # `ClickHouse` Format
/// - **Flags** (u64): Indicates structure:
///   - `HAS_ADDITIONAL_KEYS_BIT` (0x40000000): Additional dictionary keys are present.
///   - `NEED_GLOBAL_DICTIONARY_BIT` (0x80000000): A global dictionary is included.
///   - `NEED_UPDATE_DICTIONARY_BIT` (0x100000000): The global dictionary needs updating.
///   - Lower 8 bits: Index type (`TUINT8=0x01`, `TUINT16=0x02`, `TUINT32=0x03`, `TUINT64=0x04`).
/// - **Dictionary Size** (u64): Number of dictionary entries.
/// - **Dictionary Values**: Serialized by the inner type’s deserializer (e.g., strings as
///   `var_uint` length + bytes).
/// - **Chunk Rows** (u64): Number of rows in a chunk.
/// - **Indices**: Variable-width integers (u8, u16, u32, or u64) referencing dictionary entries.
///
/// # Arguments
/// - `inner`: The `Type` of the dictionary values (e.g., `String`, `Int32`, `Array`).
/// - `reader`: An async reader providing the `ClickHouse` binary data.
/// - `rows`: The number of rows to deserialize.
/// - `_null_mask`: Ignored (`ClickHouse` handles nulls within the inner type).
/// - `state`: A mutable `DeserializerState` for deserialization context.
///
/// # Returns
/// A `Result` containing an `ArrayRef` (a `DictionaryArray<Int32Type>`) or a
/// `Error` if deserialization fails.
///
/// # Errors
/// - `DeserializeError` if:
///   - The index type is invalid.
///   - No dictionary is provided when required.
///   - A `UInt64` index exceeds `i32::MAX`.
///   - Chunk rows exceed the remaining row limit.
///   - The inner type’s deserialization fails.
/// - `Io` if reading from the reader fails.
///
/// # Example
/// ```rust,ignore
/// use std::io::Cursor;
/// use std::sync::Arc;
/// use arrow::array::{ArrayRef, DictionaryArray, Int32Type, StringArray};
/// use clickhouse_arrow::native::types::{DeserializerState, Type};
/// use clickhouse_arrow::ClickHouseRead;
///
/// let inner_type = Type::String;
/// let rows = 3;
/// let input = vec![
///     0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
///     2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
///     1, b'a', 1, b'b', // Dict: ["a", "b"]
///     3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
///     0, 1, 0, // Indices: [0, 1, 0]
/// ];
/// let mut reader = Cursor::new(input);
/// let mut state = DeserializerState::default();
/// let result = deserialize(&inner_type, &mut reader, rows, &[])
///     .await
///     .expect("Failed to deserialize LowCardinality(String)");
/// let dict_array = result.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
/// let indices = dict_array.keys();
/// let values = dict_array.values().as_any().downcast_ref::<StringArray>().unwrap();
/// assert_eq!(indices, &Int32Array::from(vec![0, 1, 0]));
/// assert_eq!(values, &StringArray::from(vec!["a", "b"]));
/// ```
#[expect(clippy::too_many_lines)]
#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn deserialize_async<R: ClickHouseRead>(
    inner: &Type,
    data_type: &DataType,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    let DataType::Dictionary(key_type, value_type) = data_type else {
        return Err(Error::ArrowDeserialize(format!("Unexpected dict value type: {data_type:?}")));
    };

    // Precompute non-null count
    let non_null_count = nulls.iter().fold(0, |acc, &b| acc + usize::from(b == 0));

    // Read flags to determine structure
    let flags = reader.read_u64_le().await?;
    let has_additional_keys = (flags & HAS_ADDITIONAL_KEYS_BIT) != 0;
    let needs_global_dictionary = (flags & NEED_GLOBAL_DICTIONARY_BIT) != 0;
    let needs_update_dictionary = (flags & NEED_UPDATE_DICTIONARY_BIT) != 0;

    // Determine index type
    let indexed_type = match flags & 0xff {
        TUINT8 => Type::UInt8,
        TUINT16 => Type::UInt16,
        TUINT32 => Type::UInt32,
        TUINT64 => Type::UInt64,
        x => {
            return Err(Error::DeserializeError(format!("LowCardinality: bad index type: {x}")));
        }
    };

    let dict_size = reader.read_u64_le().await? as usize;

    // Deserialize global dictionary or additional keys
    let dictionary = if needs_global_dictionary || needs_update_dictionary || has_additional_keys {
        // If the inner type is nullable, then the first value deserialized will be a "default"
        // value. Use the null mask to enforce this. The serializer does not write a null
        let null_mask = if inner.is_nullable() {
            let mut mask = vec![0_u8; dict_size];
            mask[0] = 1;
            mask
        } else {
            vec![]
        };

        // mask, the deserializer does not read one.
        inner
            .strip_null()
            .deserialize_arrow_async(reader, value_type, dict_size, &null_mask, rbuffer)
            .await?

    // No dictionary found
    } else {
        return Err(Error::DeserializeError("LowCardinality: no dictionary provided".to_string()));
    };

    // Read number of rows in this chunk
    let num_rows = reader.read_u64_le().await? as usize;
    if num_rows != rows {
        return Err(Error::DeserializeError(format!(
            "LowCardinality must be read in full. Expect {rows} rows, got {num_rows}"
        )));
    }

    macro_rules! deser_key {
        (($sz:ty, $m:path, $b:ident) => [$(($osz:ty, $o:path, $ob:ident)),* $(,)?]) => {
            match **key_type {
                $m => {
                    let mut keys = $b::with_capacity(non_null_count);
                    deser_bulk_async!(&mut keys, reader, rows, nulls, rbuffer, $sz);
                    Ok(Arc::new(DictionaryArray::new(keys.finish(), dictionary)))
                },
                $(
                    $o => {
                        let mut k = $ob::with_capacity(non_null_count);
                        deser_bulk_async!(raw; &mut k, reader, rows, nulls, rbuffer, $sz => $osz);
                        Ok(Arc::new(DictionaryArray::new(k.finish(), dictionary)))
                    },
                )*
                DataType::Int8 => {
                    let mut keys = Int8Builder::with_capacity(non_null_count);
                    deser_bulk_async!(raw; &mut keys, reader, rows, nulls, rbuffer, $sz => i8);
                    Ok(Arc::new(DictionaryArray::new(keys.finish(), dictionary)))
                },
                DataType::Int16 => {
                    let mut keys = Int16Builder::with_capacity(non_null_count);
                    deser_bulk_async!(raw; &mut keys, reader, rows, nulls, rbuffer, $sz => i16);
                    Ok(Arc::new(DictionaryArray::new(keys.finish(), dictionary)))
                },
                DataType::Int32 => {
                    let mut keys = Int32Builder::with_capacity(non_null_count);
                    deser_bulk_async!(raw; &mut keys, reader, rows, nulls, rbuffer, $sz => i32);
                    Ok(Arc::new(DictionaryArray::new(keys.finish(), dictionary)))
                },
                DataType::Int64 => {
                    let mut keys = Int64Builder::with_capacity(non_null_count);
                    deser_bulk_async!(raw; &mut keys, reader, rows, nulls, rbuffer, $sz => i64);
                    Ok(Arc::new(DictionaryArray::new(keys.finish(), dictionary)))
                },
                _ => return Err(Error::DeserializeError(format!(
                    "LowCardinality: invalid index data type {indexed_type:?}"
                )))
            }
        }
    }
    match indexed_type {
        Type::UInt8 => deser_key!((u8, DataType::UInt8, UInt8Builder) => [
            (u16, DataType::UInt16, UInt16Builder),
            (u32, DataType::UInt32, UInt32Builder),
            (u64, DataType::UInt64, UInt64Builder),
        ]),
        Type::UInt16 => deser_key!((u16, DataType::UInt16, UInt16Builder) => [
            (u8, DataType::UInt8, UInt8Builder),
            (u32, DataType::UInt32, UInt32Builder),
            (u64, DataType::UInt64, UInt64Builder),
        ]),
        Type::UInt32 => deser_key!((u32, DataType::UInt32, UInt32Builder) => [
            (u8, DataType::UInt8, UInt8Builder),
            (u16, DataType::UInt16, UInt16Builder),
            (u64, DataType::UInt64, UInt64Builder),
        ]),
        Type::UInt64 => deser_key!((u64, DataType::UInt64, UInt64Builder) => [
            (u8, DataType::UInt8, UInt8Builder),
            (u16, DataType::UInt16, UInt16Builder),
            (u32, DataType::UInt32, UInt32Builder),
        ]),
        _ => Err(Error::DeserializeError(format!(
            "LowCardinality: invalid index type {indexed_type:?}"
        ))),
    }
}

pub(crate) fn deserialize<R: ClickHouseBytesRead>(
    builder: &mut LowCardinalityBuilder,
    reader: &mut R,
    type_hint: &Type,
    data_type: &DataType,
    rows: usize,
    nulls: &[u8],
    rbuffer: &mut Vec<u8>,
) -> Result<ArrayRef> {
    type Lckb = LowCardinalityKeyBuilder;

    let DataType::Dictionary(_, value_type) = data_type else {
        return Err(Error::ArrowDeserialize(format!("LowCardinality: data type {data_type:?}")));
    };

    let LowCardinalityBuilder { key_builder: keys, value_builder } = builder;

    // Read flags to determine structure
    let flags = reader.try_get_u64_le()?;
    let has_additional_keys = (flags & HAS_ADDITIONAL_KEYS_BIT) != 0;
    let needs_global_dictionary = (flags & NEED_GLOBAL_DICTIONARY_BIT) != 0;
    let needs_update_dictionary = (flags & NEED_UPDATE_DICTIONARY_BIT) != 0;

    // Determine index type
    let indexed_type = match flags & 0xff {
        TUINT8 => Type::UInt8,
        TUINT16 => Type::UInt16,
        TUINT32 => Type::UInt32,
        TUINT64 => Type::UInt64,
        x => {
            return Err(Error::ArrowDeserialize(format!("LowCardinality: bad index type: {x}")));
        }
    };

    #[expect(clippy::cast_possible_truncation)]
    let dict_size = reader.try_get_u64_le()? as usize;

    // Deserialize global dictionary or additional keys
    let dictionary = if needs_global_dictionary || needs_update_dictionary || has_additional_keys {
        // If the inner type is nullable, then the first value deserialized will be a "default"
        // value. Use the null mask to enforce this. The serializer does not write a null
        let null_mask = if type_hint.is_nullable() {
            let mut mask = vec![0_u8; dict_size];
            mask[0] = 1;
            mask
        } else {
            vec![]
        };

        // mask, the deserializer does not read one.

        let inner = type_hint.strip_null();
        inner.deserialize_arrow(
            value_builder,
            reader,
            value_type,
            dict_size,
            &null_mask,
            rbuffer,
        )?

    // No dictionary found
    } else {
        return Err(Error::DeserializeError("LowCardinality: no dictionary provided".to_string()));
    };

    // Read number of rows in this chunk
    #[expect(clippy::cast_possible_truncation)]
    let num_rows = reader.try_get_u64_le()? as usize;
    if num_rows != rows {
        return Err(Error::DeserializeError(format!(
            "LowCardinality must be read in full. Expect {rows} rows, got {num_rows}"
        )));
    }

    macro_rules! deser_key {
        (($sz:ty, $m:ident) => [$(($osz:ty, $o:ident)),* $(,)?]) => {
            match keys {
                Lckb::$m(b) => {
                    super::deser_bulk!(b, reader, rows, nulls, rbuffer, $sz);
                    Ok(Arc::new(DictionaryArray::new(b.finish(), dictionary)))
                },
                $(
                    Lckb::$o(b) => {
                        super::deser_bulk!(raw; b, reader, rows, nulls, rbuffer, $sz => $osz);
                        Ok(Arc::new(DictionaryArray::new(b.finish(), dictionary)))
                    },
                )*
                Lckb::Int8(b) => {
                    super::deser_bulk!(raw; b, reader, rows, nulls, rbuffer, $sz => i8);
                    Ok(Arc::new(DictionaryArray::new(b.finish(), dictionary)))
                },
                Lckb::Int16(b) => {
                    super::deser_bulk!(raw; b, reader, rows, nulls, rbuffer, $sz => i16);
                    Ok(Arc::new(DictionaryArray::new(b.finish(), dictionary)))
                },
                Lckb::Int32(b) => {
                    super::deser_bulk!(raw; b, reader, rows, nulls, rbuffer, $sz => i32);
                    Ok(Arc::new(DictionaryArray::new(b.finish(), dictionary)))
                },
                Lckb::Int64(b) => {
                    super::deser_bulk!(raw; b, reader, rows, nulls, rbuffer, $sz => i64);
                    Ok(Arc::new(DictionaryArray::new(b.finish(), dictionary)))
                },
            }
        }
    }

    match indexed_type {
        Type::UInt8 => deser_key!((u8, UInt8) => [(u16, UInt16), (u32, UInt32), (u64, UInt64)]),
        Type::UInt16 => deser_key!((u16, UInt16) => [(u8, UInt8), (u32, UInt32), (u64, UInt64)]),
        Type::UInt32 => deser_key!((u32, UInt32) => [(u8, UInt8), (u16, UInt16), (u64, UInt64)]),
        Type::UInt64 => deser_key!((u64, UInt64) => [(u8, UInt8), (u16, UInt16), (u32, UInt32)]),
        _ => Err(Error::DeserializeError(format!("LowCardinality: index type {indexed_type:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{DictionaryArray, Int32Array, StringArray};

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::ch_to_arrow_type;
    use crate::native::types::Type;

    // Helper function for testing LowCardinality deserialization
    async fn test_low_cardinality(
        inner_type: Type,
        input: Vec<u8>,
        nulls: &[u8],
        expected_indices: Vec<Option<i32>>,
        expected_values: Vec<Option<&str>>,
    ) -> Result<ArrayRef> {
        let mut reader = Cursor::new(input);
        let rows = expected_indices.len();
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        let key_type = DataType::Int32;
        let value_type = ch_to_arrow_type(&inner_type, opts)?.0;
        let data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let result =
            deserialize_async(&inner_type, &data_type, &mut reader, rows, nulls, &mut vec![])
                .await?;

        let dict_array = result.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        let indices = dict_array.keys();

        let dictionary = dict_array.downcast_dict::<StringArray>().unwrap();
        let mapped_values: Vec<Option<&str>> = dictionary.into_iter().collect::<Vec<_>>();
        let expected_array = StringArray::from(mapped_values);

        assert_eq!(indices, &Int32Array::from(expected_indices), "Indices mismatch");
        assert_eq!(&expected_array, &StringArray::from(expected_values), "Values mismatch");

        Ok(result)
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_string() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Indices: [0, 1, 0]
        ];

        let expected_idx = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(inner_type, input, &[], expected_idx, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_nullable_string() {
        let inner_type = Type::Nullable(Box::new(Type::String));
        let nulls = [];
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            3, 0, 0, 0, 0, 0, 0, 0, // Dict size: 3
            0, // Null value
            1, b'a', 1, b'b', // Dict: ["a", null, "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            1, 0, 2, // Indices: [1, 0, 2]
        ];
        let expected_idx = vec![Some(1), Some(0), Some(2)];
        let expected_values = vec![Some("a"), None, Some("b")];
        drop(
            test_low_cardinality(inner_type, input, &nulls, expected_idx, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_string_uint16() {
        let inner_type = Type::String;
        let input = vec![
            1, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt16 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 1, 0, 0, 0, // Indices: [0, 1, 0] (UInt16)
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(inner_type, input, &[], expected_indices, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_string_uint32() {
        let inner_type = Type::String;
        let input = vec![
            2, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt32 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, // Indices: [0, 1, 0] (UInt32)
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(inner_type, input, &[], expected_indices, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_string_uint64() {
        let inner_type = Type::String;
        let input = vec![
            3, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt64 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // Indices: [0, 1, 0] (UInt64)
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(inner_type, input, &[], expected_indices, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_deserialize_nullable_low_cardinality_string() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 1, // Indices: [0, 0, 1]
        ];
        let nulls = vec![0, 1, 0]; // 0=non-null, 1=null
        let expected_indices = vec![Some(0), None, Some(1)];
        let expected_values = vec![Some("a"), None, Some("b")];
        drop(
            test_low_cardinality(inner_type, input, &nulls, expected_indices, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_global_dictionary() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 1, 0, 0,
            0, // Flags: UInt8 | HasAdditionalKeysBit | NeedsGlobalDictionaryBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Indices: [0, 1, 0]
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(inner_type, input, &[], expected_indices, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_zero_rows() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            0, 0, 0, 0, 0, 0, 0, 0, // Dict size: 0
            0, 0, 0, 0, 0, 0, 0, 0, // Key count: 0
        ];
        let expected_indices = vec![];
        let expected_values = vec![];
        drop(
            test_low_cardinality(inner_type, input, &[], expected_indices, expected_values)
                .await
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_low_cardinality_large_dataset() {
        let inner_type = Type::String;
        let rows = 1000;
        // Generate large dataset
        let mut input = Vec::new();
        // Flags: UInt8 | HasAdditionalKeysBit
        input.extend_from_slice(&[0, 2, 0, 0, 0, 0, 0, 0]);
        // Dict size: a-z (26)
        input.extend_from_slice(&[26, 0, 0, 0, 0, 0, 0, 0]);
        // Dictionary: a-z
        for ch in b'a'..=b'z' {
            input.push(1); // string length
            input.push(ch); // character
        }
        // Key count: 1000
        input.extend_from_slice(&(rows as u64).to_le_bytes());

        // Expected values: a-z
        let char_values: Vec<String> =
            (b'a'..=b'z').map(|c| String::from_utf8(vec![c]).unwrap()).collect();

        // Indices: 0-25 repeated
        let mut indices = Vec::with_capacity(rows);
        let mut expected_indices = Vec::with_capacity(rows);
        let mut expected_values = Vec::with_capacity(rows);
        #[expect(clippy::cast_possible_truncation)]
        for i in 0..rows {
            let idx = (i % 26) as u8;
            indices.push(idx);
            expected_indices.push(Some(i32::from(idx)));
            expected_values.push(Some(char_values[idx as usize].as_str()));
        }
        input.extend_from_slice(&indices);

        drop(
            test_low_cardinality(inner_type, input, &[], expected_indices, expected_values)
                .await
                .expect("Failed to deserialize large LowCardinality(String) dataset"),
        );
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_invalid_num_rows() {
        let inner_type = Type::String;
        let rows = 3;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            4, 0, 0, 0, 0, 0, 0, 0, // Key count: 4 (invalid)
            0, 1, 0, 1, // Indices: [0, 1, 0, 1]
        ];
        let mut reader = Cursor::new(input);
        let key_type = DataType::Int32;
        let value_type = ch_to_arrow_type(&inner_type, None).unwrap().0;
        let data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let result =
            deserialize_async(&inner_type, &data_type, &mut reader, rows, &[], &mut vec![]).await;
        assert!(matches!(
            result,
            Err(Error::DeserializeError(msg))
            if msg.contains("LowCardinality must be read in full. Expect 3 rows, got 4")
        ));
    }

    #[tokio::test]
    async fn test_deserialize_low_cardinality_missing_dictionary() {
        let inner_type = Type::String;
        let rows = 3;
        let input = vec![
            0, 0, 0, 0, 0, 0, 0, 0, // Flags: UInt8 (no HasAdditionalKeysBit)
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 2, // Indices: [0, 1, 2]
        ];
        let mut reader = Cursor::new(input);
        let key_type = DataType::Int32;
        let value_type = ch_to_arrow_type(&inner_type, None).unwrap().0;
        let data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let result =
            deserialize_async(&inner_type, &data_type, &mut reader, rows, &[], &mut vec![]).await;
        assert!(matches!(
            result,
            Err(Error::DeserializeError(msg)) if msg.contains("no dictionary provided")
        ));
    }
}

#[cfg(test)]
mod tests_sync {
    use std::io::Cursor;

    use arrow::array::{DictionaryArray, Int32Array, StringArray};

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::ch_to_arrow_type;
    use crate::native::types::Type;

    // nction for testing LowCardinality deserialization
    fn test_low_cardinality(
        inner_type: &Type,
        input: Vec<u8>,
        nulls: &[u8],
        expected_indices: Vec<Option<i32>>,
        expected_values: Vec<Option<&str>>,
    ) -> Result<ArrayRef> {
        let mut reader = Cursor::new(input);
        let rows = expected_indices.len();
        let opts = Some(ArrowOptions::default().with_strings_as_strings(true));
        let key_type = DataType::Int32;
        let value_type = ch_to_arrow_type(inner_type, opts)?.0;
        let data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let mut builder = LowCardinalityBuilder::try_new(inner_type, &data_type)?;
        let result = deserialize(
            &mut builder,
            &mut reader,
            inner_type,
            &data_type,
            rows,
            nulls,
            &mut vec![],
        )?;

        let dict_array = result.as_any().downcast_ref::<DictionaryArray<Int32Type>>().unwrap();
        let indices = dict_array.keys();

        let dictionary = dict_array.downcast_dict::<StringArray>().unwrap();
        let mapped_values: Vec<Option<&str>> = dictionary.into_iter().collect::<Vec<_>>();
        let expected_array = StringArray::from(mapped_values);

        assert_eq!(indices, &Int32Array::from(expected_indices), "Indices mismatch");
        assert_eq!(&expected_array, &StringArray::from(expected_values), "Values mismatch");

        Ok(result)
    }

    #[test]
    fn test_deserialize_low_cardinality_string() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Indices: [0, 1, 0]
        ];

        let expected_idx = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(test_low_cardinality(&inner_type, input, &[], expected_idx, expected_values).unwrap());
    }

    #[test]
    fn test_deserialize_low_cardinality_nullable_string() {
        let inner_type = Type::Nullable(Box::new(Type::String));
        let nulls = [];
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            3, 0, 0, 0, 0, 0, 0, 0, // Dict size: 3
            0, // Null value
            1, b'a', 1, b'b', // Dict: ["a", null, "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            1, 0, 2, // Indices: [1, 0, 2]
        ];
        let expected_idx = vec![Some(1), Some(0), Some(2)];
        let expected_values = vec![Some("a"), None, Some("b")];
        drop(
            test_low_cardinality(&inner_type, input, &nulls, expected_idx, expected_values)
                .unwrap(),
        );
    }

    #[test]
    fn test_deserialize_low_cardinality_string_uint16() {
        let inner_type = Type::String;
        let input = vec![
            1, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt16 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 1, 0, 0, 0, // Indices: [0, 1, 0] (UInt16)
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(&inner_type, input, &[], expected_indices, expected_values)
                .unwrap(),
        );
    }

    #[test]
    fn test_deserialize_low_cardinality_string_uint32() {
        let inner_type = Type::String;
        let input = vec![
            2, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt32 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, // Indices: [0, 1, 0] (UInt32)
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(&inner_type, input, &[], expected_indices, expected_values)
                .unwrap(),
        );
    }

    #[test]
    fn test_deserialize_low_cardinality_string_uint64() {
        let inner_type = Type::String;
        let input = vec![
            3, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt64 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, // Indices: [0, 1, 0] (UInt64)
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(&inner_type, input, &[], expected_indices, expected_values)
                .unwrap(),
        );
    }

    #[test]
    fn test_deserialize_nullable_low_cardinality_string() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 0, 1, // Indices: [0, 0, 1]
        ];
        let nulls = vec![0, 1, 0]; // 0=non-null, 1=null
        let expected_indices = vec![Some(0), None, Some(1)];
        let expected_values = vec![Some("a"), None, Some("b")];
        drop(
            test_low_cardinality(&inner_type, input, &nulls, expected_indices, expected_values)
                .unwrap(),
        );
    }

    #[test]
    fn test_deserialize_low_cardinality_global_dictionary() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 1, 0, 0,
            0, // Flags: UInt8 | HasAdditionalKeysBit | NeedsGlobalDictionaryBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Indices: [0, 1, 0]
        ];
        let expected_indices = vec![Some(0), Some(1), Some(0)];
        let expected_values = vec![Some("a"), Some("b"), Some("a")];
        drop(
            test_low_cardinality(&inner_type, input, &[], expected_indices, expected_values)
                .unwrap(),
        );
    }

    #[test]
    fn test_deserialize_low_cardinality_zero_rows() {
        let inner_type = Type::String;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            0, 0, 0, 0, 0, 0, 0, 0, // Dict size: 0
            0, 0, 0, 0, 0, 0, 0, 0, // Key count: 0
        ];
        let expected_indices = vec![];
        let expected_values = vec![];
        drop(
            test_low_cardinality(&inner_type, input, &[], expected_indices, expected_values)
                .unwrap(),
        );
    }

    #[test]
    fn test_low_cardinality_large_dataset() {
        let inner_type = Type::String;
        let rows = 1000;
        // Generate large dataset
        let mut input = Vec::new();
        // Flags: UInt8 | HasAdditionalKeysBit
        input.extend_from_slice(&[0, 2, 0, 0, 0, 0, 0, 0]);
        // Dict size: a-z (26)
        input.extend_from_slice(&[26, 0, 0, 0, 0, 0, 0, 0]);
        // Dictionary: a-z
        for ch in b'a'..=b'z' {
            input.push(1); // string length
            input.push(ch); // character
        }
        // Key count: 1000
        input.extend_from_slice(&(rows as u64).to_le_bytes());

        // Expected values: a-z
        let char_values: Vec<String> =
            (b'a'..=b'z').map(|c| String::from_utf8(vec![c]).unwrap()).collect();

        // Indices: 0-25 repeated
        let mut indices = Vec::with_capacity(rows);
        let mut expected_indices = Vec::with_capacity(rows);
        let mut expected_values = Vec::with_capacity(rows);
        #[expect(clippy::cast_possible_truncation)]
        for i in 0..rows {
            let idx = (i % 26) as u8;
            indices.push(idx);
            expected_indices.push(Some(i32::from(idx)));
            expected_values.push(Some(char_values[idx as usize].as_str()));
        }
        input.extend_from_slice(&indices);

        drop(
            test_low_cardinality(&inner_type, input, &[], expected_indices, expected_values)
                .expect("Failed to deserialize large LowCardinality(String) dataset"),
        );
    }

    #[test]
    fn test_deserialize_low_cardinality_invalid_num_rows() {
        let inner_type = Type::String;
        let rows = 3;
        let input = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', 1, b'b', // Dict: ["a", "b"]
            4, 0, 0, 0, 0, 0, 0, 0, // Key count: 4 (invalid)
            0, 1, 0, 1, // Indices: [0, 1, 0, 1]
        ];
        let mut reader = Cursor::new(input);
        let key_type = DataType::Int32;
        let value_type = ch_to_arrow_type(&inner_type, None).unwrap().0;
        let data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let mut builder = LowCardinalityBuilder::try_new(&inner_type, &data_type).unwrap();
        let result =
            deserialize(&mut builder, &mut reader, &inner_type, &data_type, rows, &[], &mut vec![]);
        assert!(matches!(
            result,
            Err(Error::DeserializeError(msg))
            if msg.contains("LowCardinality must be read in full. Expect 3 rows, got 4")
        ));
    }

    #[test]
    fn test_deserialize_low_cardinality_missing_dictionary() {
        let inner_type = Type::String;
        let rows = 3;
        let input = vec![
            0, 0, 0, 0, 0, 0, 0, 0, // Flags: UInt8 (no HasAdditionalKeysBit)
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 2, // Indices: [0, 1, 2]
        ];
        let mut reader = Cursor::new(input);
        let key_type = DataType::Int32;
        let value_type = ch_to_arrow_type(&inner_type, None).unwrap().0;
        let data_type = DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let mut builder = LowCardinalityBuilder::try_new(&inner_type, &data_type).unwrap();
        let result =
            deserialize(&mut builder, &mut reader, &inner_type, &data_type, rows, &[], &mut vec![]);
        assert!(matches!(
            result,
            Err(Error::DeserializeError(msg)) if msg.contains("no dictionary provided")
        ));
    }
}
