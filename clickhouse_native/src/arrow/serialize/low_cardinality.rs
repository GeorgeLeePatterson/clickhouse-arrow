/// Serialization logic for `ClickHouse` `LowCardinality` types from Arrow arrays.
///
/// This module provides functions to serialize Arrow `DictionaryArray` (with numeric keys) or
/// string-like arrays (`Utf8`, `LargeUtf8`, `Utf8View`) into `ClickHouse`’s native format for
/// `LowCardinality` types.
///
/// The `serialize` function dispatches to `write_values` for `DictionaryArray` or
/// `write_string_values` for string-like arrays. The native format includes:
/// - Flags (`u64`): Indicates key type (`UInt8` = 0, `UInt16` = 1, `UInt32` = 2, `UInt64` = 3)
///   and `HasAdditionalKeysBit` (512), written as little-endian `u64`.
/// - Dictionary size (`u64`): Number of unique values.
/// - Dictionary values: Serialized via the inner type (e.g., `String` as `var_uint` length +
///   bytes).
/// - Key count (`u64`): Number of rows.
/// - Keys: Indices into the dictionary, written as `u8`, `u16`, `u32`, or `u64` based on key
///   type.
///
/// # Examples
/// ```rust,ignore
/// use arrow::array::{ArrayRef, DictionaryArray, Int8Array, StringArray};
/// use arrow::datatypes::{Field, Int8Type};
/// use clickhouse_native::types::{Type, low_cardinality::serialize, SerializerState};
/// use std::sync::Arc;
///
/// #[tokio::test]
/// async fn test_serialize_low_cardinality() {
///   let keys = Int8Array::from(vec![0, 1, 0]);
///   let values = StringArray::from(vec!["a", "b"]);
///   let array = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).unwrap())
///       as ArrayRef;
///   let field = Field::new("", array.data_type().clone(), false);
///   let mut writer = MockWriter::new();
///   let mut state = SerializerState::default();
///   serialize(
///       &Type::LowCardinality(Box::new(Type::String)),
///       &field,
///       &array,
///       &mut writer,
///       &mut state,
///   )
///   .await
///   .unwrap();
///   let expected = vec![
///       0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
///       2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
///       1, b'a', // Dict: "a" (var_uint length)
///       1, b'b', // Dict: "b" (var_uint length)
///       3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
///       0, 1, 0, // Keys: [0, 1, 0]
///   ];
///   assert_eq!(writer, expected);
/// }
/// ```
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Field, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};

use super::ClickhouseArrowSerializer;
use crate::formats::SerializerState;
use crate::io::ClickhouseWrite;
use crate::native::types::low_cardinality::*;
use crate::{ClickhouseNativeError, Result, Type};

/// Serializes an Arrow array to `ClickHouse`’s native format for `LowCardinality` types.
///
/// Dispatches to `write_values` for `DictionaryArray` (numeric keys) or `write_string_values` for
/// string-like arrays (`Utf8`, `LargeUtf8`, `Utf8View`). The format includes flags, dictionary
/// size, dictionary values, and keys. Null values are written as `0` (numeric) or empty strings
/// (string), with nullability handled by `null.rs` for `Nullable(LowCardinality)` types. Returns
/// early for empty arrays to produce no output, matching `ClickHouse`’s behavior for empty columns.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` `Type` (`LowCardinality(inner)`) indicating the target type.
/// - `field`: The Arrow `Field` providing schema information.
/// - `values`: The Arrow array containing the data to serialize.
/// - `writer`: The async writer to serialize to.
/// - `state`: A mutable `SerializerState` for serialization context.
///
/// # Returns
/// A `Result` indicating success or a `ClickhouseNativeError` if serialization fails.
///
/// # Errors
/// - Returns `ArrowSerialize` if:
///   - The `type_hint` is not `LowCardinality`.
///   - The input array type is unsupported (not `DictionaryArray` or string-like).
///   - The dictionary key type is unsupported (not `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`,
///     `UInt16`, `UInt32`, `UInt64`).
///   - Downcasting fails for keys or values.
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn serialize<W: ClickhouseWrite>(
    type_hint: &Type,
    field: &Field,
    values: &ArrayRef,
    writer: &mut W,
    state: &mut SerializerState,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }

    match type_hint.strip_null() {
        Type::LowCardinality(inner) => match field.data_type() {
            DataType::Dictionary(key_type, _) => match **key_type {
                DataType::Int8 => {
                    write_values::<W, Int8Type>(inner, values, writer, state).await?;
                }
                DataType::Int16 => {
                    write_values::<W, Int16Type>(inner, values, writer, state).await?;
                }
                DataType::Int32 => {
                    write_values::<W, Int32Type>(inner, values, writer, state).await?;
                }
                DataType::Int64 => {
                    write_values::<W, Int64Type>(inner, values, writer, state).await?;
                }
                DataType::UInt8 => {
                    write_values::<W, UInt8Type>(inner, values, writer, state).await?;
                }
                DataType::UInt16 => {
                    write_values::<W, UInt16Type>(inner, values, writer, state).await?;
                }
                DataType::UInt32 => {
                    write_values::<W, UInt32Type>(inner, values, writer, state).await?;
                }
                DataType::UInt64 => {
                    write_values::<W, UInt64Type>(inner, values, writer, state).await?;
                }
                _ => {
                    return Err(ClickhouseNativeError::ArrowSerialize(format!(
                        "Dictionary type key is not supported in `ClickHouse`: {key_type:?}"
                    )));
                }
            },
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView => {
                write_string_values(writer, values, type_hint.is_nullable(), state).await?;
            }
            _ => {
                return Err(ClickhouseNativeError::ArrowSerialize(format!(
                    "`LowCardinality` must be either String or Dictionary: {field:?}"
                )));
            }
        },
        _ => {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "Unsupported data type: {type_hint:?}"
            )));
        }
    }

    Ok(())
}

/// Macro to write dictionary keys for `LowCardinality` types.
///
/// Writes keys to the writer using the appropriate integer type (`u8`, `u16`, `u32`, `u64`) based
/// on the dictionary size, as indicated by the `flags`. The macro downcasts the keys array to the
/// specified type and writes each key after casting to the target type.
///
/// # Arguments
/// - `$writer`: The async writer to serialize to.
/// - `$flags`: Flags indicating the key type (e.g., `TUINT8`, `TUINT16`).
/// - `$keys`: The Arrow array containing the dictionary keys.
/// - `$key_type`: The expected Arrow array type for the keys.
///
/// # Returns
/// A `Result` indicating success or a `ClickhouseNativeError` if serialization fails.
macro_rules! write_dictionary_keys {
    ($writer:expr, $flags:expr, $keys:expr, $key_type:ty, $nullable:expr) => {{
        let keys = $keys.as_any().downcast_ref::<$key_type>().ok_or(
            ClickhouseNativeError::ArrowSerialize(format!(
                "Failed to downcast keys to {}",
                stringify!($key_type)
            )),
        )?;

        #[allow(clippy::cast_sign_loss)]
        #[allow(clippy::cast_lossless)]
        #[allow(clippy::cast_possible_truncation)]
        #[allow(trivial_numeric_casts)]
        for key in keys.iter() {
            let key = key.map(|k| k as usize + $nullable).unwrap_or(0);
            match $flags & KEY_TYPE_MASK {
                TUINT64 => $writer.write_u64_le(key as u64).await?,
                TUINT32 => $writer.write_u32_le(key as u32).await?,
                TUINT16 => $writer.write_u16_le(key as u16).await?,
                TUINT8 => $writer.write_u8(key as u8).await?,
                _ => unreachable!(),
            }
        }
    }};
}

/// Serializes a `DictionaryArray` to `ClickHouse`’s `LowCardinality` format with numeric keys.
///
/// Writes flags, dictionary size, dictionary values, and keys. The flags indicate the key type
/// (`UInt8`, `UInt16`, `UInt32`, `UInt64`) based on dictionary size and include
/// `HasAdditionalKeysBit`. Keys are written using the `dictionary_keys!` macro after validating the
/// dictionary.
///
/// # Arguments
/// - `inner_type`: The `ClickHouse` type of the dictionary values (e.g., `String`, `Int32`).
/// - `values`: The `DictionaryArray` containing the data.
/// - `nullable`: Whether the values are nullable.
/// - `writer`: The async writer to serialize to.
/// - `state`: A mutable `SerializerState` for serialization context.
///
/// # Returns
/// A `Result` indicating success or a `ClickhouseNativeError` if serialization fails.
///
/// # Errors
/// - Returns `ArrowSerialize` if:
///   - The input array is not a `DictionaryArray` with the expected key type.
///   - The dictionary key type is unsupported.
/// - Returns `Io` if writing to the writer fails.
async fn write_values<W: ClickhouseWrite, K: ArrowDictionaryKeyType>(
    inner_type: &Type,
    values: &ArrayRef,
    writer: &mut W,
    state: &mut SerializerState,
) -> Result<()> {
    let array = values.as_any().downcast_ref::<DictionaryArray<K>>().ok_or(
        ClickhouseNativeError::ArrowSerialize("Failed to downcast to DictionaryArray".to_string()),
    )?;

    if array.is_empty() {
        return Ok(());
    }

    let key_data_type = array.keys().data_type();
    let value_data_type = array.values().data_type();

    let keys = array.keys();
    let dictionary = array.values();
    let dict_len = dictionary.len();

    // If null is already present in the dictionary, the code does not need to provide it.
    let already_has_null = dictionary.null_count() > 0;

    // ClickHouse expects the serialized values to include a null value in the case of nullable
    let modifier = usize::from(inner_type.is_nullable() && !already_has_null);
    let adjusted_dict_len = dict_len + modifier;

    // Write unique values
    let mut flags = HAS_ADDITIONAL_KEYS_BIT;
    if adjusted_dict_len > u32::MAX as usize {
        flags |= TUINT64;
    } else if adjusted_dict_len > u16::MAX as usize {
        flags |= TUINT32;
    } else if adjusted_dict_len > u8::MAX as usize {
        flags |= TUINT16;
    } else {
        flags |= TUINT8;
    }
    writer.write_u64_le(flags).await?;

    // Write dict values length
    writer.write_u64_le(adjusted_dict_len as u64).await?;

    // Handle nullability, skipping if the first value is already null
    if modifier == 1 {
        // Write the first "value" for the nullable dictionary, aka default value
        inner_type.write_default(writer).await?; // default
    }

    // Serialize dictionary values
    let value_field = Arc::new(Field::new("", value_data_type.clone(), false));
    inner_type.strip_null().serialize(writer, dictionary, &value_field, state).await?;

    // Write keys
    writer.write_u64_le(keys.len() as u64).await?;
    match key_data_type {
        DataType::Int8 => write_dictionary_keys!(writer, flags, keys, Int8Array, modifier),
        DataType::Int16 => write_dictionary_keys!(writer, flags, keys, Int16Array, modifier),
        DataType::Int32 => write_dictionary_keys!(writer, flags, keys, Int32Array, modifier),
        DataType::Int64 => write_dictionary_keys!(writer, flags, keys, Int64Array, modifier),
        DataType::UInt8 => write_dictionary_keys!(writer, flags, keys, UInt8Array, modifier),
        DataType::UInt16 => write_dictionary_keys!(writer, flags, keys, UInt16Array, modifier),
        DataType::UInt32 => write_dictionary_keys!(writer, flags, keys, UInt32Array, modifier),
        DataType::UInt64 => write_dictionary_keys!(writer, flags, keys, UInt64Array, modifier),
        _ => {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "Unsupported dictionary key type: {key_data_type:?}"
            )));
        }
    }

    Ok(())
}

/// Serializes a string-like array (`Utf8`, `LargeUtf8`, `Utf8View`) to `ClickHouse`’s
/// `LowCardinality` format.
///
/// Builds a dictionary of unique string values using a `BTreeMap` to assign indices, with the empty
/// string (`""`) at index `0` for nulls to match `ClickHouse`’s null handling. Writes flags,
/// dictionary size, unique string values, and indices. Null values are written as empty strings,
/// with nullability handled by `null.rs` for `Nullable(LowCardinality)` types.
///
/// # Arguments
/// - `writer`: The async writer to serialize to.
/// - `values`: The string-like array containing the data (`StringArray`, `LargeStringArray`, or
///   `StringViewArray`).
/// - `nullable`: Whether the values are nullable.
/// - `state`: A mutable `SerializerState` for serialization context.
///
/// # Returns
/// A `Result` indicating success or a `ClickhouseNativeError` if serialization fails.
///
/// # Errors
/// - Returns `ArrowSerialize` if the input array is not a string-like type.
/// - Returns `Io` if writing to the writer fails.
async fn write_string_values<W: ClickhouseWrite>(
    writer: &mut W,
    values: &ArrayRef,
    nullable: bool,
    state: &mut SerializerState,
) -> Result<()> {
    // Helper functions for macro
    fn bytes(v: &str) -> &[u8] { v.as_bytes() }
    fn passthrough(v: &[u8]) -> &[u8] { v }

    // Handle string-like array
    let mut dict = Vec::with_capacity(64.min(values.len()));
    let mut keys = Vec::with_capacity(values.len());
    let nullable = values.null_count() > 0 || nullable;

    // Pre-seed with an empty string, aka default value
    if nullable {
        dict.push(b"" as &[u8]);
    }

    macro_rules! handle_string_array {
        ($array_ty:ty, $coerce:expr) => {{
            let array = values.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                ClickhouseNativeError::ArrowSerialize(format!(
                    "Failed to downcast to {}",
                    stringify!($array_ty)
                ))
            })?;
            for i in 0..array.len() {
                if array.is_null(i) {
                    // TODO: Should a fallback be used here, shift the vec for instance?
                    // Performance hit?
                    debug_assert!(nullable, "Null encountered in non-nullable array");
                    keys.push(0);
                } else {
                    let value = array.value(i);
                    let value = $coerce(value);
                    let index = dict.iter().position(|v| *v == value).unwrap_or_else(|| {
                        dict.push(value);
                        dict.len() - 1
                    });

                    #[expect(clippy::cast_possible_wrap)]
                    #[expect(clippy::cast_possible_truncation)]
                    keys.push(index as i32);
                };
            }
        }};
    }

    match values.data_type() {
        DataType::Utf8 => handle_string_array!(StringArray, bytes),
        DataType::LargeUtf8 => handle_string_array!(LargeStringArray, bytes),
        DataType::Utf8View => handle_string_array!(StringViewArray, bytes),
        DataType::Binary => handle_string_array!(BinaryArray, passthrough),
        DataType::BinaryView => handle_string_array!(BinaryViewArray, passthrough),
        DataType::LargeBinary => handle_string_array!(LargeBinaryArray, passthrough),
        dt => {
            return Err(ClickhouseNativeError::ArrowSerialize(format!(
                "Expected string-like array, got {dt}",
            )));
        }
    }

    let dict_size = dict.len();
    let flags = (if dict_size > u32::MAX as usize {
        TUINT64
    } else if dict_size > u16::MAX as usize {
        TUINT32
    } else if dict_size > u8::MAX as usize {
        TUINT16
    } else {
        TUINT8
    }) | HAS_ADDITIONAL_KEYS_BIT;

    // Write flags and dictionary size
    writer.write_u64_le(flags).await?;
    writer.write_u64_le(dict_size as u64).await?;

    // Write dictionary values
    // Binary is used to write the bytes directly, although I'm not sure there's any gains
    let values_array = Arc::new(BinaryArray::from_iter_values(dict)) as ArrayRef;
    let inner_field = Arc::new(Field::new("", DataType::Binary, nullable));
    let inner_type = Type::Binary;
    inner_type.serialize(writer, &values_array, &inner_field, state).await?;

    // Write keys
    writer.write_u64_le(keys.len() as u64).await?;

    #[expect(clippy::cast_sign_loss)]
    #[expect(clippy::cast_possible_truncation)]
    for key in keys {
        match flags & KEY_TYPE_MASK {
            TUINT64 => writer.write_u64_le(key as u64).await?,
            TUINT32 => writer.write_u32_le(key as u32).await?,
            TUINT16 => writer.write_u16_le(key as u16).await?,
            TUINT8 => writer.write_u8(key as u8).await?,
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        DictionaryArray, Int8Array, LargeStringArray, StringArray, StringViewArray,
    };
    use arrow::datatypes::{Field, Int8Type};

    use super::*;
    use crate::ArrowOptions;

    type MockWriter = Vec<u8>;

    /// Helper function used by individual type serializers
    pub(crate) async fn test_type_serializer(
        expected: Vec<u8>,
        type_: &Type,
        field: &Field,
        array: &ArrayRef,
    ) {
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default()
            .with_arrow_options(ArrowOptions::default().with_strings_as_strings(true));
        serialize(type_, field, array, &mut writer, &mut state).await.unwrap();
        assert_eq!(*writer, expected);
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_dictionary() {
        let array = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(vec!["a", "b"])),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new("", array.data_type().clone(), false);
        let expected = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', // Dict: "a" (var_uint length)
            1, b'b', // Dict: "b" (var_uint length)
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Keys: [0, 1, 0]
        ];
        test_type_serializer(
            expected,
            &Type::LowCardinality(Box::new(Type::String)),
            &field,
            &array,
        )
        .await;
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_dictionary_nullable() {
        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(3), Some(1), None, Some(2)]),
                Arc::new(StringArray::from(vec!["active", "inactive", "pending", "absent"])),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        // Serialized values are prepended and keys are shifted to account for nulls
        let expected = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            5, 0, 0, 0, 0, 0, 0, 0, // Dict size: 4
            0, // Prepended Null value
            // Dictionary values: ["active", "inactive", "pending", "absent"]
            6, b'a', b'c', b't', b'i', b'v', b'e', // "active"
            8, b'i', b'n', b'a', b'c', b't', b'i', b'v', b'e', // "inactive"
            7, b'p', b'e', b'n', b'd', b'i', b'n', b'g', // "pending"
            6, b'a', b'b', b's', b'e', b'n', b't', // "absent"
            5, 0, 0, 0, 0, 0, 0, 0, // Key count: 5
            1, 4, 2, 0, 3, // Keys: [1, 4, 2, 0, 3]
        ];
        test_type_serializer(
            expected,
            &Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String)))),
            &field,
            &array,
        )
        .await;
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_dictionary_nullable_accounted_for() {
        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(3), Some(1), None, Some(2)]),
                Arc::new(StringArray::from(vec![
                    Some("active"),
                    None,
                    Some("inactive"),
                    Some("pending"),
                    Some("absent"),
                ])),
            )
            .unwrap(),
        ) as ArrayRef;
        let field = Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        // Serialized values are prepended and keys are shifted to account for nulls
        let expected = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            5, 0, 0, 0, 0, 0, 0, 0, // Dict size: 4
            // Dictionary values: ["active", "inactive", "pending", "absent"]
            6, b'a', b'c', b't', b'i', b'v', b'e', // "active"
            0,    // Null value
            8, b'i', b'n', b'a', b'c', b't', b'i', b'v', b'e', // "inactive"
            7, b'p', b'e', b'n', b'd', b'i', b'n', b'g', // "pending"
            6, b'a', b'b', b's', b'e', b'n', b't', // "absent"
            5, 0, 0, 0, 0, 0, 0, 0, // Key count: 5
            0, 3, 1, 0, 2, // Keys: [0, 3, 1, 0, 2]
        ];
        test_type_serializer(
            expected,
            &Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String)))),
            &field,
            &array,
        )
        .await;
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_string() {
        let array = Arc::new(StringArray::from(vec!["a", "b", "a"])) as ArrayRef;
        let field = Field::new("", DataType::Utf8, false);
        let expected = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', // Dict: "a" (var_uint length)
            1, b'b', // Dict: "b" (var_uint length)
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Keys: [0, 1, 0]
        ];
        test_type_serializer(
            expected,
            &Type::LowCardinality(Box::new(Type::String)),
            &field,
            &array,
        )
        .await;
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_large_string() {
        let array = Arc::new(LargeStringArray::from(vec!["a", "b", "a"])) as ArrayRef;
        let field = Field::new("", DataType::LargeUtf8, false);
        let expected = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', // Dict: "a" (var_uint length)
            1, b'b', // Dict: "b" (var_uint length)
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Keys: [0, 1, 0]
        ];
        test_type_serializer(
            expected,
            &Type::LowCardinality(Box::new(Type::String)),
            &field,
            &array,
        )
        .await;
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_view_string() {
        let array = Arc::new(StringViewArray::from(vec!["a", "b", "a"])) as ArrayRef;
        let field = Field::new("", DataType::Utf8View, false);
        let expected = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
            2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
            1, b'a', // Dict: "a" (var_uint length)
            1, b'b', // Dict: "b" (var_uint length)
            3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
            0, 1, 0, // Keys: [0, 1, 0]
        ];
        test_type_serializer(
            expected,
            &Type::LowCardinality(Box::new(Type::String)),
            &field,
            &array,
        )
        .await;
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_nullable_variations() {
        async fn run_test(type_: &Type, field: &Field, array: &ArrayRef) {
            let expected = vec![
                0, 2, 0, 0, 0, 0, 0, 0, // Flags: UInt8 | HasAdditionalKeysBit
                2, 0, 0, 0, 0, 0, 0, 0, // Dict size: 2
                0, // Dict: "" (var_uint length)
                1, b'a', // Dict: "a" (var_uint length)
                3, 0, 0, 0, 0, 0, 0, 0, // Key count: 3
                1, 0, 1, // Keys: [1, 0, 1]
            ];
            test_type_serializer(expected, type_, field, array).await;
        }

        let tests = [
            (
                Type::LowCardinality(Box::new(Type::String)),
                Field::new("", DataType::Utf8, false),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("a")])) as ArrayRef,
            ),
            (
                Type::LowCardinality(Box::new(Type::String)),
                Field::new("", DataType::Utf8View, false),
                Arc::new(StringViewArray::from(vec![Some("a"), None, Some("a")])) as ArrayRef,
            ),
            (
                Type::LowCardinality(Box::new(Type::String)),
                Field::new("", DataType::LargeUtf8, false),
                Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("a")])) as ArrayRef,
            ),
            (
                Type::LowCardinality(Box::new(Type::String)),
                Field::new("", DataType::Binary, false),
                Arc::new(BinaryArray::from_opt_vec(vec![Some(b"a"), None, Some(b"a")])) as ArrayRef,
            ),
            (
                Type::LowCardinality(Box::new(Type::String)),
                Field::new("", DataType::BinaryView, false),
                Arc::new(BinaryViewArray::from(vec![Some(b"a" as &[u8]), None, Some(b"a")]))
                    as ArrayRef,
            ),
            (
                Type::LowCardinality(Box::new(Type::String)),
                Field::new("", DataType::LargeBinary, false),
                Arc::new(LargeBinaryArray::from_opt_vec(vec![Some(b"a"), None, Some(b"a")]))
                    as ArrayRef,
            ),
        ];

        for (t, f, a) in &tests {
            run_test(t, f, a).await;
        }
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_empty() {
        let array = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
        let field = Field::new("", DataType::Utf8, false);
        test_type_serializer(vec![], &Type::LowCardinality(Box::new(Type::String)), &field, &array)
            .await;
    }

    #[tokio::test]
    async fn test_serialize_low_cardinality_invalid_type() {
        let array = Arc::new(Int8Array::from(vec![1, 2, 1])) as ArrayRef;
        let field = Field::new("", DataType::Int8, false);
        let mut writer = MockWriter::new();
        let mut state = SerializerState::default();
        let result = serialize(
            &Type::LowCardinality(Box::new(Type::String)),
            &field,
            &array,
            &mut writer,
            &mut state,
        )
        .await;
        assert!(matches!(
            result,
            Err(ClickhouseNativeError::ArrowSerialize(msg)) if msg.contains("`LowCardinality` must be either String or Dictionary")
        ));
    }

    #[tokio::test]
    async fn test_low_cardinality_nullable() {
        let field = Field::new(
            "low_cardinality_nullable_string_col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), Some(3), Some(1), None, Some(2)]),
                Arc::new(StringArray::from(vec!["active", "inactive", "pending", "absent"])),
            )
            .unwrap(),
        ) as ArrayRef;
        let expected = vec![
            0, 2, 0, 0, 0, 0, 0, 0, // Flags
            5, 0, 0, 0, 0, 0, 0, 0, // Dict length
            0, // Null value
            6, 97, 99, 116, 105, 118, 101, // Dict value
            8, 105, 110, 97, 99, 116, 105, 118, 101, // Dict value
            7, 112, 101, 110, 100, 105, 110, 103, // Dict value
            6, 97, 98, 115, 101, 110, 116, // Dict value
            5, 0, 0, 0, 0, 0, 0, 0, // Key length (# of rows)
            1, 4, 2, 0, 3, // Key indices
        ];
        test_type_serializer(
            expected,
            &Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String)))),
            &field,
            &array,
        )
        .await;
    }
}
