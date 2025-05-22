#![expect(clippy::cast_possible_wrap)]
#![expect(clippy::cast_possible_truncation)]
/// Deserialization logic for `ClickHouse` string and binary types into Arrow arrays.
///
/// This module provides a function to deserialize ClickHouse’s native format for string and
/// binary-like types into Arrow arrays, such as `StringArray` for `String`, `BinaryArray` for
/// `Binary`, and `FixedSizeBinaryArray` for fixed-length types like `FixedSizedString`,
/// `Uuid`, `Ipv4`, `Ipv6`, `Int128`, `UInt128`, `Int256`, and `UInt256`.
///
/// The `deserialize` function dispatches to specialized logic based on the `Type` variant,
/// reading variable-length or fixed-length data from the input stream. It respects the
/// `ClickHouse` null mask convention (`1`=null, `0`=non-null) and includes default values for
/// nulls (e.g., empty strings for `Nullable(String)`, zeroed buffers for
/// `Nullable(FixedSizedString)`).
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use arrow::array::*;
use tokio::io::AsyncReadExt;

use crate::formats::DeserializerState;
use crate::io::ClickhouseRead;
use crate::{ClickhouseNativeError, Result, Type};

/// Macro to deserialize variable-length binary types into Arrow arrays.
///
/// Generates code to read `var_uint` lengths and bytes for each row, building an Arrow array
/// using the specified builder type (e.g., `StringBuilder`, `BinaryBuilder`). Handles
/// nullability via the null mask (`1`=null, `0`=non-null), appending empty values for nulls.
///
/// # Arguments
/// - `$builder_type`: The Arrow builder type (e.g., `StringBuilder`).
/// - `$rows`: The number of rows to deserialize.
/// - `$null_mask`: The null mask slice (`1`=null, `0`=non-null).
/// - `$st`: The statement to read and convert the value (e.g., `String::from_utf8_lossy`).
macro_rules! deserialize_binary {
    ($builder_type:ty, $rows:expr, $off:expr, $null_mask:expr, { $st:expr }) => {{
        #[allow(unused_imports)]
        use ::tokio::io::AsyncReadExt as _;

        let mut builder = <$builder_type>::with_capacity($rows, $off);
        for i in 0..$rows {
            let value = $st;
            if $null_mask.is_empty() || $null_mask[i] == 0 {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        Ok(::std::sync::Arc::new(builder.finish()) as ArrayRef)
    }};
}

/// Macro to deserialize fixed-length binary types into Arrow arrays.
///
/// Generates code to read fixed-size byte arrays for each row, building an Arrow array using
/// the specified builder type (e.g., `FixedSizeBinaryBuilder`). Handles nullability via the
/// null mask (`1`=null, `0`=non-null), appending zeroed buffers for nulls.
///
/// # Arguments
/// - `$builder_type`: The Arrow builder type (e.g., `FixedSizeBinaryBuilder`).
/// - `$rows`: The number of rows to deserialize.
/// - `$len`: The fixed length of each value (e.g., 16 for `Uuid`).
/// - `$null_mask`: The null mask slice (`1`=null, `0`=non-null).
/// - `$st`: The statement to read and convert the value (e.g., read bytes or convert `Ipv4Addr`).
macro_rules! deserialize_binary_fixed {
    ($builder_type:ty, $rows:expr, $off:expr, $null_mask:expr, { $st:expr }) => {{
        #[allow(unused_imports)]
        use ::tokio::io::AsyncReadExt as _;

        let mut builder = <$builder_type>::with_capacity($rows, $off);
        for i in 0..$rows {
            let value = $st;
            if $null_mask.is_empty() || $null_mask[i] == 0 {
                builder.append_value(value)?;
            } else {
                builder.append_null();
            }
        }
        Ok(::std::sync::Arc::new(builder.finish()) as ArrayRef)
    }};
}

/// Deserializes a `ClickHouse` string or binary type into an Arrow array.
///
/// Reads variable-length or fixed-length data from the input stream, constructing an Arrow array
/// based on the `Type` variant. Supports `String`, `FixedSizedString`, `Binary`,
/// `FixedSizedBinary`, `Uuid`, `Ipv4`, `Ipv6`, `Int128`, `UInt128`, `Int256`, and `UInt256`.
/// Handles nullability via the provided null mask (`1`=null, `0`=non-null), producing empty
/// strings for `Nullable(String)` nulls, zeroed buffers for fixed-length types, and appropriate
/// defaults for other types.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` type to deserialize (e.g., `String`, `Uuid`).
/// - `reader`: The async reader providing the `ClickHouse` native format data.
/// - `rows`: The number of rows to deserialize.
/// - `null_mask`: A slice indicating null values (`1` for null, `0` for non-null).
/// - `_state`: A mutable `DeserializerState` for deserialization context (unused).
///
/// # Returns
/// A `Result` containing the deserialized `ArrayRef` or a `ClickhouseNativeError` if
/// deserialization fails.
///
/// # Errors
/// - Returns `Io` if reading from the reader fails (e.g., EOF).
/// - Returns `ArrowDeserialize` if the `type_hint` is unsupported or data is malformed.
///
/// # Example
/// ```rust,ignore
/// use arrow::array::{ArrayRef, StringArray};
/// use clickhouse_native::types::{Type, DeserializerState};
/// use std::io::Cursor;
/// use std::sync::Arc;
///
/// #[tokio::test]
/// async fn test_deserialize_binary() {
///     let data = vec![
///         // Strings: ["hello", "", "world"]
///         5, b'h', b'e', b'l', b'l', b'o', // "hello"
///         0, // "" (empty string)
///         5, b'w', b'o', b'r', b'l', b'd', // "world"
///     ];
///     let mut reader = Cursor::new(data);
///     let mut state = DeserializerState::default();
///     let array = crate::arrow::deserialize::binary::deserialize(
///         &Type::String,
///         &mut reader,
///         3,
///         &[],
///         &mut state,
///     )
///     .await
///     .unwrap();
///     let expected = Arc::new(StringArray::from(vec!["hello", "", "world"])) as ArrayRef;
///     assert_eq!(array.as_ref(), expected.as_ref());
/// }
/// ```
#[expect(clippy::too_many_lines)]
pub(crate) async fn deserialize<R: ClickhouseRead>(
    type_hint: &Type,
    reader: &mut R,
    rows: usize,
    null_mask: &[u8],
    _state: &mut DeserializerState,
) -> Result<ArrayRef> {
    let nullability = if type_hint.is_nullable() { rows * 2 } else { rows };
    let offset = nullability * type_hint.estimate_capacity(); // account for nullability

    match type_hint.strip_null() {
        // String, Binary, UUID, IPv4, IPv6
        Type::String | Type::Object => {
            deserialize_binary!(StringBuilder, rows, offset, null_mask, {
                { String::from_utf8_lossy(&reader.read_string().await?).to_string() }
            })
        }
        Type::FixedSizedString(n) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, *n as i32);
            for i in 0..rows {
                let mut buf = vec![0u8; *n];
                let _ = reader.read_exact(&mut buf).await?;
                if !null_mask.is_empty() && null_mask[i] != 0 {
                    builder.append_null();
                } else {
                    builder.append_value(&buf)?;
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        Type::Binary => {
            deserialize_binary!(BinaryBuilder, rows, offset, null_mask, {
                { reader.read_string().await? }
            })
        }
        Type::FixedSizedBinary(n) => {
            let len = *n as i32;
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, len, null_mask, {
                {
                    let mut buf = vec![0u8; *n];
                    let _ = reader.read_exact(&mut buf).await?;
                    buf
                }
            })
        }
        Type::Uuid => {
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, 16, null_mask, {
                {
                    let mut buf = [0u8; 16];
                    let _ = reader.read_exact(&mut buf).await?;
                    buf.to_vec()
                }
            })
        }
        Type::Ipv4 => {
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, 4, null_mask, {
                {
                    let ipv4_int = reader.read_u32_le().await?;
                    let ip_addr = Ipv4Addr::from(ipv4_int);
                    ip_addr.octets()
                }
            })
        }
        Type::Ipv6 => {
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, 16, null_mask, {
                {
                    let mut octets = [0u8; 16];
                    let _ = reader.read_exact(&mut octets[..]).await?;
                    Ipv6Addr::from(octets).octets()
                }
            })
        }
        // Special numeric types that need to be read as bytes
        Type::Int128 => {
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, 16, null_mask, {
                {
                    let mut buf = [0u8; 16];
                    let _ = reader.read_exact(&mut buf).await?;
                    buf
                }
            })
        }
        Type::Int256 => {
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, 32, null_mask, {
                {
                    let mut buf = [0u8; 32];
                    let _ = reader.read_exact(&mut buf).await?;
                    buf.reverse();
                    buf
                }
            })
        }
        Type::UInt128 => {
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, 16, null_mask, {
                {
                    let mut buf = [0u8; 16];
                    let _ = reader.read_exact(&mut buf).await?;
                    buf
                }
            })
        }
        Type::UInt256 => {
            deserialize_binary_fixed!(FixedSizeBinaryBuilder, rows, 32, null_mask, {
                {
                    let mut buf = [0u8; 32];
                    let _ = reader.read_exact(&mut buf).await?;
                    buf.reverse();
                    buf
                }
            })
        }
        _ => Err(ClickhouseNativeError::ArrowDeserialize(format!(
            "Expected binary, got {type_hint:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::net::{Ipv4Addr, Ipv6Addr};

    use arrow::array::*;

    use super::*;
    use crate::formats::DeserializerState;
    use crate::native::types::Type;

    /// Tests deserialization of `String` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_string() {
        let type_hint = Type::String;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Strings: ["hello", "", "world"]
            5, b'h', b'e', b'l', b'l', b'o', // "hello"
            0,    // ""
            5, b'w', b'o', b'r', b'l', b'd', // "world"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize String");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array, &StringArray::from(vec!["hello", "", "world"]));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(String)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_string() {
        let type_hint = Type::Nullable(Box::new(Type::String));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Strings: ["a", "", "c"]
            1, b'a', // "a"
            0,    // "" (null)
            1, b'c', // "c"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(String)");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array, &StringArray::from(vec![Some("a"), None, Some("c")]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `FixedSizedString` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_fixed_sized_string() {
        let type_hint = Type::FixedSizedString(3);
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Strings: ["abc", "de", "fgh"]
            b'a', b'b', b'c', // "abc"
            b'd', b'e', 0, // "de" + padding
            b'f', b'g', b'h', // "fgh"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize FixedSizedString(3)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), b"abc");
        assert_eq!(array.value(1), b"de\0");
        assert_eq!(array.value(2), b"fgh");
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(FixedSizedString)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_fixed_sized_string() {
        let type_hint = Type::Nullable(Box::new(Type::FixedSizedString(3)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Strings: ["a", [0,0,0], "bc"]
            b'a', 0, 0, // "a" + padding
            0, 0, 0, // null (zeroed)
            b'b', b'c', 0, // "bc" + padding
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(FixedSizedString(3))");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), b"a\0\0");
        assert!(!array.is_valid(1));
        assert_eq!(array.value(2), b"bc\0");
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Binary` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_binary() {
        let type_hint = Type::Binary;
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Binary: ["abc", "", "def"]
            3, b'a', b'b', b'c', // "abc"
            0,    // ""
            3, b'd', b'e', b'f', // "def"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Binary");
        let array = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(array.value(0), b"abc");
        assert_eq!(array.value(1), b"");
        assert_eq!(array.value(2), b"def");
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Binary)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_binary() {
        let type_hint = Type::Nullable(Box::new(Type::Binary));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Binary: ["ab", "", "cd"]
            2, b'a', b'b', // "ab"
            0,    // "" (null)
            2, b'c', b'd', // "cd"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Binary)");
        let array = result.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(array.value(0), b"ab");
        assert!(!array.is_valid(1));
        assert_eq!(array.value(2), b"cd");
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `FixedSizedBinary` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_fixed_sized_binary() {
        let type_hint = Type::FixedSizedBinary(3);
        let rows = 3;
        let null_mask = vec![];
        let input = vec![
            // Binary: ["abc", "de", "fgh"]
            b'a', b'b', b'c', // "abc"
            b'd', b'e', 0, // "de" + padding
            b'f', b'g', b'h', // "fgh"
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize FixedSizedBinary(3)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), b"abc");
        assert_eq!(array.value(1), b"de\0");
        assert_eq!(array.value(2), b"fgh");
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(FixedSizedBinary)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_fixed_sized_binary() {
        let type_hint = Type::Nullable(Box::new(Type::FixedSizedBinary(3)));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Binary: ["ab", [0,0,0], "cd"]
            b'a', b'b', 0, // "ab" + padding
            0, 0, 0, // null (zeroed)
            b'c', b'd', 0, // "cd" + padding
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(FixedSizedBinary(3))");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), b"ab\0");
        assert!(!array.is_valid(1));
        assert_eq!(array.value(2), b"cd\0");
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Uuid` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_uuid() {
        let type_hint = Type::Uuid;
        let rows = 2;
        let null_mask = vec![];
        let input = vec![
            // UUIDs: [00010203-0405-0607-0809-0a0b0c0d0e0f, 10111213-1415-1617-1819-1a1b1c1d1e1f]
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Uuid");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(
            array.value(0),
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
        );
        assert_eq!(
            array.value(1),
            b"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"
        );
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Uuid)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_uuid() {
        let type_hint = Type::Nullable(Box::new(Type::Uuid));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // UUIDs: [00010203-0405-0607-0809-0a0b0c0d0e0f, [0;16],
            // 10111213-1415-1617-1819-1a1b1c1d1e1f]
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, // non-null
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // null (zeroed)
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f, // non-null
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Uuid)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(
            array.value(0),
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
        );
        assert!(!array.is_valid(1));
        assert_eq!(
            array.value(2),
            b"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"
        );
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Ipv4` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_ipv4() {
        let type_hint = Type::Ipv4;
        let rows = 2;
        let null_mask = vec![];
        let input = vec![
            // IPv4: [192.168.1.1, 10.0.0.1]
            1, 1, 168, 192, // 192.168.1.1 (little-endian u32)
            1, 0, 0, 10, // 10.0.0.1
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Ipv4");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), Ipv4Addr::new(192, 168, 1, 1).octets());
        assert_eq!(array.value(1), Ipv4Addr::new(10, 0, 0, 1).octets());
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Ipv4)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_ipv4() {
        let type_hint = Type::Nullable(Box::new(Type::Ipv4));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // IPv4: [192.168.1.1, [0;4], 10.0.0.1]
            1, 1, 168, 192, // 192.168.1.1
            0, 0, 0, 0, // null (zeroed)
            1, 0, 0, 10, // 10.0.0.1
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Ipv4)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), Ipv4Addr::new(192, 168, 1, 1).octets());
        assert!(!array.is_valid(1));
        assert_eq!(array.value(2), Ipv4Addr::new(10, 0, 0, 1).octets());
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Ipv6` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_ipv6() {
        let type_hint = Type::Ipv6;
        let rows = 2;
        let null_mask = vec![];
        let input = vec![
            // IPv6: [2001:db8::1, ::1]
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01, // 2001:db8::1
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01, // ::1
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Ipv6");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1).octets());
        assert_eq!(array.value(1), Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1).octets());
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Ipv6)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_ipv6() {
        let type_hint = Type::Nullable(Box::new(Type::Ipv6));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // IPv6: [2001:db8::1, [0;16], ::1]
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01, // 2001:db8::1
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // null (zeroed)
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01, // ::1
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Ipv6)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1).octets());
        assert!(!array.is_valid(1));
        assert_eq!(array.value(2), Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1).octets());
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Int128` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_int128() {
        let type_hint = Type::Int128;
        let rows = 2;
        let null_mask = vec![];
        let input = vec![
            // Int128: [1, 2]
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Int128");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(
            array.value(0),
            b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(
            array.value(1),
            b"\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Int128)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int128() {
        let type_hint = Type::Nullable(Box::new(Type::Int128));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Int128: [1, [0;16], 2]
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 1
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // null
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 2
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int128)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(
            array.value(0),
            b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert!(!array.is_valid(1));
        assert_eq!(
            array.value(2),
            b"\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `Int256` with non-nullable values.
    #[tokio::test]
    async fn test_deserialize_int256() {
        let type_hint = Type::Int256;
        let rows = 2;
        let null_mask = vec![];
        let input = vec![
            // Int256: [1, 2] (little-endian)
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 1, // 1
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 2, // 2
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Int256");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        let mut expected1 = vec![0u8; 31];
        expected1.insert(0, 1); // [1, 0, 0, ..., 0]
        let mut expected2 = vec![0u8; 31];
        expected2.insert(0, 2); // [2, 0, 0, ..., 0]
        assert_eq!(array.value(0), expected1.as_slice());
        assert_eq!(array.value(1), expected2.as_slice());
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `Nullable(Int256)` with null values.
    #[tokio::test]
    async fn test_deserialize_nullable_int256() {
        let type_hint = Type::Nullable(Box::new(Type::Int256));
        let rows = 3;
        let null_mask = vec![0, 1, 0]; // [not null, null, not null]
        let input = vec![
            // Int256: [1, [0;32], 2] (little-endian)
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 1, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 0, 0, 0, 0, 0, //
            0, 0, 0, 0, 2, //
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize Nullable(Int256)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        let mut expected1 = vec![0u8; 31];
        expected1.push(1);
        expected1.reverse();
        let mut expected2 = vec![0u8; 31];
        expected2.push(2);
        expected2.reverse();
        assert_eq!(array.value(0), expected1.as_slice());
        assert!(!array.is_valid(1));
        assert_eq!(array.value(2), expected2.as_slice());
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    /// Tests deserialization of `String` with zero rows.
    #[tokio::test]
    async fn test_deserialize_string_zero_rows() {
        let type_hint = Type::String;
        let rows = 0;
        let null_mask = vec![];
        let input = vec![];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result = deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state)
            .await
            .expect("Failed to deserialize String with zero rows");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.len(), 0);
        assert_eq!(array, &StringArray::from(Vec::<String>::new()));
        assert_eq!(array.nulls(), None);
    }

    /// Tests deserialization of `String` with invalid UTF-8 data.
    #[tokio::test]
    async fn test_deserialize_string_invalid_utf8() {
        let type_hint = Type::String;
        let rows = 1;
        let null_mask = vec![];
        let input = vec![
            // Invalid UTF-8: [0xFF]
            1, 0xFF,
        ];
        let mut reader = Cursor::new(input);
        let mut state = DeserializerState::default();

        let result =
            deserialize(&type_hint, &mut reader, rows, &null_mask, &mut state).await.unwrap();
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.len(), 1);
        assert_eq!(array.value(0), "\u{FFFD}"); // Replacement character for invalid UTF-8
        assert_eq!(array.nulls(), None);
    }
}
