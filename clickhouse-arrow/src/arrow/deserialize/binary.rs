/// Deserialization logic for `ClickHouse` string and binary types into Arrow arrays.
///
/// This module provides a function to deserialize `ClickHouse`’s native format for string and
/// binary-like types into Arrow arrays, such as `StringArray` for `String`, `BinaryArray` for
/// `Binary`, and `FixedSizeBinaryArray` for fixed-length types like `FixedSizedString`,
/// `Uuid`, `Ipv4`, `Ipv6`, `Int128`, `UInt128`, `Int256`, and `UInt256`.
///
/// The `deserialize` function dispatches to specialized logic based on the `Type` variant,
/// reading variable-length or fixed-length data from the input stream. It respects the
/// `ClickHouse` null mask convention (`1`=null, `0`=non-null) and includes default values for
/// nulls (e.g., empty strings for `Nullable(String)`, zeroed buffers for
/// `Nullable(FixedSizedString)`).
use std::sync::Arc;

use arrow::array::*;
use tokio::io::AsyncReadExt;

use super::ArrowFieldCtx;
use crate::arrow::builder::TypedBuilder;
use crate::io::ClickHouseRead;
use crate::{Error, Result, Type};

macro_rules! binary_async {
    // Infallible aka panic
    (String => $reader:expr) => {
        String::from_utf8_lossy(&$reader.read_string().await?).as_ref()
    };
    (Binary => $reader:expr) => {{ $reader.read_string().await? }};
    // TODO: Perhaps serde_json deserialization should be behind feature flag due to overhead
    (Object => $reader:expr) => {{
        #[cfg(feature = "serde")]
        {
            let byts = $reader.read_string().await?;
            let value = String::from_utf8_lossy(&byts);
            match serde_json::from_str::<serde_json::Value>(value.as_ref()) {
                Ok(val) => val.to_string(),
                Err(_) => value.to_string(),
            }
        }
        #[cfg(not(feature = "serde"))]
        String::from_utf8_lossy(&$reader.read_string().await?).to_string()
    }};
    (FixedBinary($n:expr) => $reader:expr) => {{
        {
            let mut buf = vec![0u8; $n];
            let _ = $reader.read_exact(&mut buf).await?;
            buf
        }
    }};
    (Fixed($n:expr) => $reader:expr) => {{
        {
            let mut buf = [0u8; $n];
            let _ = $reader.read_exact(&mut buf).await?;
            buf
        }
    }};
    (FixedRev($n:expr) => $reader:expr) => {{
        {
            let mut buf = [0u8; $n];
            let _ = $reader.read_exact(&mut buf).await?;
            buf.reverse();
            buf
        }
    }};
    (Ipv4 => $reader:expr) => {{
        {
            let ipv4_int = $reader.read_u32_le().await?;
            let ip_addr = ::std::net::Ipv4Addr::from(ipv4_int);
            ip_addr.octets()
        }
    }};
    (Ipv6 => $reader:expr) => {{
        {
            let mut octets = [0u8; 16];
            let _ = $reader.read_exact(&mut octets[..]).await?;
            std::net::Ipv6Addr::from(octets).octets()
        }
    }};
    // See sync `Uuid` arm — CH wire is two LE u64s in (high, low) order.
    (Uuid => $reader:expr) => {{
        {
            let mut buf = [0u8; 16];
            let _ = $reader.read_exact(&mut buf).await?;
            buf[0..8].reverse();
            buf[8..16].reverse();
            buf
        }
    }};
}

macro_rules! append_default {
    ($builder:expr, $default:expr) => {{
        $builder.append_value($default);
    }};
    (ok => $builder:expr, $default:expr) => {{
        drop($builder.append_value($default));
    }};
}

macro_rules! deserialize_binary_rows {
    ($builder:expr, $reader:expr, $rows:expr, $nulls:expr, $sparse_offsets:expr, $default:expr, $read:expr) => {{
        if let Some(offsets) = $sparse_offsets {
            let mut sparse_idx = 0usize;
            for row in 0..$rows {
                if sparse_idx < offsets.len() && offsets[sparse_idx] == row {
                    super::append_opt!($builder, sparse_idx, $nulls, $read);
                    sparse_idx += 1;
                } else {
                    append_default!($builder, $default);
                }
            }
        } else {
            for i in 0..$rows {
                super::append_opt!($builder, i, $nulls, $read);
            }
        }
    }};
    (ok => $builder:expr, $reader:expr, $rows:expr, $nulls:expr, $sparse_offsets:expr, $default:expr, $read:expr) => {{
        if let Some(offsets) = $sparse_offsets {
            let mut sparse_idx = 0usize;
            for row in 0..$rows {
                if sparse_idx < offsets.len() && offsets[sparse_idx] == row {
                    super::append_opt!(ok => $builder, sparse_idx, $nulls, $read);
                    sparse_idx += 1;
                } else {
                    append_default!(ok => $builder, $default);
                }
            }
        } else {
            for i in 0..$rows {
                super::append_opt!(ok => $builder, i, $nulls, $read);
            }
        }
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
/// A `Result` containing the deserialized `ArrayRef` or a `Error` if
/// deserialization fails.
///
/// # Errors
/// - Returns `Io` if reading from the reader fails (e.g., EOF).
/// - Returns `ArrowDeserialize` if the `type_hint` is unsupported or data is malformed.
///
/// # Example
/// ```rust,ignore
/// use arrow::array::{ArrayRef, StringArray};
/// use clickhouse_arrow::types::{Type, DeserializerState};
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
///
///     let type_ = Type::String;
///     let data_type = DataType::Utf8;
///     let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
///     let array = crate::arrow::deserialize::binary::deserialize_async(
///         &Type::String,
///         &mut builder,
///         &mut reader,
///         3,
///         &[],
///         &mut vec![],
///     )
///     .await
///     .unwrap();
///     let expected = Arc::new(StringArray::from(vec!["hello", "", "world"])) as ArrayRef;
///     assert_eq!(array.as_ref(), expected.as_ref());
/// }
/// ```
pub(crate) async fn deserialize<R: ClickHouseRead>(
    type_hint: &Type,
    builder: &mut TypedBuilder,
    reader: &mut R,
    rows: usize,
    nulls: &[u8],
    ctx: &mut ArrowFieldCtx<'_>,
) -> Result<ArrayRef> {
    type B = TypedBuilder;
    let sparse_offsets = ctx.sparse_offsets();

    // Use pattern matching on the builder to deserialize the appropriate type
    Ok(super::deser!(() => builder => {
    B::String(b) => {{
        deserialize_binary_rows!(b, reader, rows, nulls, sparse_offsets, "", binary_async!(String => reader));
        Arc::new(b.finish())
    }},
    B::Binary(b) => {{
        deserialize_binary_rows!(b, reader, rows, nulls, sparse_offsets, [], binary_async!(Binary => reader));
        Arc::new(b.finish())
    }},
    B::Object(b) => {{
        deserialize_binary_rows!(b, reader, rows, nulls, sparse_offsets, "", binary_async!(Object => reader));
        Arc::new(b.finish())
    }},
    B::FixedSizeBinary(b) => {{
        match type_hint.strip_null() {
            Type::FixedSizedString(n) | Type::FixedSizedBinary(n) => {
                let default = vec![0_u8; *n];
                deserialize_binary_rows!(
                    ok => b,
                    reader,
                    rows,
                    nulls,
                    sparse_offsets,
                    &default,
                    binary_async!(FixedBinary(*n) => reader)
                );
                Arc::new(b.finish())
            },
            Type::Ipv4 => {
                deserialize_binary_rows!(
                    ok => b,
                    reader,
                    rows,
                    nulls,
                    sparse_offsets,
                    [0_u8; 4],
                    binary_async!(Ipv4 => reader)
                );
                Arc::new(b.finish())
            },
            Type::Ipv6 => {
                deserialize_binary_rows!(
                    ok => b,
                    reader,
                    rows,
                    nulls,
                    sparse_offsets,
                    [0_u8; 16],
                    binary_async!(Ipv6 => reader)
                );
                Arc::new(b.finish())
            },
            Type::Uuid => {
                deserialize_binary_rows!(
                    ok => b,
                    reader,
                    rows,
                    nulls,
                    sparse_offsets,
                    [0_u8; 16],
                    binary_async!(Uuid => reader)
                );
                Arc::new(b.finish())
            },
            Type::Int128 | Type::UInt128 => {
                deserialize_binary_rows!(
                    ok => b,
                    reader,
                    rows,
                    nulls,
                    sparse_offsets,
                    [0_u8; 16],
                    binary_async!(Fixed(16) => reader)
                );
                Arc::new(b.finish())
            },
            Type::Int256 | Type::UInt256 => {
                deserialize_binary_rows!(
                    ok => b,
                    reader,
                    rows,
                    nulls,
                    sparse_offsets,
                    [0_u8; 32],
                    binary_async!(FixedRev(32) => reader)
                );
                Arc::new(b.finish())
            },
            _ => return Err(Error::ArrowDeserialize(format!(
                "Unexpected type for FixedSizeBinary builder: {type_hint:?}"
            )))
        }
    }}}
    _ => { return Err(Error::ArrowDeserialize(format!(
        "Unexpected builder type for binary: {type_hint:?}"
    ))) }))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::net::{Ipv4Addr, Ipv6Addr};

    use arrow::array::*;
    use arrow::datatypes::DataType;

    use super::*;
    use crate::native::types::Type;

    fn test_ctx(row_buffer: &mut Vec<u8>) -> ArrowFieldCtx<'_> { ArrowFieldCtx::new(row_buffer) }

    async fn deserialize_for_test(
        type_hint: &Type,
        builder: &mut TypedBuilder,
        reader: &mut Cursor<Vec<u8>>,
        rows: usize,
        nulls: &[u8],
    ) -> Result<ArrayRef> {
        let mut row_buffer = Vec::new();
        let mut ctx = test_ctx(&mut row_buffer);
        deserialize(type_hint, builder, reader, rows, nulls, &mut ctx).await
    }

    async fn deserialize_sparse_for_test(
        type_hint: &Type,
        builder: &mut TypedBuilder,
        reader: &mut Cursor<Vec<u8>>,
        rows: usize,
        nulls: &[u8],
        sparse_offsets: Vec<usize>,
    ) -> Result<ArrayRef> {
        let mut row_buffer = Vec::new();
        let mut ctx = test_ctx(&mut row_buffer).with_sparse_offsets(sparse_offsets);
        deserialize(type_hint, builder, reader, rows, nulls, &mut ctx).await
    }

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

        let type_ = Type::String;
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::String;
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
            .await
            .expect("Failed to deserialize Nullable(String)");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array, &StringArray::from(vec![Some("a"), None, Some("c")]));
        assert_eq!(array.nulls().unwrap().iter().collect::<Vec<bool>>(), vec![true, false, true]);
    }

    #[tokio::test]
    async fn test_deserialize_sparse_string_defaults() {
        let type_hint = Type::String;
        let rows = 4;
        let input = vec![
            1, b'a', // row 0
            2, b'b', b'c', // row 2
        ];
        let mut reader = Cursor::new(input);
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_hint, &data_type).unwrap();
        let result =
            deserialize_sparse_for_test(&type_hint, &mut builder, &mut reader, rows, &[], vec![
                0, 2,
            ])
            .await
            .expect("Failed to deserialize sparse String");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array, &StringArray::from(vec!["a", "", "bc", ""]));
    }

    #[tokio::test]
    async fn test_deserialize_sparse_nullable_string() {
        let type_hint = Type::Nullable(Box::new(Type::String));
        let rows = 4;
        let null_mask = vec![0, 1];
        let input = vec![
            1, b'x', // row 0 non-null
            0,    // row 2 null payload slot
        ];
        let mut reader = Cursor::new(input);
        let builder_type = type_hint.strip_null();
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(builder_type, &data_type).unwrap();
        let result = deserialize_sparse_for_test(
            builder_type,
            &mut builder,
            &mut reader,
            rows,
            &null_mask,
            vec![0, 2],
        )
        .await
        .expect("Failed to deserialize sparse nullable String");
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array, &StringArray::from(vec![Some("x"), Some(""), None, Some("")]));
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

        let type_ = Type::FixedSizedString(3);
        let data_type = DataType::FixedSizeBinary(3);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::FixedSizedString(3);
        let data_type = DataType::FixedSizeBinary(3);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Binary;
        let data_type = DataType::Binary;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Binary;
        let data_type = DataType::Binary;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::FixedSizedBinary(3);
        let data_type = DataType::FixedSizeBinary(3);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::FixedSizedBinary(3);
        let data_type = DataType::FixedSizeBinary(3);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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
        // CH wire form: each UUID is a 128-bit LE value stored as two LE
        // u64s in (high, low) order. The deserializer reverses each 8-byte
        // half so the Arrow FixedSizeBinary(16) ends up in canonical RFC
        // 4122 byte order.
        let input = vec![
            // wire (per 8-byte half, little-endian) -> arrow bytes:
            // 0706050403020100 0f0e0d0c0b0a0908 -> 00010203-0405-0607-0809-0a0b0c0d0e0f
            0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a,
            0x09, 0x08,
            // 1716151413121110 1f1e1d1c1b1a1918 -> 10111213-1415-1617-1819-1a1b1c1d1e1f
            0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x10, 0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a,
            0x19, 0x18,
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Uuid;
        let data_type = DataType::FixedSizeBinary(16);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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
        // Wire bytes are per-8-byte-half little-endian; the deserializer
        // reverses each half so the Arrow value is canonical big-endian.
        let input = vec![
            // wire -> arrow 00010203-0405-0607-0809-0a0b0c0d0e0f
            0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a,
            0x09, 0x08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // null (zeroed)
            // wire -> arrow 10111213-1415-1617-1819-1a1b1c1d1e1f
            0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x10, 0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a,
            0x19, 0x18,
        ];
        let mut reader = Cursor::new(input);

        let type_ = Type::Uuid;
        let data_type = DataType::FixedSizeBinary(16);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Ipv4;
        let data_type = DataType::FixedSizeBinary(4);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Ipv4;
        let data_type = DataType::FixedSizeBinary(4);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Ipv6;
        let data_type = DataType::FixedSizeBinary(16);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
            .await
            .expect("Failed to deserialize Ipv6");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1).octets());
        assert_eq!(array.value(1), Ipv6Addr::LOCALHOST.octets());
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

        let type_ = Type::Ipv6;
        let data_type = DataType::FixedSizeBinary(16);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
            .await
            .expect("Failed to deserialize Nullable(Ipv6)");
        let array = result.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(array.value(0), Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 1).octets());
        assert!(!array.is_valid(1));
        assert_eq!(array.value(2), Ipv6Addr::LOCALHOST.octets());
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

        let type_ = Type::Int128;
        let data_type = DataType::FixedSizeBinary(16);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Int128;
        let data_type = DataType::FixedSizeBinary(16);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Int256;
        let data_type = DataType::FixedSizeBinary(32);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::Int256;
        let data_type = DataType::FixedSizeBinary(32);
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::String;
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
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

        let type_ = Type::String;
        let data_type = DataType::Utf8;
        let mut builder = TypedBuilder::try_new(&type_, &data_type).unwrap();
        let result = deserialize_for_test(&type_hint, &mut builder, &mut reader, rows, &null_mask)
            .await
            .unwrap();
        let array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.len(), 1);
        assert_eq!(array.value(0), "\u{FFFD}"); // Replacement character for invalid UTF-8
        assert_eq!(array.nulls(), None);
    }
}
