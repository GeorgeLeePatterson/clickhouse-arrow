mod binary;
mod enums;
mod list;
mod low_cardinality;
mod map;
mod null;
mod primitive;
mod tuple;

use arrow::array::*;
use arrow::datatypes::*;

use crate::formats::SerializerState;
use crate::geo::normalize_geo_type;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Result, Type};

/// Trait for serializing Arrow arrays into `ClickHouse`'s native protocol.
///
/// Implementations of this trait convert an Arrow array (`ArrayRef`) into the binary format
/// expected by `ClickHouse`'s native protocol, writing the data to an async writer (e.g., a TCP
/// stream). The serialization process respects the `ClickHouse` type system and handles
/// nullability, including nested nullability.
///
/// # Methods
/// - `serialize`: Writes the Arrow array to the writer, using the provided `Field` for metadata and
///   `SerializerState` for stateful serialization.
pub(crate) trait ClickHouseArrowSerializer {
    /// Serializes an Arrow array to `ClickHouse`'s native format.
    ///
    /// # Arguments
    /// - `writer`: The async writer to serialize the data to (e.g., a TCP stream).
    /// - `column`: The Arrow array containing the column data.
    /// - `field`: The Arrow `Field` describing the column.
    /// - `state`: A mutable `SerializerState` for maintaining serialization context.
    ///
    /// # Returns
    /// A `Future` resolving to a `Result` indicating success or a `Error` if
    /// serialization fails.
    ///
    /// # Errors
    /// - Returns `ArrowSerialize` if the Arrow array type is unsupported or incompatible with the
    ///   `ClickHouse` type.
    /// - Returns `Io` if writing to the writer fails.
    async fn serialize_async<W: ClickHouseWrite>(
        &self,
        writer: &mut W,
        column: &ArrayRef,
        data_type: &DataType,
        state: &mut SerializerState,
    ) -> Result<()>;

    fn serialize<W: ClickHouseBytesWrite>(
        &self,
        writer: &mut W,
        column: &ArrayRef,
        data_type: &DataType,
        state: &mut SerializerState,
    ) -> Result<()>;
}

/// Serialize an Arrow [`Field`] to `ClickHouse`’s native format.
///
/// This implementation dispatches serialization to specialized modules based on the `Type` variant:
/// - Nullable types: Writes nullability bitmaps via `null::write_nullability`.
/// - Primitives (e.g., `Int32`, `Float64`, `Date`): Delegates to `primitive::serialize`.
/// - Strings and binaries: Delegates to `string::serialize`.
/// - `LowCardinality`: Delegates to `low_cardinality::serialize`.
/// - Enum8: Delegates to `enums::serialize`.
/// - Arrays: Delegates to `list::serialize`
/// - Maps: Delegates to `map::serialize`.
/// - Tuples: Delegates to `tuple::serialize`..
///
/// # Examples
/// ```rust,ignore
/// use arrow::array::Int32Array;
/// use arrow::datatypes::{DataType, Field};
/// use clickhouse_arrow::types::{Type, SerializerState};
/// use clickhouse_arrow::ClickHouseArrowSerializer;
/// use std::sync::Arc;
/// use tokio::io::AsyncWriteExt;
///
/// let field = Field::new("id", DataType::Int32, false);
/// let column = Arc::new(Int32Array::from(vec![1, 2, 3]));
/// let mut buffer = Cursor::new(Vec::new());
/// let mut state = SerializerState::default();
/// Type::Int32
///     .serialize(&mut buffer, &column, &field, &mut state)
///     .await
///     .unwrap();
/// ```
///
/// # Errors
/// - Returns `ArrowSerialize` for unsupported types (e.g., `Tuple`, `Map`).
/// - Propagates errors from sub-modules (e.g., `Io` for write failures, `ArrowSerialize` for type
///   mismatches).
impl ClickHouseArrowSerializer for Type {
    async fn serialize_async<W: ClickHouseWrite>(
        &self,
        writer: &mut W,
        column: &ArrayRef,
        data_type: &DataType,
        state: &mut SerializerState,
    ) -> Result<()> {
        // TODO: Should this take into account the field? My gut says no since the internal type is
        // intended to encode all of the ClickHouse information. BUT, list serialize, for example,
        // requires it.
        let base_type = self.strip_null();

        if self.is_nullable() {
            null::serialize_nulls_async(self, writer, column, state).await?;
        }

        match base_type {
            // Primitives
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Date
            | Type::Date32
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Uuid => {
                primitive::serialize_async(self, writer, column, data_type).await?;
            }
            // Strings/Binary
            Type::String
            | Type::Binary
            | Type::FixedSizedString(_)
            | Type::FixedSizedBinary(_)
            | Type::Object => {
                binary::serialize_async(self, writer, column).await?;
            }
            // Dictionary-Like
            Type::Enum8(_) | Type::Enum16(_) => {
                enums::serialize_async(self, writer, column).await?;
            }
            // LowCardinality
            Type::LowCardinality(_) => {
                Box::pin(low_cardinality::serialize_async(self, writer, column, data_type, state))
                    .await?;
            }
            // Lists
            Type::Array(_) => {
                Box::pin(list::serialize_async(self, writer, column, data_type, state)).await?;
            }
            // Maps
            Type::Map(_, _) => {
                Box::pin(map::serialize_async(self, writer, column, data_type, state)).await?;
            }
            // Tuples
            Type::Tuple(_) => {
                Box::pin(tuple::serialize_async(self, writer, column, state)).await?;
            }
            Type::Ring | Type::Polygon | Type::Point | Type::MultiPolygon => {
                // Type should be converted earlier, if not this is a fallback
                let normalized = normalize_geo_type(base_type).unwrap();
                Box::pin(normalized.serialize_async(writer, column, data_type, state)).await?;
            }
            // Null stripped above
            Type::Nullable(_) => unreachable!(),
        }

        Ok(())
    }

    fn serialize<W: ClickHouseBytesWrite>(
        &self,
        writer: &mut W,
        column: &ArrayRef,
        data_type: &DataType,
        state: &mut SerializerState,
    ) -> Result<()> {
        // TODO: Should this take into account the field? My gut says no since the internal type is
        // intended to encode all of the ClickHouse information. BUT, list serialize, for example,
        // requires it.
        let base_type = self.strip_null();

        if self.is_nullable() {
            null::serialize_nulls(self, writer, column, state);
        }

        match base_type {
            // Primitives
            Type::Int8
            | Type::Int16
            | Type::Int32
            | Type::Int64
            | Type::Int128
            | Type::Int256
            | Type::UInt8
            | Type::UInt16
            | Type::UInt32
            | Type::UInt64
            | Type::UInt128
            | Type::UInt256
            | Type::Float32
            | Type::Float64
            | Type::Decimal32(_)
            | Type::Decimal64(_)
            | Type::Decimal128(_)
            | Type::Decimal256(_)
            | Type::Date
            | Type::Date32
            | Type::DateTime(_)
            | Type::DateTime64(_, _)
            | Type::Ipv4
            | Type::Ipv6
            | Type::Uuid => {
                primitive::serialize(self, writer, column, data_type)?;
            }
            // Strings/Binary
            Type::String
            | Type::Binary
            | Type::FixedSizedString(_)
            | Type::FixedSizedBinary(_)
            | Type::Object => {
                binary::serialize(self, writer, column)?;
            }
            // Dictionary-Like
            Type::Enum8(_) | Type::Enum16(_) => enums::serialize(self, writer, column)?,
            // LowCardinality
            Type::LowCardinality(_) => {
                low_cardinality::serialize(self, writer, column, data_type, state)?;
            }
            // Lists
            Type::Array(_) => {
                list::serialize(self, writer, column, data_type, state)?;
            }
            // Maps
            Type::Map(_, _) => {
                map::serialize(self, writer, column, data_type, state)?;
            }
            // Tuples
            Type::Tuple(_) => {
                tuple::serialize(self, writer, column, state)?;
            }
            Type::Ring | Type::Polygon | Type::Point | Type::MultiPolygon => {
                // Type should be converted earlier, if not this is a fallback
                let normalized = normalize_geo_type(base_type).unwrap();
                normalized.serialize(writer, column, data_type, state)?;
            }
            // Null stripped above
            Type::Nullable(_) => unreachable!(),
        }

        Ok(())
    }
}

// TODO: Remove - geo unit tests
#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::deserialize::ClickHouseArrowDeserializer;
    use crate::arrow::types::{
        LIST_ITEM_FIELD_NAME, MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME,
    };
    use crate::native::types::Type;

    /// Tests serialization of `Int32` array.
    #[tokio::test]
    async fn test_serialize_int32() {
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::Int32
            .serialize_async(&mut buffer, &column, &DataType::Int32, &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert_eq!(output, vec![
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
        ]);
    }

    /// Tests serialization of `Nullable(Int32)` array with nulls.
    #[tokio::test]
    async fn test_serialize_nullable_int32() {
        let column = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::Nullable(Box::new(Type::Int32))
            .serialize_async(&mut buffer, &column, &DataType::Int32, &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert_eq!(output, vec![
            // Null mask: [0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, // Values: [1, 0, 3]
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
        ]);
    }

    /// Tests serialization of `String` array.
    #[tokio::test]
    async fn test_serialize_string() {
        let column = Arc::new(StringArray::from(vec!["hello", "", "world"])) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::String
            .serialize_async(&mut buffer, &column, &DataType::Utf8, &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert_eq!(output, vec![
            5, b'h', b'e', b'l', b'l', b'o', // "hello"
            0,    // ""
            5, b'w', b'o', b'r', b'l', b'd', // "world"
        ]);
    }

    /// Tests serialization of `Nullable(String)` array with nulls.
    #[tokio::test]
    async fn test_serialize_nullable_string() {
        let column = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::Nullable(Box::new(Type::String))
            .serialize_async(&mut buffer, &column, &DataType::Utf8, &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert_eq!(output, vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: ["a", "", "c"]
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
        ]);
    }

    /// Tests serialization of `Array(Int32)` with non-nullable inner values.
    #[tokio::test]
    async fn test_serialize_array_int32() {
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let column =
            Arc::new(ListArray::new(Arc::clone(&inner_field), offsets, values, None)) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::Array(Box::new(Type::Int32))
            .serialize_async(&mut buffer, &column, &DataType::List(inner_field), &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert_eq!(output, vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ]);
    }

    /// Tests serialization of `Nullable(Array(Int32))` with null arrays.
    #[tokio::test]
    async fn test_serialize_nullable_array_int32() {
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let field = Field::new("col", DataType::List(Arc::clone(&inner_field)), true);
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 5].into());
        let null_buffer = Some(NullBuffer::from(vec![true, false, true]));
        let column =
            Arc::new(ListArray::new(inner_field, offsets, values, null_buffer)) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))))
            .serialize_async(&mut buffer, &column, field.data_type(), &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert_eq!(output, vec![
            // Null mask: [] (0=non-null, 1=null)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (null)
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ]);
    }

    /// Tests serialization of `Map(String, Int32)` with non-nullable key-value pairs.
    #[tokio::test]
    async fn test_serialize_map_string_int32() {
        let key_field = Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false);
        let value_field = Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false);
        let struct_field = Arc::new(Field::new(
            MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![key_field.clone(), value_field.clone()])),
            false,
        ));
        let field = Field::new("col", DataType::Map(Arc::clone(&struct_field), false), false);
        let keys = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let struct_array = StructArray::from(vec![
            (Arc::new(key_field), keys as ArrayRef),
            (Arc::new(value_field), values as ArrayRef),
        ]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let column =
            Arc::new(MapArray::new(struct_field, offsets, struct_array, None, false)) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::Map(Box::new(Type::String), Box::new(Type::Int32))
            .serialize_async(&mut buffer, &column, field.data_type(), &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert_eq!(output, vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys: ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ]);
    }

    /// Tests serialization of `Int32` array with zero rows.
    #[tokio::test]
    async fn test_serialize_int32_zero_rows() {
        let field = Field::new("col", DataType::Int32, false);
        let column = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        Type::Int32
            .serialize_async(&mut buffer, &column, field.data_type(), &mut state)
            .await
            .unwrap();

        let output = buffer.into_inner();
        assert!(output.is_empty()); // No data for zero rows
    }

    #[tokio::test]
    async fn test_serialize_list_zero_rows() {
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let data_type = DataType::List(Arc::clone(&inner_field));
        let values = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let offsets = OffsetBuffer::new(vec![0].into());
        let array = ListArray::new(inner_field, offsets, values, None);
        let column = Arc::new(array) as ArrayRef;

        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();

        let type_ = Type::Array(Type::Int32.into());
        type_.serialize_async(&mut buffer, &column, &data_type, &mut state).await.unwrap();

        let output = buffer.into_inner();
        assert!(output.is_empty()); // No data for zero rows
    }

    #[tokio::test]
    async fn test_serialize_lowcard_zero_rows() {
        let type_ = Type::LowCardinality(Box::new(Type::String));
        let data_type = DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into());

        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(Vec::<i32>::new()),
                Arc::new(StringArray::from(Vec::<&str>::new())),
            )
            .unwrap(),
        ) as ArrayRef;

        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default()
            .with_arrow_options(ArrowOptions::default().with_strings_as_strings(true));
        type_.serialize_async(&mut buffer, &array, &data_type, &mut state).await.unwrap();

        let output = buffer.into_inner();
        assert!(output.is_empty()); // No data for zero rows
    }

    /// Tests serialization of a `Point` array.
    #[tokio::test]
    async fn test_serialize_point() {
        use std::io::Read;

        // Point = Tuple(Float64, Float64)
        let type_ = Type::Point;
        let (data_type, _) = type_.arrow_type(None).unwrap();

        // Create a struct array with 3 points
        let x_values = Float64Array::from(vec![1.0, 2.5, 3.7]);
        let y_values = Float64Array::from(vec![10.0, 20.5, 30.7]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );
        let column = Arc::new(struct_array) as ArrayRef;

        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        type_.serialize_async(&mut buffer, &column, &data_type, &mut state).await.unwrap();

        let output = buffer.into_inner();
        assert_eq!(output.len(), 6 * 8); // 3 points * 2 coordinates * 8 bytes each

        // Verify the serialized data (column-wise: all x values, then all y values)
        let mut cursor = Cursor::new(output);
        let mut bytes = [0u8; 8];

        // X values
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 1.0).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 2.5).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 3.7).abs() < f64::EPSILON);

        // Y values
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 10.0).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 20.5).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 30.7).abs() < f64::EPSILON);
    }

    /// Tests serialization of a `Ring` array.
    #[tokio::test]
    async fn test_serialize_ring() {
        use std::io::Read;

        // Ring = Array(Point) = Array(Tuple(Float64, Float64))
        let type_ = Type::Ring;
        let (data_type, _) = type_.arrow_type(None).unwrap();

        // Create a ring with 4 points
        let x_values = Float64Array::from(vec![0.0, 1.0, 0.5, 0.0]);
        let y_values = Float64Array::from(vec![0.0, 0.0, 1.0, 0.0]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );

        let offsets = OffsetBuffer::new(vec![0, 4].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", struct_array.data_type().clone(), false)),
            offsets,
            Arc::new(struct_array) as ArrayRef,
            None,
        );
        let column = Arc::new(list_array) as ArrayRef;

        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        type_.serialize_async(&mut buffer, &column, &data_type, &mut state).await.unwrap();

        let output = buffer.into_inner();
        assert_eq!(output.len(), 8 + 4 * 2 * 8); // size (8) + 4 points * 2 coords * 8 bytes

        let mut cursor = Cursor::new(output);
        let mut bytes = [0u8; 8];

        // Read size
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 4);
    }

    /// Tests serialization of a `Polygon` array.
    #[tokio::test]
    async fn test_serialize_polygon() {
        use std::io::Read;

        // Polygon = Array(Ring) = Array(Array(Tuple(Float64, Float64)))
        let type_ = Type::Polygon;
        let (data_type, _) = type_.arrow_type(None).unwrap();

        // Create a polygon with one ring of 4 points
        let x_values = Float64Array::from(vec![0.0, 1.0, 0.5, 0.0]);
        let y_values = Float64Array::from(vec![0.0, 0.0, 1.0, 0.0]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );

        // Create ring array
        let ring_offsets = OffsetBuffer::new(vec![0, 4].into());
        let ring_array = ListArray::new(
            Arc::new(Field::new("item", struct_array.data_type().clone(), false)),
            ring_offsets,
            Arc::new(struct_array) as ArrayRef,
            None,
        );

        // Create polygon array
        let polygon_offsets = OffsetBuffer::new(vec![0, 1].into());
        let polygon_array = ListArray::new(
            Arc::new(Field::new("item", ring_array.data_type().clone(), false)),
            polygon_offsets,
            Arc::new(ring_array) as ArrayRef,
            None,
        );
        let column = Arc::new(polygon_array) as ArrayRef;

        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        type_.serialize_async(&mut buffer, &column, &data_type, &mut state).await.unwrap();

        let output = buffer.into_inner();
        assert_eq!(output.len(), 8 + 8 + 4 * 2 * 8); // num_rings + ring_size + points

        let mut cursor = Cursor::new(output);
        let mut bytes = [0u8; 8];

        // Read number of rings
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 1);

        // Read ring size
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 4);
    }

    /// Tests serialization of a `MultiPolygon` array.
    #[tokio::test]
    async fn test_serialize_multipolygon() {
        use std::io::Read;

        // MultiPolygon = Array(Polygon) = Array(Array(Array(Tuple(Float64, Float64))))
        let type_ = Type::MultiPolygon;
        let (data_type, _) = type_.arrow_type(None).unwrap();

        // Create a multipolygon with one polygon containing one ring of 4 points
        let x_values = Float64Array::from(vec![0.0, 1.0, 0.5, 0.0]);
        let y_values = Float64Array::from(vec![0.0, 0.0, 1.0, 0.0]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );

        // Create ring array
        let ring_offsets = OffsetBuffer::new(vec![0, 4].into());
        let ring_array = ListArray::new(
            Arc::new(Field::new("item", struct_array.data_type().clone(), false)),
            ring_offsets,
            Arc::new(struct_array) as ArrayRef,
            None,
        );

        // Create polygon array
        let polygon_offsets = OffsetBuffer::new(vec![0, 1].into());
        let polygon_array = ListArray::new(
            Arc::new(Field::new("item", ring_array.data_type().clone(), false)),
            polygon_offsets,
            Arc::new(ring_array) as ArrayRef,
            None,
        );

        // Create multipolygon array
        let multipolygon_offsets = OffsetBuffer::new(vec![0, 1].into());
        let multipolygon_array = ListArray::new(
            Arc::new(Field::new("item", polygon_array.data_type().clone(), false)),
            multipolygon_offsets,
            Arc::new(polygon_array) as ArrayRef,
            None,
        );
        let column = Arc::new(multipolygon_array) as ArrayRef;

        let mut buffer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        type_.serialize_async(&mut buffer, &column, &data_type, &mut state).await.unwrap();

        let output = buffer.into_inner();
        assert_eq!(output.len(), 8 + 8 + 8 + 4 * 2 * 8); // num_polygons + num_rings + ring_size + points

        let mut cursor = Cursor::new(output);
        let mut bytes = [0u8; 8];

        // Read number of polygons
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 1);

        // Read number of rings
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 1);

        // Read ring size
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 4);
    }
}

#[cfg(test)]
mod tests_sync {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::ArrowOptions;
    use crate::arrow::deserialize::ClickHouseArrowDeserializer;
    use crate::arrow::types::{
        LIST_ITEM_FIELD_NAME, MAP_FIELD_NAME, STRUCT_KEY_FIELD_NAME, STRUCT_VALUE_FIELD_NAME,
    };
    use crate::native::types::Type;

    /// Tests serialization of `Int32` array.
    #[test]
    fn test_serialize_int32() {
        let column = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::Int32.serialize(&mut buffer, &column, &DataType::Int32, &mut state).unwrap();

        assert_eq!(buffer, vec![
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
        ]);
    }

    /// Tests serialization of `Nullable(Int32)` array with nulls.
    #[test]
    fn test_serialize_nullable_int32() {
        let column = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::Nullable(Box::new(Type::Int32))
            .serialize(&mut buffer, &column, &DataType::Int32, &mut state)
            .unwrap();

        assert_eq!(buffer, vec![
            // Null mask: [0, 1, 0] (0=non-null, 1=null)
            0, 1, 0, // Values: [1, 0, 3]
            1, 0, 0, 0, // 1
            0, 0, 0, 0, // null
            3, 0, 0, 0, // 3
        ]);
    }

    /// Tests serialization of `String` array.
    #[test]
    fn test_serialize_string() {
        let column = Arc::new(StringArray::from(vec!["hello", "", "world"])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::String.serialize(&mut buffer, &column, &DataType::Utf8, &mut state).unwrap();

        assert_eq!(buffer, vec![
            5, b'h', b'e', b'l', b'l', b'o', // "hello"
            0,    // ""
            5, b'w', b'o', b'r', b'l', b'd', // "world"
        ]);
    }

    /// Tests serialization of `Nullable(String)` array with nulls.
    #[test]
    fn test_serialize_nullable_string() {
        let column = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::Nullable(Box::new(Type::String))
            .serialize(&mut buffer, &column, &DataType::Utf8, &mut state)
            .unwrap();

        assert_eq!(buffer, vec![
            // Null mask: [0, 1, 0]
            0, 1, 0, // Values: ["a", "", "c"]
            1, b'a', // "a"
            0,    // null (empty string)
            1, b'c', // "c"
        ]);
    }

    /// Tests serialization of `Array(Int32)` with non-nullable inner values.
    #[test]
    fn test_serialize_array_int32() {
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let column =
            Arc::new(ListArray::new(Arc::clone(&inner_field), offsets, values, None)) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::Array(Box::new(Type::Int32))
            .serialize(&mut buffer, &column, &DataType::List(inner_field), &mut state)
            .unwrap();

        assert_eq!(buffer, vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ]);
    }

    /// Tests serialization of `Nullable(Array(Int32))` with null arrays.
    #[test]
    fn test_serialize_nullable_array_int32() {
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let field = Field::new("col", DataType::List(Arc::clone(&inner_field)), true);
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let offsets = OffsetBuffer::new(vec![0, 2, 2, 5].into());
        let null_buffer = Some(NullBuffer::from(vec![true, false, true]));
        let column =
            Arc::new(ListArray::new(inner_field, offsets, values, null_buffer)) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::Nullable(Box::new(Type::Array(Box::new(Type::Int32))))
            .serialize(&mut buffer, &column, field.data_type(), &mut state)
            .unwrap();

        assert_eq!(buffer, vec![
            // Null mask: []
            // Offsets: [2, 2, 5] (skipping first 0, null array repeats offset)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            2, 0, 0, 0, 0, 0, 0, 0, // 2 (null)
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ]);
    }

    /// Tests serialization of `Map(String, Int32)` with non-nullable key-value pairs.
    #[test]
    fn test_serialize_map_string_int32() {
        let key_field = Field::new(STRUCT_KEY_FIELD_NAME, DataType::Utf8, false);
        let value_field = Field::new(STRUCT_VALUE_FIELD_NAME, DataType::Int32, false);
        let struct_field = Arc::new(Field::new(
            MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![key_field.clone(), value_field.clone()])),
            false,
        ));
        let field = Field::new("col", DataType::Map(Arc::clone(&struct_field), false), false);
        let keys = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let struct_array = StructArray::from(vec![
            (Arc::new(key_field), keys as ArrayRef),
            (Arc::new(value_field), values as ArrayRef),
        ]);
        let offsets = OffsetBuffer::new(vec![0, 2, 3, 5].into());
        let column =
            Arc::new(MapArray::new(struct_field, offsets, struct_array, None, false)) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::Map(Box::new(Type::String), Box::new(Type::Int32))
            .serialize(&mut buffer, &column, field.data_type(), &mut state)
            .unwrap();

        assert_eq!(buffer, vec![
            // Offsets: [2, 3, 5] (skipping first 0)
            2, 0, 0, 0, 0, 0, 0, 0, // 2
            3, 0, 0, 0, 0, 0, 0, 0, // 3
            5, 0, 0, 0, 0, 0, 0, 0, // 5
            // Keys: ["a", "b", "c", "d", "e"]
            1, b'a', // "a"
            1, b'b', // "b"
            1, b'c', // "c"
            1, b'd', // "d"
            1, b'e', // "e"
            // Values: [1, 2, 3, 4, 5]
            1, 0, 0, 0, // 1
            2, 0, 0, 0, // 2
            3, 0, 0, 0, // 3
            4, 0, 0, 0, // 4
            5, 0, 0, 0, // 5
        ]);
    }

    /// Tests serialization of `Int32` array with zero rows.
    #[test]
    fn test_serialize_int32_zero_rows() {
        let field = Field::new("col", DataType::Int32, false);
        let column = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        Type::Int32.serialize(&mut buffer, &column, field.data_type(), &mut state).unwrap();

        assert!(buffer.is_empty()); // No data for zero rows
    }

    #[test]
    fn test_serialize_list_zero_rows() {
        let inner_field = Arc::new(Field::new(LIST_ITEM_FIELD_NAME, DataType::Int32, false));
        let data_type = DataType::List(Arc::clone(&inner_field));
        let values = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let offsets = OffsetBuffer::new(vec![0].into());
        let array = ListArray::new(inner_field, offsets, values, None);
        let column = Arc::new(array) as ArrayRef;

        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        let type_ = Type::Array(Type::Int32.into());
        type_.serialize(&mut buffer, &column, &data_type, &mut state).unwrap();

        assert!(buffer.is_empty()); // No data for zero rows
    }

    #[test]
    fn test_serialize_lowcard_zero_rows() {
        let type_ = Type::LowCardinality(Box::new(Type::String));
        let data_type = DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into());

        let array = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(Vec::<i32>::new()),
                Arc::new(StringArray::from(Vec::<&str>::new())),
            )
            .unwrap(),
        ) as ArrayRef;

        let mut buffer = Vec::new();
        let mut state = SerializerState::default()
            .with_arrow_options(ArrowOptions::default().with_strings_as_strings(true));
        type_.serialize(&mut buffer, &array, &data_type, &mut state).unwrap();

        assert!(buffer.is_empty()); // No data for zero rows
    }

    /// Tests serialization of a `Point` array.
    #[test]
    fn test_serialize_point() {
        use std::io::Read;

        // Point = Tuple(Float64, Float64)
        let type_ = Type::Point;
        let (data_type, _) = type_.arrow_type(None).unwrap();

        // Create a struct array with 3 points
        let x_values = Float64Array::from(vec![1.0, 2.5, 3.7]);
        let y_values = Float64Array::from(vec![10.0, 20.5, 30.7]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );
        let column = Arc::new(struct_array) as ArrayRef;

        let mut buffer = Vec::new();
        let mut state = SerializerState::default();
        type_.serialize(&mut buffer, &column, &data_type, &mut state).unwrap();

        assert_eq!(buffer.len(), 6 * 8); // 3 points * 2 coordinates * 8 bytes each

        // Verify the serialized data (column-wise: all x values, then all y values)
        let mut cursor = std::io::Cursor::new(buffer);
        let mut bytes = [0u8; 8];

        // X values
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 1.0).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 2.5).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 3.7).abs() < f64::EPSILON);

        // Y values
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 10.0).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 20.5).abs() < f64::EPSILON);
        cursor.read_exact(&mut bytes).unwrap();
        assert!((f64::from_le_bytes(bytes) - 30.7).abs() < f64::EPSILON);
    }

    /// Tests serialization of a `Ring` array.
    #[test]
    fn test_serialize_ring() {
        use std::io::Read;

        // Ring = Array(Point) = Array(Tuple(Float64, Float64))
        let type_ = Type::Ring;

        // Create a ring with 4 points
        let x_values = Float64Array::from(vec![0.0, 1.0, 0.5, 0.0]);
        let y_values = Float64Array::from(vec![0.0, 0.0, 1.0, 0.0]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );

        let offsets = OffsetBuffer::new(vec![0, 4].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", struct_array.data_type().clone(), false)),
            offsets,
            Arc::new(struct_array) as ArrayRef,
            None,
        );

        let mut buffer = Vec::new();
        let column = Arc::new(list_array) as ArrayRef;
        let (data_type, _) = type_.arrow_type(None).unwrap();
        let mut state = SerializerState::default();
        type_.serialize(&mut buffer, &column, &data_type, &mut state).unwrap();

        assert_eq!(buffer.len(), 8 + 4 * 2 * 8); // size (8) + 4 points * 2 coords * 8 bytes

        let mut cursor = std::io::Cursor::new(buffer);
        let mut bytes = [0u8; 8];

        // Read size
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 4);
    }

    /// Tests serialization of a `Polygon` array.
    #[test]
    fn test_serialize_polygon() {
        use std::io::Read;

        // Polygon = Array(Ring) = Array(Array(Tuple(Float64, Float64)))
        let type_ = Type::Polygon;

        // Create a polygon with one ring of 4 points
        let x_values = Float64Array::from(vec![0.0, 1.0, 0.5, 0.0]);
        let y_values = Float64Array::from(vec![0.0, 0.0, 1.0, 0.0]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );

        // Create ring array
        let ring_offsets = OffsetBuffer::new(vec![0, 4].into());
        let ring_array = ListArray::new(
            Arc::new(Field::new("item", struct_array.data_type().clone(), false)),
            ring_offsets,
            Arc::new(struct_array) as ArrayRef,
            None,
        );

        // Create polygon array
        let polygon_offsets = OffsetBuffer::new(vec![0, 1].into());
        let polygon_array = ListArray::new(
            Arc::new(Field::new("item", ring_array.data_type().clone(), false)),
            polygon_offsets,
            Arc::new(ring_array) as ArrayRef,
            None,
        );

        let mut buffer = Vec::new();
        let column = Arc::new(polygon_array) as ArrayRef;
        let (data_type, _) = type_.arrow_type(None).unwrap();
        let mut state = SerializerState::default();
        type_.serialize(&mut buffer, &column, &data_type, &mut state).unwrap();

        assert_eq!(buffer.len(), 8 + 8 + 4 * 2 * 8); // num_rings + ring_size + points

        let mut cursor = std::io::Cursor::new(buffer);
        let mut bytes = [0u8; 8];

        // Read number of rings
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 1);

        // Read ring size
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 4);
    }

    /// Tests serialization of a `MultiPolygon` array.
    #[test]
    fn test_serialize_multipolygon() {
        use std::io::Read;

        // MultiPolygon = Array(Polygon) = Array(Array(Array(Tuple(Float64, Float64))))
        let type_ = Type::MultiPolygon;

        // Create a multipolygon with one polygon containing one ring of 4 points
        let x_values = Float64Array::from(vec![0.0, 1.0, 0.5, 0.0]);
        let y_values = Float64Array::from(vec![0.0, 0.0, 1.0, 0.0]);
        let fields = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let struct_array = StructArray::new(
            fields,
            vec![Arc::new(x_values) as ArrayRef, Arc::new(y_values) as ArrayRef],
            None,
        );

        // Create ring array
        let ring_offsets = OffsetBuffer::new(vec![0, 4].into());
        let ring_array = ListArray::new(
            Arc::new(Field::new("item", struct_array.data_type().clone(), false)),
            ring_offsets,
            Arc::new(struct_array) as ArrayRef,
            None,
        );

        // Create polygon array
        let polygon_offsets = OffsetBuffer::new(vec![0, 1].into());
        let polygon_array = ListArray::new(
            Arc::new(Field::new("item", ring_array.data_type().clone(), false)),
            polygon_offsets,
            Arc::new(ring_array) as ArrayRef,
            None,
        );

        // Create multipolygon array
        let multipolygon_offsets = OffsetBuffer::new(vec![0, 1].into());
        let multipolygon_array = ListArray::new(
            Arc::new(Field::new("item", polygon_array.data_type().clone(), false)),
            multipolygon_offsets,
            Arc::new(polygon_array) as ArrayRef,
            None,
        );

        let mut buffer = Vec::new();
        let column = Arc::new(multipolygon_array) as ArrayRef;
        let (data_type, _) = type_.arrow_type(None).unwrap();
        let mut state = SerializerState::default();
        type_.serialize(&mut buffer, &column, &data_type, &mut state).unwrap();

        assert_eq!(buffer.len(), 8 + 8 + 8 + 4 * 2 * 8); // num_polygons + num_rings + ring_size + points

        let mut cursor = std::io::Cursor::new(buffer);
        let mut bytes = [0u8; 8];

        // Read number of polygons
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 1);

        // Read number of rings
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 1);

        // Read ring size
        cursor.read_exact(&mut bytes).unwrap();
        assert_eq!(u64::from_le_bytes(bytes), 4);
    }
}
