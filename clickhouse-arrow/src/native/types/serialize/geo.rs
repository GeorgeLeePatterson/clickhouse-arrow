use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Result, Value};

pub(crate) struct PointSerializer;

impl Serializer for PointSerializer {
    async fn write_prefix<W: ClickHouseWrite>(
        _type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        for _ in 0..2 {
            Type::Float64.serialize_prefix_async(writer, state).await?;
        }
        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let mut columns = (0..2).map(|_| Vec::with_capacity(values.len())).collect::<Vec<_>>();
        for value in values {
            let Value::Point(point) = value else { unreachable!() };
            for (i, col) in columns.iter_mut().enumerate() {
                col.push(Value::Float64(point.0[i]));
            }
        }
        for column in columns {
            Type::Float64.serialize_column(column, writer, state).await?;
        }
        Ok(())
    }

    fn write_sync(
        _type_: &Type,
        values: Vec<Value>,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) -> Result<()> {
        let mut columns = (0..2).map(|_| Vec::with_capacity(values.len())).collect::<Vec<_>>();
        for value in values {
            let Value::Point(point) = value else { unreachable!() };
            for (i, col) in columns.iter_mut().enumerate() {
                col.push(Value::Float64(point.0[i]));
            }
        }
        for column in columns {
            Type::Float64.serialize_column_sync(column, writer, state)?;
        }
        Ok(())
    }
}

macro_rules! array_ser {
    ($name:ident, $item:ty) => {
        paste::paste! {
            pub(crate) struct [<$name Serializer>];
            impl super::array::ArraySerializerGeneric for [<$name Serializer>] {
                fn inner_type(_type_: &Type) -> Result<&Type> {
                    Ok(&Type::$item)
                }
                fn value_len(value: &Value) -> Result<usize> {
                    match value {
                        Value::$name(array) => Ok(array.0.len()),
                        _ => Err(crate::errors::Error::SerializeError(format!(
                            "Expected Value::{}",
                            stringify!($name)
                        )))
                    }
                }
                fn values(value: Value) -> Result<Vec<Value>> {
                    match value {
                        // The into_iter/collect is annoying, but unavoidable if we want
                        // to give strong types to the user inside the containers rather than
                        // [Value]s.
                        Value::$name(array) => Ok(array.0.into_iter().map(Value::$item).collect()),
                        _ => Err(crate::errors::Error::SerializeError(format!(
                            "Expected Value::{}",
                            stringify!($name)
                        )))
                    }
                }
            }
        }
    };
}

array_ser!(Ring, Point);
array_ser!(Polygon, Ring);
array_ser!(MultiPolygon, Polygon);
