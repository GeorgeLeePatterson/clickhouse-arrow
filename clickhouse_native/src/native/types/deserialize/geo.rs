use super::{Deserializer, DeserializerState, Type};
use crate::io::ClickhouseRead;
use crate::native::values;
use crate::{Point, Result, Value};

pub(crate) struct PointDeserializer;

impl Deserializer for PointDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        for _ in 0..2 {
            Type::Float64.deserialize_prefix(reader, state).await?;
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut points = vec![Value::Point(Point::default()); rows];
        for col in 0..2 {
            for (row, value) in
                Type::Float64.deserialize_column(reader, rows, state).await?.into_iter().enumerate()
            {
                let Value::Float64(value) = value else { unreachable!() };
                match &mut points[row] {
                    Value::Point(point) => point.0[col] = value,
                    _ => {
                        unreachable!()
                    }
                }
            }
        }
        Ok(points)
    }
}
macro_rules! array_deser {
    ($name:ident, $item:ty) => {
        paste::paste! {
            pub(crate) struct [<$name Deserializer>];
            impl super::array::ArrayDeserializerGeneric for [<$name Deserializer>] {
                type Item = $crate::native::values::$item;
                fn inner_type(_type_: &Type) -> Result<&Type> {
                    Ok(&Type::$item)
                }
                fn inner_value(items: Vec<Self::Item>) -> Value {
                    Value::$name(values::$name(items))
                }
                fn item_mapping(value: Value) -> Self::Item {
                    let Value::$item(point) = value else {
                        unreachable!()
                    };
                    point
                }
            }
        }
    };
}

array_deser!(Ring, Point);
array_deser!(Polygon, Ring);
array_deser!(MultiPolygon, Polygon);
