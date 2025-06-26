use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

use crate::row::dynamic::builder;
use crate::{Error, Result, Type};

pub(crate) fn create_dyn_map_builder(
    type_: &Type,
    data_type: &DataType,
    name: &str,
) -> Result<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>> {
    let Type::Map(key, value) = type_ else {
        return Err(Error::ArrowDeserialize(format!("Expected Map got {type_:?}")));
    };
    let DataType::Map(map_field, _) = data_type else {
        return Err(Error::ArrowDeserialize(format!("Expected Map got {data_type:?}")));
    };
    let DataType::Struct(inner) = map_field.data_type() else {
        return Err(Error::ArrowDeserialize("Expected key type Struct got".into()));
    };
    let (key_field, value_field) = if inner.len() >= 2 {
        (&inner[0], &inner[1])
    } else {
        return Err(Error::ArrowDeserialize("Map inner fields malformed".into()));
    };
    let key_builder = builder::create_builder(key, key_field.data_type(), name)?;
    let value_builder = builder::create_builder(value, value_field.data_type(), name)?;
    let map_builder = MapBuilder::new(None, key_builder, value_builder)
        .with_keys_field(Arc::clone(key_field))
        .with_values_field(Arc::clone(value_field));
    Ok(map_builder)
}
