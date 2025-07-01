use arrow::datatypes::*;

use crate::{Error, Result};

pub(crate) fn get_map_fields(data_type: &DataType) -> Result<(&FieldRef, &FieldRef)> {
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
    Ok((key_field, value_field))
}
