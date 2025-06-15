use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

use super::builder::dynamic;
use super::dynamic::read_column;
use crate::io::ClickhouseBytesRead;
use crate::{Error, Result, Type};

pub(super) fn create_map_builder(
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
    let key_builder = dynamic::create_builder(key, key_field.data_type(), name)?;
    let value_builder = dynamic::create_builder(value, value_field.data_type(), name)?;
    let map_builder = MapBuilder::new(None, key_builder, value_builder)
        .with_keys_field(Arc::clone(key_field))
        .with_values_field(Arc::clone(value_field));
    Ok(map_builder)
}

#[inline]
pub(super) fn deserialize<R: ClickhouseBytesRead>(
    reader: &mut R,
    type_: &Type,
    builder: &mut dyn ArrayBuilder,
    nullable: bool,
) -> Result<()> {
    match type_.strip_null() {
        Type::Tuple(inner_types) => {
            let struct_builder =
                builder.as_any_mut().downcast_mut::<StructBuilder>().ok_or_else(|| {
                    Error::ArrowDeserialize("Expected StructBuilder for Tuple".to_string())
                })?;
            if nullable && reader.try_get_u8()? != 0 {
                struct_builder.append(false);
            } else {
                if inner_types.len() != struct_builder.num_fields() {
                    return Err(Error::ArrowDeserialize(format!(
                        "Mismatch in number of tuple fields: expected {}, got {}",
                        inner_types.len(),
                        struct_builder.num_fields()
                    )));
                }
                // Deserialize each tuple element consecutively in order
                let field_builders = struct_builder.field_builders_mut();
                for (i, element_type) in inner_types.iter().enumerate() {
                    read_column(reader, element_type, &mut *field_builders[i], false)?;
                }
                struct_builder.append(true);
            }
        }
        Type::Map(key_type, value_type) => {
            if let Some(map_builder) = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
            {
                if nullable && reader.try_get_u8()? != 0 {
                    map_builder.append(false)?;
                    return Ok(());
                }
                let size = reader.try_get_var_uint()?;
                for _ in 0..size {
                    read_column(reader, key_type, map_builder.keys(), false)?;
                    read_column(reader, value_type, map_builder.values(), false)?;
                }
                map_builder.append(true)?;
                return Ok(());
            }
            return Err(Error::ArrowDeserialize("Could not downcast to MapBuilder".into()));
        }
        // Enums
        Type::Enum8(enum_values) => {
            let dict_builder = builder
                .as_any_mut()
                .downcast_mut::<StringDictionaryBuilder<Int8Type>>()
                .ok_or_else(|| {
                    Error::ArrowDeserialize("Enum8 expects StringDictionaryBuilder<Int8>".into())
                })?;
            if nullable && reader.try_get_u8()? != 0 {
                dict_builder.append_null();
            } else {
                let key = reader.try_get_i8()?;
                let enum_string = enum_values
                    .iter()
                    .find(|(_, k)| key == *k)
                    .map_or("<invalid_enum8_value>", |(name, _)| name.as_str());
                let _ = dict_builder.append(enum_string)?;
            }
        }
        Type::Enum16(enum_values) => {
            let dict_builder = builder
                .as_any_mut()
                .downcast_mut::<StringDictionaryBuilder<Int16Type>>()
                .ok_or_else(|| {
                    Error::ArrowDeserialize("Enum16 expects StringDictionaryBuilder<Int16>".into())
                })?;
            if nullable && reader.try_get_u8()? != 0 {
                dict_builder.append_null();
            } else {
                let key = reader.try_get_i16_le()?;

                // Find the string for this enum value
                let enum_string = enum_values
                    .iter()
                    .find(|(_, k)| key == *k)
                    .map_or("<invalid_enum16_value>", |(name, _)| name.as_str());
                let _ = dict_builder.append(enum_string)?;
            }
        }
        _ => return Err(Error::ArrowDeserialize(format!("Unexpected nested type: {type_:?}"))),
    }
    Ok(())
}
