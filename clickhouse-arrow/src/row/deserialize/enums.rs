pub(crate) mod row {
    use arrow::array::*;
    use arrow::datatypes::{Int8Type, Int16Type};

    use crate::io::ClickHouseBytesRead;
    use crate::{Error, Result, Type};

    pub(crate) fn deserialize_dynamic<R: ClickHouseBytesRead>(
        reader: &mut R,
        type_: &Type,
        builder: &mut dyn ArrayBuilder,
        nullable: bool,
    ) -> Result<()> {
        match type_.strip_null() {
            Type::Enum8(enum_values) => {
                let dict_builder = builder
                    .as_any_mut()
                    .downcast_mut::<StringDictionaryBuilder<Int8Type>>()
                    .ok_or_else(|| {
                        Error::ArrowDeserialize(
                            "Enum8 expects StringDictionaryBuilder<Int8>".into(),
                        )
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
                        Error::ArrowDeserialize(
                            "Enum16 expects StringDictionaryBuilder<Int16>".into(),
                        )
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

        Err(Error::ArrowDeserialize(format!("Unexpected nested type: {type_:?}")))
    }
}
