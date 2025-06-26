pub(crate) mod row {
    use arrow::array::*;

    use crate::io::ClickHouseBytesRead;
    use crate::row::dynamic::read_column;
    use crate::{Error, Result, Type};

    pub(crate) fn deserialize_dynamic<R: ClickHouseBytesRead>(
        reader: &mut R,
        type_: &Type,
        builder: &mut dyn ArrayBuilder,
        nullable: bool,
    ) -> Result<()> {
        if let Type::Map(key_type, value_type) = type_.strip_null() {
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
        }

        Err(Error::ArrowDeserialize(format!("Unexpected nested type: {type_:?}")))
    }
}
