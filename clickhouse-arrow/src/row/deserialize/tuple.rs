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
        if let Type::Tuple(inner_types) = type_.strip_null() {
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
            return Ok(());
        }

        Err(Error::ArrowDeserialize(format!("Unexpected nested type: {type_:?}")))
    }
}
