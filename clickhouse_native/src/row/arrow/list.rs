use arrow::array::*;

use crate::io::ClickhouseBytesRead;
use crate::{Error, Result, Type};

#[inline]
fn deserialize_list<R: ClickhouseBytesRead, B: ArrayBuilder>(
    array_builder: &mut dyn ArrayBuilder,
    reader: &mut R,
    type_: &Type,
    nullable: bool,
    st: impl Fn(&mut R, &mut B, &Type, bool) -> Result<()>,
) -> Result<()> {
    let builder = array_builder
        .as_any_mut()
        .downcast_mut::<B>()
        .ok_or_else(|| Error::Protocol("Failed to downcast list builder".into()))?;
    let is_null = nullable
        && reader
            .try_get_u8()
            .map_err(|e| Error::Protocol(format!("Failed to read null flag: {e}")))?
            != 0;
    st(reader, builder, type_, is_null)
}

#[inline]
pub(super) fn deserialize<R: ClickhouseBytesRead>(
    reader: &mut R,
    inner_type: &Type,
    builder: &mut dyn ArrayBuilder,
    nullable: bool,
) -> Result<()> {
    // Helper function to build list array
    fn build_array<R: ClickhouseBytesRead, T: OffsetSizeTrait>(
        reader: &mut R,
        list_builder: &mut GenericListBuilder<T, Box<dyn ArrayBuilder>>,
        hint: &Type,
        is_null: bool,
    ) -> Result<()> {
        if is_null {
            list_builder.append_null();
        } else {
            let array_len = reader.try_get_var_uint()?;
            // Deserialize each element
            for _ in 0..array_len {
                super::dynamic::read_column(reader, hint, list_builder.values(), false)?;
            }
            list_builder.append(true);
        }
        Ok::<(), Error>(())
    }

    // List
    if deserialize_list::<R, ListBuilder<Box<dyn ArrayBuilder>>>(
        builder,
        reader,
        inner_type,
        nullable,
        build_array::<R, i32>,
    )
    .is_ok()
    {
        return Ok(());
    }

    // LargeList
    if deserialize_list::<R, LargeListBuilder<Box<dyn ArrayBuilder>>>(
        builder,
        reader,
        inner_type,
        nullable,
        build_array::<R, i64>,
    )
    .is_ok()
    {
        return Ok(());
    }

    // FixedSizeList
    if deserialize_list::<R, FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(
        builder,
        reader,
        inner_type,
        nullable,
        |reader: &mut R,
         list_builder: &mut FixedSizeListBuilder<Box<dyn ArrayBuilder>>,
         hint: &Type,
         is_null: bool| {
            if is_null {
                list_builder.append(false);
            } else {
                let array_len = reader.try_get_var_uint()?;
                // Deserialize each element
                for _ in 0..array_len {
                    super::dynamic::read_column(reader, hint, list_builder.values(), false)?;
                }
                list_builder.append(true);
            }
            Ok::<(), Error>(())
        },
    )
    .is_ok()
    {
        return Ok(());
    }
    Err(Error::Protocol("Failed to downcast to ListBuilder".to_string()))
}

// Helper function to build list array
fn build_list<R: ClickhouseBytesRead>(
    reader: &mut R,
    builder: &mut Box<dyn ArrayBuilder>,
    hint: &Type,
) -> Result<()> {
    let array_len = reader.try_get_var_uint()?;
    // Deserialize each element
    for _ in 0..array_len {
        super::dynamic::read_column(reader, hint, builder, false)?;
    }
    Ok::<(), Error>(())
}

/// List
#[inline]
pub(super) fn list<R: ClickhouseBytesRead>(
    reader: &mut R,
    type_: &Type,
    builder: &mut GenericListBuilder<i32, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    let Type::Array(inner_type) = type_ else {
        return Err(Error::UnexpectedType(type_.clone()));
    };
    build_list::<R>(reader, builder.values(), inner_type)?;
    builder.append(true);
    Ok(())
}

/// Large list
#[inline]
pub(super) fn list_large<R: ClickhouseBytesRead>(
    reader: &mut R,
    type_: &Type,
    builder: &mut GenericListBuilder<i64, Box<dyn ArrayBuilder>>,
) -> Result<()> {
    let Type::Array(inner_type) = type_ else {
        return Err(Error::UnexpectedType(type_.clone()));
    };
    build_list::<R>(reader, builder.values(), inner_type)?;
    builder.append(true);
    Ok(())
}

/// Fixed size list
#[inline]
pub(super) fn list_fixed<R: ClickhouseBytesRead>(
    reader: &mut R,
    type_: &Type,
    builder: &mut FixedSizeListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    let Type::Array(inner_type) = type_ else {
        return Err(Error::UnexpectedType(type_.clone()));
    };
    build_list::<R>(reader, builder.values(), inner_type)?;
    builder.append(true);
    Ok(())
}
