use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray,
};
use tokio::io::AsyncWriteExt;

use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

#[inline]
fn invalid_input_type(column: &ArrayRef) -> Error {
    Error::ArrowSerialize(format!(
        "AggregateFunction serialization expects Binary/Utf8 Arrow arrays with pre-encoded \
         aggregate states, found {:?}",
        column.data_type()
    ))
}

#[inline]
fn unsupported_null_row() -> Error {
    Error::ArrowSerialize(
        "AggregateFunction serialization does not support null rows; provide encoded state bytes \
         for every row"
            .to_string(),
    )
}

pub(super) async fn serialize_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
) -> Result<()> {
    if !matches!(type_hint.strip_null(), Type::AggregateFunction { .. }) {
        return Err(Error::ArrowSerialize(format!(
            "AggregateFunction serializer called with non-aggregate type: {type_hint}"
        )));
    }

    if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.write_all(values.value(row)).await?;
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.write_all(values.value(row)).await?;
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<BinaryViewArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.write_all(values.value(row)).await?;
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.write_all(values.value(row)).await?;
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.write_all(values.value(row).as_bytes()).await?;
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.write_all(values.value(row).as_bytes()).await?;
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.write_all(values.value(row).as_bytes()).await?;
        }
        return Ok(());
    }

    Err(invalid_input_type(column))
}

pub(super) fn serialize<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    column: &ArrayRef,
) -> Result<()> {
    if !matches!(type_hint.strip_null(), Type::AggregateFunction { .. }) {
        return Err(Error::ArrowSerialize(format!(
            "AggregateFunction serializer called with non-aggregate type: {type_hint}"
        )));
    }

    if let Some(values) = column.as_any().downcast_ref::<BinaryArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.put_slice(values.value(row));
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.put_slice(values.value(row));
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<BinaryViewArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.put_slice(values.value(row));
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.put_slice(values.value(row));
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.put_slice(values.value(row).as_bytes());
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.put_slice(values.value(row).as_bytes());
        }
        return Ok(());
    }
    if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(unsupported_null_row());
            }
            writer.put_slice(values.value(row).as_bytes());
        }
        return Ok(());
    }

    Err(invalid_input_type(column))
}
