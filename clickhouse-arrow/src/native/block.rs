use std::str::FromStr;

use indexmap::IndexMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::block_info::BlockInfo;
use super::protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
use crate::deserialize::ClickHouseNativeDeserializer;
use crate::formats::protocol_data::ProtocolData;
use crate::formats::{DeserializerState, SerializerState};
use crate::io::{ClickHouseBytesRead, ClickHouseBytesWrite, ClickHouseRead, ClickHouseWrite};
use crate::native::values::Value;
use crate::prelude::*;
use crate::serialize::ClickHouseNativeSerializer;
use crate::{Error, Result, Row, Type};

#[derive(Debug, Clone, Default)]
/// A chunk of data in columnar form.
pub struct Block {
    /// Metadata about the block
    pub info:         BlockInfo,
    /// The number of rows contained in the block
    pub rows:         u64,
    /// The type of each column by name, in order.
    pub column_types: Vec<(String, Type)>,
    /// The data of each column by name, in order. All `Value` should correspond to the associated
    /// type in `column_types`.
    pub column_data:  Vec<Value>,
}

// Iterator type for `take_iter_rows`
pub struct BlockRowValueIter<'a, I>
where
    I: Iterator<Item = Value>,
{
    column_data: Vec<(&'a str, &'a Type, I)>,
}

impl<'a, I> Iterator for BlockRowValueIter<'a, I>
where
    I: Iterator<Item = Value>,
{
    type Item = Vec<(&'a str, &'a Type, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.column_data.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        for (name, type_, pop) in &mut self.column_data {
            out.push((*name, *type_, pop.next()?));
        }
        Some(out)
    }
}

impl Block {
    /// Iterate over all rows with owned values.
    pub fn take_iter_rows(&mut self) -> BlockRowValueIter<'_, impl Iterator<Item = Value>> {
        #[allow(clippy::cast_possible_truncation)]
        let rows = self.rows as usize;
        let mut column_data = std::mem::take(&mut self.column_data);
        let mut out = Vec::with_capacity(rows);
        for (name, type_) in &self.column_types {
            let mut column = Vec::with_capacity(rows);
            let column_slice = column_data.drain(..rows);
            column.extend(column_slice);
            out.push((&**name, type_.strip_low_cardinality(), column.into_iter()));
        }
        BlockRowValueIter { column_data: out }
    }

    /// Estimate the serialized size of this block for buffer allocation
    pub fn estimate_size(&self) -> usize {
        let mut size = 16; // BlockInfo + columns count + rows count

        #[allow(clippy::cast_possible_truncation)]
        let rows = self.rows as usize;

        for (name, type_) in &self.column_types {
            // Column name + type string
            size += name.len() + type_.to_string().len() + 10; // +10 for length prefixes and overhead

            // Estimate data size
            size += rows * type_.estimate_capacity();
        }

        // Add 20% buffer for overhead
        size * 6 / 5
    }

    /// Create a block from a vector of rows and a schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the number of rows does not match the number of columns, serializing
    /// fails, or the field cannot be found in the schema.
    pub fn from_rows<T: Row>(rows: Vec<T>, schema: Vec<(String, Type)>) -> Result<Self> {
        let row_len = rows.len();
        let row_col_len = schema.len() * rows.len();

        let mut columns = schema
            .iter()
            .map(|(name, _)| (name.clone(), Vec::with_capacity(rows.len())))
            .collect::<IndexMap<String, Vec<_>>>();

        rows.into_iter()
            .enumerate()
            .map(|(i, x)| {
                x.serialize_row(&schema)
                    .inspect_err(|error| error!(?error, "serialize error during insert (ROW {i})"))
                    .map(|r| (i, r))
            })
            .try_for_each(|result| -> Result<()> {
                let (i, x) = result?;
                for (key, value) in x {
                    let type_ = &schema
                        .iter()
                        .find(|(n, _)| n == &*key)
                        .ok_or_else(|| {
                            Error::Protocol(format!(
                                "missing type for data in row {i}, column: {key}"
                            ))
                        })?
                        .1;
                    type_.validate_value(&value).inspect_err(|error| {
                        tracing::error!(
                            ?error,
                            ?value,
                            ?key,
                            ?type_,
                            "Value validation failed for row {i}"
                        );
                    })?;
                    let column = columns.get_mut(key.as_ref()).ok_or(Error::Protocol(format!(
                        "missing column for data in row {i}, column: {key}"
                    )))?;
                    column.push(value);
                }
                Ok(())
            })?;

        let mut column_data = Vec::with_capacity(row_col_len);

        // Move the values into a flattened vector
        for (_, mut values) in columns.drain(..) {
            column_data.append(&mut values);
        }

        Ok(Block {
            info: BlockInfo::default(),
            rows: row_len as u64,
            column_types: schema,
            column_data,
        })
    }
}

impl ProtocolData<Self, ()> for Block {
    type Options = ();

    async fn write_async<W: ClickHouseWrite>(
        mut self,
        writer: &mut W,
        revision: u64,
        _header: Option<&[(String, Type)]>,
        _options: (),
    ) -> Result<()> {
        if revision > 0 {
            self.info.write_async(writer).await?;
        }

        let columns = self.column_types.len();

        #[allow(clippy::cast_possible_truncation)]
        let rows = self.rows as usize;

        writer.write_var_uint(columns as u64).await?;
        writer.write_var_uint(self.rows).await?;

        for (name, type_) in self.column_types {
            let mut values = Vec::with_capacity(rows);
            values.extend(self.column_data.drain(..rows));

            if values.len() != rows {
                return Err(Error::Protocol(format!(
                    "row and column length mismatch. {} != {}",
                    values.len(),
                    rows
                )));
            }

            // EncodeStart
            writer.write_string(&name).await?;
            writer.write_string(type_.to_string()).await?;

            if self.rows > 0 {
                if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                    writer.write_u8(0).await?;
                }

                let mut state = SerializerState::default();
                type_.serialize_prefix_async(writer, &mut state).await?;
                type_.serialize_column(values, writer, &mut state).await?;
            }
        }
        Ok(())
    }

    fn write<W: ClickHouseBytesWrite>(
        mut self,
        writer: &mut W,
        revision: u64,
        _header: Option<&[(String, Type)]>,
        _options: (),
    ) -> Result<()> {
        if revision > 0 {
            self.info.write(writer)?;
        }

        let columns = self.column_types.len();

        #[allow(clippy::cast_possible_truncation)]
        let rows = self.rows as usize;

        writer.put_var_uint(columns as u64)?;
        writer.put_var_uint(self.rows)?;

        for (name, type_) in self.column_types {
            let mut values = Vec::with_capacity(rows);
            values.extend(self.column_data.drain(..rows));

            if values.len() != rows {
                return Err(Error::Protocol(format!(
                    "row and column length mismatch. {} != {}",
                    values.len(),
                    rows
                )));
            }

            // EncodeStart
            writer.put_string(&name)?;
            writer.put_string(type_.to_string())?;

            if self.rows > 0 {
                if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                    writer.put_u8(0);
                }

                let mut state = SerializerState::default();
                type_.serialize_prefix(writer, &mut state);
                type_.serialize_column_sync(values, writer, &mut state)?;
            }
        }
        Ok(())
    }

    async fn read_async<R: ClickHouseRead>(
        reader: &mut R,
        revision: u64,
        _options: (),
        state: &mut DeserializerState,
    ) -> Result<Self> {
        let info =
            if revision > 0 { BlockInfo::read_async(reader).await? } else { BlockInfo::default() };

        #[allow(clippy::cast_possible_truncation)]
        let columns = reader.read_var_uint().await? as usize;
        let rows = reader.read_var_uint().await?;

        let mut block = Block {
            info,
            rows,
            column_types: Vec::with_capacity(columns),
            column_data: Vec::with_capacity(columns),
        };

        for i in 0..columns {
            let name = reader
                .read_utf8_string()
                .await
                .inspect_err(|e| error!("reading column name (index {i}): {e}"))?;

            let type_name = reader
                .read_utf8_string()
                .await
                .inspect_err(|e| error!("reading column type (name {name}): {e}"))?;

            // TODO: implement
            let mut _has_custom_serialization = false;
            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                _has_custom_serialization = reader.read_u8().await? != 0;
            }

            let type_ = Type::from_str(&type_name).inspect_err(|error| {
                error!(?error, "Type deserialize failed: name={name}, type={type_name}");
            })?;

            let mut row_data = if rows > 0 {
                type_.deserialize_prefix_async(reader, state).await?;

                #[allow(clippy::cast_possible_truncation)]
                type_
                    .deserialize_column(reader, rows as usize, state)
                    .await
                    .inspect_err(|e| error!("deserialize (name {name}): {e}"))?
            } else {
                vec![]
            };

            block.column_types.push((name, type_));
            block.column_data.append(&mut row_data);
        }

        Ok(block)
    }

    fn read<R: ClickHouseBytesRead + 'static>(
        reader: &mut R,
        revision: u64,
        _options: (),
        state: &mut DeserializerState,
    ) -> Result<Self> {
        let info = if revision > 0 { BlockInfo::read(reader)? } else { BlockInfo::default() };

        #[allow(clippy::cast_possible_truncation)]
        let columns = reader.try_get_var_uint()? as usize;
        let rows = reader.try_get_var_uint()?;

        let mut block = Block {
            info,
            rows,
            column_types: Vec::with_capacity(columns),
            column_data: Vec::with_capacity(columns),
        };

        for i in 0..columns {
            let name = String::from_utf8(
                reader
                    .try_get_string()
                    .inspect_err(|e| error!("reading column name (index {i}): {e}"))?
                    .to_vec(),
            )?;

            let type_name = String::from_utf8(
                reader
                    .try_get_string()
                    .inspect_err(|e| error!("reading column type (name {name}): {e}"))?
                    .to_vec(),
            )?;

            // TODO: implement
            let mut _has_custom_serialization = false;
            if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                _has_custom_serialization = reader.try_get_u8()? != 0;
            }

            let type_ = Type::from_str(&type_name).inspect_err(|error| {
                error!(?error, "Type deserialize failed: name={name}, type={type_name}");
            })?;

            #[allow(clippy::cast_possible_truncation)]
            let mut row_data = if rows > 0 {
                type_.deserialize_prefix(reader)?;
                type_
                    .deserialize_column_sync(reader, rows as usize, state)
                    .inspect_err(|e| error!("deserialize (name {name}): {e}"))?
            } else {
                vec![]
            };

            block.column_types.push((name, type_));
            block.column_data.append(&mut row_data);
        }

        Ok(block)
    }
}
