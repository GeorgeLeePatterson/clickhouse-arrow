use std::str::FromStr;

use indexmap::IndexMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::block_info::BlockInfo;
use super::protocol::DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
use crate::deserialize::ClickHouseNativeDeserializer;
use crate::formats::protocol_data::ProtocolData;
use crate::formats::{CustomPlan, DeserializerState, SerializerState};
use crate::io::{ClickHouseBytesWrite, ClickHouseRead, ClickHouseWrite};
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

    async fn read<R: ClickHouseRead>(
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

            drop(state.take_custom_plan());

            let has_custom_serialization =
                if revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION {
                    reader.read_u8().await? != 0
                } else {
                    false
                };

            let type_ = Type::from_str(&type_name).inspect_err(|error| {
                error!(?error, "Type deserialize failed: name={name}, type={type_name}");
            })?;

            if has_custom_serialization {
                type_.deserialize_custom_serialization_prefix(reader, state).await?;
            } else {
                drop(state.replace_custom_plan(CustomPlan::from_type_structure(&type_)));
            }

            let mut row_data = if rows > 0 {
                state.reset_custom_node_to_root();
                type_.deserialize_prefix(reader, state).await?;

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
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::native::convert::ColumnDefinition;

    #[derive(Clone)]
    struct TestRow {
        id:   i32,
        name: String,
    }

    impl Row for TestRow {
        const COLUMN_COUNT: Option<usize> = Some(2);

        fn column_names() -> Option<Vec<Cow<'static, str>>> {
            Some(vec![Cow::Borrowed("id"), Cow::Borrowed("name")])
        }

        fn to_schema() -> Option<Vec<ColumnDefinition<Value>>> {
            Some(vec![
                ("id".to_string(), Type::Int32, None),
                ("name".to_string(), Type::String, None),
            ])
        }

        fn deserialize_row(_map: Vec<(&str, &Type, Value)>) -> Result<Self> {
            unreachable!("deserialize_row is not needed in these tests")
        }

        fn serialize_row(
            self,
            _type_hints: &[(String, Type)],
        ) -> Result<Vec<(Cow<'static, str>, Value)>> {
            Ok(vec![
                (Cow::Borrowed("id"), Value::Int32(self.id)),
                (Cow::Borrowed("name"), Value::String(self.name.into_bytes())),
            ])
        }
    }

    #[derive(Clone)]
    struct MissingColumnRow;

    impl Row for MissingColumnRow {
        const COLUMN_COUNT: Option<usize> = Some(1);

        fn column_names() -> Option<Vec<Cow<'static, str>>> { Some(vec![Cow::Borrowed("missing")]) }

        fn to_schema() -> Option<Vec<ColumnDefinition<Value>>> { None }

        fn deserialize_row(_map: Vec<(&str, &Type, Value)>) -> Result<Self> {
            unreachable!("deserialize_row is not needed in these tests")
        }

        fn serialize_row(
            self,
            _type_hints: &[(String, Type)],
        ) -> Result<Vec<(Cow<'static, str>, Value)>> {
            Ok(vec![(Cow::Borrowed("missing"), Value::Int32(1))])
        }
    }

    #[derive(Clone)]
    struct WrongTypeRow;

    impl Row for WrongTypeRow {
        const COLUMN_COUNT: Option<usize> = Some(1);

        fn column_names() -> Option<Vec<Cow<'static, str>>> { Some(vec![Cow::Borrowed("id")]) }

        fn to_schema() -> Option<Vec<ColumnDefinition<Value>>> { None }

        fn deserialize_row(_map: Vec<(&str, &Type, Value)>) -> Result<Self> {
            unreachable!("deserialize_row is not needed in these tests")
        }

        fn serialize_row(
            self,
            _type_hints: &[(String, Type)],
        ) -> Result<Vec<(Cow<'static, str>, Value)>> {
            Ok(vec![(Cow::Borrowed("id"), Value::String(b"oops".to_vec()))])
        }
    }

    fn schema() -> Vec<(String, Type)> {
        vec![("id".to_string(), Type::Int32), ("name".to_string(), Type::String)]
    }

    #[test]
    fn block_from_rows_take_iter_rows_and_estimate_size() {
        let rows = vec![TestRow { id: 1, name: "a".to_string() }, TestRow {
            id:   2,
            name: "b".to_string(),
        }];
        let mut block = Block::from_rows(rows, schema()).unwrap();
        assert_eq!(block.rows, 2);
        assert_eq!(block.column_types.len(), 2);
        assert_eq!(block.column_data.len(), 4);
        assert!(block.estimate_size() > 16);

        let mut iter = block.take_iter_rows();
        let first = iter.next().unwrap();
        assert_eq!(first[0].0, "id");
        assert_eq!(first[0].1, &Type::Int32);
        assert_eq!(first[0].2, Value::Int32(1));
        assert_eq!(first[1].0, "name");
        assert_eq!(first[1].2, Value::String(b"a".to_vec()));

        let second = iter.next().unwrap();
        assert_eq!(second[0].2, Value::Int32(2));
        assert_eq!(second[1].2, Value::String(b"b".to_vec()));
        assert!(iter.next().is_none());
        drop(iter);
        // take_iter_rows drains column_data
        assert!(block.column_data.is_empty());
    }

    #[test]
    fn block_from_rows_reports_missing_column_and_type_validation_errors() {
        let err = Block::from_rows(vec![MissingColumnRow], vec![("id".to_string(), Type::Int32)])
            .unwrap_err();
        assert!(matches!(err, Error::Protocol(msg) if msg.contains("missing type")));

        let err = Block::from_rows(vec![WrongTypeRow], vec![("id".to_string(), Type::Int32)])
            .unwrap_err();
        assert!(matches!(err, Error::TypeParse(msg) if msg.contains("could not assign value")));
    }

    // TODO: Remove
    // #[test]
    // fn block_write_read_sync_roundtrip_and_mismatch_error() {
    //     let block = Block::from_rows(
    //         vec![TestRow { id: 10, name: "x".to_string() }, TestRow {
    //             id:   20,
    //             name: "y".to_string(),
    //         }],
    //         schema(),
    //     )
    //     .unwrap();

    //     let revision = DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;
    //     let mut bytes = BytesMut::new();
    //     <Block as ProtocolData<Block, ()>>::write(block.clone(), &mut bytes, revision, None, ())
    //         .unwrap();

    //     let mut frozen = bytes.freeze();
    //     let mut state = DeserializerState::default();
    //     let decoded =
    //         <Block as ProtocolData<Block, ()>>::read(&mut frozen, revision, (), &mut state)
    //             .unwrap();
    //     assert_eq!(decoded.rows, 2);
    //     assert_eq!(decoded.column_types.len(), 2);
    //     assert_eq!(decoded.column_types[0].0, "id");
    //     assert_eq!(decoded.column_types[1].0, "name");

    //     let mismatch = Block {
    //         info:         BlockInfo::default(),
    //         rows:         2,
    //         column_types: vec![("id".to_string(), Type::Int32)],
    //         column_data:  vec![Value::Int32(1)],
    //     };
    //     let mut mismatch_buf = BytesMut::new();
    //     let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    //         <Block as ProtocolData<Block, ()>>::write(mismatch, &mut mismatch_buf, 0, None, ())
    //     }));
    //     assert!(panic.is_err());
    // }

    // TODO: Remove
    // #[tokio::test]
    // async fn block_write_read_async_roundtrip_and_invalid_type_error() {
    //     let block =
    //         Block::from_rows(vec![TestRow { id: 1, name: "ok".to_string() }], schema()).unwrap();
    //     let revision = DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION;

    //     let (mut tx, mut rx) = tokio::io::duplex(2048);
    //     #[expect(clippy::disallowed_methods)]
    //     let write_task = tokio::spawn(async move {
    //         <Block as ProtocolData<Block, ()>>::write_async(block, &mut tx, revision, None, ())
    //             .await
    //             .unwrap();
    //         tx.shutdown().await.unwrap();
    //     });

    //     let mut state = DeserializerState::default();
    //     let decoded = <Block as ProtocolData<Block, ()>>::read(&mut rx, revision, (), &mut state)
    //         .await
    //         .unwrap();
    //     assert_eq!(decoded.rows, 1);
    //     assert_eq!(decoded.column_types[0].0, "id");
    //     write_task.await.unwrap();

    //     // columns=1, rows=0, name="c", type="DefinitelyUnknown"
    //     let mut invalid = BytesMut::new();
    //     invalid.put_var_uint(1).unwrap();
    //     invalid.put_var_uint(0).unwrap();
    //     invalid.put_string("c").unwrap();
    //     invalid.put_string("DefinitelyUnknown").unwrap();

    //     let mut frozen = invalid.freeze();
    //     let mut state = DeserializerState::default();
    //     let err =
    //         <Block as ProtocolData<Block, ()>>::read(&mut frozen, 0, (), &mut
    // state).unwrap_err();     assert!(matches!(err, Error::TypeParseError(_)));
    // }
}
