use tokio::io::AsyncReadExt;

use super::{Deserializer, DeserializerState, Type};
use crate::io::{ClickHouseBytesRead, ClickHouseRead};
use crate::native::types::low_cardinality::*;
use crate::{Error, Result, Value};

pub(crate) struct LowCardinalityDeserializer;

impl Deserializer for LowCardinalityDeserializer {
    async fn read_prefix<R: ClickHouseRead>(
        _type_: &Type,
        reader: &mut R,
        _state: &mut DeserializerState,
    ) -> Result<()> {
        let version = reader.read_u64_le().await?;
        if version != LOW_CARDINALITY_VERSION {
            return Err(Error::DeserializeError(format!(
                "LowCardinality: invalid low cardinality version: {version}"
            )));
        }
        Ok(())
    }

    #[expect(clippy::too_many_lines)]
    #[expect(clippy::cast_possible_truncation)]
    async fn read<R: ClickHouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        Ok(match type_ {
            Type::LowCardinality(inner) => {
                let mut num_pending_rows = 0usize;
                let mut limit = rows;

                let mut indexed_type = Type::UInt8;
                let mut global_dictionary: Option<Vec<Value>> = None;
                let mut additional_keys: Option<Vec<Value>> = None;

                let mut output = vec![];

                //global dictionary
                let mut needs_global_dictionary = false;
                let mut has_additional_keys = false;
                let mut is_nullable = false;

                while limit > 0 {
                    if num_pending_rows == 0 {
                        let flags = reader.read_u64_le().await?;

                        has_additional_keys = (flags & HAS_ADDITIONAL_KEYS_BIT) != 0;
                        needs_global_dictionary = (flags & NEED_GLOBAL_DICTIONARY_BIT) != 0;
                        let needs_update_dictionary = (flags & NEED_UPDATE_DICTIONARY_BIT) != 0;

                        indexed_type = match flags & 0xff {
                            TUINT8 => Type::UInt8,
                            TUINT16 => Type::UInt16,
                            TUINT32 => Type::UInt32,
                            TUINT64 => Type::UInt64,
                            x => {
                                return Err(Error::DeserializeError(format!(
                                    "LowCardinality: bad index type: {x}"
                                )));
                            }
                        };

                        is_nullable = inner.is_nullable();
                        let inner = inner.strip_null();

                        let interior_needs_update_dictionary =
                            global_dictionary.is_none() || needs_update_dictionary;
                        if needs_global_dictionary && interior_needs_update_dictionary {
                            let index_count = reader.read_u64_le().await?;
                            let new_index = inner
                                .deserialize_column(reader, index_count as usize, state)
                                .await?;
                            global_dictionary = Some(new_index); // should this be append?
                        }

                        if has_additional_keys {
                            let key_count = reader.read_u64_le().await?;
                            additional_keys = Some(
                                inner.deserialize_column(reader, key_count as usize, state).await?,
                            );
                        }

                        num_pending_rows = reader.read_u64_le().await? as usize;
                    }

                    let reading_rows = limit.min(num_pending_rows);

                    let entries =
                        indexed_type.deserialize_column(reader, reading_rows, state).await?;
                    limit -= reading_rows;
                    num_pending_rows -= reading_rows;
                    if has_additional_keys && !needs_global_dictionary {
                        let additional_keys = additional_keys.as_ref().ok_or_else(|| {
                            Error::DeserializeError(
                                "LowCardinality: missing additional keys".to_string(),
                            )
                        })?;
                        for entry in entries {
                            let entry = entry.index_value()?;
                            let value = if is_nullable && entry == 0 {
                                Value::Null
                            } else {
                                additional_keys.get(entry).cloned().ok_or_else(|| {
                                    Error::DeserializeError(format!(
                                        "LowCardinality: illegal index {entry} in additional_keys"
                                    ))
                                })?
                            };
                            output.push(value);
                        }
                    } else if needs_global_dictionary && !has_additional_keys {
                        let global_dictionary = global_dictionary.as_ref().ok_or_else(|| {
                            Error::DeserializeError(
                                "LowCardinality: missing global dictionary".to_string(),
                            )
                        })?;
                        for entry in entries {
                            let entry = entry.index_value()?;
                            output.push(global_dictionary.get(entry).cloned().ok_or_else(
                                || {
                                    Error::DeserializeError(format!(
                                        "LowCardinality: illegal index {entry} in \
                                         global_dictionary"
                                    ))
                                },
                            )?);
                        }
                    } else if needs_global_dictionary && has_additional_keys {
                        let additional_keys = additional_keys.as_ref().ok_or_else(|| {
                            Error::DeserializeError(
                                "LowCardinality: missing additional keys".to_string(),
                            )
                        })?;
                        let global_dictionary = global_dictionary.as_ref().ok_or_else(|| {
                            Error::DeserializeError(
                                "LowCardinality: missing global dictionary".to_string(),
                            )
                        })?;
                        for entry in entries {
                            let entry = entry.index_value()?;
                            let value = if is_nullable && entry == 0 {
                                Value::Null
                            } else if entry < additional_keys.len() {
                                additional_keys.get(entry).cloned().ok_or_else(|| {
                                    Error::DeserializeError(format!(
                                        "LowCardinality: illegal index {entry} in additional_keys",
                                    ))
                                })?
                            } else {
                                global_dictionary
                                    .get(entry - additional_keys.len())
                                    .cloned()
                                    .ok_or_else(|| {
                                        Error::DeserializeError(format!(
                                            "LowCardinality: illegal index {entry} in \
                                             global_dictionary"
                                        ))
                                    })?
                            };
                            output.push(value);
                        }
                    }
                }

                output
            }
            _ => {
                return Err(Error::SerializeError(
                    "LowCardinalityDeserializer: unexpected type".to_string(),
                ));
            }
        })
    }

    fn read_sync(
        _type_: &Type,
        _reader: &mut impl ClickHouseBytesRead,
        _rows: usize,
        _state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        Err(Error::DeserializeError(
            "LowCardinalityDeserializer sync not yet implemented".to_string(),
        ))
    }
}
