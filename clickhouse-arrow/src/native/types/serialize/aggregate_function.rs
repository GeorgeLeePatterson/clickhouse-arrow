use super::{ClickHouseNativeSerializer, Serializer, SerializerState, Type};
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Value};

pub(crate) struct AggregateFunctionSerializer;

impl Serializer for AggregateFunctionSerializer {
    fn write_prefix_sync(
        type_: &Type,
        writer: &mut impl ClickHouseBytesWrite,
        state: &mut SerializerState,
    ) {
        if let Type::SimpleAggregateFunction { types, .. } = type_
            && let Some(inner) = types.first()
        {
            inner.serialize_prefix(writer, state);
        }
    }

    async fn write_prefix<W: ClickHouseWrite>(
        type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        if let Type::SimpleAggregateFunction { types, .. } = type_
            && let Some(inner) = types.first()
        {
            inner.serialize_prefix_async(writer, state).await?;
        }

        Ok(())
    }

    async fn write<W: ClickHouseWrite>(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut W,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("AggregateFunction native value serialization is not implemented"))
    }

    fn write_sync(
        _type_: &Type,
        _values: Vec<Value>,
        _writer: &mut impl ClickHouseBytesWrite,
        _state: &mut SerializerState,
    ) -> Result<()> {
        Err(Error::serialize("AggregateFunction native value serialization is not implemented"))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn simple_agg() -> Type {
        Type::SimpleAggregateFunction {
            name:       "sum".to_string(),
            parameters: vec![],
            types:      vec![Type::UInt64],
        }
    }

    fn agg() -> Type {
        Type::AggregateFunction {
            name:       "sumState".to_string(),
            parameters: vec![],
            types:      vec![Type::UInt64],
            version:    0,
        }
    }

    #[tokio::test]
    async fn write_prefix_async_serializes_simple_aggregate_inner_prefix() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        AggregateFunctionSerializer::write_prefix(&simple_agg(), &mut writer, &mut state)
            .await
            .unwrap();
        assert!(writer.into_inner().is_empty());
    }

    #[tokio::test]
    async fn write_prefix_async_ignores_non_simple_aggregate() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        AggregateFunctionSerializer::write_prefix(&agg(), &mut writer, &mut state).await.unwrap();
        assert!(writer.into_inner().is_empty());
    }

    #[test]
    fn write_prefix_sync_serializes_simple_aggregate_inner_prefix() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        AggregateFunctionSerializer::write_prefix_sync(&simple_agg(), &mut writer, &mut state);
        assert!(writer.is_empty());
    }

    #[test]
    fn write_prefix_sync_ignores_non_simple_aggregate() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        AggregateFunctionSerializer::write_prefix_sync(&agg(), &mut writer, &mut state);
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn write_async_returns_unimplemented_error() {
        let mut writer = Cursor::new(Vec::new());
        let mut state = SerializerState::default();
        let error = AggregateFunctionSerializer::write(&agg(), vec![], &mut writer, &mut state)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }

    #[test]
    fn write_sync_returns_unimplemented_error() {
        let mut writer = Vec::new();
        let mut state = SerializerState::default();
        let error =
            AggregateFunctionSerializer::write_sync(&agg(), vec![], &mut writer, &mut state)
                .unwrap_err();
        assert!(error.to_string().contains("not implemented"));
    }
}
