use arrow::datatypes::*;

use crate::{Error, Result};

pub(crate) fn get_map_fields(data_type: &DataType) -> Result<(&FieldRef, &FieldRef)> {
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
    Ok((key_field, value_field))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, TimeUnit};

    use super::*;

    #[test]
    fn test_get_map_fields_valid() {
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, false));
        let inner_fields = vec![Arc::clone(&key_field), Arc::clone(&value_field)];
        let struct_type = DataType::Struct(inner_fields.into());
        let entries_field = Arc::new(Field::new("entries", struct_type, false));
        let map_type = DataType::Map(entries_field, false);

        let result = get_map_fields(&map_type).unwrap();
        assert_eq!(result.0.name(), "key");
        assert_eq!(result.1.name(), "value");
        assert_eq!(result.0.data_type(), &DataType::Utf8);
        assert_eq!(result.1.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_get_map_fields_non_map_type() {
        let data_type = DataType::Int32;
        let result = get_map_fields(&data_type);

        assert!(result.is_err());
        if let Err(Error::ArrowDeserialize(msg)) = result {
            assert!(msg.contains("Expected Map got"));
            assert!(msg.contains("Int32"));
        } else {
            panic!("Expected ArrowDeserialize error");
        }
    }

    #[test]
    fn test_get_map_fields_non_struct_inner() {
        let entries_field = Arc::new(Field::new("entries", DataType::Int32, false));
        let map_type = DataType::Map(entries_field, false);

        let result = get_map_fields(&map_type);
        assert!(result.is_err());
        if let Err(Error::ArrowDeserialize(msg)) = result {
            assert_eq!(msg, "Expected key type Struct got");
        } else {
            panic!("Expected ArrowDeserialize error");
        }
    }

    #[test]
    fn test_get_map_fields_malformed_empty() {
        let inner_fields: Vec<Arc<Field>> = vec![];
        let struct_type = DataType::Struct(inner_fields.into());
        let entries_field = Arc::new(Field::new("entries", struct_type, false));
        let map_type = DataType::Map(entries_field, false);

        let result = get_map_fields(&map_type);
        assert!(result.is_err());
        if let Err(Error::ArrowDeserialize(msg)) = result {
            assert_eq!(msg, "Map inner fields malformed");
        } else {
            panic!("Expected ArrowDeserialize error");
        }
    }

    #[test]
    fn test_get_map_fields_malformed_single_field() {
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let inner_fields = vec![key_field];
        let struct_type = DataType::Struct(inner_fields.into());
        let entries_field = Arc::new(Field::new("entries", struct_type, false));
        let map_type = DataType::Map(entries_field, false);

        let result = get_map_fields(&map_type);
        assert!(result.is_err());
        if let Err(Error::ArrowDeserialize(msg)) = result {
            assert_eq!(msg, "Map inner fields malformed");
        } else {
            panic!("Expected ArrowDeserialize error");
        }
    }

    #[test]
    fn test_get_map_fields_nullable_fields() {
        let key_field = Arc::new(Field::new("key", DataType::Utf8, true));
        let value_field = Arc::new(Field::new("value", DataType::Int64, true));
        let inner_fields = vec![Arc::clone(&key_field), Arc::clone(&value_field)];
        let struct_type = DataType::Struct(inner_fields.into());
        let entries_field = Arc::new(Field::new("entries", struct_type, false));
        let map_type = DataType::Map(entries_field, false);

        let result = get_map_fields(&map_type).unwrap();
        assert_eq!(result.0.name(), "key");
        assert_eq!(result.1.name(), "value");
        assert!(result.0.is_nullable());
        assert!(result.1.is_nullable());
    }

    #[test]
    fn test_get_map_fields_extra_fields() {
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Float64, false));
        let extra_field = Arc::new(Field::new("extra", DataType::Boolean, false));
        let inner_fields = vec![Arc::clone(&key_field), Arc::clone(&value_field), extra_field];
        let struct_type = DataType::Struct(inner_fields.into());
        let entries_field = Arc::new(Field::new("entries", struct_type, false));
        let map_type = DataType::Map(entries_field, false);

        let result = get_map_fields(&map_type).unwrap();
        assert_eq!(result.0.name(), "key");
        assert_eq!(result.1.name(), "value");
        assert_eq!(result.0.data_type(), &DataType::Utf8);
        assert_eq!(result.1.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_get_map_fields_different_types() {
        let test_cases = vec![
            (DataType::Int32, DataType::Utf8),
            (DataType::UInt64, DataType::Binary),
            (DataType::Float32, DataType::Boolean),
            (DataType::Date32, DataType::Time32(TimeUnit::Second)),
        ];

        for (key_type, value_type) in test_cases {
            let key_field = Arc::new(Field::new("key", key_type.clone(), false));
            let value_field = Arc::new(Field::new("value", value_type.clone(), false));
            let inner_fields = vec![key_field, value_field];
            let struct_type = DataType::Struct(inner_fields.into());
            let entries_field = Arc::new(Field::new("entries", struct_type, false));
            let map_type = DataType::Map(entries_field, false);

            let result = get_map_fields(&map_type).unwrap();
            assert_eq!(result.0.data_type(), &key_type);
            assert_eq!(result.1.data_type(), &value_type);
        }
    }
}
