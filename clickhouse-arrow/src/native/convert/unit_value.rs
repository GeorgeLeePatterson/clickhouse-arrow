use std::borrow::Cow;

use crate::{Error, FromSql, Result, Row, ToSql, Type, Value};

/// A single column row
#[derive(Clone, Debug, Default)]
pub struct UnitValue<T: FromSql + ToSql>(pub T);

impl<T: FromSql + ToSql> Row for UnitValue<T> {
    const COLUMN_COUNT: Option<usize> = Some(1);

    fn column_names() -> Option<Vec<Cow<'static, str>>> { None }

    fn to_schema() -> Option<Vec<(String, Type, Option<Value>)>> { None }

    fn deserialize_row(map: Vec<(&str, &Type, Value)>) -> Result<Self> {
        if map.is_empty() {
            return Err(Error::MissingField("<unit>"));
        }
        let item = map.into_iter().next().unwrap();
        T::from_sql(item.1, item.2).map(UnitValue)
    }

    fn serialize_row(
        self,
        type_hints: &[(String, Type)],
    ) -> Result<Vec<(Cow<'static, str>, Value)>> {
        Ok(vec![(Cow::Borrowed("_"), self.0.to_sql(type_hints.iter().map(|(_, t)| t).next())?)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unit_value_creation() {
        let unit = UnitValue(42i32);
        assert_eq!(unit.0, 42);
    }

    #[test]
    fn test_unit_value_default() {
        let unit: UnitValue<i32> = UnitValue::default();
        assert_eq!(unit.0, 0);
    }

    #[test]
    fn test_unit_value_clone_debug() {
        let unit = UnitValue(123i64);
        let cloned = unit.clone();
        assert_eq!(unit.0, cloned.0);

        // Test Debug implementation
        let debug_str = format!("{unit:?}");
        assert!(debug_str.contains("UnitValue"));
        assert!(debug_str.contains("123"));
    }

    #[test]
    fn test_unit_value_deserialize_success() {
        let map = vec![("col", &Type::Int32, Value::Int32(42))];
        let unit: UnitValue<i32> = UnitValue::deserialize_row(map).unwrap();
        assert_eq!(unit.0, 42);
    }

    #[test]
    fn test_unit_value_deserialize_empty() {
        let map = vec![];
        let result: Result<UnitValue<i32>> = UnitValue::deserialize_row(map);
        assert!(matches!(result, Err(Error::MissingField(_))));
    }

    #[test]
    fn test_unit_value_serialize() {
        let unit = UnitValue(123i32);
        let type_hints = vec![("col".to_string(), Type::Int32)];
        let result = unit.serialize_row(&type_hints).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "_");
        assert_eq!(result[0].1, Value::Int32(123));
    }

    #[test]
    fn test_unit_value_serialize_no_hints() {
        let unit = UnitValue(456i64);
        let result = unit.serialize_row(&[]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "_");
        assert_eq!(result[0].1, Value::Int64(456));
    }

    #[test]
    fn test_unit_value_static_methods() {
        // Test Row trait static methods
        assert_eq!(UnitValue::<i32>::COLUMN_COUNT, Some(1));
        assert_eq!(UnitValue::<i32>::column_names(), None);
        assert_eq!(UnitValue::<i32>::to_schema(), None);
    }

    #[test]
    fn test_unit_value_string() {
        let unit = UnitValue("hello".to_string());
        let type_hints = vec![("col".to_string(), Type::String)];
        let result = unit.serialize_row(&type_hints).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, Value::String("hello".to_string().into_bytes()));
    }

    #[test]
    fn test_unit_value_deserialize_string() {
        let map = vec![("col", &Type::String, Value::String("test".to_string().into_bytes()))];
        let unit: UnitValue<String> = UnitValue::deserialize_row(map).unwrap();
        assert_eq!(unit.0, "test");
    }
}
