use super::unexpected_type;
use crate::{Error, FromSql, Result, ToSql, Type, Value};

/// A `Vec` wrapper that is encoded as a tuple in SQL as opposed to a Vec
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VecTuple<T>(pub Vec<T>);

impl<T: ToSql> ToSql for VecTuple<T> {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Tuple(
            self.0
                .into_iter()
                .enumerate()
                .map(|(i, x)| x.to_sql(type_hint.and_then(|x| x.untuple()?.get(i))))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl<T: FromSql> FromSql for VecTuple<T> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        let subtype = match type_ {
            Type::Tuple(x) => &**x,
            x => return Err(unexpected_type(x)),
        };
        let Value::Tuple(values) = value else { return Err(unexpected_type(type_)) };
        if values.len() != subtype.len() {
            return Err(Error::DeserializeError(format!(
                "unexpected type: mismatch tuple length expected {}, got {}",
                subtype.len(),
                values.len()
            )));
        }
        let mut out = Vec::with_capacity(values.len());
        for (type_, value) in subtype.iter().zip(values.into_iter()) {
            out.push(T::from_sql(type_, value)?);
        }
        Ok(VecTuple(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_tuple_creation() {
        let vec_tuple = VecTuple(vec![1, 2, 3]);
        assert_eq!(vec_tuple.0, vec![1, 2, 3]);
    }

    #[test]
    fn test_vec_tuple_default() {
        let vec_tuple: VecTuple<i32> = VecTuple::default();
        assert_eq!(vec_tuple.0, Vec::<i32>::new());
    }

    #[test]
    fn test_vec_tuple_clone_debug() {
        let vec_tuple = VecTuple(vec![1, 2, 3]);
        let cloned = vec_tuple.clone();
        assert_eq!(vec_tuple.0, cloned.0);

        // Test Debug implementation
        let debug_str = format!("{vec_tuple:?}");
        assert!(debug_str.contains("VecTuple"));
        assert!(debug_str.contains('1'));
        assert!(debug_str.contains('2'));
        assert!(debug_str.contains('3'));
    }

    #[test]
    fn test_vec_tuple_to_sql() {
        let vec_tuple = VecTuple(vec![1i32, 2i32, 3i32]);
        let tuple_type = Type::Tuple(vec![Type::Int32, Type::Int32, Type::Int32]);

        let result = vec_tuple.to_sql(Some(&tuple_type)).unwrap();

        match result {
            Value::Tuple(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], Value::Int32(1));
                assert_eq!(values[1], Value::Int32(2));
                assert_eq!(values[2], Value::Int32(3));
            }
            _ => panic!("Expected Value::Tuple"),
        }
    }

    #[test]
    fn test_vec_tuple_to_sql_no_hint() {
        let vec_tuple = VecTuple(vec![1i32, 2i32]);

        let result = vec_tuple.to_sql(None).unwrap();

        match result {
            Value::Tuple(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Value::Int32(1));
                assert_eq!(values[1], Value::Int32(2));
            }
            _ => panic!("Expected Value::Tuple"),
        }
    }

    #[test]
    fn test_vec_tuple_from_sql_success() {
        let tuple_type = Type::Tuple(vec![Type::Int32, Type::Int32, Type::Int32]);
        let tuple_value = Value::Tuple(vec![Value::Int32(10), Value::Int32(20), Value::Int32(30)]);

        let result: VecTuple<i32> = VecTuple::from_sql(&tuple_type, tuple_value).unwrap();
        assert_eq!(result.0, vec![10, 20, 30]);
    }

    #[test]
    fn test_vec_tuple_from_sql_wrong_type() {
        let int_type = Type::Int32;
        let tuple_value = Value::Tuple(vec![Value::Int32(1)]);

        let result: Result<VecTuple<i32>> = VecTuple::from_sql(&int_type, tuple_value);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::DeserializeError(_)));
    }

    #[test]
    fn test_vec_tuple_from_sql_wrong_value_type() {
        let tuple_type = Type::Tuple(vec![Type::Int32]);
        let int_value = Value::Int32(42);

        let result: Result<VecTuple<i32>> = VecTuple::from_sql(&tuple_type, int_value);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::DeserializeError(_)));
    }

    #[test]
    fn test_vec_tuple_length_mismatch() {
        let tuple_type = Type::Tuple(vec![Type::Int32, Type::Int32]); // Expects 2 elements
        let tuple_value = Value::Tuple(vec![Value::Int32(1)]); // Has 1 element

        let result: Result<VecTuple<i32>> = VecTuple::from_sql(&tuple_type, tuple_value);
        assert!(result.is_err());

        // Check the error message
        match result {
            Err(Error::DeserializeError(msg)) => {
                assert!(msg.contains("mismatch tuple length"));
                assert!(msg.contains("expected 2"));
                assert!(msg.contains("got 1"));
            }
            _ => panic!("Expected DeserializeError with specific message"),
        }
    }

    #[test]
    fn test_vec_tuple_empty() {
        let vec_tuple: VecTuple<i32> = VecTuple(vec![]);
        let tuple_type = Type::Tuple(vec![]);

        let result = vec_tuple.to_sql(Some(&tuple_type)).unwrap();
        match result {
            Value::Tuple(values) => {
                assert_eq!(values.len(), 0);
            }
            _ => panic!("Expected Value::Tuple"),
        }
    }

    #[test]
    fn test_vec_tuple_empty_from_sql() {
        let tuple_type = Type::Tuple(vec![]);
        let tuple_value = Value::Tuple(vec![]);

        let result: VecTuple<i32> = VecTuple::from_sql(&tuple_type, tuple_value).unwrap();
        assert_eq!(result.0, Vec::<i32>::new());
    }

    #[test]
    fn test_vec_tuple_mixed_types() {
        // Test with different types in tuple - using String type for testing
        let vec_tuple = VecTuple(vec!["hello".to_string(), "world".to_string()]);
        let tuple_type = Type::Tuple(vec![Type::String, Type::String]);

        let result = vec_tuple.to_sql(Some(&tuple_type)).unwrap();

        match result {
            Value::Tuple(values) => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], Value::String("hello".to_string().into_bytes()));
                assert_eq!(values[1], Value::String("world".to_string().into_bytes()));
            }
            _ => panic!("Expected Value::Tuple"),
        }
    }

    #[test]
    fn test_vec_tuple_string_from_sql() {
        let tuple_type = Type::Tuple(vec![Type::String, Type::String]);
        let tuple_value = Value::Tuple(vec![
            Value::String("test1".to_string().into_bytes()),
            Value::String("test2".to_string().into_bytes()),
        ]);

        let result: VecTuple<String> = VecTuple::from_sql(&tuple_type, tuple_value).unwrap();
        assert_eq!(result.0, vec!["test1".to_string(), "test2".to_string()]);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_vec_tuple_serde() {
        let vec_tuple = VecTuple(vec![1, 2, 3]);

        // Test serialization
        let serialized = serde_json::to_string(&vec_tuple).unwrap();
        assert!(serialized.contains('1'));
        assert!(serialized.contains('2'));
        assert!(serialized.contains('3'));

        // Test deserialization
        let deserialized: VecTuple<i32> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.0, vec![1, 2, 3]);
    }
}
