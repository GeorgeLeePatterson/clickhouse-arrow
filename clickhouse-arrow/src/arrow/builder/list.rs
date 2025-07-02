use super::{typed_arrow_build, *};
use crate::Error;

pub(crate) enum TypedListBuilder {
    List(Box<TypedBuilder>),
    LargeList(Box<TypedBuilder>),
    FixedList((i32, Box<TypedBuilder>)),
}

impl TypedListBuilder {
    pub(crate) fn try_new(type_: &Type, data_type: &DataType) -> Result<Self> {
        // Handle complex nested types
        let type_ = type_.strip_null();
        Ok(typed_arrow_build!(TypedListBuilder, data_type, {
            DataType::List(f) => (
                List,
                Box::new(TypedBuilder::try_new(type_, f.data_type())?)
            ),
            DataType::LargeList(f) => (
                LargeList,
                Box::new(TypedBuilder::try_new(type_, f.data_type())?)
            ),
            DataType::FixedSizeList(f, size) => (
                FixedList,
                (*size, Box::new(TypedBuilder::try_new(type_, f.data_type())?))
            ),
        }))
    }
}

impl std::fmt::Debug for TypedListBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypedListBuilder::List(b) => {
                write!(f, "TypedListBuilder::List({})", (**b).as_ref())
            }
            TypedListBuilder::LargeList(b) => {
                write!(f, "TypedListBuilder::LargeList({})", (**b).as_ref())
            }
            TypedListBuilder::FixedList((size, b)) => {
                write!(f, "TypedListBuilder::FixedList({size}, {})", (**b).as_ref())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_typed_list_builder_list() {
        let inner_field = Arc::new(Field::new("item", DataType::Int32, false));
        let data_type = DataType::List(inner_field);
        let type_ = Type::Int32;

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        assert!(matches!(builder, TypedListBuilder::List(_)));
    }

    #[test]
    fn test_typed_list_builder_large_list() {
        let inner_field = Arc::new(Field::new("item", DataType::Int64, false));
        let data_type = DataType::LargeList(inner_field);
        let type_ = Type::Int64;

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        assert!(matches!(builder, TypedListBuilder::LargeList(_)));
    }

    #[test]
    fn test_typed_list_builder_fixed_size_list() {
        let inner_field = Arc::new(Field::new("item", DataType::Float32, false));
        let data_type = DataType::FixedSizeList(inner_field, 5);
        let type_ = Type::Float32;

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        match builder {
            TypedListBuilder::FixedList((size, _)) => assert_eq!(size, 5),
            _ => panic!("Expected FixedList variant"),
        }
    }

    #[test]
    fn test_typed_list_builder_nullable() {
        let inner_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let data_type = DataType::List(inner_field);
        let type_ = Type::Nullable(Box::new(Type::String));

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        assert!(matches!(builder, TypedListBuilder::List(_)));
    }

    #[test]
    fn test_typed_list_builder_invalid_data_type() {
        let type_ = Type::Int32;
        let data_type = DataType::Int32; // Not a list type

        let result = TypedListBuilder::try_new(&type_, &data_type);
        assert!(result.is_err());
    }

    #[test]
    fn test_typed_list_builder_debug_list() {
        let inner_field = Arc::new(Field::new("item", DataType::Utf8, false));
        let data_type = DataType::List(inner_field);
        let type_ = Type::String;

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        let debug_str = format!("{builder:?}");
        assert!(debug_str.contains("TypedListBuilder::List"));
    }

    #[test]
    fn test_typed_list_builder_debug_large_list() {
        let inner_field = Arc::new(Field::new("item", DataType::Boolean, false));
        let data_type = DataType::LargeList(inner_field);
        let type_ = Type::UInt8; // Boolean maps to UInt8

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        let debug_str = format!("{builder:?}");
        assert!(debug_str.contains("TypedListBuilder::LargeList"));
    }

    #[test]
    fn test_typed_list_builder_debug_fixed_list() {
        let inner_field = Arc::new(Field::new("item", DataType::UInt32, false));
        let data_type = DataType::FixedSizeList(inner_field, 10);
        let type_ = Type::UInt32;

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        let debug_str = format!("{builder:?}");
        assert!(debug_str.contains("TypedListBuilder::FixedList"));
        assert!(debug_str.contains("10"));
    }

    #[test]
    fn test_typed_list_builder_nested_type() {
        let inner_inner_field = Arc::new(Field::new("item", DataType::Int32, false));
        let inner_field = Arc::new(Field::new("inner", DataType::List(inner_inner_field), false));
        let data_type = DataType::List(inner_field);
        let type_ = Type::Array(Box::new(Type::Int32));

        let builder = TypedListBuilder::try_new(&type_, &data_type).unwrap();
        assert!(matches!(builder, TypedListBuilder::List(_)));
    }
}
