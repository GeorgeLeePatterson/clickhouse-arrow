use super::{typed_arrow_build, *};
use crate::Error;

pub(crate) enum TypedListBuilder {
    List(Box<TypedBuilder>),
    LargeList(Box<TypedBuilder>),
    FixedList((i32, Box<TypedBuilder>)),
}

impl TypedListBuilder {
    pub(crate) fn try_new(type_: &Type, data_type: &DataType, name: &str) -> Result<Self> {
        // Handle complex nested types
        let type_ = type_.strip_null();
        Ok(typed_arrow_build!(TypedListBuilder, data_type, {
            DataType::List(f) => (
                List,
                Box::new(TypedBuilder::try_new(type_, f.data_type(), name)?)
            ),
            DataType::LargeList(f) => (
                LargeList,
                Box::new(TypedBuilder::try_new(type_, f.data_type(), name)?)
            ),
            DataType::FixedSizeList(f, size) => (
                FixedList,
                (*size, Box::new(TypedBuilder::try_new(type_, f.data_type(), name)?))
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
