use arrow::datatypes::{DataType, UnionMode};

use super::TypedBuilder;
use crate::{Error, Result, Type};

pub(crate) struct TypedUnionChildBuilder {
    pub(crate) type_id:   i8,
    pub(crate) name:      String,
    pub(crate) data_type: DataType,
    pub(crate) builder:   Option<TypedBuilder>,
}

pub(crate) struct TypedUnionBuilder {
    pub(crate) children: Vec<TypedUnionChildBuilder>,
}

impl TypedUnionBuilder {
    pub(crate) fn try_new(data_type: &DataType) -> Result<Self> {
        let DataType::Union(fields, UnionMode::Dense) = data_type else {
            return Err(Error::ArrowDeserialize(format!(
                "Unexpected datatype for Dynamic: {data_type:?}"
            )));
        };

        let mut children = Vec::with_capacity(fields.len());
        for (type_id, field) in fields.iter() {
            children.push(TypedUnionChildBuilder {
                type_id,
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                builder: None,
            });
        }

        Ok(Self { children })
    }

    pub(crate) fn data_type(&self, idx: usize) -> Result<&DataType> {
        self.children.get(idx).map(|child| &child.data_type).ok_or_else(|| {
            Error::ArrowDeserialize(format!("Dynamic child index out of bounds: {idx}"))
        })
    }

    pub(crate) fn child_parts_mut(
        &mut self,
        idx: usize,
        type_: &Type,
    ) -> Result<(&DataType, &mut TypedBuilder)> {
        let child = self.children.get_mut(idx).ok_or_else(|| {
            Error::ArrowDeserialize(format!("Dynamic child index out of bounds: {idx}"))
        })?;
        let TypedUnionChildBuilder { data_type, builder, .. } = child;
        if builder.is_none() {
            *builder = Some(TypedBuilder::try_new(type_, data_type)?);
        }
        let builder = builder.as_mut().ok_or_else(|| {
            Error::ArrowDeserialize(format!("missing Dynamic child builder at index: {idx}"))
        })?;
        Ok((data_type, builder))
    }
}

impl std::fmt::Debug for TypedUnionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TypedUnionBuilder(children={})", self.children.len())
    }
}
