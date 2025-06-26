use super::Type;

// TODO: Remove - Docs
pub fn normalize_map_type(key: &Type, value: &Type) -> Type {
    Type::Array(Box::new(Type::Tuple(vec![key.clone(), value.clone()])))
}
