use super::Type;

/// Normalizes a Map type to its underlying `ClickHouse` representation.
///
/// In `ClickHouse`, a Map(K, V) is internally represented as Array(Tuple(K, V)).
/// This function converts the key and value types into the normalized array format
/// that `ClickHouse` expects for map data.
///
/// # Arguments
/// * `key` - The type of the map keys
/// * `value` - The type of the map values
///
/// # Returns
/// An `Array(Tuple(key, value))` type representing the normalized map structure
pub fn normalize_map_type(key: &Type, value: &Type) -> Type {
    Type::Array(Box::new(Type::Tuple(vec![key.clone(), value.clone()])))
}
