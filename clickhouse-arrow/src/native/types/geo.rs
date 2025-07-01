//! Helper functions to represent Geo types in terms of other `ClickHouse` types.
//!
//! This enables more direct (de)serialization of Geo types.
use super::Type;
use crate::{Error, Result};

/// Convert a Geo type to a standard `ClickHouse` type.
///
/// # Errors
/// - Returns an error if a non-geo type is provided.
pub fn normalize_geo_type(type_: &Type) -> Result<Type> {
    Ok(match type_ {
        // Geo types are aliases of nested structures, delegate to underlying types
        Type::Point => {
            // Point = Tuple(Float64, Float64)
            Type::Tuple(vec![Type::Float64, Type::Float64])
        }
        Type::Ring => {
            // Ring = Array(Point) = Array(Tuple(Float64, Float64))
            Type::Array(Box::new(Type::Tuple(vec![Type::Float64, Type::Float64])))
        }
        Type::Polygon => {
            // Polygon = Array(Ring) = Array(Array(Tuple(Float64, Float64)))
            Type::Array(Box::new(Type::Array(Box::new(Type::Tuple(vec![
                Type::Float64,
                Type::Float64,
            ])))))
        }
        Type::MultiPolygon => {
            // MultiPolygon = Array(Polygon) = Array(Array(Array(Tuple(Float64, Float64))))
            Type::Array(Box::new(Type::Array(Box::new(Type::Array(Box::new(Type::Tuple(vec![
                Type::Float64,
                Type::Float64,
            ])))))))
        }
        _ => return Err(Error::TypeConversion(format!("Expected Geo type, got {type_}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_point() {
        let result = normalize_geo_type(&Type::Point).unwrap();
        assert_eq!(result, Type::Tuple(vec![Type::Float64, Type::Float64]));
    }

    #[test]
    fn test_normalize_ring() {
        let result = normalize_geo_type(&Type::Ring).unwrap();
        assert_eq!(result, Type::Array(Box::new(Type::Tuple(vec![Type::Float64, Type::Float64]))));
    }

    #[test]
    fn test_normalize_polygon() {
        let result = normalize_geo_type(&Type::Polygon).unwrap();
        assert_eq!(
            result,
            Type::Array(Box::new(Type::Array(Box::new(Type::Tuple(vec![
                Type::Float64,
                Type::Float64
            ])))))
        );
    }

    #[test]
    fn test_normalize_multipolygon() {
        let result = normalize_geo_type(&Type::MultiPolygon).unwrap();
        assert_eq!(
            result,
            Type::Array(Box::new(Type::Array(Box::new(Type::Array(Box::new(Type::Tuple(vec![
                Type::Float64,
                Type::Float64
            ])))))))
        );
    }

    #[test]
    fn test_normalize_non_geo_type_fails() {
        let result = normalize_geo_type(&Type::Int32);
        assert!(result.is_err());
        if let Err(Error::TypeConversion(msg)) = result {
            assert!(msg.contains("Expected Geo type"));
        } else {
            panic!("Expected TypeConversion error");
        }
    }

    #[test]
    fn test_normalize_nullable_geo_type_fails() {
        let result = normalize_geo_type(&Type::Nullable(Box::new(Type::Point)));
        assert!(result.is_err());
        if let Err(Error::TypeConversion(msg)) = result {
            assert!(msg.contains("Expected Geo type"));
        } else {
            panic!("Expected TypeConversion error");
        }
    }
}
