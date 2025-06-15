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

// TODO: Remove - unit tests
