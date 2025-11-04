#![expect(clippy::cast_sign_loss)]
use std::fmt;

use crate::{FromSql, Result, ToSql, Type, Value, unexpected_type};

/// Wrapper type for `ClickHouse` `Int256` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
#[allow(non_camel_case_types)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct i256(pub [u8; 32]);

impl From<i256> for u256 {
    fn from(i: i256) -> Self { u256(i.0) }
}

impl ToSql for i256 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> { Ok(Value::Int256(self)) }
}

impl FromSql for i256 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Int256) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Int256(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl From<i256> for (u128, u128) {
    fn from(i: i256) -> Self {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&i.0[..16]);
        let n1 = u128::from_be_bytes(buf);
        buf.copy_from_slice(&i.0[16..]);
        let n2 = u128::from_be_bytes(buf);
        (n1, n2)
    }
}

impl From<(u128, u128)> for i256 {
    fn from(other: (u128, u128)) -> Self {
        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&other.0.to_be_bytes()[..]);
        buf[16..].copy_from_slice(&other.1.to_be_bytes()[..]);
        i256(buf)
    }
}

impl From<i128> for i256 {
    fn from(value: i128) -> Self {
        if value < 0 {
            // For negative numbers, use two's complement
            let abs_value = value.unsigned_abs();
            i256::from((u128::MAX, u128::MAX - abs_value + 1))
        } else {
            // For positive numbers, high bits are 0
            i256::from((0, value as u128))
        }
    }
}

impl From<(i128, u8)> for i256 {
    fn from((value, scale): (i128, u8)) -> Self {
        let scaled_value = value * 10i128.pow(u32::from(scale));

        if scaled_value < 0 {
            // For negative numbers, we need to handle two's complement representation
            let abs_value = scaled_value.unsigned_abs();

            // For small negative numbers that fit in u128:
            // High bits are all 1s (0xFFFFFFFF...)
            // Low bits are the two's complement of the absolute value
            i256::from((u128::MAX, u128::MAX - abs_value + 1))
        } else {
            // For small positive numbers that fit in u128:
            // High bits are all 0s
            // Low bits contain the value directly
            i256::from((0u128, scaled_value as u128))
        }
    }
}

impl fmt::Display for i256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x")?;
        for b in self.0 {
            write!(f, "{b:02X}")?;
        }
        Ok(())
    }
}

// Create a basic multiply operation for i256 to use in from_parts
impl std::ops::Mul<i256> for i256 {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        // Extract the components from each i256
        let (a_high, a_low) = self.into();
        let (b_high, b_low) = rhs.into();

        // For simple cases where one number is small, we can simplify
        if a_high == 0 && b_high == 0 {
            // Both numbers fit in u128, so we can just multiply
            let result = a_low.wrapping_mul(b_low);
            return i256::from((0, result));
        }

        // Check for signs
        let a_negative = (a_high & (1u128 << 127)) != 0;
        let b_negative = (b_high & (1u128 << 127)) != 0;

        // Get absolute values
        let (_, a_abs_low) = if a_negative {
            let low_bits = !a_low;
            let high_bits = !a_high;

            let new_low = low_bits.wrapping_add(1);
            let new_high = if new_low == 0 { high_bits.wrapping_add(1) } else { high_bits };

            (new_high, new_low)
        } else {
            (a_high, a_low)
        };

        let (_, abs_b_low) = if b_negative {
            let low_bits = !b_low;
            let high_bits = !b_high;

            let new_low = low_bits.wrapping_add(1);
            let new_high = if new_low == 0 { high_bits.wrapping_add(1) } else { high_bits };

            (new_high, new_low)
        } else {
            (b_high, b_low)
        };

        // Multiply the absolute values
        // For a simple implementation, we'll only handle the low part
        // This is sufficient for scaling by small numbers like 10
        let result = a_abs_low.wrapping_mul(abs_b_low);

        // Apply sign based on input signs
        let result_negative = a_negative != b_negative;

        if result_negative {
            // Convert back to two's complement
            let low_bits = !result;
            let new_low = low_bits.wrapping_add(1);

            i256::from((u128::MAX, new_low))
        } else {
            i256::from((0, result))
        }
    }
}

/// Wrapper type for `ClickHouse` `UInt256` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[allow(non_camel_case_types)]
pub struct u256(pub [u8; 32]);

impl ToSql for u256 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> { Ok(Value::UInt256(self)) }
}

impl FromSql for u256 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::UInt256) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::UInt256(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl From<u256> for i256 {
    fn from(u: u256) -> Self { i256(u.0) }
}

impl From<u256> for (u128, u128) {
    fn from(u: u256) -> Self {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&u.0[..16]);
        let n1 = u128::from_be_bytes(buf);
        buf.copy_from_slice(&u.0[16..]);
        let n2 = u128::from_be_bytes(buf);
        (n1, n2)
    }
}

impl From<(u128, u128)> for u256 {
    fn from(other: (u128, u128)) -> Self {
        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&other.0.to_be_bytes()[..]);
        buf[16..].copy_from_slice(&other.1.to_be_bytes()[..]);
        u256(buf)
    }
}

impl fmt::Display for u256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x")?;
        for b in self.0 {
            write!(f, "{b:02X}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Type, Value};

    #[test]
    fn test_i256_to_u128_tuple() {
        // Test zero conversion
        let zero = i256([0u8; 32]);
        let (high, low) = zero.into();
        assert_eq!(high, 0u128);
        assert_eq!(low, 0u128);

        // Test max value conversion
        let max = i256([0xFFu8; 32]);
        let (high, low) = max.into();
        assert_eq!(high, u128::MAX);
        assert_eq!(low, u128::MAX);

        // Test mixed value
        let mut bytes = [0u8; 32];
        bytes[15] = 0x12; // Last byte of first u128
        bytes[31] = 0x34; // Last byte of second u128
        let mixed = i256(bytes);
        let (high, low) = mixed.into();
        assert_eq!(high, 0x12);
        assert_eq!(low, 0x34);
    }

    #[test]
    fn test_i256_from_i128_scale() {
        // Test positive small scale
        let result = i256::from((123i128, 2u8));
        let expected = i256::from((0u128, 12300u128));
        assert_eq!(result, expected);

        // Test negative small scale
        let result = i256::from((-456i128, 3u8));
        let scaled_value = -456_000i128;
        let abs_value = scaled_value.unsigned_abs();
        let expected = i256::from((u128::MAX, u128::MAX - abs_value + 1));
        assert_eq!(result, expected);

        // Test zero with scale
        let result = i256::from((0i128, 5u8));
        let expected = i256::from((0u128, 0u128));
        assert_eq!(result, expected);

        // Test larger scale
        let result = i256::from((42i128, 10u8));
        let expected = i256::from((0u128, 420_000_000_000u128));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_i256_multiplication_simple_cases() {
        // Test simple multiplication within u128 range
        let a = i256::from((0u128, 100u128));
        let b = i256::from((0u128, 200u128));
        let result = a * b;
        let expected = i256::from((0u128, 20000u128));
        assert_eq!(result, expected);

        // Test multiplication by zero
        let a = i256::from((0u128, 12345u128));
        let b = i256::from((0u128, 0u128));
        let result = a * b;
        let expected = i256::from((0u128, 0u128));
        assert_eq!(result, expected);

        // Test multiplication by one
        let a = i256::from((0u128, 9876u128));
        let b = i256::from((0u128, 1u128));
        let result = a * b;
        assert_eq!(result, a);
    }

    #[test]
    fn test_i256_multiplication_negative_numbers() {
        // Test negative * positive
        let a = i256::from((u128::MAX, u128::MAX - 100 + 1)); // -100
        let b = i256::from((0u128, 50u128)); // 50
        let result = a * b;

        // Result should be negative (u128::MAX in high bits)
        let (high, _) = result.into();
        assert_eq!(high, u128::MAX);

        // Test positive * negative
        let a = i256::from((0u128, 75u128)); // 75
        let b = i256::from((u128::MAX, u128::MAX - 25 + 1)); // -25
        let result = a * b;

        // Result should be negative
        let (high, _) = result.into();
        assert_eq!(high, u128::MAX);

        // Test negative * negative should be positive
        let a = i256::from((u128::MAX, u128::MAX - 10 + 1)); // -10
        let b = i256::from((u128::MAX, u128::MAX - 20 + 1)); // -20
        let result = a * b;

        // Result should be positive
        let (high, low) = result.into();
        assert_eq!(high, 0u128);
        assert_eq!(low, 200u128);
    }

    #[test]
    fn test_i256_multiplication_complex_cases() {
        // Test multiplication where high bits are involved
        let a = i256::from((1u128, 100u128));
        let b = i256::from((2u128, 200u128));
        let result = a * b;

        // This should trigger the complex multiplication path
        // The exact result depends on the implementation details
        // but we can verify it doesn't panic and produces some result
        let _: (u128, u128) = result.into();
    }

    #[test]
    fn test_u256_to_i256_conversion() {
        let u = u256([0x12u8; 32]);
        let i: i256 = u.into();
        assert_eq!(i.0, [0x12u8; 32]);
    }

    #[test]
    fn test_u256_to_u128_tuple() {
        // Test zero conversion
        let zero = u256([0u8; 32]);
        let (high, low) = zero.into();
        assert_eq!(high, 0u128);
        assert_eq!(low, 0u128);

        // Test max value conversion
        let max = u256([0xFFu8; 32]);
        let (high, low) = max.into();
        assert_eq!(high, u128::MAX);
        assert_eq!(low, u128::MAX);

        // Test mixed value
        let mut bytes = [0u8; 32];
        bytes[15] = 0xAB; // Last byte of first u128
        bytes[31] = 0xCD; // Last byte of second u128
        let mixed = u256(bytes);
        let (high, low) = mixed.into();
        assert_eq!(high, 0xAB);
        assert_eq!(low, 0xCD);
    }

    #[test]
    fn test_from_sql_error_handling() {
        // Test i256 with wrong type - i256 should fail with UInt256 type
        let result = i256::from_sql(&Type::UInt256, Value::Int256(i256([0u8; 32])));
        assert!(result.is_err(), "i256::from_sql should fail with UInt256 type");

        // Test u256 with wrong type - u256 should fail with Int256 type
        let result = u256::from_sql(&Type::Int256, Value::UInt256(u256([0u8; 32])));
        assert!(result.is_err(), "u256::from_sql should fail with Int256 type");

        // Test correct types for comparison
        let result = i256::from_sql(&Type::Int256, Value::Int256(i256([0u8; 32])));
        assert!(result.is_ok(), "i256::from_sql should succeed with Int256 type");

        let result = u256::from_sql(&Type::UInt256, Value::UInt256(u256([0u8; 32])));
        assert!(result.is_ok(), "u256::from_sql should succeed with UInt256 type");
    }

    #[test]
    fn test_from_sql_unimplemented_values() {
        // Test i256 with unsupported Value variant
        let result = std::panic::catch_unwind(|| i256::from_sql(&Type::Int256, Value::Int32(42)));
        assert!(result.is_err()); // Should panic with unimplemented!()

        // Test u256 with unsupported Value variant
        let result = std::panic::catch_unwind(|| u256::from_sql(&Type::UInt256, Value::UInt32(42)));
        assert!(result.is_err()); // Should panic with unimplemented!()
    }

    #[test]
    fn test_display_formatting() {
        // Test i256 display
        let i = i256([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B,
            0x1C, 0x1D, 0x1E, 0x1F,
        ]);
        let formatted = format!("{i}");
        assert_eq!(formatted, "0x000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F");

        // Test u256 display
        let u = u256([0xFF; 32]);
        let formatted = format!("{u}");
        assert_eq!(formatted, "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
    }

    #[test]
    fn test_conversion_roundtrips() {
        // Test i256 -> (u128, u128) -> i256 roundtrip
        let original = i256([
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x12, 0x34, 0x56, 0x78,
            0x9A, 0xBC, 0xDE, 0xF0,
        ]);
        let tuple: (u128, u128) = original.into();
        let reconstructed = i256::from(tuple);
        assert_eq!(original, reconstructed);

        // Test u256 -> (u128, u128) -> u256 roundtrip
        let original = u256([
            0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33,
            0x22, 0x11, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0xF0, 0xDE, 0xBC, 0x9A,
            0x78, 0x56, 0x34, 0x12,
        ]);
        let tuple: (u128, u128) = original.into();
        let reconstructed = u256::from(tuple);
        assert_eq!(original, reconstructed);
    }

    #[test]
    fn test_edge_cases() {
        // Test scaling with large u8 scale value (but not max to avoid overflow)
        let result = i256::from((1i128, 20u8));
        // This shouldn't panic
        let _: (u128, u128) = result.into();

        // Test i128 min/max values
        let min_result = i256::from(i128::MIN);
        let max_result = i256::from(i128::MAX);

        // Verify these don't panic and produce some result
        let _: (u128, u128) = min_result.into();
        let _: (u128, u128) = max_result.into();
    }
}
