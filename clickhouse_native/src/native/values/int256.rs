#![expect(clippy::cast_sign_loss)]
use std::fmt;

use crate::{FromSql, Result, ToSql, Type, Value, unexpected_type};

/// Wrapper type for Clickhouse `Int256` type.
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

/// Wrapper type for Clickhouse `UInt256` type.
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
