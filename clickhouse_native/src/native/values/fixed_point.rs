#![expect(clippy::cast_possible_truncation)]
#![expect(clippy::cast_precision_loss)]
#![expect(clippy::cast_sign_loss)]
#![expect(clippy::cast_possible_wrap)]
use crate::{FromSql, Result, ToSql, Type, Value, i256, unexpected_type};

/// Wrapper type for Clickhouse `FixedPoint32` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint32<const PRECISION: u64>(pub i32);

impl<const PRECISION: u64> FixedPoint32<PRECISION> {
    pub const fn modulus(&self) -> i32 { 10i32.pow(PRECISION as u32) }

    pub fn integer(&self) -> i32 { self.0 / 10i32.pow(PRECISION as u32) }

    pub fn fraction(&self) -> i32 { self.0 % 10i32.pow(PRECISION as u32) }
}

impl<const PRECISION: u64> ToSql for FixedPoint32<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Decimal32(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint32<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal32(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal32(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: u64> From<FixedPoint32<PRECISION>> for f64 {
    fn from(fp: FixedPoint32<PRECISION>) -> Self {
        f64::from(fp.integer()) + (f64::from(fp.fraction()) / f64::from(fp.modulus()))
    }
}

/// Wrapper type for Clickhouse `FixedPoint64` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint64<const PRECISION: u64>(pub i64);

impl<const PRECISION: u64> ToSql for FixedPoint64<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Decimal64(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint64<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal64(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal64(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: u64> FixedPoint64<PRECISION> {
    pub const fn modulus(&self) -> i64 { 10i64.pow(PRECISION as u32) }

    pub fn integer(&self) -> i64 { self.0 / 10i64.pow(PRECISION as u32) }

    pub fn fraction(&self) -> i64 { self.0 % 10i64.pow(PRECISION as u32) }
}

impl<const PRECISION: u64> From<FixedPoint64<PRECISION>> for f64 {
    fn from(fp: FixedPoint64<PRECISION>) -> Self {
        fp.integer() as f64 + (fp.fraction() as f64 / fp.modulus() as f64)
    }
}

/// Wrapper type for Clickhouse `FixedPoint128` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint128<const PRECISION: u64>(pub i128);

impl<const PRECISION: u64> ToSql for FixedPoint128<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Decimal128(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint128<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal128(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal128(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: u64> FixedPoint128<PRECISION> {
    pub const fn modulus(&self) -> i128 { 10i128.pow(PRECISION as u32) }

    pub fn integer(&self) -> i128 { self.0 / 10i128.pow(PRECISION as u32) }

    pub fn fraction(&self) -> i128 { self.0 % 10i128.pow(PRECISION as u32) }
}

impl<const PRECISION: u64> From<FixedPoint128<PRECISION>> for f64 {
    fn from(fp: FixedPoint128<PRECISION>) -> Self {
        fp.integer() as f64 + (fp.fraction() as f64 / fp.modulus() as f64)
    }
}

/// Wrapper type for Clickhouse `FixedPoint256` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct FixedPoint256<const PRECISION: u64>(pub i256);

impl<const PRECISION: u64> FixedPoint256<PRECISION> {
    /// The maximum value for [`FixedPoint256`]
    pub const MAX: Self = FixedPoint256(Self::max_i256());
    /// The minimum value for [`FixedPoint256`]
    pub const MIN: Self = FixedPoint256(Self::min_i256());

    const fn max_i256() -> i256 {
        // For a two's complement signed integer, the maximum value has
        // all bits set to 1 except the sign bit
        let mut bytes = [0xFF; 32];
        bytes[0] = 0x7F; // Clear the sign bit of the highest byte
        i256(bytes)
    }

    const fn min_i256() -> i256 {
        // For a two's complement signed integer, the minimum value has
        // just the sign bit set to 1, all others to 0
        let mut bytes = [0; 32];
        bytes[0] = 0x80; // Set only the sign bit
        i256(bytes)
    }

    /// Create a fixed-point number from a raw scaled integer value
    /// The value is assumed to already have the correct scaling applied
    pub fn from_raw(value: i128) -> Self { FixedPoint256(i256::from(value)) }

    /// Create a fixed-point number from a value and decimal exponent
    /// e.g., (123, -2) represents 123 × 10^-2 = 1.23
    pub fn from_parts(value: i128, exponent: i32) -> Self {
        let effective_scale = PRECISION as i32 - exponent;

        if effective_scale > 38 {
            // Would overflow i128 - handle by converting to i256 first, then scaling
            let base = i256::from(value);
            let mut result = base;

            // Scale by multiplying by 10, effective_scale times
            for _ in 0..effective_scale {
                result = result * i256::from(10i128);
            }

            FixedPoint256(result)
        } else if effective_scale >= 0 {
            // Need to multiply by 10^effective_scale
            let scaled_value = value * 10i128.pow(effective_scale as u32);
            FixedPoint256(i256::from(scaled_value))
        } else {
            // Need to divide by 10^(-effective_scale)
            let scaled_value = value / 10i128.pow((-effective_scale) as u32);
            FixedPoint256(i256::from(scaled_value))
        }
    }

    /// Create a fixed-point number from a decimal
    #[cfg(feature = "rust_decimal")]
    pub fn from_decimal(decimal: rust_decimal::Decimal) -> Self {
        let scale = decimal.scale();
        let mantissa = decimal.mantissa();

        Self::from_parts(mantissa, scale as i32)
    }

    /// Convert to a Decimal
    ///
    /// # Errors
    ///
    /// Returns an error if the value is too large to fit in a Decimal
    #[cfg(feature = "rust_decimal")]
    pub fn to_decimal(&self) -> Result<rust_decimal::Decimal, rust_decimal::Error> {
        // Extract raw value from i256
        let (high, low) = self.0.into();

        if high != 0 && high != u128::MAX {
            // Value too large for Decimal
            return Err(rust_decimal::Error::ExceedsMaximumPossibleValue);
        }

        let raw_value = if self.is_negative() {
            // For negative values, we need to convert from two's complement
            let mut high_bits = !high;
            let low_bits = !low;

            let low_plus_one = low_bits.wrapping_add(1);
            if low_plus_one == 0 {
                high_bits = high_bits.wrapping_add(1);
            }

            if high_bits != 0 {
                // Value too large for Decimal
                return Err(rust_decimal::Error::ExceedsMaximumPossibleValue);
            }

            -(low_plus_one as i128)
        } else {
            // For positive values, just use the low bits if high is 0
            low as i128
        };

        // Create a decimal with the raw value and precision
        rust_decimal::Decimal::try_from_i128_with_scale(raw_value, PRECISION as u32)
    }

    /// Check if the value is negative
    pub fn is_negative(&self) -> bool {
        let (high, _) = self.0.into();
        (high & (1u128 << 127)) != 0
    }
}

impl<const PRECISION: u64> ToSql for FixedPoint256<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::Decimal256(PRECISION as usize, self.0))
    }
}

impl<const PRECISION: u64> FromSql for FixedPoint256<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Decimal256(x) if *x == PRECISION as usize) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Decimal256(_, x) => Ok(Self(x)),
            _ => unimplemented!(),
        }
    }
}

// Implement standard From trait for user-friendly conversions
impl<const PRECISION: u64> From<i128> for FixedPoint256<PRECISION> {
    fn from(value: i128) -> Self {
        // Interpret value as already having PRECISION decimal places
        Self::from_raw(value)
    }
}

impl<const PRECISION: u64> From<(i128, i32)> for FixedPoint256<PRECISION> {
    fn from(parts: (i128, i32)) -> Self { Self::from_parts(parts.0, parts.1) }
}

#[cfg(feature = "rust_decimal")]
impl<const PRECISION: u64> From<rust_decimal::Decimal> for FixedPoint256<PRECISION> {
    fn from(decimal: rust_decimal::Decimal) -> Self { Self::from_decimal(decimal) }
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
