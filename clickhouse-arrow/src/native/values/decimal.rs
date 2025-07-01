use rust_decimal::Decimal;

use crate::{Error, FromSql, Result, ToSql, Type, Value, unexpected_type};

fn count_digits_i128(n: i128) -> u32 { if n == 0 { 1 } else { n.abs().ilog10() + 1 } }

impl FromSql for Decimal {
    #[expect(clippy::cast_possible_truncation)]
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        fn out_of_range(name: &str) -> Error {
            Error::DeserializeError(format!("{name} out of bounds for rust_decimal"))
        }
        fn decimal_out_of_range<T: std::fmt::Display>(
            name: &str,
            type_: &Type,
            scale: usize,
            value: T,
        ) -> Error {
            Error::DeserializeError(format!(
                "{name} out of bounds for rust_decimal: type={type_:?} scale={scale} value={value}"
            ))
        }

        match value {
            Value::Int8(i) => Ok(Decimal::new(i64::from(i), 0)),
            Value::Int16(i) => Ok(Decimal::new(i64::from(i), 0)),
            Value::Int32(i) => Ok(Decimal::new(i64::from(i), 0)),
            Value::Int64(i) => Ok(Decimal::new(i, 0)),
            Value::Int128(i) => {
                Decimal::try_from_i128_with_scale(i, 0).map_err(|_| out_of_range("i128"))
            }
            Value::UInt8(i) => Ok(Decimal::new(i64::from(i), 0)),
            Value::UInt16(i) => Ok(Decimal::new(i64::from(i), 0)),
            Value::UInt32(i) => Ok(Decimal::new(i64::from(i), 0)),
            Value::UInt64(i) => {
                Decimal::try_from_i128_with_scale(i.into(), 0).map_err(|_| out_of_range("u128"))
            }
            Value::UInt128(i) => Decimal::try_from_i128_with_scale(
                i.try_into().map_err(|_| out_of_range("u128"))?,
                0,
            )
            .map_err(|_| out_of_range("u128")),
            Value::Decimal32(scale, value) => {
                if count_digits_i128(i128::from(value)) > 9 {
                    return Err(decimal_out_of_range("Decimal32", type_, scale, value));
                }
                Decimal::try_from_i128_with_scale(i128::from(value), scale as u32)
                    .map_err(|_| decimal_out_of_range("Decimal32", type_, scale, value))
            }
            Value::Decimal64(scale, value) => {
                if count_digits_i128(i128::from(value)) > 18 {
                    return Err(decimal_out_of_range("Decimal64", type_, scale, value));
                }
                Decimal::try_from_i128_with_scale(i128::from(value), scale as u32)
                    .map_err(|_| decimal_out_of_range("Decimal64", type_, scale, value))
            }
            Value::Decimal128(scale, value) => {
                if count_digits_i128(value) > 28 {
                    return Err(decimal_out_of_range("Decimal128", type_, scale, value));
                }
                Decimal::try_from_i128_with_scale(value, scale as u32)
                    .map_err(|_| decimal_out_of_range("Decimal128", type_, scale, value))
            }
            _ => Err(unexpected_type(type_)),
        }
    }
}

impl ToSql for Decimal {
    #[expect(clippy::cast_possible_truncation)]
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        fn out_of_range(name: &str) -> Error {
            Error::SerializeError(format!("{name} out of bounds for rust_decimal"))
        }

        let scale = self.scale();
        let mantissa = self.mantissa();

        match type_hint {
            None => Ok(Value::Decimal128(scale as usize, mantissa)),
            Some(Type::Decimal32(s)) => {
                if count_digits_i128(mantissa) > 9 {
                    return Err(out_of_range("Decimal32"));
                }
                if scale > *s as u32 {
                    return Err(out_of_range("Decimal32 scale"));
                }
                Ok(Value::Decimal32(
                    scale as usize,
                    mantissa.try_into().map_err(|_| out_of_range("Decimal32 mantissa"))?,
                ))
            }
            Some(Type::Decimal64(s)) => {
                if count_digits_i128(mantissa) > 18 {
                    return Err(out_of_range("Decimal64"));
                }
                if scale > *s as u32 {
                    return Err(out_of_range("Decimal64 scale"));
                }
                Ok(Value::Decimal64(
                    scale as usize,
                    mantissa.try_into().map_err(|_| out_of_range("Decimal64"))?,
                ))
            }
            Some(Type::Decimal128(s)) => {
                if count_digits_i128(mantissa) > 38 {
                    return Err(out_of_range("Decimal128"));
                }
                if scale > *s as u32 {
                    return Err(out_of_range("Decimal128 scale"));
                }
                Ok(Value::Decimal128(scale as usize, mantissa))
            }
            Some(x) => {
                Err(Error::SerializeError(format!("unexpected type for scale {scale}: {x}")))
            }
        }
    }
}
