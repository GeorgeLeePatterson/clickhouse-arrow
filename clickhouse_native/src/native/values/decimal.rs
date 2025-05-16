use rust_decimal::Decimal;

use crate::{ClickhouseNativeError, FromSql, Result, ToSql, Type, Value, unexpected_type};

impl FromSql for Decimal {
    #[expect(clippy::cast_possible_truncation)]
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        fn out_of_range(name: &str) -> ClickhouseNativeError {
            ClickhouseNativeError::DeserializeError(format!(
                "{name} out of bounds for rust_decimal"
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
            Value::Decimal32(precision, value) => {
                Decimal::try_new(i64::from(value), precision as u32)
                    .map_err(|_| out_of_range("Decimal32"))
            }
            Value::Decimal64(precision, value) => {
                Decimal::try_new(value, precision as u32).map_err(|_| out_of_range("Decimal64"))
            }
            Value::Decimal128(precision, value) => {
                Decimal::try_from_i128_with_scale(value, precision as u32)
                    .map_err(|_| out_of_range("Decimal128"))
            }
            _ => Err(unexpected_type(type_)),
        }
    }
}

impl ToSql for Decimal {
    #[expect(clippy::cast_possible_truncation)]
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        fn out_of_range(name: &str) -> ClickhouseNativeError {
            ClickhouseNativeError::SerializeError(format!("{name} out of bounds for rust_decimal"))
        }

        fn mantissa_to_scale(mantissa: i128, scale: u32, precision: u32) -> Result<i128> {
            assert!(precision >= scale);
            if precision == scale {
                Ok(mantissa)
            } else {
                mantissa
                    .checked_mul(10i128.pow(precision - scale))
                    .ok_or_else(|| out_of_range("mantissa"))
            }
        }

        let scale = self.scale();
        let mantissa = self.mantissa();

        match type_hint {
            None => Ok(Value::Decimal128(scale as usize, mantissa)),
            Some(Type::Decimal32(precision)) if *precision as u32 >= scale => Ok(Value::Decimal32(
                *precision,
                mantissa_to_scale(mantissa, scale, *precision as u32)?
                    .try_into()
                    .map_err(|_| out_of_range("Decimal32"))?,
            )),
            Some(Type::Decimal64(precision)) if *precision as u32 >= scale => Ok(Value::Decimal64(
                *precision,
                mantissa_to_scale(mantissa, scale, *precision as u32)?
                    .try_into()
                    .map_err(|_| out_of_range("Decimal64"))?,
            )),
            Some(Type::Decimal128(precision)) if *precision as u32 >= scale => {
                Ok(Value::Decimal128(
                    *precision,
                    mantissa_to_scale(mantissa, scale, *precision as u32)?,
                ))
            }
            Some(x) => Err(ClickhouseNativeError::SerializeError(format!(
                "unexpected type for scale {scale}: {x}"
            ))),
        }
    }
}
