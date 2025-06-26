use crate::{FromSql, Result, ToSql, Type, Uuid, Value, unexpected_type};

impl ToSql for Uuid {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> { Ok(Value::Uuid(self)) }
}

impl FromSql for Uuid {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Uuid) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Uuid(x) => Ok(x),
            _ => Err(unexpected_type(type_)),
        }
    }
}
