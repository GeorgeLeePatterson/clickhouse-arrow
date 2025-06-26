use std::num::TryFromIntError;
use std::sync::Arc;

use chrono::{Duration, FixedOffset, NaiveDate, TimeZone, Utc};
use chrono_tz::{Tz, UTC};

use crate::{Error, FromSql, Result, ToSql, Type, Value, unexpected_type};

/// Wrapper type for `ClickHouse` `Date` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct Date(pub u16);

#[expect(clippy::cast_sign_loss)]
#[expect(clippy::cast_possible_truncation)]
impl Date {
    /// # Panics
    ///
    /// Panics if the number of days is out of range for a `u16`.
    pub fn from_days(days: i32) -> Self {
        assert!(!(days < 0 || days > i32::from(u16::MAX)), "Date out of range for u16: {days}");
        Date(days as u16)
    }

    /// # Panics
    ///
    /// Panics if the number of milliseconds is out of range for a `u16`.
    pub fn from_millis(ms: i64) -> Self {
        let days = ms / 86_400_000; // Milliseconds per day
        assert!(!(days < 0 || days > i64::from(u16::MAX)), "Date out of range for u16: {days}");
        Date(days as u16)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Date {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: NaiveDate = (*self).into();
        date.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Date {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let date: NaiveDate = NaiveDate::deserialize(deserializer)?;
        Ok(date.into())
    }
}

impl ToSql for Date {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> { Ok(Value::Date(self)) }
}

impl FromSql for Date {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Date) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Date(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl From<NaiveDate> for Date {
    fn from(other: NaiveDate) -> Self {
        #[expect(clippy::cast_possible_truncation)]
        #[expect(clippy::cast_sign_loss)]
        Self(other.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days()
            as u16)
    }
}

impl From<Date> for NaiveDate {
    fn from(date: Date) -> Self {
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(i64::from(date.0))
    }
}

/// Wrapper type for `ClickHouse` `Date32` type.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug, Default)]
pub struct Date32(pub i32);

#[expect(clippy::cast_possible_truncation)]
impl Date32 {
    /// Creates a `Date32` from days since 1900-01-01.
    pub fn from_days(days: i32) -> Self { Date32(days) }

    /// Creates a `Date32` from milliseconds since 1970-01-01, adjusting to 1900-01-01 epoch.
    pub fn from_millis(ms: i64) -> Self {
        const DAYS_1900_TO_1970: i64 = 25_567; // Days from 1900-01-01 to 1970-01-01
        let days = ms / 86_400_000; // Milliseconds per day
        Date32((days - DAYS_1900_TO_1970) as i32)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Date32 {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: NaiveDate = (*self).into();
        date.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Date32 {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let date: NaiveDate = NaiveDate::deserialize(deserializer)?;
        Ok(date.into())
    }
}

impl ToSql for Date32 {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> { Ok(Value::Date32(self)) }
}

impl FromSql for Date32 {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::Date32) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::Date32(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl From<NaiveDate> for Date32 {
    fn from(other: NaiveDate) -> Self {
        let days =
            other.signed_duration_since(NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()).num_days();
        #[expect(clippy::cast_possible_truncation)]
        Date32(days as i32)
    }
}

impl From<Date32> for NaiveDate {
    fn from(date: Date32) -> Self {
        NaiveDate::from_ymd_opt(1900, 1, 1).unwrap() + Duration::days(i64::from(date.0))
    }
}

/// Wrapper type for `ClickHouse` `DateTime` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime(pub Tz, pub u32);

#[expect(clippy::cast_sign_loss)]
#[expect(clippy::cast_possible_truncation)]
impl DateTime {
    /// Infallible from implemenation for [`chrono::DateTime<Tz>`].
    ///
    /// IMPORTANT: This will truncate a value that is too large for the target type.
    /// Use `TryFrom` for the fallible version.
    #[must_use]
    pub fn from_chrono_infallible(other: chrono::DateTime<Tz>) -> Self {
        // Get the timestamp and convert directly to u32, letting the error bubble up
        let timestamp = other.timestamp() as u32;
        Self(other.timezone(), timestamp)
    }

    /// Infallible from implemenation for [`chrono::DateTime<Utc>`].
    ///
    /// IMPORTANT: This will truncate a value that is too large for the target type.
    /// Use `TryFrom` for the fallible version.
    #[must_use]
    pub fn from_chrono_infallible_utc(other: chrono::DateTime<Utc>) -> Self {
        // Get the timestamp and convert directly to u32, letting the error bubble up
        let timestamp = other.timestamp() as u32;
        Self(UTC, timestamp)
    }

    /// # Panics
    ///
    /// Panics if the number of seconds is out of range for a `u32`.
    pub fn from_seconds(seconds: i64, tz: Option<Arc<str>>) -> Self {
        let tz = tz.map_or(UTC, |s| s.parse::<Tz>().unwrap());
        assert!(
            !(seconds < 0 || seconds > i64::from(u32::MAX)),
            "DateTime out of range for u32: {seconds}"
        );
        DateTime(tz, seconds as u32)
    }

    /// # Panics
    ///
    /// Panics if the number of milliseconds is out of range for a `u32`.
    pub fn from_millis(ms: i64, tz: Option<Arc<str>>) -> Self {
        let seconds = ms / 1000;
        assert!(
            !(seconds < 0 || seconds > i64::from(u32::MAX)),
            "DateTime out of range for u32: {seconds}"
        );
        DateTime(tz.map_or(UTC, |s| s.parse::<Tz>().unwrap()), seconds as u32)
    }

    /// # Panics
    ///
    /// Panics if the number of microseconds is out of range for a `u32`.
    pub fn from_micros(us: i64, tz: Option<Arc<str>>) -> Self {
        let seconds = us / 1_000_000;
        assert!(
            !(seconds < 0 || seconds > i64::from(u32::MAX)),
            "DateTime out of range for u32: {seconds}"
        );
        DateTime(tz.map_or(UTC, |s| s.parse::<Tz>().unwrap()), seconds as u32)
    }

    /// # Panics
    ///
    /// Panics if the number of nanoseconds is out of range for a `u32`.
    pub fn from_nanos(ns: i64, tz: Option<Arc<str>>) -> Self {
        let seconds = ns / 1_000_000_000;
        assert!(
            !(seconds < 0 || seconds > i64::from(u32::MAX)),
            "DateTime out of range for u32: {seconds}"
        );
        DateTime(tz.map_or(UTC, |s| s.parse::<Tz>().unwrap()), seconds as u32)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for DateTime {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for DateTime {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<FixedOffset> =
            chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: chrono::ParseError| serde::de::Error::custom(e.to_string()))?;

        date.try_into().map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

impl ToSql for DateTime {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> { Ok(Value::DateTime(self)) }
}

impl FromSql for DateTime {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime(x) => Ok(x),
            _ => unimplemented!(),
        }
    }
}

impl Default for DateTime {
    fn default() -> Self { Self(UTC, 0) }
}

impl TryFrom<DateTime> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date.0.timestamp_opt(date.1.into(), 0).unwrap())
    }
}

impl TryFrom<DateTime> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date.0.timestamp_opt(date.1.into(), 0).unwrap().fixed_offset())
    }
}

impl TryFrom<DateTime> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime) -> Result<Self, TryFromIntError> {
        Ok(date.0.timestamp_opt(date.1.into(), 0).unwrap().with_timezone(&Utc))
    }
}

impl TryFrom<chrono::DateTime<Tz>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Tz>) -> Result<Self, TryFromIntError> {
        // Get the timestamp and convert directly to u32, letting the error bubble up
        let timestamp = u32::try_from(other.timestamp())?;
        Ok(Self(other.timezone(), timestamp))
    }
}

impl TryFrom<chrono::DateTime<FixedOffset>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<FixedOffset>) -> Result<Self, TryFromIntError> {
        Tz::UTC.from_utc_datetime(&other.naive_utc()).with_timezone(&other.timezone()).try_into()
    }
}

impl TryFrom<chrono::DateTime<Utc>> for DateTime {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Utc>) -> Result<Self, TryFromIntError> {
        Ok(Self(UTC, other.timestamp().try_into()?))
    }
}

/// Wrapper type for `ClickHouse` `DateTime64` type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime64<const PRECISION: usize>(pub Tz, pub u64);

/// Wrapper type for `ClickHouse` `DateTime64` type with dynamic precision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DynDateTime64(pub Tz, pub u64, pub usize);

// TODO: Remove all panics, return error
#[expect(clippy::cast_sign_loss)]
impl DynDateTime64 {
    /// # Panics
    ///
    /// Panics if seconds is negative.
    pub fn from_seconds(seconds: i64, tz: Option<Arc<str>>) -> Self {
        let tz = tz.map_or(UTC, |s| s.parse::<Tz>().unwrap());
        assert!(seconds >= 0, "DynDateTime64 does not support negative seconds: {seconds}");
        DynDateTime64(tz, seconds as u64, 0) // Precision 0 for seconds
    }

    /// # Panics
    ///
    /// Panics if milliseconds is negative.
    pub fn from_millis(ms: i64, tz: Option<Arc<str>>) -> Self {
        let tz = tz.map_or(UTC, |s| s.parse::<Tz>().unwrap());
        assert!(ms >= 0, "DynDateTime64 does not support negative milliseconds: {ms}");
        DynDateTime64(tz, ms as u64, 3) // Precision 3 for milliseconds
    }

    /// # Panics
    ///
    /// Panics if micros is negative.
    pub fn from_micros(us: i64, tz: Option<Arc<str>>) -> Self {
        let tz = tz.map_or(UTC, |s| s.parse::<Tz>().unwrap());
        assert!(us >= 0, "DynDateTime64 does not support negative microseconds: {us}");
        DynDateTime64(tz, us as u64, 6) // Precision 6 for microseconds
    }

    /// # Panics
    ///
    /// Panics if nanos is negative.
    pub fn from_nanos(ns: i64, tz: Option<Arc<str>>) -> Self {
        let tz = tz.map_or(UTC, |s| s.parse::<Tz>().unwrap());
        assert!(ns >= 0, "DynDateTime64 does not support negative nanoseconds: {ns}");
        DynDateTime64(tz, ns as u64, 9) // Precision 9, adjust for ClickHouse
    }
}

impl<const PRECISION: usize> From<DateTime64<PRECISION>> for DynDateTime64 {
    fn from(value: DateTime64<PRECISION>) -> Self { Self(value.0, value.1, PRECISION) }
}

#[cfg(feature = "serde")]
impl serde::Serialize for DynDateTime64 {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for DynDateTime64 {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<Utc> = Utc.from_utc_datetime(
            &chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: chrono::ParseError| serde::de::Error::custom(e.to_string()))?
                .naive_utc(),
        );

        DynDateTime64::try_from_utc(date, 6)
            .map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

#[cfg(feature = "serde")]
impl<const PRECISION: usize> serde::Serialize for DateTime64<PRECISION> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let date: chrono::DateTime<Tz> = (*self)
            .try_into()
            .map_err(|e: TryFromIntError| serde::ser::Error::custom(e.to_string()))?;
        date.to_rfc3339().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, const PRECISION: usize> serde::Deserialize<'de> for DateTime64<PRECISION> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: String = String::deserialize(deserializer)?;
        let date: chrono::DateTime<Utc> = Utc.from_utc_datetime(
            &chrono::DateTime::<FixedOffset>::parse_from_rfc3339(&raw)
                .map_err(|e: chrono::ParseError| serde::de::Error::custom(e.to_string()))?
                .naive_utc(),
        );

        date.try_into().map_err(|e: TryFromIntError| serde::de::Error::custom(e.to_string()))
    }
}

impl<const PRECISION: usize> ToSql for DateTime64<PRECISION> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(self.into()))
    }
}

impl<const PRECISION: usize> FromSql for DateTime64<PRECISION> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(x, _) if *x == PRECISION) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => Ok(Self(datetime.0, datetime.1)),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: usize> Default for DateTime64<PRECISION> {
    fn default() -> Self { Self(UTC, 0) }
}

impl ToSql for chrono::DateTime<Utc> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(DynDateTime64(
            UTC,
            self.timestamp_micros().try_into().map_err(|e| {
                Error::DeserializeError(format!("failed to convert DateTime64: {e:?}"))
            })?,
            6,
        )))
    }
}

impl FromSql for chrono::DateTime<Utc> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(_, _) | Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => {
                #[expect(clippy::cast_possible_truncation)]
                let datetime_2 = datetime.2 as u32;
                let seconds = datetime.1 / 10u64.pow(datetime_2);
                let units = datetime.1 % 10u64.pow(datetime_2);
                let units_ns = units * 10u64.pow(9 - datetime_2);
                let (seconds, units_ns): (i64, u32) =
                    seconds.try_into().and_then(|k| Ok((k, units_ns.try_into()?))).map_err(
                        |e| Error::DeserializeError(format!("failed to convert DateTime: {e:?}")),
                    )?;
                Ok(datetime.0.timestamp_opt(seconds, units_ns).unwrap().with_timezone(&Utc))
            }
            Value::DateTime(date) => Ok(date.try_into().map_err(|e| {
                Error::DeserializeError(format!("failed to convert DateTime: {e:?}"))
            })?),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let precision = PRECISION as u32;
        let seconds = date.1 / 10u64.pow(precision);
        let units = date.1 % 10u64.pow(precision);
        let units_ns = units * 10u64.pow(9 - precision);
        Ok(date
            .0
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap()
            .with_timezone(&Utc))
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<Utc> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let date_2 = date.2 as u32;
        let seconds = date.1 / 10u64.pow(date_2);
        let units = date.1 % 10u64.pow(date_2);
        let units_ns = units * 10u64.pow(9 - date_2);
        Ok(date
            .0
            .timestamp_opt(seconds.try_into()?, units_ns.try_into()?)
            .unwrap()
            .with_timezone(&Utc))
    }
}

impl ToSql for chrono::DateTime<Tz> {
    fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
        Ok(Value::DateTime64(DynDateTime64(
            self.timezone(),
            self.timestamp_micros().try_into().map_err(|e| {
                Error::DeserializeError(format!("failed to convert DateTime64: {e:?}"))
            })?,
            6,
        )))
    }
}

impl FromSql for chrono::DateTime<Tz> {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        if !matches!(type_, Type::DateTime64(_, _) | Type::DateTime(_)) {
            return Err(unexpected_type(type_));
        }
        match value {
            Value::DateTime64(datetime) => {
                #[expect(clippy::cast_possible_truncation)]
                let datetime_2 = datetime.2 as u32;
                let seconds = datetime.1 / 10u64.pow(datetime_2);
                let units = datetime.1 % 10u64.pow(datetime_2);
                let units_ns = units * 10u64.pow(9 - datetime_2);
                let (seconds, units_ns): (i64, u32) =
                    seconds.try_into().and_then(|k| Ok((k, units_ns.try_into()?))).map_err(
                        |e| Error::DeserializeError(format!("failed to convert DateTime: {e:?}")),
                    )?;
                Ok(datetime.0.timestamp_opt(seconds, units_ns).unwrap())
            }
            Value::DateTime(date) => Ok(date.try_into().map_err(|e| {
                Error::DeserializeError(format!("failed to convert DateTime: {e:?}"))
            })?),
            _ => unimplemented!(),
        }
    }
}

impl<const PRECISION: usize> TryFrom<chrono::DateTime<Utc>> for DateTime64<PRECISION> {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Utc>) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let precision = PRECISION as u32;
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = u64::from(other.timestamp_subsec_nanos());
        let total = seconds * 10u64.pow(precision) + sub_seconds / 10u64.pow(9 - precision);
        Ok(Self(UTC, total))
    }
}

impl DynDateTime64 {
    /// # Errors
    ///
    /// Returns an error if the timestamp cannot be converted to a u64.
    pub fn try_from_utc(
        other: chrono::DateTime<Utc>,
        precision: usize,
    ) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let precision_u32 = precision as u32;
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = u64::from(other.timestamp_subsec_nanos());
        let total = seconds * 10u64.pow(precision_u32) + sub_seconds / 10u64.pow(9 - precision_u32);
        Ok(Self(UTC, total, precision))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let precision = PRECISION as u32;
        let seconds = date.1 / 10u64.pow(precision);
        let units = date.1 % 10u64.pow(precision);
        let units_ns = units * 10u64.pow(9 - precision);
        Ok(date.0.timestamp_opt(seconds.try_into()?, units_ns.try_into()?).unwrap())
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<Tz> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let date_2 = date.2 as u32;
        let seconds = date.1 / 10u64.pow(date_2);
        let units = date.1 % 10u64.pow(date_2);
        let units_ns = units * 10u64.pow(9 - date_2);
        Ok(date.0.timestamp_opt(seconds.try_into()?, units_ns.try_into()?).unwrap())
    }
}

impl<const PRECISION: usize> TryFrom<chrono::DateTime<Tz>> for DateTime64<PRECISION> {
    type Error = TryFromIntError;

    fn try_from(other: chrono::DateTime<Tz>) -> Result<Self, TryFromIntError> {
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = u64::from(other.timestamp_subsec_nanos());
        #[expect(clippy::cast_possible_truncation)]
        let total =
            seconds * 10u64.pow(PRECISION as u32) + sub_seconds / 10u64.pow(9 - PRECISION as u32);
        Ok(Self(other.timezone(), total))
    }
}

impl DynDateTime64 {
    /// # Errors
    ///
    /// Returns an error if the timestamp cannot be converted to a u64.
    pub fn try_from_tz(
        other: chrono::DateTime<Tz>,
        precision: usize,
    ) -> Result<Self, TryFromIntError> {
        let seconds: u64 = other.timestamp().try_into()?;
        let sub_seconds: u64 = u64::from(other.timestamp_subsec_nanos());
        #[expect(clippy::cast_possible_truncation)]
        let total =
            seconds * 10u64.pow(precision as u32) + sub_seconds / 10u64.pow(9 - precision as u32);
        Ok(Self(other.timezone(), total, precision))
    }
}

impl<const PRECISION: usize> TryFrom<DateTime64<PRECISION>> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DateTime64<PRECISION>) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let precision = PRECISION as u32;
        let seconds = date.1 / 10u64.pow(precision);
        let units = date.1 % 10u64.pow(precision);
        let units_ns = units * 10u64.pow(9 - precision);
        Ok(date.0.timestamp_opt(seconds.try_into()?, units_ns.try_into()?).unwrap().fixed_offset())
    }
}

impl TryFrom<DynDateTime64> for chrono::DateTime<FixedOffset> {
    type Error = TryFromIntError;

    fn try_from(date: DynDateTime64) -> Result<Self, TryFromIntError> {
        #[expect(clippy::cast_possible_truncation)]
        let date_2 = date.2 as u32;
        let seconds = date.1 / 10u64.pow(date_2);
        let units = date.1 % 10u64.pow(date_2);
        let units_ns = units * 10u64.pow(9 - date_2);
        Ok(date.0.timestamp_opt(seconds.try_into()?, units_ns.try_into()?).unwrap().fixed_offset())
    }
}

#[cfg(test)]
mod chrono_tests {
    use chrono::TimeZone;
    use chrono_tz::UTC;

    use super::*;

    #[test]
    fn test_naivedate() {
        for i in 0..30000u16 {
            let date = Date(i);
            let chrono_date: NaiveDate = date.into();
            let new_date = Date::from(chrono_date);
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime() {
        for i in (0..30000u32).map(|x| x * 10000) {
            let date = DateTime(UTC, i);
            let chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();
            let new_date = DateTime::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64() {
        for i in (0..30000u64).map(|x| x * 10000) {
            let date = DateTime64::<6>(UTC, i);
            let chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();
            let new_date = DateTime64::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64_precision() {
        for i in (0..30000u64).map(|x| x * 10000) {
            let date = DateTime64::<6>(UTC, i);
            let date_value = date.to_sql(None).unwrap();
            assert_eq!(date_value, Value::DateTime64(DynDateTime64(UTC, i, 6)));
            let chrono_date: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(6, UTC), date_value).unwrap();
            let new_date = DateTime64::try_from(chrono_date).unwrap();
            assert_eq!(new_date, date);
        }
    }

    #[test]
    fn test_datetime64_precision2() {
        for i in (0..300u64).map(|x| x * 1_000_000) {
            #[expect(clippy::cast_possible_wrap)]
            #[expect(clippy::cast_possible_truncation)]
            let chrono_time = Utc.timestamp_opt(i as i64, i as u32).unwrap();
            let date = chrono_time.to_sql(None).unwrap();
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();
            assert_eq!(chrono_time, out_time);
            let date = match date {
                Value::DateTime64(mut datetime) => {
                    datetime.2 -= 3;
                    datetime.1 /= 1000;
                    Value::DateTime64(datetime)
                }
                _ => unimplemented!(),
            };
            let out_time: chrono::DateTime<Utc> =
                FromSql::from_sql(&Type::DateTime64(9, UTC), date.clone()).unwrap();

            assert_eq!(chrono_time, out_time);
        }
    }

    #[test]
    fn test_from_seconds() {
        let dt = DynDateTime64::from_seconds(1000, Some(Arc::from("UTC")));
        assert_eq!(dt.0, Tz::UTC);
        assert_eq!(dt.1, 1000);
        assert_eq!(dt.2, 0);
    }

    #[test]
    fn test_from_millis() {
        let dt = DynDateTime64::from_millis(1000, Some(Arc::from("UTC")));
        assert_eq!(dt.0, Tz::UTC);
        assert_eq!(dt.1, 1000); // 1000 ms
        assert_eq!(dt.2, 3);
    }

    #[test]
    fn test_from_micros() {
        let dt = DynDateTime64::from_micros(1_000_000, Some(Arc::from("UTC")));
        assert_eq!(dt.0, Tz::UTC);
        assert_eq!(dt.1, 1_000_000); // 1,000,000 µs
        assert_eq!(dt.2, 6);
    }

    #[test]
    fn test_from_nanos() {
        let dt = DynDateTime64::from_nanos(1_000_000_000, Some(Arc::from("UTC")));
        assert_eq!(dt.0, Tz::UTC);
        assert_eq!(dt.1, 1_000_000_000); // 1,000,000,000 ns
        assert_eq!(dt.2, 9);
    }

    #[test]
    fn test_from_seconds_zero() {
        let dt = DynDateTime64::from_seconds(0, None);
        assert_eq!(dt.0, Tz::UTC);
        assert_eq!(dt.1, 0);
        assert_eq!(dt.2, 0);
    }

    #[test]
    #[should_panic(expected = "DynDateTime64 does not support negative seconds: -1")]
    fn test_from_seconds_negative() {
        let _ = DynDateTime64::from_seconds(-1, Some(Arc::from("UTC")));
    }

    #[test]
    #[should_panic(expected = "DynDateTime64 does not support negative milliseconds: -1000")]
    fn test_from_millis_negative() {
        let _ = DynDateTime64::from_millis(-1000, Some(Arc::from("UTC")));
    }

    #[test]
    #[should_panic(expected = "DynDateTime64 does not support negative microseconds: -1000000")]
    fn test_from_micros_negative() {
        let _ = DynDateTime64::from_micros(-1_000_000, Some(Arc::from("UTC")));
    }

    #[test]
    #[should_panic(expected = "DynDateTime64 does not support negative nanoseconds: -1000000000")]
    fn test_from_nanos_negative() {
        let _ = DynDateTime64::from_nanos(-1_000_000_000, Some(Arc::from("UTC")));
    }

    #[test]
    fn test_from_millis_custom_tz() {
        let dt = DynDateTime64::from_millis(1000, Some(Arc::from("America/New_York")));
        assert_eq!(dt.0, Tz::America__New_York);
        assert_eq!(dt.1, 1000);
        assert_eq!(dt.2, 3);
    }

    #[test]
    #[allow(deprecated)]
    fn test_consistency_with_convert_for_str() {
        let test_date = "2022-04-22 00:00:00";

        let dt = chrono::NaiveDateTime::parse_from_str(test_date, "%Y-%m-%d %H:%M:%S").unwrap();

        let chrono_date =
            chrono::DateTime::<Tz>::from_utc(dt, chrono_tz::UTC.offset_from_utc_datetime(&dt));

        #[expect(clippy::cast_possible_truncation)]
        #[expect(clippy::cast_sign_loss)]
        let date = DateTime(UTC, dt.timestamp() as u32);

        let new_chrono_date: chrono::DateTime<Tz> = date.try_into().unwrap();

        assert_eq!(new_chrono_date, chrono_date);
    }
}
