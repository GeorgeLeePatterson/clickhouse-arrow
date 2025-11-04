/// Manages `ClickHouse` query settings for the native protocol.
///
/// This module provides types and methods to define, manipulate, and serialize
/// `ClickHouse` query settings, which are key-value pairs sent with queries to
/// configure server behavior (e.g., `max_threads`, `allow_experimental_features`).
/// The [`Settings`] struct holds a collection of [`Setting`]s, each representing
/// a key, value, and optional flags (`important`, `custom`). The [`SettingValue`]
/// enum supports various data types (integers, booleans, floats, strings) with
/// conversions from Rust primitives.
///
/// # Features
/// - Converts Rust primitives (e.g., `i32`, `&str`, `bool`) to [`SettingValue`] using the
///   `From` trait.
/// - Serializes settings to the `ClickHouse` native protocol, supporting both legacy
///   (pre-revision 54429) and modern formats.
/// - Optional `serde` integration for serialization/deserialization (enabled with the `serde`
///   feature).
///
/// # `ClickHouse` Documentation
/// - For a list of available query settings, see the [ClickHouse Settings Reference](https://clickhouse.com/docs/en/operations/settings).
/// - For details on the native protocol’s settings serialization, see the [ClickHouse Native Protocol Documentation](https://clickhouse.com/docs/en/interfaces/tcp).
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::query::settings::Settings;
///
/// // Create settings with key-value pairs
/// let mut settings = Settings::from([
///     ("max_threads".to_string(), 8_i32),
///     ("allow_experimental_features".to_string(), true),
/// ]);
///
/// // Add a setting
/// settings.add_setting("max_execution_time", 300_i64);
///
/// // Convert to key-value strings
/// let kv_pairs = settings.encode_to_key_value_strings();
/// assert_eq!(kv_pairs, vec![
///     ("max_threads".to_string(), "8".to_string()),
///     ("allow_experimental_features".to_string(), "true".to_string()),
///     ("max_execution_time".to_string(), "300".to_string()),
/// ]);
/// ```
///
/// # Notes
/// - Settings are serialized according to the `ClickHouse` server’s protocol revision. For
///   revisions ≤ 54429, only integer and boolean settings are supported.
/// - The `serde` feature enables serialization/deserialization of [`Setting`] and [`Settings`]
///   with `serde::Serialize` and `serde::Deserialize`.
use std::fmt;

use crate::io::{ClickHouseRead, ClickHouseWrite};
use crate::native::protocol::DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS;
use crate::{Error, Result};

const SETTING_FLAG_IMPORTANT: u64 = 0x01;
pub(crate) const SETTING_FLAG_CUSTOM: u64 = 0x02;

/// Supported value types for `ClickHouse` query settings.
///
/// This enum represents the possible data types for a [`Setting`]'s value, including
/// integers, booleans, floats, and strings. It implements `From` for various Rust
/// primitive types (e.g., `i32`, `&str`, `f64`) to simplify setting creation.
///
/// # Variants
/// - `Int(i64)`: A 64-bit integer (e.g., for `max_threads`).
/// - `Bool(bool)`: A boolean (e.g., for `allow_experimental_features`).
/// - `Float(f64)`: A 64-bit float (e.g., for `quantile`).
/// - `String(String)`: A string (e.g., for `default_format`).
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::query::settings::SettingValue;
///
/// let int_value: SettingValue = 8_i32.into();
/// let bool_value: SettingValue = true.into();
/// let string_value: SettingValue = "JSON".to_string().into();
/// assert!(matches!(int_value, SettingValue::Int(8)));
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum SettingValue {
    Int(i64),
    Bool(bool),
    Float(f64),
    String(String),
}

impl SettingValue {
    // Helper to extract f64 from Float variant for testing
    #[allow(unused)]
    pub(crate) fn unwrap_float(&self) -> f64 {
        match self {
            SettingValue::Float(f) => *f,
            _ => panic!("Expected Float variant"),
        }
    }
}

impl Eq for SettingValue {}

macro_rules! setting_value {
    ($ty:ident, $inner:ty) => {
        impl From<$inner> for SettingValue {
            fn from(value: $inner) -> Self { SettingValue::$ty(value) }
        }
    };
    ($ty:ident, $inner:ty, $override:ty) => {
        impl From<$override> for SettingValue {
            #[allow(clippy::cast_lossless)]
            #[allow(clippy::cast_possible_wrap)]
            fn from(value: $override) -> Self { SettingValue::$ty(value as $inner) }
        }
    };
    ($ty:ident, $inner:ty, $v:tt =>  { $override:expr }) => {
        impl From<$inner> for SettingValue {
            fn from($v: $inner) -> Self { SettingValue::$ty($override) }
        }
    };
}

setting_value!(Int, i64, u8);
setting_value!(Int, i64, u16);
setting_value!(Int, i64, u32);
setting_value!(Int, i64, u64);
setting_value!(Int, i64, i8);
setting_value!(Int, i64, i16);
setting_value!(Int, i64, i32);
setting_value!(Int, i64);
setting_value!(Bool, bool);
setting_value!(Float, f64, f32);
setting_value!(Float, f64);
setting_value!(String, &str, v => { v.to_string() });
setting_value!(String, Box<str>, v => { v.to_string() });
setting_value!(String, std::sync::Arc<str>, v => { v.to_string() });
setting_value!(String, String);

// Array conversions - serialize to field dump format for ClickHouse parameters
macro_rules! setting_value_array {
    ($ty:ty) => {
        impl From<Vec<$ty>> for SettingValue {
            fn from(value: Vec<$ty>) -> Self {
                let formatted = value.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
                SettingValue::String(format!("[{formatted}]"))
            }
        }

        impl From<&[$ty]> for SettingValue {
            fn from(value: &[$ty]) -> Self {
                let formatted = value.iter().map(ToString::to_string).collect::<Vec<_>>().join(",");
                SettingValue::String(format!("[{formatted}]"))
            }
        }
    };
}

// Numeric array types
setting_value_array!(i8);
setting_value_array!(i16);
setting_value_array!(i32);
setting_value_array!(i64);
setting_value_array!(u8);
setting_value_array!(u16);
setting_value_array!(u32);
setting_value_array!(u64);
setting_value_array!(f32);
setting_value_array!(f64);

// String array types - need special escaping
impl From<Vec<String>> for SettingValue {
    fn from(value: Vec<String>) -> Self {
        let formatted = value
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(",");
        SettingValue::String(format!("[{formatted}]"))
    }
}

impl From<&[String]> for SettingValue {
    fn from(value: &[String]) -> Self {
        let formatted = value
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(",");
        SettingValue::String(format!("[{formatted}]"))
    }
}

impl From<Vec<&str>> for SettingValue {
    fn from(value: Vec<&str>) -> Self {
        let formatted = value
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(",");
        SettingValue::String(format!("[{formatted}]"))
    }
}

impl From<&[&str]> for SettingValue {
    fn from(value: &[&str]) -> Self {
        let formatted = value
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(",");
        SettingValue::String(format!("[{formatted}]"))
    }
}

impl fmt::Display for SettingValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SettingValue::Int(i) => write!(f, "{i}"),
            SettingValue::Bool(b) => write!(f, "{b}"),
            SettingValue::Float(fl) => write!(f, "{fl}"),
            SettingValue::String(s) => write!(f, "{s}"),
        }
    }
}

/// A single `ClickHouse` query setting, consisting of a key, value, and flags.
///
/// A setting represents a key-value pair sent to the `ClickHouse` server to
/// configure query execution. The `key` is a string (e.g., `max_threads`), and
/// the `value` is a [`SettingValue`] (integer, boolean, float, or string). The
/// `important` and `custom` flags control serialization behavior in the native
/// protocol.
///
/// # Fields
/// - `key`: The setting name (e.g., `max_threads`).
/// - `value`: The setting value, stored as a [`SettingValue`].
/// - `important`: If `true`, marks the setting as important (affects serialization).
/// - `custom`: If `true`, serializes the value as a custom string (e.g., for complex types).
///
/// # `ClickHouse` Reference
/// See the [ClickHouse Settings Reference](https://clickhouse.com/docs/en/operations/settings)
/// for valid setting names and their types.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Setting {
    key:       String,
    value:     SettingValue,
    important: bool,
    custom:    bool,
}

impl Setting {
    /// Encodes the setting to the `ClickHouse` native protocol.
    ///
    /// For legacy revisions (≤ 54429), only integer and boolean settings are supported,
    /// and attempting to encode a string or float will return an error. For modern revisions,
    /// all setting types are supported, with strings optionally encoded as custom fields if
    /// `custom` is `true`.
    ///
    /// # Arguments
    /// - `writer`: The writer to serialize the setting to.
    /// - `revision`: The `ClickHouse` server protocol revision.
    ///
    /// # Errors
    /// Returns `Err(Error::UnsupportedSettingType)` if the setting value is a
    /// string or float in legacy revisions.
    async fn encode<W: ClickHouseWrite>(&self, writer: &mut W, revision: u64) -> Result<()> {
        tracing::trace!(setting = ?self, "Writing setting");

        if revision <= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
            if !matches!(self.value, SettingValue::Int(_) | SettingValue::Bool(_)) {
                return Err(Error::UnsupportedSettingType(self.key.clone()));
            }

            // Write key
            writer.write_string(&self.key).await?;

            // Write value
            #[expect(clippy::cast_sign_loss)]
            match &self.value {
                SettingValue::Int(i) => writer.write_var_uint(*i as u64).await?,
                SettingValue::Bool(b) => writer.write_var_uint(u64::from(*b)).await?,
                _ => unreachable!("Checked above"),
            }
        } else {
            // Write key
            writer.write_string(&self.key).await?;

            // Write flags
            let mut flags = 0u64;
            if self.important {
                flags |= SETTING_FLAG_IMPORTANT;
            }
            if self.custom {
                flags |= SETTING_FLAG_CUSTOM;
            }
            writer.write_var_uint(flags).await?;

            // Write value
            if self.custom {
                let field_dump = self.encode_field_dump()?;
                writer.write_string(&field_dump).await?;
            } else {
                writer.write_string(self.value.to_string()).await?;
            }
        }

        Ok(())
    }

    /// Decodes a setting from the `ClickHouse` native protocol.
    ///
    /// For legacy revisions (≤ 54429), only integer and boolean settings are supported.
    /// For modern revisions, all setting types are supported, with custom settings
    /// decoded from field dumps when the custom flag is set.
    ///
    /// # Arguments
    /// - `reader`: The reader to deserialize the setting from.
    /// - `revision`: The `ClickHouse` server protocol revision.
    ///
    /// # Errors
    /// Returns an error if the data cannot be read or parsed correctly.
    async fn decode<R: ClickHouseRead>(reader: &mut R, key: String) -> Result<Self> {
        // Read flags (for STRINGS_WITH_FLAGS format)
        let flags = reader.read_var_uint().await?;
        let is_important = (flags & SETTING_FLAG_IMPORTANT) != 0;
        let is_custom = (flags & SETTING_FLAG_CUSTOM) != 0;

        // Read value based on whether it's custom or not
        let value = if is_custom {
            // Custom setting: read as field dump
            let field_dump = reader.read_string().await?;
            SettingValue::String(String::from_utf8_lossy(&field_dump).to_string())
        } else {
            // Standard setting: read as string and parse
            let value_str = reader.read_string().await?;
            Self::parse_setting_value(&String::from_utf8_lossy(&value_str))
        };

        Ok(Setting { key, value, important: is_important, custom: is_custom })
    }

    /// Encodes the setting value as a string for custom settings.
    ///
    /// For string values, the result is the raw string without additional escaping
    /// (e.g., `"val'ue"` remains `"val'ue"`). Non-string values return an error.
    ///
    /// # Errors
    /// Returns `Err(Error::UnsupportedFieldType)` if the value is not a string.
    fn encode_field_dump(&self) -> Result<String> {
        match &self.value {
            SettingValue::String(s) => Ok(s.clone()),
            _ => Err(Error::UnsupportedFieldType(format!("{:?}", self.value))),
        }
    }

    /// Parses a setting value from its string representation.
    ///
    /// Attempts to parse the string as different types in order:
    /// 1. Boolean (true/false, 1/0)
    /// 2. Integer
    /// 3. Float
    /// 4. String (fallback)
    ///
    /// # Arguments
    /// - `value_str`: The string representation of the setting value.
    fn parse_setting_value(value_str: &str) -> SettingValue {
        // Try parsing as boolean first
        match value_str.to_lowercase().as_str() {
            "true" | "1" => return SettingValue::Bool(true),
            "false" | "0" => return SettingValue::Bool(false),
            _ => {}
        }

        // Try parsing as integer
        if let Ok(int_val) = value_str.parse::<i64>() {
            return SettingValue::Int(int_val);
        }

        // Try parsing as float
        if let Ok(float_val) = value_str.parse::<f64>() {
            return SettingValue::Float(float_val);
        }

        // Default to string
        SettingValue::String(value_str.to_string())
    }
}

impl<T: Into<String>, U: Into<SettingValue>> From<(T, U)> for Setting {
    fn from(value: (T, U)) -> Self {
        Setting {
            key:       value.0.into(),
            value:     value.1.into(),
            important: false,
            custom:    false,
        }
    }
}

/// A collection of `ClickHouse` query settings.
///
/// This struct holds a list of [`Setting`]s and provides methods to add settings,
/// convert them to strings, and serialize them to the `ClickHouse` native protocol.
/// It implements `From` for iterators of key-value pairs and `Deref` to access
/// the underlying settings as a slice.
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::query::settings::Settings;
///
/// let mut settings = Settings::default();
/// settings.add_setting("max_threads", 8_i32);
/// settings.add_setting("default_format", "JSON");
///
/// let strings = settings.encode_to_strings();
/// assert_eq!(strings, vec!["max_threads = 8", "default_format = JSON"]);
/// ```
///
/// # Serialization
/// Settings are serialized according to the `ClickHouse` native protocol. For
/// revisions ≤ 54429, only integer and boolean settings are supported. For newer
/// revisions, all setting types are serialized as strings with optional flags.
///
/// # `ClickHouse` Reference
/// See the [ClickHouse Native Protocol Documentation](https://clickhouse.com/docs/en/interfaces/tcp)
/// for details on settings serialization.
#[derive(Debug, Clone, Default, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Settings(pub Vec<Setting>);

impl Settings {
    /// Adds a new setting with the given name and value.
    ///
    /// The value is converted to a [`SettingValue`] using the `From` trait. The
    /// setting is marked as neither `important` nor `custom`.
    ///
    /// # Arguments
    /// - `name`: The setting name (e.g., `max_threads`).
    /// - `setting`: The setting value (e.g., `8_i32`, `true`, `"JSON"`).
    ///
    /// # Example
    /// ```rust,ignore
    /// use clickhouse_arrow::query::settings::Settings;
    ///
    /// let mut settings = Settings::default();
    /// settings.add_setting("max_threads", 8_i32);
    /// assert_eq!(settings.0.len(), 1);
    /// ```
    pub fn add_setting<S>(&mut self, name: impl Into<String>, setting: S)
    where
        SettingValue: From<S>,
    {
        let key = name.into();
        if let Some(current) = self.0.iter_mut().find(|s| s.key == key) {
            current.value = setting.into();
        } else {
            self.0.push(Setting { key, value: setting.into(), important: false, custom: false });
        }
    }

    /// Return new settings with the given name and value added.
    ///
    /// The value is converted to a [`SettingValue`] using the `From` trait. The
    /// setting is marked as neither `important` nor `custom`.
    ///
    /// # Arguments
    /// - `name`: The setting name (e.g., `max_threads`).
    /// - `setting`: The setting value (e.g., `8_i32`, `true`, `"JSON"`).
    ///
    /// # Example
    /// ```rust,ignore
    /// use clickhouse_arrow::query::settings::Settings;
    ///
    /// let mut settings = Settings::default();
    /// settings.add_setting("max_threads", 8_i32);
    /// assert_eq!(settings.0.len(), 1);
    /// ```
    #[must_use]
    pub fn with_setting<S>(mut self, name: impl Into<String>, setting: S) -> Self
    where
        SettingValue: From<S>,
    {
        let key = name.into();
        if let Some(current) = self.0.iter_mut().find(|s| s.key == key) {
            current.value = setting.into();
        } else {
            self.0.push(Setting { key, value: setting.into(), important: false, custom: false });
        }
        self
    }

    /// Converts settings to a vector of key-value string pairs.
    ///
    /// Each setting is represented as a tuple of `(key, value.to_string())`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use clickhouse_arrow::query::settings::Settings;
    ///
    /// let settings = Settings::from([("max_threads".to_string(), 8_i32)]);
    /// let kv_pairs = settings.encode_to_key_value_strings();
    /// assert_eq!(kv_pairs, vec![("max_threads".to_string(), "8".to_string())]);
    /// ```
    pub fn encode_to_key_value_strings(&self) -> Vec<(String, String)> {
        self.0.iter().map(|setting| (setting.key.clone(), setting.value.to_string())).collect()
    }

    /// Converts settings to a vector of formatted strings.
    ///
    /// Each setting is formatted as `key = value`.
    ///
    /// # Example
    /// ```rust,ignore
    /// use clickhouse_arrow::query::settings::Settings;
    ///
    /// let settings = Settings::from([("max_threads".to_string(), 8_i32)]);
    /// let strings = settings.encode_to_strings();
    /// assert_eq!(strings, vec!["max_threads = 8"]);
    /// ```
    pub fn encode_to_strings(&self) -> Vec<String> {
        self.0.iter().map(|setting| format!("{} = {}", setting.key, setting.value)).collect()
    }

    // TODO: Remove - docs
    pub(crate) async fn encode<W: ClickHouseWrite>(
        &self,
        writer: &mut W,
        revision: u64,
    ) -> Result<()> {
        for setting in &self.0 {
            setting.encode(writer, revision).await?;
        }
        Ok(())
    }

    // TODO: Remove - docs
    pub(crate) async fn encode_with_ignore<W: ClickHouseWrite>(
        &self,
        writer: &mut W,
        revision: u64,
        ignore: &Settings,
    ) -> Result<()> {
        for setting in &self.0 {
            if ignore.get(&setting.key).is_some_and(|s| s.value == setting.value) {
                continue;
            }

            setting.encode(writer, revision).await?;
        }
        Ok(())
    }

    /// Decodes a collection of settings from the `ClickHouse` native protocol.
    ///
    /// Based on `BaseSettings<TTraits>::read()` from cpp source, the format is:
    /// 1. Loop reading setting name strings
    /// 2. Empty string marks end of settings
    /// 3. For each setting: name -> flags (if `STRINGS_WITH_FLAGS`) -> value
    ///
    /// # Arguments
    /// - `reader`: The reader to deserialize the settings from.
    /// - `revision`: The `ClickHouse` server protocol revision.
    ///
    /// # Errors
    /// Returns an error if the data cannot be read or parsed correctly.
    pub(crate) async fn decode<R: ClickHouseRead>(reader: &mut R) -> Result<Self> {
        let mut settings = Vec::new();
        loop {
            // Read setting name
            let key = reader.read_string().await?;
            // Empty string marks end of settings
            if key.is_empty() {
                break;
            }
            settings
                .push(Setting::decode(reader, String::from_utf8_lossy(&key).to_string()).await?);
        }
        Ok(Settings(settings))
    }

    /// Internal helper to find a specific settings
    pub(crate) fn get(&self, key: &str) -> Option<&Setting> { self.0.iter().find(|s| s.key == key) }
}

impl<T, K, S> From<T> for Settings
where
    T: IntoIterator<Item = (K, S)>,
    K: Into<String>,
    SettingValue: From<S>,
{
    fn from(value: T) -> Self {
        Self(
            value
                .into_iter()
                .map(|(k, v)| Setting {
                    key:       k.into(),
                    value:     v.into(),
                    important: false,
                    custom:    false,
                })
                .collect(),
        )
    }
}

impl<K, S> FromIterator<(K, S)> for Settings
where
    K: Into<String>,
    SettingValue: From<S>,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, S)>,
    {
        Self::from(iter)
    }
}

impl std::ops::Deref for Settings {
    type Target = [Setting];

    fn deref(&self) -> &Self::Target { &self.0 }
}

#[cfg(feature = "serde")]
pub mod deser {
    use serde::{Deserialize, Serialize};

    use super::*;

    impl Serialize for SettingValue {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            match self {
                SettingValue::Int(i) => ::serde::Serialize::serialize(i, serializer),
                SettingValue::Bool(b) => ::serde::Serialize::serialize(b, serializer),
                SettingValue::Float(f) => ::serde::Serialize::serialize(f, serializer),
                SettingValue::String(s) => ::serde::Serialize::serialize(s, serializer),
            }
        }
    }

    impl<'de> Deserialize<'de> for SettingValue {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            fn deserialize_setting<'d, De>(deserializer: De) -> Result<SettingValue, De::Error>
            where
                De: serde::Deserializer<'d>,
            {
                use serde::de::Visitor;

                struct SettingVisitor;

                type Result<E> = std::result::Result<SettingValue, E>;

                impl Visitor<'_> for SettingVisitor {
                    type Value = SettingValue;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a number, float or string")
                    }

                    fn visit_bool<E>(self, value: bool) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_u8<E>(self, value: u8) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_u16<E>(self, value: u16) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_u32<E>(self, value: u32) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_u64<E>(self, value: u64) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_i8<E>(self, value: i8) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_i16<E>(self, value: i16) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_i32<E>(self, value: i32) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_i64<E>(self, value: i64) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_f32<E>(self, value: f32) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_f64<E>(self, value: f64) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_str<E>(self, value: &str) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        Ok(value.into())
                    }

                    fn visit_string<E>(self, value: String) -> Result<E>
                    where
                        E: serde::de::Error,
                    {
                        self.visit_str(&value)
                    }
                }

                deserializer.deserialize_any(SettingVisitor)
            }
            deserialize_setting(deserializer)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_setting_value_serialize_deserialize() {
            // Test serialization and deserialization for all SettingValue variants
            let values = vec![
                SettingValue::Int(42),
                SettingValue::Bool(true),
                SettingValue::Float(3.15),
                SettingValue::String("test".to_string()),
            ];

            for value in values {
                // Serialize
                let json = serde_json::to_string(&value).unwrap();

                // Deserialize
                let deserialized: SettingValue = serde_json::from_str(&json).unwrap();

                // Verify round-trip
                match (value, deserialized) {
                    (SettingValue::Int(a), SettingValue::Int(b)) => assert_eq!(a, b),
                    (SettingValue::Bool(a), SettingValue::Bool(b)) => assert_eq!(a, b),
                    (SettingValue::Float(a), SettingValue::Float(b)) => {
                        assert!((a - b).abs() < 1e-6);
                    }
                    (SettingValue::String(a), SettingValue::String(b)) => assert_eq!(a, b),
                    _ => panic!("Mismatched variants"),
                }
            }
        }

        #[test]
        fn test_setting_value_deserialize_variants() {
            // Test deserialization from various JSON inputs
            assert_eq!(serde_json::from_str::<SettingValue>("42").unwrap(), SettingValue::Int(42));
            assert_eq!(
                serde_json::from_str::<SettingValue>("true").unwrap(),
                SettingValue::Bool(true)
            );
            assert!(
                (serde_json::from_str::<SettingValue>("3.15").unwrap().unwrap_float() - 3.15).abs()
                    < 1e-6
            );
            assert_eq!(
                serde_json::from_str::<SettingValue>("\"test\"").unwrap(),
                SettingValue::String("test".to_string())
            );

            // Test integer variants
            assert_eq!(
                serde_json::from_str::<SettingValue>("255").unwrap(),
                SettingValue::Int(255)
            ); // u8
            assert_eq!(
                serde_json::from_str::<SettingValue>("65535").unwrap(),
                SettingValue::Int(65535)
            ); // u16
            assert_eq!(
                serde_json::from_str::<SettingValue>("4294967295").unwrap(),
                SettingValue::Int(4_294_967_295)
            ); // u32
            assert_eq!(
                serde_json::from_str::<SettingValue>("-128").unwrap(),
                SettingValue::Int(-128)
            ); // i8
            assert_eq!(
                serde_json::from_str::<SettingValue>("-32768").unwrap(),
                SettingValue::Int(-32768)
            ); // i16
            assert_eq!(
                serde_json::from_str::<SettingValue>("-2147483648").unwrap(),
                SettingValue::Int(-2_147_483_648)
            ); // i32
        }

        #[test]
        fn test_setting_value_deserialize_invalid() {
            // Test deserialization of invalid JSON
            assert!(serde_json::from_str::<SettingValue>("null").is_err());
            assert!(serde_json::from_str::<SettingValue>("[]").is_err());
            assert!(serde_json::from_str::<SettingValue>("{}").is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::io::ClickHouseRead;

    type MockWriter = Cursor<Vec<u8>>;

    // Helper to create a Setting
    fn create_setting<S>(key: &str, value: S, important: bool, custom: bool) -> Setting
    where
        SettingValue: From<S>,
    {
        Setting { key: key.to_string(), value: value.into(), important, custom }
    }

    #[test]
    fn test_setting_value_from_primitives() {
        // Test all supported From implementations for SettingValue
        assert_eq!(SettingValue::from(8_i8), SettingValue::Int(8));
        assert_eq!(SettingValue::from(8_i16), SettingValue::Int(8));
        assert_eq!(SettingValue::from(8_i32), SettingValue::Int(8));
        assert_eq!(SettingValue::from(8_i64), SettingValue::Int(8));
        assert_eq!(SettingValue::from(8_u8), SettingValue::Int(8));
        assert_eq!(SettingValue::from(8_u16), SettingValue::Int(8));
        assert_eq!(SettingValue::from(8_u32), SettingValue::Int(8));
        assert_eq!(SettingValue::from(8_u64), SettingValue::Int(8));
        assert_eq!(SettingValue::from(true), SettingValue::Bool(true));
        assert!((SettingValue::from(3.15_f32).unwrap_float() - 3.15).abs() < 1e-6);
        assert_eq!(SettingValue::from(3.15_f64), SettingValue::Float(3.15));
        assert_eq!(SettingValue::from("test"), SettingValue::String("test".to_string()));
        assert_eq!(
            SettingValue::from("test".to_string()),
            SettingValue::String("test".to_string())
        );
        assert_eq!(
            SettingValue::from(Box::<str>::from("test")),
            SettingValue::String("test".to_string())
        );
        assert_eq!(
            SettingValue::from(std::sync::Arc::<str>::from("test")),
            SettingValue::String("test".to_string())
        );
    }

    #[test]
    fn test_setting_value_display() {
        // Test Display implementation for SettingValue
        assert_eq!(SettingValue::Int(42).to_string(), "42");
        assert_eq!(SettingValue::Bool(true).to_string(), "true");
        assert_eq!(SettingValue::Float(3.15).to_string(), "3.15");
        assert_eq!(SettingValue::String("test".to_string()).to_string(), "test");
    }

    #[test]
    fn test_setting_encode_field_dump() {
        // Test encode_field_dump for string values
        let setting = create_setting("key", "value", false, true);
        assert_eq!(setting.encode_field_dump().unwrap(), "value");

        // Test string with quotes
        let setting = create_setting("key", "val'ue", false, true);
        assert_eq!(setting.encode_field_dump().unwrap(), "val'ue");

        // Test non-string value (should error)
        let setting = create_setting("key", 42_i32, false, true);
        assert!(setting.encode_field_dump().is_err());
    }

    #[tokio::test]
    async fn test_setting_encode_legacy_revision() {
        // Test encode for legacy revision (≤ DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
        let setting = create_setting("max_threads", 8_i32, false, false);
        let mut writer = MockWriter::default();
        setting
            .encode(&mut writer, DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        // Decode and verify using ClickHouseRead
        let mut reader = Cursor::new(writer.into_inner());
        let key = reader.read_utf8_string().await.unwrap();
        assert_eq!(key, "max_threads");
        let value = reader.read_var_uint().await.unwrap();
        assert_eq!(value, 8);

        // Test boolean setting
        let setting = create_setting("allow_experimental", true, false, false);
        let mut writer = MockWriter::default();
        setting
            .encode(&mut writer, DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let key = reader.read_utf8_string().await.unwrap();
        assert_eq!(key, "allow_experimental");
        let value = reader.read_var_uint().await.unwrap();
        assert_eq!(value, 1);

        // Test unsupported type (should error)
        let setting = create_setting("default_format", "JSON", false, false);
        let mut writer = MockWriter::default();
        assert!(matches!(
            setting
                .encode(&mut writer, DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
                .await,
            Err(Error::UnsupportedSettingType(key)) if key == "default_format"
        ));
    }

    #[tokio::test]
    async fn test_setting_encode_modern_revision() {
        // Test encode for modern revision (> DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
        let setting = create_setting("max_threads", 8_i32, false, false);
        let mut writer = MockWriter::default();
        setting
            .encode(&mut writer, DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS + 1)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        // Decode and verify using ClickHouseRead
        let mut reader = Cursor::new(writer.into_inner());
        let key = reader.read_utf8_string().await.unwrap();
        assert_eq!(key, "max_threads");
        let flags = reader.read_var_uint().await.unwrap();
        assert_eq!(flags, 0);
        let value = reader.read_utf8_string().await.unwrap();
        assert_eq!(value, "8");

        // Test with important and custom flags
        let setting = create_setting("custom_key", "value", true, true);
        let mut writer = MockWriter::default();
        setting
            .encode(&mut writer, DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS + 1)
            .await
            .unwrap();
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let key = reader.read_utf8_string().await.unwrap();
        assert_eq!(key, "custom_key");
        let flags = reader.read_var_uint().await.unwrap();
        assert_eq!(flags, SETTING_FLAG_IMPORTANT | SETTING_FLAG_CUSTOM);
        let value = reader.read_utf8_string().await.unwrap();
        assert_eq!(value, "value");
    }

    #[test]
    fn test_settings_add_setting() {
        let mut settings = Settings::default();
        settings.add_setting("max_threads", 8_i32);
        settings.add_setting("default_format", "JSON");

        assert_eq!(settings.0.len(), 2);
        assert_eq!(settings.0[0].key, "max_threads");
        assert_eq!(settings.0[0].value, SettingValue::Int(8));
        assert_eq!(settings.0[1].key, "default_format");
        assert_eq!(settings.0[1].value, SettingValue::String("JSON".to_string()));
    }

    #[test]
    fn test_settings_from_iterator() {
        let settings = Settings::from(vec![
            ("max_threads".to_string(), SettingValue::Int(8)),
            ("allow_experimental".to_string(), SettingValue::Bool(true)),
        ]);

        assert_eq!(settings.0.len(), 2);
        assert_eq!(settings.0[0].key, "max_threads");
        assert_eq!(settings.0[0].value, SettingValue::Int(8));
        assert_eq!(settings.0[1].key, "allow_experimental");
        assert_eq!(settings.0[1].value, SettingValue::Bool(true));
    }

    #[test]
    fn test_settings_encode_to_key_value_strings() {
        let settings = Settings::from(vec![
            ("max_threads".to_string(), SettingValue::Int(8)),
            ("default_format".to_string(), "JSON".into()),
        ]);

        let kv_pairs = settings.encode_to_key_value_strings();
        assert_eq!(kv_pairs, vec![
            ("max_threads".to_string(), "8".to_string()),
            ("default_format".to_string(), "JSON".to_string()),
        ]);
    }

    #[test]
    fn test_settings_encode_to_strings() {
        let settings = Settings::from(vec![
            ("max_threads".to_string(), SettingValue::Int(8)),
            ("default_format".to_string(), "JSON".into()),
        ]);

        let strings = settings.encode_to_strings();
        assert_eq!(strings, vec!["max_threads = 8", "default_format = JSON"]);
    }

    #[tokio::test]
    async fn test_settings_encode() {
        let settings = Settings::from(vec![
            ("max_threads".to_string(), SettingValue::Int(8)),
            ("allow_experimental".to_string(), SettingValue::Bool(true)),
        ]);

        let mut writer = MockWriter::default();
        settings
            .encode(&mut writer, DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS + 1)
            .await
            .unwrap();
        writer.write_string("").await.unwrap();
        writer.flush().await.unwrap();

        // Decode and verify using ClickHouseRead
        let mut reader = Cursor::new(writer.into_inner());
        let key1 = reader.read_utf8_string().await.unwrap();
        assert_eq!(key1, "max_threads");
        let flags1 = reader.read_var_uint().await.unwrap();
        assert_eq!(flags1, 0);
        let value1 = reader.read_utf8_string().await.unwrap();
        assert_eq!(value1, "8");

        let key2 = reader.read_utf8_string().await.unwrap();
        assert_eq!(key2, "allow_experimental");
        let flags2 = reader.read_var_uint().await.unwrap();
        assert_eq!(flags2, 0);
        let value2 = reader.read_utf8_string().await.unwrap();
        assert_eq!(value2, "true");
    }

    #[test]
    fn test_settings_deref() {
        let settings = Settings::from(vec![("max_threads".to_string(), 8_i32)]);
        let slice: &[Setting] = &settings;
        assert_eq!(slice.len(), 1);
        assert_eq!(slice[0].key, "max_threads");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_serialization() {
        use serde_json;

        let settings = Settings::from(vec![
            ("max_threads".to_string(), SettingValue::Int(8)),
            ("allow_experimental".to_string(), SettingValue::Bool(true)),
            ("default_format".to_string(), "JSON".into()),
        ]);

        let json = serde_json::to_string(&settings).unwrap();
        let deserialized: Settings = serde_json::from_str(&json).unwrap();
        assert_eq!(settings, deserialized);

        // Test single Setting
        let setting = create_setting("max_threads", 8_i32, true, false);
        let json = serde_json::to_string(&setting).unwrap();
        let deserialized: Setting = serde_json::from_str(&json).unwrap();
        assert_eq!(setting, deserialized);
    }

    #[tokio::test]
    async fn test_settings_decode_empty() {
        // Test decoding empty settings (just end marker)
        let mut writer = Cursor::new(Vec::new());

        // Write empty string as end marker
        writer.write_string("").await.unwrap();
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let settings = Settings::decode(&mut reader).await.unwrap();

        assert_eq!(settings.0.len(), 0);
    }

    #[tokio::test]
    async fn test_settings_decode_single_standard_setting() {
        // Test decoding a single standard (non-custom) setting
        let mut writer = MockWriter::default();

        // Write setting: name -> flags -> value -> end marker
        writer.write_string("max_threads").await.unwrap();
        writer.write_var_uint(0).await.unwrap(); // No flags
        writer.write_string("8").await.unwrap();
        writer.write_string("").await.unwrap(); // End marker
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let settings = Settings::decode(&mut reader).await.unwrap();

        assert_eq!(settings.0.len(), 1);
        assert_eq!(settings.0[0].key, "max_threads");
        assert_eq!(settings.0[0].value, SettingValue::Int(8));
        assert!(!settings.0[0].important);
        assert!(!settings.0[0].custom);
    }

    #[tokio::test]
    async fn test_settings_decode_custom_setting() {
        // Test decoding a custom setting
        let mut writer = MockWriter::default();

        // Write custom setting: name -> custom flag -> field dump -> end marker
        writer.write_string("custom_setting").await.unwrap();
        writer.write_var_uint(SETTING_FLAG_CUSTOM).await.unwrap();
        writer.write_string("custom_value").await.unwrap();
        writer.write_string("").await.unwrap(); // End marker
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let settings = Settings::decode(&mut reader).await.unwrap();

        assert_eq!(settings.0.len(), 1);
        assert_eq!(settings.0[0].key, "custom_setting");
        assert_eq!(settings.0[0].value, SettingValue::String("custom_value".to_string()));
        assert!(!settings.0[0].important);
        assert!(settings.0[0].custom);
    }

    #[tokio::test]
    async fn test_settings_decode_important_setting() {
        // Test decoding an important setting
        let mut writer = MockWriter::default();

        // Write important setting
        writer.write_string("critical_setting").await.unwrap();
        writer.write_var_uint(SETTING_FLAG_IMPORTANT).await.unwrap();
        writer.write_string("true").await.unwrap();
        writer.write_string("").await.unwrap(); // End marker
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let settings = Settings::decode(&mut reader).await.unwrap();

        assert_eq!(settings.0.len(), 1);
        assert_eq!(settings.0[0].key, "critical_setting");
        assert_eq!(settings.0[0].value, SettingValue::Bool(true));
        assert!(settings.0[0].important);
        assert!(!settings.0[0].custom);
    }

    #[tokio::test]
    async fn test_settings_decode_multiple_settings() {
        // Test decoding multiple settings with different types and flags
        let mut writer = MockWriter::default();

        // Setting 1: Standard integer
        writer.write_string("max_threads").await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("4").await.unwrap();

        // Setting 2: Important boolean
        writer.write_string("allow_experimental").await.unwrap();
        writer.write_var_uint(SETTING_FLAG_IMPORTANT).await.unwrap();
        writer.write_string("false").await.unwrap();

        // Setting 3: Custom setting
        writer.write_string("custom_config").await.unwrap();
        writer.write_var_uint(SETTING_FLAG_CUSTOM).await.unwrap();
        writer.write_string("custom_data").await.unwrap();

        // Setting 4: Important + Custom
        writer.write_string("important_custom").await.unwrap();
        writer.write_var_uint(SETTING_FLAG_IMPORTANT | SETTING_FLAG_CUSTOM).await.unwrap();
        writer.write_string("special_value").await.unwrap();

        // Setting 5: Float value
        writer.write_string("timeout_ratio").await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("1.5").await.unwrap();

        // End marker
        writer.write_string("").await.unwrap();
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let settings = Settings::decode(&mut reader).await.unwrap();

        assert_eq!(settings.0.len(), 5);

        // Verify Setting 1
        assert_eq!(settings.0[0].key, "max_threads");
        assert_eq!(settings.0[0].value, SettingValue::Int(4));
        assert!(!settings.0[0].important);
        assert!(!settings.0[0].custom);

        // Verify Setting 2
        assert_eq!(settings.0[1].key, "allow_experimental");
        assert_eq!(settings.0[1].value, SettingValue::Bool(false));
        assert!(settings.0[1].important);
        assert!(!settings.0[1].custom);

        // Verify Setting 3
        assert_eq!(settings.0[2].key, "custom_config");
        assert_eq!(settings.0[2].value, SettingValue::String("custom_data".to_string()));
        assert!(!settings.0[2].important);
        assert!(settings.0[2].custom);

        // Verify Setting 4
        assert_eq!(settings.0[3].key, "important_custom");
        assert_eq!(settings.0[3].value, SettingValue::String("special_value".to_string()));
        assert!(settings.0[3].important);
        assert!(settings.0[3].custom);

        // Verify Setting 5
        assert_eq!(settings.0[4].key, "timeout_ratio");
        assert_eq!(settings.0[4].value, SettingValue::Float(1.5));
        assert!(!settings.0[4].important);
        assert!(!settings.0[4].custom);
    }

    #[tokio::test]
    async fn test_settings_decode_parse_setting_value_edge_cases() {
        // Test various edge cases for value parsing
        let mut writer = MockWriter::default();

        // Test "0" as boolean false
        writer.write_string("bool_zero").await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("0").await.unwrap();

        // Test "1" as boolean true
        writer.write_string("bool_one").await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("1").await.unwrap();

        // Test negative integer
        writer.write_string("negative_int").await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("-42").await.unwrap();

        // Test string that looks like number but isn't parseable as int/float
        writer.write_string("string_val").await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("not_a_number").await.unwrap();

        // Test empty string value
        writer.write_string("empty_val").await.unwrap();
        writer.write_var_uint(0).await.unwrap();
        writer.write_string("").await.unwrap();

        // End marker
        writer.write_string("").await.unwrap();
        writer.flush().await.unwrap();

        let mut reader = Cursor::new(writer.into_inner());
        let settings = Settings::decode(&mut reader).await.unwrap();

        assert_eq!(settings.0.len(), 5);
        assert_eq!(settings.0[0].value, SettingValue::Bool(false)); // "0" -> false
        assert_eq!(settings.0[1].value, SettingValue::Bool(true)); // "1" -> true
        assert_eq!(settings.0[2].value, SettingValue::Int(-42)); // "-42" -> int
        assert_eq!(settings.0[3].value, SettingValue::String("not_a_number".to_string())); // fallback to string
        assert_eq!(settings.0[4].value, SettingValue::String(String::new())); // empty string
    }

    #[tokio::test]
    async fn test_settings_decode_roundtrip() {
        // Test that encode -> decode produces the same data
        let original_settings = Settings::from(vec![
            ("max_threads".to_string(), SettingValue::Int(8)),
            ("allow_experimental".to_string(), SettingValue::Bool(true)),
            ("custom_setting".to_string(), SettingValue::String("custom_value".to_string())),
        ]);

        // Mark one as custom for testing
        let mut settings_with_custom = original_settings.clone();
        settings_with_custom.0[2].custom = true;
        settings_with_custom.0[1].important = true;

        // Encode
        let mut writer = MockWriter::default();
        settings_with_custom
            .encode(&mut writer, DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS + 1)
            .await
            .unwrap();
        writer.write_string("").await.unwrap();
        writer.flush().await.unwrap();

        // Decode
        let mut reader = Cursor::new(writer.into_inner());

        let decoded_settings = Settings::decode(&mut reader).await.unwrap();

        // Compare (note: encode doesn't write end marker, so we need to account for that)
        assert_eq!(decoded_settings.0.len(), 3);
        assert_eq!(decoded_settings.0[0].key, "max_threads");
        assert_eq!(decoded_settings.0[0].value, SettingValue::Int(8));
        assert!(!decoded_settings.0[0].important);
        assert!(!decoded_settings.0[0].custom);

        assert_eq!(decoded_settings.0[1].key, "allow_experimental");
        assert_eq!(decoded_settings.0[1].value, SettingValue::Bool(true));
        assert!(decoded_settings.0[1].important);
        assert!(!decoded_settings.0[1].custom);

        assert_eq!(decoded_settings.0[2].key, "custom_setting");
        assert_eq!(decoded_settings.0[2].value, SettingValue::String("custom_value".to_string()));
        assert!(!decoded_settings.0[2].important);
        assert!(decoded_settings.0[2].custom);
    }

    #[test]
    fn test_from_iterator_no_stack_overflow() {
        // This test verifies the fix for issue #52 - FromIterator was causing stack overflow
        // by calling .collect() which called from_iter which called .collect() again

        let data = vec![
            ("param1", SettingValue::from("value1")),
            ("param2", SettingValue::from(42_i32)),
            ("param3", SettingValue::from(true)),
        ];

        // This used to cause stack overflow before the fix
        let settings: Settings = data.into_iter().collect();

        assert_eq!(settings.0.len(), 3);
        assert_eq!(settings.0[0].key, "param1");
        assert_eq!(settings.0[0].value, SettingValue::String("value1".to_string()));
        assert_eq!(settings.0[1].key, "param2");
        assert_eq!(settings.0[1].value, SettingValue::Int(42));
        assert_eq!(settings.0[2].key, "param3");
        assert_eq!(settings.0[2].value, SettingValue::Bool(true));
    }

    #[test]
    fn test_setting_value_from_integer_arrays() {
        // Test Vec<T> conversions
        assert_eq!(
            SettingValue::from(vec![1_i32, 2_i32, 3_i32]),
            SettingValue::String("[1,2,3]".to_string())
        );
        assert_eq!(
            SettingValue::from(vec![1_i64, 2_i64, 3_i64]),
            SettingValue::String("[1,2,3]".to_string())
        );
        assert_eq!(
            SettingValue::from(vec![1_u32, 2_u32, 3_u32]),
            SettingValue::String("[1,2,3]".to_string())
        );

        // Test &[T] conversions
        let arr = [1_i32, 2_i32, 3_i32];
        assert_eq!(SettingValue::from(&arr[..]), SettingValue::String("[1,2,3]".to_string()));

        // Test empty array
        assert_eq!(SettingValue::from(Vec::<i32>::new()), SettingValue::String("[]".to_string()));
    }

    #[test]
    fn test_setting_value_from_float_arrays() {
        assert_eq!(
            SettingValue::from(vec![1.5_f64, 2.5_f64, 3.15_f64]),
            SettingValue::String("[1.5,2.5,3.15]".to_string())
        );
        assert_eq!(
            SettingValue::from(vec![1.5_f32, 2.5_f32]),
            SettingValue::String("[1.5,2.5]".to_string())
        );

        // Test &[T] conversion
        let arr = [1.5_f64, 2.5_f64];
        assert_eq!(SettingValue::from(&arr[..]), SettingValue::String("[1.5,2.5]".to_string()));
    }

    #[test]
    fn test_setting_value_from_string_arrays() {
        // Test Vec<String>
        assert_eq!(
            SettingValue::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
            SettingValue::String("['a','b','c']".to_string())
        );

        // Test Vec<&str>
        assert_eq!(
            SettingValue::from(vec!["a", "b", "c"]),
            SettingValue::String("['a','b','c']".to_string())
        );

        // Test &[&str]
        let arr = ["a", "b", "c"];
        assert_eq!(SettingValue::from(&arr[..]), SettingValue::String("['a','b','c']".to_string()));

        // Test string with quotes (should be escaped)
        assert_eq!(
            SettingValue::from(vec!["it's", "a", "test"]),
            SettingValue::String("['it\\'s','a','test']".to_string())
        );

        // Test empty array
        assert_eq!(
            SettingValue::from(Vec::<String>::new()),
            SettingValue::String("[]".to_string())
        );
    }

    #[test]
    fn test_setting_value_array_edge_cases() {
        // Single element
        assert_eq!(SettingValue::from(vec![42_i32]), SettingValue::String("[42]".to_string()));

        // Large numbers
        assert_eq!(
            SettingValue::from(vec![i64::MAX, i64::MIN]),
            SettingValue::String(format!("[{},{}]", i64::MAX, i64::MIN))
        );

        // Multiple quotes in strings
        assert_eq!(
            SettingValue::from(vec!["'quoted'", "normal"]),
            SettingValue::String("['\\'quoted\\'','normal']".to_string())
        );
    }
}
