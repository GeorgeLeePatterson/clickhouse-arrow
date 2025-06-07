//! ## Convenience exports for working with the library.
pub use tracing::{Instrument, Span, debug, error, info, instrument, trace, trace_span, warn};

pub use crate::arrow::types::SchemaConversions;
pub use crate::errors::*;
pub use crate::formats::{ArrowFormat, ClientFormat, NativeFormat};
pub use crate::native::protocol::*;
pub use crate::native::values::*;
pub use crate::query::{ParamValue, ParsedQuery, Qid, QueryParams};
pub use crate::schema::*;
pub use crate::settings::*;
pub use crate::telemetry::*;
pub use crate::{ArrowClient, Client, ClientBuilder, CompressionMethod, NativeClient, Row};

// TODO: Encrypt
/// Newtype to protect secrets from being logged
#[derive(Clone, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
pub struct Secret(String);

impl Secret {
    pub fn new<P: AsRef<str>>(s: P) -> Self { Self(s.as_ref().to_string()) }

    #[must_use]
    pub fn get(&self) -> &str { &self.0 }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Password(*****)")
    }
}

impl<T: AsRef<str>> From<T> for Secret {
    fn from(s: T) -> Self { Self(s.as_ref().to_string()) }
}

/// Custom Deserialize implementation to prevent storing passwords
#[cfg(feature = "serde")]
impl serde::Serialize for Secret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{self:?}"))
    }
}
