mod arrow;
mod native;
pub(crate) mod protocol_data;

// Re-exports
pub use arrow::ArrowFormat;
pub use native::NativeFormat;

use crate::ArrowOptions;

/// Marker trait for various client formats.
///
/// Currently only two formats are in use: `ArrowFormat` and `NativeFormat`. This approach provides
/// a simple mechanism to introduce new formats to work with `ClickHouse` data without a lot of
/// overhead and a fullblown serde implementation.
#[expect(private_bounds)]
pub trait ClientFormat: sealed::ClientFormatImpl<Self::Data> + Send + Sync + 'static {
    type Data: std::fmt::Debug + Clone + Send + Sync + 'static;

    const FORMAT: &'static str;
}

pub(crate) mod sealed {
    use crate::client::connection::ConnectionMetadata;
    use crate::errors::Result;
    use crate::query::Qid;
    use crate::{ClickhouseRead, ClickhouseWrite, Type};

    pub(crate) trait ClientFormatImpl<T>: std::fmt::Debug
    where
        T: std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        type Schema: std::fmt::Debug + Clone + Send + Sync + 'static;

        fn read<R: ClickhouseRead + 'static>(
            reader: &mut R,
            metadata: ConnectionMetadata,
        ) -> impl Future<Output = Result<Option<T>>> + Send + '_;

        fn write<'a, W: ClickhouseWrite + 'static>(
            writer: &'a mut W,
            data: T,
            qid: Qid,
            header: Option<&'a [(String, Type)]>,
            metadata: ConnectionMetadata,
        ) -> impl Future<Output = Result<()>> + Send + 'a;
    }
}

/// Context maintained during deserialization
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeserializerState {
    pub(crate) options: Option<ArrowOptions>,
}

impl DeserializerState {
    #[must_use]
    pub(crate) fn with_arrow_options(mut self, options: ArrowOptions) -> Self {
        self.options = Some(options);
        self
    }
}

/// Context maintained during serialization
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SerializerState {
    pub(crate) options: Option<ArrowOptions>,
}

impl SerializerState {
    #[must_use]
    pub(crate) fn with_arrow_options(mut self, options: ArrowOptions) -> Self {
        self.options = Some(options);
        self
    }
}
