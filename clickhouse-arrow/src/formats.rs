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
    #[cfg(feature = "row_binary")]
    use futures_util::Stream;

    use super::DeserializerState;
    use crate::Type;
    use crate::client::connection::ClientMetadata;
    use crate::errors::Result;
    use crate::io::{ClickHouseRead, ClickHouseWrite};
    use crate::query::Qid;

    pub(crate) trait ClientFormatImpl<T>: std::fmt::Debug
    where
        T: std::fmt::Debug + Clone + Send + Sync + 'static,
    {
        type Schema: std::fmt::Debug + Clone + Send + Sync + 'static;
        type Deser: Default + Send + Sync + 'static;

        fn reset_state(_state: &mut DeserializerState<Self::Deser>) {}

        fn write<'a, W: ClickHouseWrite>(
            writer: &'a mut W,
            data: T,
            qid: Qid,
            header: Option<&'a [(String, Type)]>,
            revision: u64,
            metadata: ClientMetadata,
        ) -> impl Future<Output = Result<()>> + Send + 'a;

        fn read<'a, R: ClickHouseRead + 'static>(
            reader: &'a mut R,
            revision: u64,
            metadata: ClientMetadata,
            state: &'a mut DeserializerState<Self::Deser>,
        ) -> impl Future<Output = Result<Option<T>>> + Send + 'a;

        // Only relevant for RowBinary
        #[cfg(feature = "row_binary")]
        fn read_rows<S>(
            _reader: &mut S,
            _schema: Self::Schema,
            _overrides: Option<crate::prelude::SchemaConversions>,
            _metadata: ClientMetadata,
            _summmary: crate::row::protocol::HttpSummary,
        ) -> impl Future<Output = Result<Vec<T>>> + Send
        where
            S: Stream<Item = Result<bytes::Bytes>> + Unpin + Send,
        {
            async { unimplemented!() }
        }
    }
}

/// Context maintained during deserialization
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeserializerState<T: Default = ()> {
    pub(crate) options:      Option<ArrowOptions>,
    pub(crate) deserializer: T,
}

impl<T: Default> DeserializerState<T> {
    #[must_use]
    pub(crate) fn with_arrow_options(mut self, options: ArrowOptions) -> Self {
        self.options = Some(options);
        self
    }

    #[must_use]
    pub(crate) fn deserializer(&mut self) -> &mut T { &mut self.deserializer }
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
