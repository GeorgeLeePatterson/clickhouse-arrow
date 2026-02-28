mod arrow;
mod native;
pub(crate) mod protocol_data;

// Re-exports
pub use arrow::ArrowFormat;
pub use native::NativeFormat;

use crate::ArrowOptions;
#[cfg(feature = "extended-types")]
use crate::Type;

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
    use super::{DeserializerState, SerializerState};
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
        type Ser: Default + Send + Sync + 'static;

        #[expect(unused)]
        fn finish_ser(_state: &mut SerializerState<Self::Ser>) {}

        fn finish_deser(_state: &mut DeserializerState<Self::Deser>) {}

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
    }
}

#[cfg(feature = "extended-types")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DynamicPrefixState {
    pub(crate) serialization_version: u64,
    pub(crate) flattened_types:       Vec<Type>,
}

#[cfg(feature = "extended-types")]
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub(crate) struct VariantPrefixState {
    pub(crate) discriminator_mode: u8,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CustomSerializationEntry {
    pub(crate) stack_type: u8,
    pub(crate) kinds:      Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CustomSerializationState {
    pub(crate) entries: Vec<CustomSerializationEntry>,
}

/// Context maintained during deserialization
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DeserializerState<T: Default = ()> {
    pub(crate) format_state: T,
    custom_serialization:    Option<CustomSerializationState>,
    #[cfg(feature = "extended-types")]
    dynamic_prefix:          Option<DynamicPrefixState>,
    #[cfg(feature = "extended-types")]
    variant_prefix:          Option<VariantPrefixState>,
}

impl<T: Default> DeserializerState<T> {
    #[must_use]
    pub(crate) fn format_state(&mut self) -> &mut T { &mut self.format_state }

    pub(crate) fn replace_custom_serialization(
        &mut self,
        custom_serialization: CustomSerializationState,
    ) -> Option<CustomSerializationState> {
        self.custom_serialization.replace(custom_serialization)
    }

    pub(crate) fn take_custom_serialization(&mut self) -> Option<CustomSerializationState> {
        self.custom_serialization.take()
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn replace_dynamic_prefix(
        &mut self,
        dynamic_prefix: DynamicPrefixState,
    ) -> Option<DynamicPrefixState> {
        self.dynamic_prefix.replace(dynamic_prefix)
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn take_dynamic_prefix(&mut self) -> Option<DynamicPrefixState> {
        self.dynamic_prefix.take()
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn replace_variant_prefix(
        &mut self,
        variant_prefix: VariantPrefixState,
    ) -> Option<VariantPrefixState> {
        self.variant_prefix.replace(variant_prefix)
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn take_variant_prefix(&mut self) -> Option<VariantPrefixState> {
        self.variant_prefix.take()
    }
}

/// Context maintained during serialization
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SerializerState<T: Default = ()> {
    pub(crate) options:    Option<ArrowOptions>,
    pub(crate) serializer: T,
    custom_serialization:  Option<CustomSerializationState>,
    #[cfg(feature = "extended-types")]
    dynamic_prefix:        Option<DynamicPrefixState>,
}

impl<T: Default> SerializerState<T> {
    #[must_use]
    pub(crate) fn with_arrow_options(mut self, options: ArrowOptions) -> Self {
        self.options = Some(options);
        self
    }

    #[expect(unused)]
    #[must_use]
    pub(crate) fn serializer(&mut self) -> &mut T { &mut self.serializer }

    #[allow(dead_code)]
    pub(crate) fn replace_custom_serialization(
        &mut self,
        custom_serialization: CustomSerializationState,
    ) -> Option<CustomSerializationState> {
        self.custom_serialization.replace(custom_serialization)
    }

    #[allow(dead_code)]
    pub(crate) fn take_custom_serialization(&mut self) -> Option<CustomSerializationState> {
        self.custom_serialization.take()
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn replace_dynamic_prefix(
        &mut self,
        dynamic_prefix: DynamicPrefixState,
    ) -> Option<DynamicPrefixState> {
        self.dynamic_prefix.replace(dynamic_prefix)
    }

    #[cfg(feature = "extended-types")]
    pub(crate) fn take_dynamic_prefix(&mut self) -> Option<DynamicPrefixState> {
        self.dynamic_prefix.take()
    }
}
