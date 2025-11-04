use super::DeserializerState;
use crate::io::{ClickHouseBytesRead, ClickHouseBytesWrite, ClickHouseRead, ClickHouseWrite};
use crate::{Result, Type};

/// Trait for serializing and deserializing data into `ClickHouse`'s native block format.
///
/// This trait defines methods for writing data to a `ClickHouse` native block and reading data from
/// a native block, typically used for communication over the TCP protocol. Implementations handle
/// the conversion between in-memory data structures (e.g., Arrow `RecordBatch`) and the wire
/// format, including block headers, column metadata, and compressed data.
///
/// # Type Parameters
/// - `Return`: The type of data produced by deserialization (e.g., `RecordBatch`).
pub(crate) trait ProtocolData<Return, Deser: Default> {
    /// The implementation specific options
    type Options;

    /// Writes the data to a `ClickHouse` native block.
    ///
    /// # Arguments
    /// - `writer`: The async writer to serialize the block to (e.g., a TCP stream).
    /// - `header`: Optional column name and type mappings for type disambiguation.
    ///
    /// # Returns
    /// A `Future` resolving to a `Result` indicating success or a `Error` if
    /// serialization fails.
    fn write_async<W: ClickHouseWrite>(
        self,
        writer: &mut W,
        revision: u64,
        header: Option<&[(String, Type)]>,
        options: Self::Options,
    ) -> impl Future<Output = Result<()>> + Send;

    fn write<W: ClickHouseBytesWrite>(
        self,
        _writer: &mut W,
        _revision: u64,
        _header: Option<&[(String, Type)]>,
        _options: Self::Options,
    ) -> Result<()>
    where
        Self: Sized;

    /// Reads a `ClickHouse` native block and constructs the data.
    ///
    /// # Arguments
    /// - `reader`: The async reader providing the block data (e.g., a TCP stream).
    /// - `revision`: The protocol revision to use for deserialization.
    /// - `strings_as_strings`: If `true`, `ClickHouse` `String` types are deserialized as Arrow
    ///   `Utf8`; otherwise, as `Binary`.
    ///
    /// # Returns
    /// A `Future` resolving to a `Result` of `Return` (e.g., `RecordBatch`) or
    /// a `Error` if deserialization fails.
    fn read_async<R: ClickHouseRead>(
        reader: &mut R,
        revision: u64,
        options: Self::Options,
        state: &mut DeserializerState<Deser>,
    ) -> impl Future<Output = Result<Return>> + Send;

    #[allow(dead_code)] // TODO: remove once synchronous ProtocolData path is fully retired
    fn read<R: ClickHouseBytesRead + 'static>(
        _reader: &mut R,
        _revision: u64,
        _options: Self::Options,
        _state: &mut DeserializerState<Deser>,
    ) -> Result<Return>;
}

/// Simple trait to determine whether a `Block` of data (whatever impls `ProtocolData`) is empty, ie
/// no columns no rows. `ClickHouse` sometimes sends data with no columns and no rows, internally
/// these are ignored.
pub(crate) trait EmptyBlock {
    fn no_data(&self) -> bool;

    fn into_option(self) -> Option<Self>
    where
        Self: Sized,
    {
        (!self.no_data()).then_some(self)
    }
}

impl EmptyBlock for crate::native::block::Block {
    fn no_data(&self) -> bool { self.column_data.is_empty() && self.column_types.is_empty() }
}

impl EmptyBlock for arrow::record_batch::RecordBatch {
    fn no_data(&self) -> bool { self.num_rows() == 0 && self.num_columns() == 0 }
}
