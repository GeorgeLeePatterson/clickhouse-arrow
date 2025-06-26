use arrow::array::*;
use arrow::datatypes::*;

use crate::io::ClickHouseBytesRead;
use crate::{Result, Type};

// TODO: Remove or keep
pub(crate) mod column {
    use crate::row::builder::TypedBuilder;

    #[expect(unused)]
    pub(crate) enum TypedColumnListBuilder {
        List(Box<TypedBuilder>),
        LargeList(Box<TypedBuilder>),
        FixedList(Box<TypedBuilder>),
    }
}

pub(crate) mod row {
    use std::sync::Arc;

    use super::super::typed_arrow_build;
    use super::*;
    use crate::Error;
    use crate::row::{deserialize, dynamic};

    pub(crate) enum TypedListBuilder {
        // List types
        List(ListBuilder<Box<dyn ArrayBuilder>>),
        LargeList(LargeListBuilder<Box<dyn ArrayBuilder>>),
        FixedList(FixedSizeListBuilder<Box<dyn ArrayBuilder>>),
    }

    impl TypedListBuilder {
        pub(crate) fn try_new(type_: &Type, data_type: &DataType, name: &str) -> Result<Self> {
            // Handle complex nested types
            if !matches!(type_, Type::Array(_)) {
                return Err(Error::UnexpectedType(type_.clone()));
            }

            Ok(typed_arrow_build!(TypedListBuilder, data_type, {
                DataType::List(f) => (
                    List, ListBuilder::new(dynamic::builder::create_builder(type_, f.data_type(), name)?)
                ),
                DataType::LargeList(f) => (
                    LargeList,
                    LargeListBuilder::new(dynamic::builder::create_builder(type_, f.data_type(), name)?)
                ),
                DataType::FixedSizeList(f, n) => (
                    FixedList,
                    FixedSizeListBuilder::new(
                        dynamic::builder::create_builder(type_, f.data_type(), name)?, *n
                    )
                ),
            }))
        }

        /// Append a null value to this builder
        pub(crate) fn append_null<R: ClickHouseBytesRead>(
            &mut self,
            reader: &mut R,
            type_: &Type,
        ) -> Result<bool> {
            if !type_.is_nullable() {
                return Ok(false);
            }
            let is_null = reader
                .try_get_u8()
                .map_err(|e| Error::Protocol(format!("Failed to read null flag: {e}")))?
                != 0;
            if is_null {
                match self {
                    Self::List(b) => b.append_null(),
                    Self::LargeList(b) => b.append_null(),
                    Self::FixedList(b) => b.append(false),
                }
            }
            Ok(is_null)
        }

        /// Append a null value to this builder
        pub(crate) fn append_value<R: ClickHouseBytesRead>(
            &mut self,
            reader: &mut R,
            type_: &Type,
        ) -> Result<()> {
            match self {
                Self::List(b) => deserialize::list::row::list(reader, type_, b)?,
                Self::LargeList(b) => deserialize::list::row::list_large(reader, type_, b)?,
                Self::FixedList(b) => deserialize::list::row::list_fixed(reader, type_, b)?,
            }
            Ok(())
        }

        /// Finish building and return the array
        pub(crate) fn finish(&mut self) -> ArrayRef {
            match self {
                Self::List(b) => Arc::new(b.finish()),
                Self::LargeList(b) => Arc::new(b.finish()),
                Self::FixedList(b) => Arc::new(b.finish()),
            }
        }
    }
}

// TODO: Remove
// impl TypedListBuilder {
//     pub fn new(typed_builder: TypedBuilder) -> Self { TypedListBuilder(Box::new(typed_builder)) }
// }

// impl TypedListBuilder {
//     /// Append a null value to this builder
//     #[expect(clippy::too_many_lines)]
//     pub(crate) fn create_columns_array<R: ClickHouseBytesRead>(
//         &mut self,
//         type_: &Type,
//         reader: &mut R,
//         nulls: &[u8],
//         rows: usize,
//     ) -> Result<ArrayRef> {
//         self.0.create_columns_array(type_, reader, nulls, rows)
//     }
// }
