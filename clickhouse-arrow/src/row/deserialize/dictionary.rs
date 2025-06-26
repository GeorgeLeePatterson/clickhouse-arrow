pub(crate) mod row {
    use arrow::array::*;
    use arrow::datatypes::*;

    use crate::io::ClickHouseBytesRead;
    use crate::row::deserialize::binary::binary;
    use crate::row::deserialize::primitive::primitive;
    use crate::row::dynamic::deserialize;
    use crate::{Error, Result, Type};

    #[inline]
    pub(crate) fn read_column<R: ClickHouseBytesRead, DictKey: ArrowDictionaryKeyType>(
        reader: &mut R,
        type_: &Type,
        builder: &mut dyn ArrayBuilder,
        null_check_only: bool,
    ) -> Result<()> {
        type PrimDict<Key, Val> = PrimitiveDictionaryBuilder<Key, Val>;

        let nil = type_.is_nullable();
        deserialize!(type_.strip_null(), reader, builder, null_check_only => {
            // Numeric, DateTime, Decimal
            Type::Int8 => (PrimDict::<DictKey, Int8Type>, nil, b => { b.append_value(primitive!(Int8 => reader)) }),
            Type::Int16 => (PrimDict::<DictKey, Int16Type>, nil, b => { b.append_value(primitive!(Int16 => reader)) }),
            Type::Int32 => (PrimDict::<DictKey, Int32Type>, nil, b => { b.append_value(primitive!(Int32 => reader)) }),
            Type::Int64 => (PrimDict::<DictKey, Int64Type>, nil, b => { b.append_value(primitive!(Int64 => reader)) }),
            Type::UInt8 => (PrimDict::<DictKey, UInt8Type>, nil, b => { b.append_value(primitive!(UInt8 => reader)) }),
            Type::UInt16 => (PrimDict::<DictKey, UInt16Type>, nil, b => {
                b.append_value(primitive!(UInt16 => reader))
            }),
            Type::UInt32 => (PrimDict::<DictKey, UInt32Type>, nil, b => {
                b.append_value(primitive!(UInt32 => reader))
            }),
            Type::UInt64 => (PrimDict::<DictKey, UInt64Type>, nil, b => {
                b.append_value(primitive!(UInt64 => reader))
            }),
            Type::Float32 => (PrimDict::<DictKey, Float32Type>, nil, b => {
                b.append_value(primitive!(Float32 => reader))
            }),
            Type::Float64 => (PrimDict::<DictKey, Float64Type>, nil, b => {
                b.append_value(primitive!(Float64 => reader))
            }),
            Type::Date => (PrimDict::<DictKey, Date32Type>, nil, b => {
                b.append_value(primitive!(Date => reader))
            }),
            Type::Date32 => (PrimDict::<DictKey, Date32Type>, nil, b => {
                b.append_value(primitive!(Date32 => reader))
            }),
            Type::DateTime(_) => (PrimDict::<DictKey, TimestampSecondType>, nil, b => {
                b.append_value(primitive!(DateTime => reader))
            }),
            Type::DateTime64(0, _) => (PrimDict::<DictKey, TimestampSecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(0) => reader))
            }),
            Type::DateTime64(1..=3, _) => (PrimDict::<DictKey, TimestampMillisecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(3) => reader))
            }),
            Type::DateTime64(4..=6, _) => (PrimDict::<DictKey, TimestampMicrosecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(6) => reader))
            }),
            Type::DateTime64(7..=9, _) => (PrimDict::<DictKey, TimestampNanosecondType>, nil, b => {
                b.append_value(primitive!(DateTime64(9) => reader))
            }),
            Type::Decimal32(_) => (PrimDict::<DictKey, Decimal128Type>, nil, b => {
                b.append_value(primitive!(Decimal32 => reader))
            }),
            Type::Decimal64(_) => (PrimDict::<DictKey, Decimal128Type>, nil, b => {
                b.append_value(primitive!(Decimal64 => reader))
            }),
            Type::Decimal128(_) => (PrimDict::<DictKey, Decimal128Type>, nil, b => {
                b.append_value(primitive!(Decimal128 => reader))
            }),
            Type::Decimal256(_) => (PrimDict::<DictKey, Decimal256Type>, nil, b => {
                b.append_value(primitive!(Decimal256 => reader))
            }),
            // Strings and Binary
            Type::String => (StringDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(String => reader))
            }),
            Type::Object => (StringDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Object => reader))
            }),
            Type::FixedSizedString(n) => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedBinary(*n)=> reader))
            }),
            Type::Binary => (BinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Binary=> reader))
            }),
            Type::FixedSizedBinary(n) => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedBinary(*n)=> reader))
            }),
            Type::Uuid => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Fixed(16)=> reader))
            }),
            Type::Ipv4 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Ipv4 => reader))
            }),
            Type::Ipv6 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Ipv6 => reader))
            }),
            // Special numeric types that need to be read as bytes
            Type::Int128 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Fixed(16)=> reader))
            }),
            Type::Int256 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedRev(32)=> reader))
            }),
            Type::UInt128 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(Fixed(16)=> reader))
            }),
            Type::UInt256 => (FixedSizeBinaryDictionaryBuilder<DictKey>, nil, b => {
                b.append_value(binary!(FixedRev(32)=> reader))
            })
        }
        _ => {
            return Err(Error::ArrowDeserialize(format!(
                "Unsupported type for Dictionary: {type_:?}"
            )))
        });

        Ok(())
    }
}
