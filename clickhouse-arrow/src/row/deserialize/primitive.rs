macro_rules! primitive {
    (Int8 => $reader:expr) => {{ $reader.try_get_i8()? }};
    (Int16 => $reader:expr) => {{ $reader.try_get_i16_le()? }};
    (Int32 => $reader:expr) => {{ $reader.try_get_i32_le()? }};
    (Int64 => $reader:expr) => {{ $reader.try_get_i64_le()? }};
    (UInt8 => $reader:expr) => {{ $reader.try_get_u8()? }};
    (UInt16 => $reader:expr) => {{ $reader.try_get_u16_le()? }};
    (UInt32 => $reader:expr) => {{ $reader.try_get_u32_le()? }};
    (UInt64 => $reader:expr) => {{ $reader.try_get_u64_le()? }};
    (Float32 => $reader:expr) => {{ $reader.try_get_f32_le()? }};
    (Float64 => $reader:expr) => {{ $reader.try_get_f64_le()? }};
    (Date => $reader:expr) => {{ $reader.try_get_u16_le().map(i32::from)? }};
    (Date32 => $reader:expr) => {{
        {
            let days = $reader.try_get_i32_le()?;
            days - crate::deserialize::DAYS_1900_TO_1970 // Adjust to days since 1970-01-01
        }
    }};
    (DateTime => $reader:expr) => {{ $reader.try_get_u32_le().map(i64::from)? }};
    (DateTime64(0) => $reader:expr) => {{ $reader.try_get_i64_le()? }};
    (DateTime64(3) => $reader:expr) => {{ $reader.try_get_i64_le()? }};
    (DateTime64(6) => $reader:expr) => {{ $reader.try_get_i64_le()? }};
    (DateTime64(9) => $reader:expr) => {{ $reader.try_get_i64_le()? }};
    (Decimal32 => $reader:expr) => {{ $reader.try_get_i32_le().map(i128::from)? }};
    (Decimal64 => $reader:expr) => {{ $reader.try_get_i64_le().map(i128::from)? }};
    (Decimal128 => $reader:expr) => {{
        {
            let mut buf = [0u8; 16];
            $reader.try_copy_to_slice(&mut buf)?;
            i128::from_le_bytes(buf)
        }
    }};
    (Decimal256 => $reader:expr) => {{
        {
            let mut buf = [0u8; 32];
            $reader.try_copy_to_slice(&mut buf)?;
            buf.reverse();
            i256::from_le_bytes(buf)
        }
    }};
}
pub(crate) use primitive;
