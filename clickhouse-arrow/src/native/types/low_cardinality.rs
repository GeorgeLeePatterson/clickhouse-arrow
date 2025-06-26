pub(crate) const NEED_GLOBAL_DICTIONARY_BIT: u64 = 1u64 << 8;
pub(crate) const HAS_ADDITIONAL_KEYS_BIT: u64 = 1u64 << 9;
pub(crate) const NEED_UPDATE_DICTIONARY_BIT: u64 = 1u64 << 10;
pub(crate) const KEY_TYPE_MASK: u64 = 0xFF;

pub(crate) const TUINT8: u64 = 0;
pub(crate) const TUINT16: u64 = 1;
pub(crate) const TUINT32: u64 = 2;
pub(crate) const TUINT64: u64 = 3;

pub(crate) const LOW_CARDINALITY_VERSION: u64 = 1;
