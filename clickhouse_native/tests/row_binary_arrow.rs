#![allow(unused_crate_dependencies)]

pub mod common;
#[cfg(feature = "test_utils")]
pub mod tests;

// Test arrow row binary e2e lz4
#[cfg(feature = "test_utils")]
e2e_test!(
    arrow_row_binary_lz4,
    tests::arrow::test_lz4_row_binary,
    &[("testcontainers", "debug")],
    None
);
