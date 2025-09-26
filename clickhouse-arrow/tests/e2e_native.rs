#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[("testcontainers", "debug")];

// Test native e2e no compression
#[cfg(feature = "derive")]
e2e_test!(e2e_native_none, tests::native::test_round_trip_none, TRACING_DIRECTIVES, None);

// Test native e2e lz4
#[cfg(feature = "derive")]
e2e_test!(e2e_native_lz4, tests::native::test_round_trip_lz4, TRACING_DIRECTIVES, None);

// Test native e2e zstd
#[cfg(feature = "derive")]
e2e_test!(e2e_native_zstd, tests::native::test_round_trip_zstd, TRACING_DIRECTIVES, None);
