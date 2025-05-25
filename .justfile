set positional-arguments := true

LOG := env_var_or_default('RUST_LOG', '')
BACKTRACE := env_var_or_default('RUST_BACKTRACE', '1')
ARROW_DEBUG := env_var_or_default('CLICKHOUSE_NATIVE_DEBUG_ARROW', '')

default:
    just --list

test:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     --release -F test_utils -- --nocapture --show-output

coverage:
    cargo llvm-cov -r --html \
     --ignore-filename-regex "(clickhouse_native_derive|error_codes|examples|test_utils).*" \
     --output-dir coverage -F test_utils --open

bench:
    cd clickhouse_native && RUST_LOG={{ LOG }} cargo bench --profile=release -F test_utils && \
     open ../target/criterion/report/index.html

bench-many:
    cd clickhouse_native && RUST_LOG={{ LOG }} cargo bench --profile=release -F test_utils --bench insert_many && \
     open ../target/criterion/report/index.html

bench-lto:
    cd clickhouse_native && RUST_LOG={{ LOG }} cargo bench --profile=release-lto -F test_utils && \
     open ../target/criterion/report/index.html

example-client +args:
    cargo run --profile=release-lto -F test_utils --example client -- "$@"

release-debug:
    cd clickhouse_native && RUSTFLAGS='-g' cargo build --profile=release-with-debug -F test_utils --example insert
    codesign -s - -v -f --entitlements assets/mac.entitlements target/release-lto/examples/insert

release-lto:
    cd clickhouse_native && RUSTFLAGS='-g' cargo build --profile=release-lto -F test_utils --example insert
    codesign -s - -v -f --entitlements assets/mac.entitlements target/release-lto/examples/insert

docs:
    cd clickhouse_native && cargo doc --open

flamegraph +args:
    CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --root --flamechart --open \
     --profile=release-with-debug \
     --features test_utils \
     --min-width="0.0001" \
     --example insert -- "$@"
