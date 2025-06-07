LOG := env('RUST_LOG', '')
ARROW_DEBUG := env('CLICKHOUSE_NATIVE_DEBUG_ARROW', '')

# List of features
# features := ["fast_mode", "pool", "serde", "derive", "cloud", "rust_decimal"]
features := 'fast_mode pool serde derive cloud rust_decimal'

# List of Examples

examples := "insert pool select"

default:
    @just --list

# --- TESTS ---
test:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test_utils -- --nocapture --show-output

test-one test_name:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test_utils "{{ test_name }}" -- --nocapture --show-output

test-integration test_name:
    CLICKHOUSE_NATIVE_DEBUG_ARROW={{ ARROW_DEBUG }} RUST_LOG={{ LOG }} cargo test \
     -F test_utils --test "{{ test_name }}" -- --nocapture --show-output

coverage:
    cargo llvm-cov --html \
     --ignore-filename-regex "(clickhouse_native_derive|errors|error_codes|examples|test_utils).*" \
     --output-dir coverage -F test_utils --open

# --- DOCS ---
docs:
    cd clickhouse_native && cargo doc --open

# --- BENCHES ---
[confirm('Delete all benchmark reports?')]
clear-benches:
    rm -rf target/criterion/*

bench:
    cd clickhouse_native && RUST_LOG={{ LOG }} cargo bench --profile=release -F test_utils && \
     open ../target/criterion/report/index.html

bench-one bench:
    cd clickhouse_native && RUST_LOG={{ LOG }} cargo bench \
     --profile=release \
     -F test_utils \
     --bench "{{ bench }}" && \
     open ../target/criterion/report/index.html

bench-lto:
    cd clickhouse_native && RUST_LOG={{ LOG }} cargo bench \
     --profile=release-lto \
     -F test_utils && \
     open ../target/criterion/report/index.html

# --- EXAMPLE ---
debug-profile example:
    cd clickhouse_native && RUSTFLAGS='-g' cargo build \
     -F test_utils \
     --example "{{ example }}"

release-debug example:
    cd clickhouse_native && RUSTFLAGS='-g' cargo build \
     --profile=release-with-debug \
     -F test_utils \
     --example "{{ example }}"
    codesign -s - -v -f --entitlements assets/mac.entitlements "target/release-with-debug/examples/{{ example }}"

release-lto example:
    cd clickhouse_native && cargo build \
     --profile=release-lto \
     -F test_utils \
     --example "{{ example }}"
    codesign -s - -v -f --entitlements assets/mac.entitlements "target/release-lto/examples/{{ example }}"

example example *args='':
    cargo run --profile=release-lto -F test_utils --example "{{ example }}" -- "{{ args }}"

example-debug example *args='':
    cargo run -F test_utils --example "{{ example }}" -- "{{ args }}"

examples *args='':
    @for ex in {{ examples }}; do \
        echo "Running example: $ex"; \
        cargo run --profile=release-lto -F test_utils --example "$ex" -- "{{ args }}"
    done

# --- PROFILING ---
flamegraph example *args='':
    CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --root --flamechart --open \
     --profile=release-with-debug \
     --features test_utils \
     --min-width="0.0001" \
     --example "{{ example }}" -- "{{ args }}"

samply example *args='': (release-debug example)
    # TODO: Add install check here
    samply record -r 100000 \
     "target/release-with-debug/examples/{{ example }}" "{{ args }}"

# --- CLIPPY AND FORMATTING ---

# Check all feature combinations
check-features *ARGS=features:
    @echo "Checking no features..."
    cargo clippy --no-default-features --all-targets
    @echo "Checking default features..."
    cargo clippy --all-targets
    @echo "Checking all features..."
    cargo clippy --all-features --all-targets
    @for feature in {{ ARGS }}; do \
        echo "Checking feature: $feature"; \
        cargo clippy --no-default-features --features $feature --all-targets; \
    done
    @echo "Checking all provided features..."
    cargo clippy --no-default-features --features "{{ ARGS }}" --all-targets

fmt:
    @echo "Running rustfmt..."
    # cd clickhouse_native && cargo +nightly fmt --all --check
    cargo +nightly fmt --check -- --config-path ./rustfmt.toml
