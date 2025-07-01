LOG := env('RUST_LOG', '')
ARROW_DEBUG := env('CLICKHOUSE_NATIVE_DEBUG_ARROW', '')

# List of features
# features := ["inner_pool", "pool", "serde", "derive", "cloud", "rust_decimal"]
features := 'inner_pool pool serde derive cloud rust_decimal'

# List of Examples

examples := "insert insert_multi_threaded pool scalar"

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
     --ignore-filename-regex "(clickhouse-arrow-derive|errors|error_codes|examples|test_utils).*" \
     --output-dir coverage -F test_utils --open

# --- DOCS ---
docs:
    cd clickhouse-arrow && cargo doc --open

# --- BENCHES ---
[confirm('Delete all benchmark reports?')]
clear-benches:
    rm -rf target/criterion/*

bench:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} cargo bench --profile=release -F test_utils && \
     open ../target/criterion/report/index.html

bench-lto:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} cargo bench --profile=release-lto -F test_utils && \
     open ../target/criterion/report/index.html

bench-one bench:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} cargo bench \
     --profile=release \
     -F test_utils \
     --bench "{{ bench }}" && \
     open ../target/criterion/report/index.html

bench-one-lto bench:
    cd clickhouse-arrow && RUST_LOG={{ LOG }} cargo bench \
     --profile=release-lto \
     -F test_utils \
     --bench "{{ bench }}" && \
     open ../target/criterion/report/index.html

# --- EXAMPLES ---
debug-profile example:
    cd clickhouse-arrow && RUSTFLAGS='-g' cargo build \
     -F test_utils \
     --example "{{ example }}"

release-debug example:
    cd clickhouse-arrow && RUSTFLAGS='-g' cargo build \
     --profile=release-with-debug \
     -F test_utils \
     --example "{{ example }}"
    codesign -s - -v -f --entitlements assets/mac.entitlements "target/release-with-debug/examples/{{ example }}"

release-lto example:
    cd clickhouse-arrow && cargo build \
     --profile=release-lto \
     -F test_utils \
     --example "{{ example }}"
    codesign -s - -v -f --entitlements assets/mac.entitlements "target/release-lto/examples/{{ example }}"

example example:
    cargo run -F test_utils --example "{{ example }}"

example-lto example:
    cargo run --profile=release-lto -F test_utils --example "{{ example }}"

example-release-debug example:
    cargo run --profile=release-with-debug -F test_utils --example "{{ example }}"

examples:
    @for ex in {{ examples }}; do \
        echo "Running example: $ex"; \
        cargo run -F test_utils --example "$ex"; \
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
    @echo "Building no features..."
    cargo check --no-default-features --all-targets
    @echo "Checking default features..."
    cargo clippy --all-targets
    @echo "Building default features..."
    cargo check --all-targets
    @echo "Checking all features..."
    cargo clippy --all-features --all-targets
    @echo "Building all features..."
    cargo check --all-features --all-targets
    @echo "Checking each feature..."
    @for feature in {{ ARGS }}; do \
        echo "Checking & Building feature: $feature"; \
        cargo clippy --no-default-features --features $feature --all-targets; \
        cargo check --no-default-features --features $feature --all-targets; \
    done
    @echo "Checking each feature with defaults..."
    @for feature in {{ ARGS }}; do \
        echo "Checking feature (with defaults): $feature"; \
        cargo clippy --features $feature --all-targets; \
        cargo check --features $feature --all-targets; \
    done
    @echo "Checking all provided features..."
    cargo clippy --no-default-features --features "{{ ARGS }}" --all-targets
    cargo check --no-default-features --features "{{ ARGS }}" --all-targets

fmt:
    @echo "Running rustfmt..."
    # cd clickhouse-arrow && cargo +nightly fmt --all --check
    cargo +nightly fmt --check -- --config-path ./rustfmt.toml
fix:
    cargo clippy --fix --all-features --all-targets --allow-dirty

# --- MAINTENANCE ---

# Initialize development environment for maintainers
init-dev:
    @echo "Installing development tools..."
    cargo install cargo-release || true
    cargo install git-cliff || true
    cargo install cargo-edit || true
    cargo install cargo-outdated || true
    cargo install cargo-audit || true
    @echo ""
    @echo "✅ Development tools installed!"
    @echo ""
    @echo "Next steps:"
    @echo "1. Get your crates.io API token from https://crates.io/settings/tokens"
    @echo "2. Add it as CARGO_REGISTRY_TOKEN in GitHub repo settings → Secrets"
    @echo "3. Use 'cargo release patch/minor/major' to create releases"
    @echo ""
    @echo "Useful commands:"
    @echo "  just release-dry patch  # Preview what would happen"
    @echo "  just check-outdated     # Check for outdated dependencies"
    @echo "  just audit              # Security audit"

# Preview a release without actually doing it
release-dry version:
    cargo release {{version}} --dry-run --verbose

# Check for outdated dependencies
check-outdated:
    cargo outdated

# Run security audit
audit:
    cargo audit

# Release patch version
patch:
    cargo release patch

# Release minor version
minor:
    cargo release minor
