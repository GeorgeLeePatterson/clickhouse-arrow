# AGENTS

This file defines mandatory implementation and review rules for this repository. These are non-optional.

1. `clippy::pedantic` warnings MUST be resolved across ALL feature combinations. No pedantic warning may remain.
2. `#[expect(...)]` MUST be used instead of `#[allow(...)]` unless there is literally no viable alternative.
3. Any lint `#[expect(...)]` that applies only to a feature subset MUST be scoped with `#[cfg_attr(...)]` unless there is literally no viable alternative.
4. Performance is the highest-priority engineering principle in this library.
5. Arrow is the primary supported (de)serialization format. Native format work is secondary.
6. Native protocol primitives are shared. Shared concerns include (at minimum) `Type`, `Type::from_str`, and prefix (de)serialization; these are used by Arrow paths and are not Arrow-specific.
7. Separation MUST stay clear:
   Arrow-only logic goes in `clickhouse-arrow/src/arrow`.
   Shared native protocol logic defaults to `clickhouse-arrow/src/native` when it is not Arrow-specific.
8. Existing idioms MUST be reused whenever possible. Examples include:
   `TypedBuilder` for cached append paths and reduced allocation churn.
   Macros where boilerplate harms readability/maintainability.
   Recursive (de)serialization patterns already established in the codebase.
9. Round-trip fidelity is best-effort, not guaranteed. Do not introduce abstraction leaks, performance regressions, or convoluted code to force round trips. Any new round-trip exception MUST be documented and reflected in e2e Arrow tests.
10. One-off helper functions used only once MUST be inlined at call sites unless reuse is clearly justified.
11. Prefer functional style over imperative style when practical.
12. Prefer compositional, algebraic, and homomorphic designs when practical.
13. High-level core behavior (especially under `clickhouse-arrow/src/client`) MUST NOT be changed without explicit maintainer approval.
14. `Copy -> non-Copy` changes are high-risk and MUST be treated as such. New allocations MUST be minimized; alternatives MUST be explored first.
15. Work is not complete until ALL of the following are true:
    Unit tests pass across all feature combinations.
    Integration tests pass across all feature combinations.
    Formatting is clean using nightly rustfmt (`cargo +nightly fmt`, respecting `rustfmt.toml`).
    Lint warnings are resolved across all feature combinations, including pedantic.
    Line coverage is greater than 90%.
16. Follow the existing folder structure, module layout conventions (modern modules, no `mod.rs` pattern for new work), and established code patterns. When adding new behavior, find and follow the closest existing pattern first.
