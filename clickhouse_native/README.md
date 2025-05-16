# 🛰️ Clickhouse *Native Protocol* Rust Client w/ Arrow Compatibility

ClickHouse access in rust over ClickHouse's native protocol.

## Details

The crate supports two "modes" of operation:

### `ArrowFormat` 

Support allowing interoperability with [arrow](https://docs.rs/arrow/latest/arrow/).

### `NativeFormat`

Uses internal types and custom traits if a dependency on arrow is not required. 

> [!NOTE] I am considering on putting arrow behind a feature flag for a subset of features without arrow dependencies. If that sounds interesting to you, let me know. 

## Queries

### Query Settings

The `clickhouse_native::query::settings` module allows configuring ClickHouse query settings. Refer to the module documentation for details and examples.

## Arrow Round-Trip

There are cases where a round trip may deserialize a different type by schema or array than the schema and array you used to create the table. 

 will try to maintain an accurate and updated list as they occur. In addition, when possible, I will provide options or other functionality to alter this behavior.

#### `(String|Binary)View`/`Large(List|String|Binary)` variations are normalized.
- **Behavior**: ClickHouse does not make the same distinction between Utf8, Utf8View, or LargeUtf8. All of these are mapped to either `Type::Binary` (the default, see above) or `Type::String`
- **Option**: None
- **Default**: Unsupported
- **Impact**: When deserializing from ClickHouse, manual modification will be necessary to use these data types.

#### `Utf8` -> `Binary`
- **Behavior**: By default, Type::String/DataType::Utf8 will be represented as Binary.
- **Option**: `strings_as_strings` (default: `false`).
- **Default**: Disabled (`false`).
- **Impact**: Set to `true` to strip map Type::String -> DataType::Utf8. Binary tends to be more efficient to work with in high throughput scenarios

#### Nullable `Array`s
- **Behavior**: ClickHouse does not allow `Nullable(Array(...))`, but insertion with non-null data is allowed by default. To modify this behavior, set `array_nullable_error` to `true`.
- **Option**: `array_nullable_error` (default: `false`).
- **Default**: Disabled (`false`).
- **Impact**: Enables flexible insertion but may cause schema mismatches if nulls are present.

#### `LowCardinality(Nullable(...))` vs `Nullable(LowCardinality(...))`
- **Behavior**: Like arrays mentioned above, ClickHouse does not allow nullable low cardinality. The default behavior is to push down the nullability.
- **Option**: `low_cardinality_nullable_error` (default: `false`).
- **Default**: Disabled (`false`).
- **Impact**: Enables flexible insertion but may cause schema mismatches if nulls are present.

#### `Enum8`/`Enum16` vs. `LowCardinality`
- **Behavior**: Arrow `Dictionary` types map to `LowCardinality`, but ClickHouse `Enum` types may also map to `Dictionary`, altering the type on round-trip.
- **Option**: No options available rather provide hash maps for either `enum_i8` and/or `enum_i16` for `CreateOptions` during schema creation.
- **Impact**: The default behavior will ignore enums when starting from arrow.

> [!NOTE] For examples of these cases, refer to the tests in the module [arrow::types](src/arrow/types.rs)

> [!NOTE] The configuration for the options above can be found in [options](src/client/options.rs)

> [!NOTE] For a builder of create options use during schema creation (eg `Engine`, `Order By`, `Enum8` and `Enum16` lookups), refer to [CreateOptions](src/ddl.rs)
