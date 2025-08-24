use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use tracing::error;

use super::settings::{SettingValue, Settings};
use crate::arrow::types::{SchemaConversions, schema_conversion};
use crate::{ArrowOptions, ColumnDefinition, Error, Result, Row, Type};

/// Non-exhaustive list of `ClickHouse` engines. Helps prevent typos when configuring the engine.
///
/// [`Self::Other`] can always be used in the case the list does not include the engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClickHouseEngine {
    MergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
    Memory,
    Log,
    StripeLog,
    TinyLog,
    Other(String),
}

impl<S> From<S> for ClickHouseEngine
where
    S: Into<String>,
{
    fn from(value: S) -> Self {
        let engine = value.into();
        match engine.to_uppercase().as_str() {
            "MERGETREE" => Self::MergeTree,
            "AGGREGATINGMERGETREE" => Self::AggregatingMergeTree,
            "COLLAPSINGMERGETREE" => Self::CollapsingMergeTree,
            "REPLACINGMERGETREE" => Self::ReplacingMergeTree,
            "SUMMINGMERGETREE" => Self::SummingMergeTree,
            "MEMORY" => Self::Memory,
            "LOG" => Self::Log,
            "STRIPELOG" => Self::StripeLog,
            "TINYLOG" => Self::TinyLog,
            // Be sure to add any new engines here
            _ => Self::Other(engine),
        }
    }
}

impl std::fmt::Display for ClickHouseEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't use wildcard, that way it gets updated as well
        match self {
            Self::MergeTree => write!(f, "MergeTree"),
            Self::AggregatingMergeTree => write!(f, "AggregatingMergeTree"),
            Self::CollapsingMergeTree => write!(f, "CollapsingMergeTree"),
            Self::ReplacingMergeTree => write!(f, "ReplacingMergeTree"),
            Self::SummingMergeTree => write!(f, "SummingMergeTree"),
            Self::Memory => write!(f, "Memory"),
            Self::Log => write!(f, "Log"),
            Self::StripeLog => write!(f, "StripeLog"),
            Self::TinyLog => write!(f, "TinyLog"),
            Self::Other(engine) => write!(f, "{engine}"),
        }
    }
}

/// Options for creating a `ClickHouse` table, specifying engine, ordering, partitioning, and other
/// settings.
///
/// This struct is used to configure the creation of a `ClickHouse` table via
/// `create_table_statement_from_arrow`. It supports common table options like `ORDER BY`,
/// `PRIMARY KEY`, `PARTITION BY`, `SAMPLE BY`, `TTL`, and custom settings. It also allows
/// specifying default values for columns and enabling defaults for nullable columns.
///
/// # Examples
/// ```rust,ignore
/// use clickhouse_arrow::sql::CreateOptions;
/// use clickhouse_arrow::Settings;
///
/// let options = CreateOptions::new("MergeTree")
///     .with_order_by(&["id".to_string()])
///     .with_setting("index_granularity", 4096)
///     .with_ttl("1 DAY");
/// ```
#[derive(Debug, Default, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CreateOptions {
    pub engine:                String,
    pub order_by:              Vec<String>,
    pub primary_keys:          Vec<String>,
    pub partition_by:          Option<String>,
    pub sampling:              Option<String>,
    pub settings:              Settings,
    pub ttl:                   Option<String>,
    pub schema_conversions:    Option<SchemaConversions>,
    pub defaults:              Option<HashMap<String, String>>,
    pub defaults_for_nullable: bool,
}

impl CreateOptions {
    /// Creates a new `CreateOptions` with the specified engine.
    ///
    /// # Arguments
    /// - `engine`: The `ClickHouse` table engine (e.g., `MergeTree`, `Memory`).
    ///
    /// # Returns
    /// A new `CreateOptions` instance with the specified engine.
    #[must_use]
    pub fn new(engine: impl Into<String>) -> Self {
        Self { engine: engine.into(), ..Default::default() }
    }

    /// Creates a new `CreateOptions` with the specified engine.
    ///
    /// # Arguments
    /// - `engine`: The `ClickHouseEngine` .
    ///
    /// # Returns
    /// A new `CreateOptions` instance with the specified engine.
    #[must_use]
    pub fn from_engine(engine: impl Into<ClickHouseEngine>) -> Self {
        Self { engine: engine.into().to_string(), ..Default::default() }
    }

    /// Sets the `ORDER BY` clause for the table.
    ///
    /// Filters out empty strings from the provided list.
    ///
    /// # Arguments
    /// - `order_by`: A slice of column names to order by.
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_order_by(mut self, order_by: &[String]) -> Self {
        self.order_by =
            order_by.iter().filter(|k| !k.is_empty()).map(ToString::to_string).collect();
        self
    }

    /// Sets the `PRIMARY KEY` clause for the table.
    ///
    /// Filters out empty strings from the provided list.
    ///
    /// # Arguments
    /// - `keys`: A slice of column names to use as primary keys.
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_primary_keys(mut self, keys: &[String]) -> Self {
        self.primary_keys =
            keys.iter().filter(|k| !k.is_empty()).map(ToString::to_string).collect();
        self
    }

    /// Sets the `PARTITION BY` clause for the table.
    ///
    /// Ignores empty strings.
    ///
    /// # Arguments
    /// - `partition_by`: The partitioning expression.
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_partition_by(mut self, partition_by: impl Into<String>) -> Self {
        let partition_by = partition_by.into();
        if !partition_by.is_empty() {
            self.partition_by = Some(partition_by);
        }
        self
    }

    /// Sets the `SAMPLE BY` clause for the table.
    ///
    /// Ignores empty strings.
    ///
    /// # Arguments
    /// - `sampling`: The sampling expression.
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_sample_by(mut self, sampling: impl Into<String>) -> Self {
        let sampling = sampling.into();
        if !sampling.is_empty() {
            self.sampling = Some(sampling);
        }
        self
    }

    /// Sets the table settings.
    ///
    /// # Arguments
    /// - `settings`: The `Settings` object containing key-value pairs.
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Sets the `TTL` clause for the table.
    ///
    /// Ignores empty strings.
    ///
    /// # Arguments
    /// - `ttl`: The TTL expression (e.g., `1 DAY`).
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_ttl(mut self, ttl: impl Into<String>) -> Self {
        let ttl = ttl.into();
        if !ttl.is_empty() {
            self.ttl = Some(ttl);
        }
        self
    }

    /// Adds a single setting to the table.
    ///
    /// # Arguments
    /// - `name`: The setting name (e.g., `index_granularity`).
    /// - `setting`: The setting value (e.g., `4096`).
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_setting<S>(mut self, name: impl Into<String>, setting: S) -> Self
    where
        SettingValue: From<S>,
    {
        self.settings.add_setting(name.into(), setting);
        self
    }

    /// Sets default values for columns.
    ///
    /// # Arguments
    /// - `defaults`: An iterator of (column name, default value) pairs.
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_defaults<I>(mut self, defaults: I) -> Self
    where
        I: Iterator<Item = (String, String)>,
    {
        self.defaults = Some(defaults.into_iter().collect::<HashMap<_, _>>());
        self
    }

    /// Enables default values for nullable columns (e.g., `NULL`).
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_defaults_for_nullable(mut self) -> Self {
        self.defaults_for_nullable = true;
        self
    }

    /// Provide a map of resolved type conversions.
    ///
    /// For example, since arrow does not support enum types, providing a map of column name to
    /// `Type::Enum8` with enum values ensures [`arrow::datatypes::DataType::Dictionary`] is
    /// serialized as [`crate::Type::Enum16`] instead of the default [`crate::Type::LowCardinality`]
    ///
    /// # Returns
    /// Self for method chaining.
    #[must_use]
    pub fn with_schema_conversions(mut self, map: SchemaConversions) -> Self {
        self.schema_conversions = Some(map);
        self
    }

    /// Returns the configured default values, if any.
    ///
    /// # Returns
    /// An optional reference to the `HashMap` of column names to default values.
    pub fn defaults(&self) -> Option<&HashMap<String, String>> { self.defaults.as_ref() }

    /// Returns the configured default values, if any.
    ///
    /// # Returns
    /// An optional reference to the `HashMap` of column names to default values.
    pub fn schema_conversions(&self) -> Option<&SchemaConversions> {
        self.schema_conversions.as_ref()
    }

    /// Builds the table options part of a `ClickHouse` `CREATE TABLE` statement.
    ///
    /// Constructs the SQL for engine, `ORDER BY`, `PRIMARY KEY`, `PARTITION BY`, `SAMPLE BY`,
    /// `TTL`, and `SETTINGS` clauses. Validates constraints, such as ensuring primary keys are
    /// a subset of `ORDER BY` columns and sampling references a primary key.
    ///
    /// # Returns
    /// A `Result` containing the SQL string for the table options or a `Error` if
    /// validation fails (e.g., empty engine, invalid primary keys).
    ///
    /// # Errors
    /// - Returns `DDLMalformed` if the engine is empty, primary keys are invalid, or sampling
    ///   doesn’t reference a primary key.
    fn build(&self) -> Result<String> {
        let engine = self.engine.clone();
        if engine.is_empty() {
            return Err(Error::DDLMalformed("An engine is required, received empty string".into()));
        }

        let mut options = vec![format!("ENGINE = {engine}")];

        // Log engines don't support options
        if ["log", "LOG", "Log"].iter().any(|s| engine.contains(s)) {
            return Ok(options.remove(0));
        }

        // Make sure order by is set
        if self.order_by.is_empty() {
            // Validations
            if !self.primary_keys.is_empty() || !self.sampling.as_ref().is_none_or(String::is_empty)
            {
                return Err(Error::DDLMalformed(
                    "Cannot specify primary keys or sampling when order by is empty".into(),
                ));
            }

            options.push("ORDER BY tuple()".into());
        } else {
            let order_by = self.order_by.clone();

            // Validate primary keys
            if !self.primary_keys.is_empty()
                && !self.primary_keys.iter().enumerate().all(|(i, k)| order_by.get(i) == Some(k))
            {
                return Err(Error::DDLMalformed(format!(
                    "Primary keys but be present in order by and the ordering must match: order \
                     by = {order_by:?}, primary keys = {:?}",
                    self.primary_keys
                )));
            }

            // Validate sampling
            if let Some(sample) = self.sampling.as_ref()
                && !order_by.iter().any(|o| sample.contains(o.as_str()))
            {
                return Err(Error::DDLMalformed(format!(
                    "Sampling must refer to a primary key: order by = {order_by:?}, sampling={:?}",
                    self.sampling
                )));
            }

            options.push(format!("ORDER BY ({})", order_by.join(", ")));
        }

        if !self.primary_keys.is_empty() {
            let primary_keys = self.primary_keys.clone();
            options.push(format!("PRIMARY KEY ({})", primary_keys.join(", ")));
        }

        if let Some(partition) = self.partition_by.as_ref() {
            options.push(format!("PARTITION BY {partition}"));
        }

        if let Some(sample) = self.sampling.as_ref() {
            options.push(format!("SAMPLE BY {sample}"));
        }

        if let Some(ttl) = self.ttl.as_ref() {
            options.push(format!("TTL {ttl}"));
        }

        if !self.settings.is_empty() {
            options.push(format!("SETTINGS {}", self.settings.encode_to_strings().join(", ")));
        }

        Ok(options.join("\n"))
    }
}

/// Generates a `ClickHouse` `CREATE DATABASE` statement.
///
/// # Arguments
/// - `database`: The name of the database to create.
///
/// # Returns
/// A `Result` containing the SQL statement or a `Error` if the database name is
/// invalid.
///
/// # Errors
/// - Returns `DDLMalformed` if the database name is empty or is `"default"`.
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::sql::create_db_statement;
///
/// let sql = create_db_statement("my_db").unwrap();
/// assert_eq!(sql, "CREATE DATABASE IF NOT EXISTS my_db");
/// ```
pub(crate) fn create_db_statement(database: &str) -> Result<String> {
    if database.is_empty() {
        return Err(Error::DDLMalformed("Database name cannot be empty".into()));
    }

    if database.eq_ignore_ascii_case("default") {
        return Err(Error::DDLMalformed("Cannot create `default` database".into()));
    }

    Ok(format!("CREATE DATABASE IF NOT EXISTS {database}"))
}

/// Generates a `ClickHouse` `DROP DATABASE` statement.
///
/// # Arguments
/// - `database`: The name of the database to drop.
/// - `sync`: If `true`, adds the `SYNC` clause for synchronous dropping.
///
/// # Returns
/// A `Result` containing the SQL statement or a `Error` if the database name is
/// invalid.
///
/// # Errors
/// - Returns `DDLMalformed` if the database name is empty or is `"default"`.
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::sql::drop_db_statement;
///
/// let sql = drop_db_statement("my_db", true).unwrap();
/// assert_eq!(sql, "DROP DATABASE IF EXISTS my_db SYNC");
/// ```
pub(crate) fn drop_db_statement(database: &str, sync: bool) -> Result<String> {
    if database.is_empty() {
        return Err(Error::DDLMalformed("Database name cannot be empty".into()));
    }

    if database.eq_ignore_ascii_case("default") {
        return Err(Error::DDLMalformed("Cannot create `default` database".into()));
    }

    let mut ddl = "DROP DATABASE IF EXISTS ".to_string();
    ddl.push_str(database);
    if sync {
        ddl.push_str(" SYNC");
    }
    Ok(ddl)
}

/// Generates a `ClickHouse` `CREATE TABLE` statement from an Arrow schema and table options.
///
/// # Arguments
/// - `database`: Optional database name (e.g., `my_db`). If `None`, the table is created in the
///   default database.
/// - `table`: The table name.
/// - `schema`: The Arrow schema defining the table’s columns.
/// - `options`: The `CreateOptions` specifying engine, ordering, and other settings.
///
/// # Returns
/// A `Result` containing the SQL statement or a `Error` if the schema is invalid or
/// options fail validation.
///
/// # Errors
/// - Returns `DDLMalformed` if the schema is empty or options validation fails (e.g., invalid
///   engine).
/// - Returns `ArrowDeserialize` if the Arrow `DataType` cannot be converted to a `ClickHouse` type.
/// - Returns `TypeConversion` if the schema is disallowed by `ClickHouse`
///
/// # Example
/// ```rust,ignore
/// use arrow::datatypes::{DataType, Field, Schema};
/// use crate::sql::{CreateOptions, create_table_statement_from_arrow};
/// use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("name", DataType::Utf8, true),
/// ]));
/// let options = CreateOptions::new("MergeTree")
///     .with_order_by(&["id".to_string()]);
/// let sql = create_table_statement_from_arrow(None, "my_table", &schema, &options).unwrap();
/// assert!(sql.contains("CREATE TABLE IF NOT EXISTS `my_table`"));
/// ```
pub(crate) fn create_table_statement_from_arrow(
    database: Option<&str>,
    table: &str,
    schema: &SchemaRef,
    options: &CreateOptions,
    arrow_options: Option<ArrowOptions>,
) -> Result<String> {
    if schema.fields().is_empty() {
        return Err(Error::DDLMalformed("Arrow Schema is empty, cannot create table".into()));
    }
    let definition = RecordBatchDefinition {
        arrow_options,
        schema: Arc::clone(schema),
        defaults: options.defaults().cloned(),
    };
    create_table_statement(database, table, Some(definition), options)
}

/// Generates a `ClickHouse` `CREATE TABLE` statement from a type that implements [`crate::Row`] and
/// [`CreateOptions`].
///
/// # Arguments
/// - `database`: Optional database name (e.g., `my_db`). If `None`, the table is created in the
///   default database.
/// - `table`: The table name.
/// - `options`: The `CreateOptions` specifying engine, ordering, and other settings.
///
/// # Returns
/// A `Result` containing the SQL statement or a `Error` if the schema is invalid or
/// options fail validation.
///
/// # Errors
/// - Returns `DDLMalformed` if the schema is empty or options validation fails (e.g., invalid
///   engine).
/// - Returns `TypeConversion` if the schema is disallowed by `ClickHouse`
///
/// # Example
/// ```rust,ignore
/// use clickhouse_arrow::Row;
/// use clickhouse_arrow::sql::{CreateOptions, create_table_statement_from_native};
///
/// #[derive(Row)]
/// struct MyRow {
///     id: String,
///     name: String,
/// }
///
/// let options = CreateOptions::new("MergeTree")
///     .with_order_by(&["id".to_string()]);
/// let sql = create_table_statement_from_native::<MyRow>(None, "my_table", &options).unwrap();
/// assert!(sql.contains("CREATE TABLE IF NOT EXISTS `my_table`"));
/// ```
pub(crate) fn create_table_statement_from_native<T: Row>(
    database: Option<&str>,
    table: &str,
    options: &CreateOptions,
) -> Result<String> {
    create_table_statement::<T>(database, table, None, options)
}

pub(crate) fn create_table_statement<T: ColumnDefine>(
    database: Option<&str>,
    table: &str,
    schema: Option<T>,
    options: &CreateOptions,
) -> Result<String> {
    let column_definitions = schema
        .map(|s| s.runtime_definitions(options.schema_conversions.as_ref()))
        .transpose()?
        .flatten()
        .or(T::definitions());

    let Some(definitions) = column_definitions.filter(|c| !c.is_empty()) else {
        return Err(Error::DDLMalformed("Schema is empty, cannot create table".into()));
    };

    let db_pre = database.map(|c| format!("{c}.")).unwrap_or_default();
    let table = table.trim_matches('`');
    let mut sql = String::new();
    let _ = writeln!(sql, "CREATE TABLE IF NOT EXISTS {db_pre}`{table}` (");

    let total = definitions.len();
    for (i, (name, type_, default_value)) in definitions.into_iter().enumerate() {
        let _ = write!(sql, "  {name} {type_}");
        if let Some(d) = options
            .defaults
            .as_ref()
            .and_then(|d| d.get(&name))
            .or(default_value.map(|d| d.to_string()).as_ref())
        {
            let _ = write!(sql, " DEFAULT");
            if !d.is_empty() && d != "NULL" {
                let _ = write!(sql, " {d}");
            }
        } else if options.defaults_for_nullable && matches!(type_, Type::Nullable(_)) {
            let _ = write!(sql, " DEFAULT");
        }

        if i < (total - 1) {
            let _ = writeln!(sql, ",");
        }
    }

    let _ = writeln!(sql, "\n)");
    let _ = write!(sql, "{}", options.build()?);

    Ok(sql)
}

/// A type that describe the schema of its fields to be used in a `CREATE TABLE ...` query.
///
/// Generally this is not implemented manually, but using `clickhouse_arrow::Row` since it's
/// implemented on any `T: Row`. But it's helpful to implement manually if additional formats are
/// created.
pub trait ColumnDefine: Sized {
    type DefaultValue: std::fmt::Display + std::fmt::Debug;

    /// Provide the static schema
    fn definitions() -> Option<Vec<ColumnDefinition<Self::DefaultValue>>>;

    /// Infers the schema and returns it.
    ///
    /// # Errors
    ///
    /// Returns an error defined by the implementation
    fn runtime_definitions(
        &self,
        _: Option<&HashMap<String, Type>>,
    ) -> Result<Option<Vec<ColumnDefinition<Self::DefaultValue>>>> {
        Ok(Self::definitions())
    }
}

impl<T: Row> ColumnDefine for T {
    type DefaultValue = crate::Value;

    fn definitions() -> Option<Vec<ColumnDefinition>> { Self::to_schema() }

    fn runtime_definitions(
        &self,
        conversions: Option<&HashMap<String, Type>>,
    ) -> Result<Option<Vec<ColumnDefinition<Self::DefaultValue>>>> {
        let Some(static_definitions) = Self::definitions() else {
            return Ok(None);
        };

        if let Some(conversions) = conversions {
            return Ok(Some(
                static_definitions
                    .into_iter()
                    .map(|(name, type_, default_value)| {
                        let resolved_type = conversions.get(&name).cloned().unwrap_or(type_);
                        (name, resolved_type, default_value)
                    })
                    .collect::<Vec<_>>(),
            ));
        }

        Ok(Some(static_definitions))
    }
}

/// Helper struct to encapsulate schema creation logic for Arrow schemas.
pub(crate) struct RecordBatchDefinition {
    pub(crate) arrow_options: Option<ArrowOptions>,
    pub(crate) schema:        SchemaRef,
    pub(crate) defaults:      Option<HashMap<String, String>>,
}

impl ColumnDefine for RecordBatchDefinition {
    type DefaultValue = String;

    fn definitions() -> Option<Vec<ColumnDefinition<String>>> { None }

    fn runtime_definitions(
        &self,
        conversions: Option<&HashMap<String, Type>>,
    ) -> Result<Option<Vec<ColumnDefinition<String>>>> {
        let mut fields = Vec::with_capacity(self.schema.fields.len());
        for field in self.schema.fields() {
            let type_ =
                schema_conversion(field, conversions, self.arrow_options).inspect_err(|error| {
                    error!("Arrow conversion failed for field {field:?}: {error}");
                })?;
            let default_val =
                if let Some(d) = self.defaults.as_ref().and_then(|d| d.get(field.name())) {
                    if !d.is_empty() && d != "NULL" { Some(d.clone()) } else { None }
                } else {
                    None
                };
            fields.push((field.name().clone(), type_, default_val));
        }
        Ok(Some(fields))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{ClickHouseEngine, *};
    use crate::Type;

    #[allow(clippy::needless_pass_by_value)]
    fn compare_sql(left: impl AsRef<str> + Into<String>, right: impl AsRef<str> + Into<String>) {
        assert_eq!(left.as_ref().replace(['\n', ' '], ""), right.as_ref().replace(['\n', ' '], ""));
    }

    #[test]
    fn test_create_options_new() {
        let options = CreateOptions::new("MergeTree");
        assert_eq!(options.engine, "MergeTree");
        assert!(options.order_by.is_empty());
        assert!(options.primary_keys.is_empty());
        assert!(options.partition_by.is_none());
        assert!(options.sampling.is_none());
        assert!(options.settings.is_empty());
        assert!(options.ttl.is_none());
        assert!(options.defaults.is_none());
        assert!(!options.defaults_for_nullable);
    }

    #[test]
    fn test_create_options_with_order_by() {
        let options = CreateOptions::new("MergeTree").with_order_by(&[
            "id".to_string(),
            String::new(),
            "name".to_string(),
        ]);
        assert_eq!(options.order_by, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_create_options_with_primary_keys() {
        let options = CreateOptions::new("MergeTree").with_primary_keys(&[
            "id".to_string(),
            String::new(),
            "name".to_string(),
        ]);
        assert_eq!(options.primary_keys, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_create_options_with_partition_by() {
        let options = CreateOptions::new("MergeTree").with_partition_by("toYYYYMM(date)");
        assert_eq!(options.partition_by, Some("toYYYYMM(date)".to_string()));

        let options = CreateOptions::new("MergeTree").with_partition_by("");
        assert_eq!(options.partition_by, None);
    }

    #[test]
    fn test_create_options_with_sample_by() {
        let options = CreateOptions::new("MergeTree").with_sample_by("cityHash64(id)");
        assert_eq!(options.sampling, Some("cityHash64(id)".to_string()));

        let options = CreateOptions::new("MergeTree").with_sample_by("");
        assert_eq!(options.sampling, None);
    }

    #[test]
    fn test_create_options_with_settings() {
        let settings = Settings::default().with_setting("index_granularity", 4096);
        let options = CreateOptions::new("MergeTree").with_settings(settings.clone());
        assert_eq!(options.settings, settings);
    }

    #[test]
    fn test_create_options_with_setting() {
        let options = CreateOptions::new("MergeTree").with_setting("index_granularity", 4096);
        assert_eq!(options.settings.encode_to_strings(), vec![
            "index_granularity = 4096".to_string()
        ]);
    }

    #[test]
    fn test_create_options_with_ttl() {
        let options = CreateOptions::new("MergeTree").with_ttl("1 DAY");
        assert_eq!(options.ttl, Some("1 DAY".to_string()));

        let options = CreateOptions::new("MergeTree").with_ttl("");
        assert_eq!(options.ttl, None);
    }

    #[test]
    fn test_create_options_with_defaults() {
        let defaults = vec![
            ("id".to_string(), "0".to_string()),
            ("name".to_string(), "'unknown'".to_string()),
        ];
        let options = CreateOptions::new("MergeTree").with_defaults(defaults.into_iter());
        assert_eq!(
            options.defaults,
            Some(HashMap::from([
                ("id".to_string(), "0".to_string()),
                ("name".to_string(), "'unknown'".to_string()),
            ]))
        );
    }

    #[test]
    fn test_create_options_with_defaults_for_nullable() {
        let options = CreateOptions::new("MergeTree").with_defaults_for_nullable();
        assert!(options.defaults_for_nullable);
    }

    #[test]
    fn test_create_options_build_merge_tree() {
        let options = CreateOptions::new("MergeTree")
            .with_order_by(&["id".to_string(), "date".to_string()])
            .with_primary_keys(&["id".to_string()])
            .with_partition_by("toYYYYMM(date)")
            .with_sample_by("cityHash64(id)")
            .with_ttl("1 DAY")
            .with_setting("index_granularity", 4096);
        let sql = options.build().unwrap();
        compare_sql(
            sql,
            "ENGINE = MergeTree\nORDER BY (id, date)\nPRIMARY KEY (id)\nPARTITION BY \
             toYYYYMM(date)\nSAMPLE BY cityHash64(id)\nTTL 1 DAY\nSETTINGS index_granularity = \
             4096",
        );
    }

    #[test]
    fn test_create_options_build_log_engine() {
        let options = CreateOptions::new("TinyLog");
        let sql = options.build().unwrap();
        assert_eq!(sql, "ENGINE = TinyLog");
    }

    #[test]
    fn test_create_options_build_empty_order_by() {
        let options = CreateOptions::new("MergeTree");
        let sql = options.build().unwrap();
        compare_sql(sql, "ENGINE = MergeTree\nORDER BY tuple()");
    }

    #[test]
    fn test_create_options_build_invalid_engine() {
        let options = CreateOptions::new("");
        let result = options.build();
        assert!(matches!(result, Err(Error::DDLMalformed(_))));
    }

    #[test]
    fn test_create_options_build_invalid_primary_keys() {
        let options = CreateOptions::new("MergeTree")
            .with_order_by(&["id".to_string()])
            .with_primary_keys(&["name".to_string()]);
        let result = options.build();
        assert!(matches!(result, Err(Error::DDLMalformed(_))));
    }

    #[test]
    fn test_create_options_build_invalid_sampling() {
        let options = CreateOptions::new("MergeTree")
            .with_order_by(&["id".to_string()])
            .with_sample_by("cityHash64(name)");
        let result = options.build();
        assert!(matches!(result, Err(Error::DDLMalformed(_))));
    }

    #[test]
    fn test_create_db_statement() {
        let sql = create_db_statement("my_db").unwrap();
        assert_eq!(sql, "CREATE DATABASE IF NOT EXISTS my_db");

        let result = create_db_statement("");
        assert!(matches!(result, Err(Error::DDLMalformed(_))));

        let result = create_db_statement("default");
        assert!(matches!(result, Err(Error::DDLMalformed(_))));
    }

    #[test]
    fn test_drop_db_statement() {
        let sql = drop_db_statement("my_db", false).unwrap();
        compare_sql(sql, "DROP DATABASE IF EXISTS my_db");

        let sql = drop_db_statement("my_db", true).unwrap();
        compare_sql(sql, "DROP DATABASE IF EXISTS my_db SYNC");

        let result = drop_db_statement("", false);
        assert!(matches!(result, Err(Error::DDLMalformed(_))));

        let result = drop_db_statement("default", false);
        assert!(matches!(result, Err(Error::DDLMalformed(_))));
    }

    #[test]
    fn test_create_table_statement() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let options = CreateOptions::new("MergeTree")
            .with_order_by(&["id".to_string()])
            .with_defaults(vec![("name".to_string(), "'unknown'".to_string())].into_iter())
            .with_defaults_for_nullable();
        let sql =
            create_table_statement_from_arrow(None, "my_table", &schema, &options, None).unwrap();
        compare_sql(
            sql,
            "CREATE TABLE IF NOT EXISTS `my_table` (\n  id Int32,\n  name Nullable(String) \
             DEFAULT 'unknown'\n)\nENGINE = MergeTree\nORDER BY (id)",
        );
    }

    #[test]
    fn test_create_table_statement_with_database() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let options = CreateOptions::new("Memory");
        let sql =
            create_table_statement_from_arrow(Some("my_db"), "my_table", &schema, &options, None)
                .unwrap();
        compare_sql(
            sql,
            "CREATE TABLE IF NOT EXISTS my_db.`my_table` (\nid Int32\n)\nENGINE = Memory\nORDER \
             BY tuple()",
        );
    }

    #[test]
    fn test_create_table_statement_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let options = CreateOptions::new("MergeTree");
        let result = create_table_statement_from_arrow(None, "my_table", &schema, &options, None);
        assert!(matches!(result, Err(Error::DDLMalformed(_))));
    }

    #[test]
    fn test_create_table_with_nullable_dictionary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("id", DataType::Int32, false),
        ]));

        let enum_i8 = HashMap::from_iter([(
            "status".to_string(),
            Type::Enum8(vec![("active".to_string(), 1_i8), ("inactive".to_string(), 2)]),
        )]);

        let options = CreateOptions::new("MergeTree").with_order_by(&["id".to_string()]);
        let enum_options = options.clone().with_schema_conversions(enum_i8);

        // If the nullable dictionary will not be converted to enum, this will fail
        assert!(
            create_table_statement_from_arrow(None, "test_table", &schema, &options, None).is_err()
        );

        // Otherwise it will succeed
        let sql =
            create_table_statement_from_arrow(None, "test_table", &schema, &enum_options, None)
                .expect("Should generate valid SQL");

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_table`"));
        assert!(sql.contains("status Nullable(Enum8('active' = 1,'inactive' = 2))"));
        assert!(sql.contains("id Int32"));
        assert!(sql.contains("ENGINE = MergeTree"));
        assert!(sql.contains("ORDER BY (id)"));
    }

    #[test]
    fn test_create_table_with_enum8() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("id", DataType::Int32, false),
        ]));

        let enum_i8 = HashMap::from_iter([(
            "status".to_string(),
            Type::Enum8(vec![("active".to_string(), 1_i8), ("inactive".to_string(), 2)]),
        )]);

        let options = CreateOptions::new("MergeTree")
            .with_order_by(&["id".to_string()])
            .with_schema_conversions(enum_i8);

        let sql = create_table_statement_from_arrow(None, "test_table", &schema, &options, None)
            .expect("Should generate valid SQL");

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_table`"));
        assert!(sql.contains("status Enum8('active' = 1,'inactive' = 2)"));
        assert!(sql.contains("id Int32"));
        assert!(sql.contains("ENGINE = MergeTree"));
        assert!(sql.contains("ORDER BY (id)"));
    }

    #[test]
    fn test_create_table_with_enum16() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "category",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("value", DataType::Float32, true),
        ]));

        let enum_i16 = HashMap::from_iter([(
            "category".to_string(),
            Type::Enum16(vec![("x".to_string(), 1), ("y".to_string(), 2), ("z".to_string(), 3)]),
        )]);
        let options = CreateOptions::new("MergeTree")
            .with_order_by(&["category".to_string()])
            .with_schema_conversions(enum_i16);

        let sql = create_table_statement_from_arrow(None, "test_table", &schema, &options, None)
            .expect("Should generate valid SQL");

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_table`"));
        assert!(sql.contains("category Enum16('x' = 1,'y' = 2,'z' = 3)"));
        assert!(sql.contains("value Nullable(Float32)"));
        assert!(sql.contains("ENGINE = MergeTree"));
        assert!(sql.contains("ORDER BY (category)"));
    }

    #[test]
    fn test_create_table_with_invalid_enum_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("status", DataType::Int32, true)]));

        let enum_i8 = HashMap::from_iter([(
            "status".to_string(),
            Type::Enum8(vec![("active".to_string(), 1_i8), ("inactive".to_string(), 2)]),
        )]);

        let options = CreateOptions::new("MergeTree").with_schema_conversions(enum_i8);

        let result = create_table_statement_from_arrow(None, "test_table", &schema, &options, None);

        assert!(matches!(
            result,
            Err(Error::TypeConversion(msg))
            if msg.contains("expected LowCardinality(String) or String/Binary, found Nullable(Int32)")
        ));
    }

    #[test]
    fn test_create_table_with_non_low_cardinality_enum() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));

        let enum_i8 = HashMap::from_iter([(
            "name".to_string(),
            Type::Enum8(vec![("active".to_string(), 1_i8), ("inactive".to_string(), 2)]),
        )]);
        let options = CreateOptions::new("MergeTree").with_schema_conversions(enum_i8);

        let sql =
            create_table_statement_from_arrow(None, "test_table", &schema, &options, None).unwrap();

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_table`"));
        assert!(sql.contains("name Nullable(Enum8('active' = 1,'inactive' = 2))"));
        assert!(sql.contains("ENGINE = MergeTree"));
    }

    // The arrow data type drives nullability
    #[test]
    fn test_create_table_with_nullable_field_non_nullable_enum() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
        ]));

        let enum_i8 = HashMap::from_iter([
            (
                "name".to_string(),
                Type::Enum8(vec![("active".to_string(), 1_i8), ("inactive".to_string(), 2)])
                    .into_nullable(),
            ),
            (
                "status".to_string(),
                Type::Enum8(vec![("active".to_string(), 1_i8), ("inactive".to_string(), 2)])
                    .into_nullable(),
            ),
        ]);
        let options = CreateOptions::new("MergeTree").with_schema_conversions(enum_i8);
        let arrow_options = ArrowOptions::default()
            // Deserialize strings as Utf8, not Binary
            .with_strings_as_strings(true)
            // Deserialize Date as Date32
            .with_use_date32_for_date(true)
            // Ignore fields that ClickHouse doesn't support.
            .with_strict_schema(false)
            .with_disable_strict_schema_ddl(true);

        let sql = create_table_statement_from_arrow(
            None,
            "test_table",
            &schema,
            &options,
            Some(arrow_options),
        )
        .unwrap();

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_table`"));
        assert!(sql.contains("name Nullable(Enum8('active' = 1,'inactive' = 2))"));
        assert!(sql.contains("status Enum8('active' = 1,'inactive' = 2)"));
        assert!(sql.contains("ENGINE = MergeTree"));
    }

    #[test]
    fn test_create_table_with_mixed_enum_and_non_enum() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "category",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                false,
            ),
        ]));

        let enums = HashMap::from_iter([
            (
                "status".to_string(),
                Type::Enum8(vec![("active".to_string(), 1_i8), ("inactive".to_string(), 2)]),
            ),
            (
                "category".to_string(),
                Type::Enum16(vec![("x".to_string(), 1), ("y".to_string(), 2)]),
            ),
        ]);

        let options = CreateOptions::new("MergeTree")
            .with_order_by(&["category".to_string()])
            .with_schema_conversions(enums);

        let sql = create_table_statement_from_arrow(None, "test_table", &schema, &options, None)
            .expect("Should generate valid SQL");

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `test_table`"));
        assert!(sql.contains("status Nullable(Enum8('active' = 1,'inactive' = 2))"));
        assert!(sql.contains("name Nullable(String)"));
        assert!(sql.contains("category Enum16('x' = 1,'y' = 2)"));
        assert!(sql.contains("ENGINE = MergeTree"));
        assert!(sql.contains("ORDER BY (category)"));
    }

    #[test]
    fn test_engines() {
        use super::ClickHouseEngine::*;

        let engines = [
            MergeTree,
            AggregatingMergeTree,
            CollapsingMergeTree,
            ReplacingMergeTree,
            SummingMergeTree,
            Memory,
            Log,
            StripeLog,
            TinyLog,
            Other("NonExistentEngine".into()),
        ];

        for engine in engines {
            let engine_str = engine.to_string();
            let engine_from = ClickHouseEngine::from(engine_str);
            assert_eq!(engine, engine_from);
        }
    }
}
