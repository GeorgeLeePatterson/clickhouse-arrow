//! EXPLAIN query support.

use std::fmt;

use arrow::record_batch::RecordBatch;

use crate::limits::QueryLimits;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ExplainOperation {
    Ast,
    Syntax,
    #[default]
    Plan,
    Pipeline,
    Estimate,
}

impl ExplainOperation {
    #[must_use]
    pub fn as_sql(&self) -> &'static str {
        match self {
            ExplainOperation::Ast => "AST",
            ExplainOperation::Syntax => "SYNTAX",
            ExplainOperation::Plan => "PLAN",
            ExplainOperation::Pipeline => "PIPELINE",
            ExplainOperation::Estimate => "ESTIMATE",
        }
    }

    #[must_use]
    pub fn supports_json(&self) -> bool { matches!(self, ExplainOperation::Plan) }

    #[must_use]
    pub fn is_tabular(&self) -> bool { matches!(self, ExplainOperation::Estimate) }
}

impl fmt::Display for ExplainOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.as_sql()) }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ExplainFormat {
    #[default]
    Auto,
    Text,
    Json,
    Arrow,
}

impl ExplainFormat {
    #[must_use]
    pub fn resolve(self, operation: ExplainOperation) -> ExplainFormat {
        match self {
            ExplainFormat::Auto => {
                if operation.is_tabular() {
                    ExplainFormat::Arrow
                } else {
                    ExplainFormat::Text
                }
            }
            explicit => explicit,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ExplainMode {
    #[default]
    Parallel,
    ExplainOnly,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExplainOptions {
    pub operation: ExplainOperation,
    pub format:    ExplainFormat,
    pub mode:      ExplainMode,
}

impl ExplainOptions {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    #[must_use]
    pub fn ast() -> Self { Self { operation: ExplainOperation::Ast, ..Default::default() } }

    #[must_use]
    pub fn syntax() -> Self { Self { operation: ExplainOperation::Syntax, ..Default::default() } }

    #[must_use]
    pub fn plan() -> Self { Self { operation: ExplainOperation::Plan, ..Default::default() } }

    #[must_use]
    pub fn pipeline() -> Self {
        Self { operation: ExplainOperation::Pipeline, ..Default::default() }
    }

    #[must_use]
    pub fn estimate() -> Self {
        Self { operation: ExplainOperation::Estimate, ..Default::default() }
    }

    #[must_use]
    pub fn with_operation(mut self, operation: ExplainOperation) -> Self {
        self.operation = operation;
        self
    }

    #[must_use]
    pub fn with_format(mut self, format: ExplainFormat) -> Self {
        self.format = format;
        self
    }

    #[must_use]
    pub fn with_json(mut self) -> Self {
        self.format = ExplainFormat::Json;
        self
    }

    #[must_use]
    pub fn with_mode(mut self, mode: ExplainMode) -> Self {
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn explain_only(mut self) -> Self {
        self.mode = ExplainMode::ExplainOnly;
        self
    }

    #[must_use]
    pub fn build_prefix(&self) -> String {
        let resolved_format = self.format.resolve(self.operation);
        let json_suffix =
            if resolved_format == ExplainFormat::Json && self.operation.supports_json() {
                " json=1"
            } else {
                ""
            };

        format!("EXPLAIN {}{}", self.operation.as_sql(), json_suffix)
    }
}

#[derive(Debug, Clone)]
pub enum ExplainResult {
    Text(String),
    #[cfg(feature = "serde")]
    Json(serde_json::Value),
    Arrow(RecordBatch),
}

impl ExplainResult {
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        if let ExplainResult::Text(text) = self { Some(text) } else { None }
    }

    #[must_use]
    pub fn as_arrow(&self) -> Option<&RecordBatch> {
        if let ExplainResult::Arrow(batch) = self { Some(batch) } else { None }
    }

    #[cfg(feature = "serde")]
    #[must_use]
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            ExplainResult::Json(json) => Some(json),
            _ => None,
        }
    }
}

impl fmt::Display for ExplainResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainResult::Text(text) => write!(f, "{text}"),
            #[cfg(feature = "serde")]
            ExplainResult::Json(json) => {
                if let Ok(pretty) = serde_json::to_string_pretty(json) {
                    write!(f, "{pretty}")
                } else {
                    write!(f, "{json}")
                }
            }
            ExplainResult::Arrow(batch) => {
                write!(f, "RecordBatch({} rows, {} columns)", batch.num_rows(), batch.num_columns())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainEstimateRow {
    pub database: String,
    pub table:    String,
    pub parts:    u64,
    pub rows:     u64,
    pub marks:    u64,
}

impl ExplainEstimateRow {
    /// # Errors
    /// - Returns error if columns don't exist on `RecordBatch`
    pub fn from_batch(batch: &RecordBatch) -> crate::Result<Vec<Self>> {
        use arrow::array::{AsArray, StringArray};

        let database_col = batch
            .column_by_name("database")
            .ok_or_else(|| crate::Error::Deserialize("Missing 'database' column".into()))?;
        let table_col = batch
            .column_by_name("table")
            .ok_or_else(|| crate::Error::Deserialize("Missing 'table' column".into()))?;
        let parts_col = batch
            .column_by_name("parts")
            .ok_or_else(|| crate::Error::Deserialize("Missing 'parts' column".into()))?;
        let rows_col = batch
            .column_by_name("rows")
            .ok_or_else(|| crate::Error::Deserialize("Missing 'rows' column".into()))?;
        let marks_col = batch
            .column_by_name("marks")
            .ok_or_else(|| crate::Error::Deserialize("Missing 'marks' column".into()))?;

        let databases = database_col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            crate::Error::Deserialize("'database' column is not a string array".into())
        })?;
        let tables = table_col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            crate::Error::Deserialize("'table' column is not a string array".into())
        })?;
        let parts =
            parts_col.as_primitive_opt::<arrow::datatypes::UInt64Type>().ok_or_else(|| {
                crate::Error::Deserialize("'parts' column is not a UInt64 array".into())
            })?;
        let rows =
            rows_col.as_primitive_opt::<arrow::datatypes::UInt64Type>().ok_or_else(|| {
                crate::Error::Deserialize("'rows' column is not a UInt64 array".into())
            })?;
        let marks =
            marks_col.as_primitive_opt::<arrow::datatypes::UInt64Type>().ok_or_else(|| {
                crate::Error::Deserialize("'marks' column is not a UInt64 array".into())
            })?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            out.push(Self {
                database: databases.value(i).to_string(),
                table:    tables.value(i).to_string(),
                parts:    parts.value(i),
                rows:     rows.value(i),
                marks:    marks.value(i),
            });
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryOptions {
    pub limits:  Option<QueryLimits>,
    pub explain: Option<ExplainOptions>,
}

impl QueryOptions {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    #[must_use]
    pub fn with_limits(mut self, limits: QueryLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    #[must_use]
    pub fn with_explain(mut self, explain: ExplainOptions) -> Self {
        self.explain = Some(explain);
        self
    }

    #[must_use]
    pub fn has_options(&self) -> bool { self.limits.is_some() || self.explain.is_some() }

    #[must_use]
    pub fn has_explain(&self) -> bool { self.explain.is_some() }

    #[must_use]
    pub fn is_explain_only(&self) -> bool {
        self.explain.as_ref().is_some_and(|e| e.mode == ExplainMode::ExplainOnly)
    }
}

#[cfg_attr(
    not(feature = "serde"),
    expect(
        clippy::unnecessary_wraps,
        reason = "Signature stays uniform across serde/non-serde builds"
    )
)]
pub(crate) fn explain_result_from_batches(
    batches: Vec<RecordBatch>,
    format: ExplainFormat,
) -> crate::Result<ExplainResult> {
    match format {
        ExplainFormat::Arrow | ExplainFormat::Auto => {
            if let Some(batch) = batches.into_iter().next() {
                Ok(ExplainResult::Arrow(batch))
            } else {
                Ok(ExplainResult::Text(String::new()))
            }
        }
        #[cfg(feature = "serde")]
        ExplainFormat::Json => {
            let text = explain_text_from_batches(&batches);
            let json = serde_json::from_str::<serde_json::Value>(&text).map_err(|e| {
                crate::Error::Deserialize(format!("Failed to parse EXPLAIN JSON: {e}"))
            })?;
            Ok(ExplainResult::Json(json))
        }
        #[cfg(not(feature = "serde"))]
        ExplainFormat::Json => Ok(ExplainResult::Text(explain_text_from_batches(&batches))),
        ExplainFormat::Text => Ok(ExplainResult::Text(explain_text_from_batches(&batches))),
    }
}

pub(crate) fn explain_text_from_batches(batches: &[RecordBatch]) -> String {
    use arrow::array::{Array, StringArray};

    let mut lines = Vec::new();
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }

        let Some(strings) = batch.column(0).as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for i in 0..strings.len() {
            if !strings.is_null(i) {
                lines.push(strings.value(i).to_string());
            }
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn text_batch(values: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let array = StringArray::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    fn estimate_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("database", DataType::Utf8, false),
            Field::new("table", DataType::Utf8, false),
            Field::new("parts", DataType::UInt64, false),
            Field::new("rows", DataType::UInt64, false),
            Field::new("marks", DataType::UInt64, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["db1", "db2"])),
            Arc::new(StringArray::from(vec!["t1", "t2"])),
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(UInt64Array::from(vec![10, 20])),
            Arc::new(UInt64Array::from(vec![100, 200])),
        ])
        .unwrap()
    }

    #[test]
    fn explain_prefix_generation_is_stable() {
        assert_eq!(ExplainOptions::plan().build_prefix(), "EXPLAIN PLAN");
        assert_eq!(ExplainOptions::plan().with_json().build_prefix(), "EXPLAIN PLAN json=1");
        assert_eq!(ExplainOptions::estimate().build_prefix(), "EXPLAIN ESTIMATE");
    }

    #[test]
    fn explain_operation_helpers_are_consistent() {
        assert_eq!(ExplainOperation::Ast.as_sql(), "AST");
        assert_eq!(ExplainOperation::Syntax.as_sql(), "SYNTAX");
        assert_eq!(ExplainOperation::Plan.as_sql(), "PLAN");
        assert_eq!(ExplainOperation::Pipeline.as_sql(), "PIPELINE");
        assert_eq!(ExplainOperation::Estimate.as_sql(), "ESTIMATE");

        assert!(ExplainOperation::Plan.supports_json());
        assert!(!ExplainOperation::Ast.supports_json());
        assert!(ExplainOperation::Estimate.is_tabular());
        assert!(!ExplainOperation::Plan.is_tabular());
        assert_eq!(ExplainOperation::Pipeline.to_string(), "PIPELINE");
    }

    #[test]
    fn explain_format_auto_resolution_is_operation_aware() {
        assert_eq!(ExplainFormat::Auto.resolve(ExplainOperation::Estimate), ExplainFormat::Arrow);
        assert_eq!(ExplainFormat::Auto.resolve(ExplainOperation::Plan), ExplainFormat::Text);
        assert_eq!(ExplainFormat::Json.resolve(ExplainOperation::Plan), ExplainFormat::Json);
        assert_eq!(ExplainFormat::Arrow.resolve(ExplainOperation::Ast), ExplainFormat::Arrow);
    }

    #[test]
    fn explain_prefix_respects_json_capability() {
        assert_eq!(ExplainOptions::plan().with_json().build_prefix(), "EXPLAIN PLAN json=1");
        // JSON suffix is only valid for PLAN.
        assert_eq!(ExplainOptions::syntax().with_json().build_prefix(), "EXPLAIN SYNTAX");
    }

    #[test]
    fn query_options_explain_only_detection() {
        let options = QueryOptions::new().with_explain(ExplainOptions::plan().explain_only());
        assert!(options.is_explain_only());
        assert!(options.has_options());
    }

    #[test]
    fn query_options_helpers_cover_all_combinations() {
        let empty = QueryOptions::new();
        assert!(!empty.has_options());
        assert!(!empty.has_explain());
        assert!(!empty.is_explain_only());

        let with_limits = QueryOptions::new().with_limits(QueryLimits::none().with_max_rows(1));
        assert!(with_limits.has_options());
        assert!(!with_limits.has_explain());

        let with_explain = QueryOptions::new().with_explain(ExplainOptions::plan());
        assert!(with_explain.has_options());
        assert!(with_explain.has_explain());
        assert!(!with_explain.is_explain_only());
    }

    #[test]
    fn explain_text_from_batches_ignores_non_utf8_and_nulls() {
        let utf8 = text_batch(&[Some("line-1"), None, Some("line-2")]);
        let non_utf8_schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let non_utf8 =
            RecordBatch::try_new(non_utf8_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let empty_cols = RecordBatch::new_empty(Arc::new(Schema::empty()));

        let out = explain_text_from_batches(&[utf8, non_utf8, empty_cols]);
        assert_eq!(out, "line-1\nline-2");
    }

    #[test]
    fn explain_result_from_batches_text_and_arrow_paths() {
        let batch = text_batch(&[Some("x"), Some("y")]);

        let text = explain_result_from_batches(vec![batch.clone()], ExplainFormat::Text).unwrap();
        assert_eq!(text.as_text(), Some("x\ny"));
        assert!(text.as_arrow().is_none());
        assert_eq!(text.to_string(), "x\ny");

        let arrow = explain_result_from_batches(vec![batch.clone()], ExplainFormat::Arrow).unwrap();
        let arrow_batch = arrow.as_arrow().expect("expected arrow result");
        assert_eq!(arrow_batch.num_rows(), 2);
        assert!(arrow.to_string().contains("RecordBatch(2 rows, 1 columns)"));

        let auto = explain_result_from_batches(vec![batch], ExplainFormat::Auto).unwrap();
        assert!(auto.as_arrow().is_some());

        let empty_arrow = explain_result_from_batches(Vec::new(), ExplainFormat::Arrow).unwrap();
        assert_eq!(empty_arrow.as_text(), Some(""));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn explain_result_from_batches_json_path() {
        let batch = text_batch(&[Some("{\"k\":1}")]);
        let json = explain_result_from_batches(vec![batch], ExplainFormat::Json).unwrap();
        let value = json.as_json().expect("json should be available");
        assert_eq!(value.get("k").and_then(serde_json::Value::as_i64), Some(1));
        assert!(json.to_string().contains("\"k\""));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn explain_result_from_batches_json_invalid_payload_errors() {
        let batch = text_batch(&[Some("not json")]);
        let err = explain_result_from_batches(vec![batch], ExplainFormat::Json).unwrap_err();
        assert!(err.to_string().contains("Failed to parse EXPLAIN JSON"));
    }

    #[test]
    fn explain_estimate_row_from_batch_success() {
        let rows = ExplainEstimateRow::from_batch(&estimate_batch()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].database, "db1");
        assert_eq!(rows[1].table, "t2");
        assert_eq!(rows[1].parts, 2);
        assert_eq!(rows[1].rows, 20);
        assert_eq!(rows[1].marks, 200);
    }

    #[test]
    fn explain_estimate_row_from_batch_missing_column_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new("database", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["db"]))]).unwrap();
        let err = ExplainEstimateRow::from_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("Missing 'table' column"));
    }

    #[test]
    fn explain_estimate_row_from_batch_wrong_type_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("database", DataType::Utf8, false),
            Field::new("table", DataType::Utf8, false),
            Field::new("parts", DataType::Int32, false),
            Field::new("rows", DataType::UInt64, false),
            Field::new("marks", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["db"])),
            Arc::new(StringArray::from(vec!["tbl"])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(UInt64Array::from(vec![1])),
        ])
        .unwrap();
        let err = ExplainEstimateRow::from_batch(&batch).unwrap_err();
        assert!(err.to_string().contains("'parts' column is not a UInt64 array"));
    }
}
