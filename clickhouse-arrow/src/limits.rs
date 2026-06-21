//! Query result limits and truncation handling.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use futures_util::Stream;
use pin_project::pin_project;

use crate::Result;

/// Reason why query results were truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncationReason {
    MemoryLimit,
    RowLimit,
    BatchLimit,
}

impl std::fmt::Display for TruncationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TruncationReason::MemoryLimit => write!(f, "memory limit exceeded"),
            TruncationReason::RowLimit => write!(f, "row limit exceeded"),
            TruncationReason::BatchLimit => write!(f, "batch limit exceeded"),
        }
    }
}

/// Statistics about the limited query results.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryStats {
    pub rows_returned:     u64,
    pub batches_returned:  u64,
    pub memory_bytes:      usize,
    pub truncated:         bool,
    pub truncation_reason: Option<TruncationReason>,
}

impl QueryStats {
    #[must_use]
    pub fn is_truncated(&self) -> bool { self.truncated }

    #[must_use]
    pub fn summary(&self) -> String {
        let truncation = if self.truncated {
            format!(
                " (TRUNCATED: {})",
                self.truncation_reason.map_or("unknown".to_string(), |r| r.to_string())
            )
        } else {
            String::new()
        };

        format!(
            "{} rows, {} batches, {} bytes{}",
            self.rows_returned, self.batches_returned, self.memory_bytes, truncation
        )
    }
}

/// Configuration for query result limits.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryLimits {
    pub max_memory_bytes: Option<usize>,
    pub max_rows:         Option<u64>,
    pub max_batches:      Option<u64>,
}

impl QueryLimits {
    #[must_use]
    pub fn none() -> Self { Self::default() }

    #[must_use]
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }

    #[must_use]
    pub fn with_max_memory_mb(self, mb: usize) -> Self { self.with_max_memory(mb * 1024 * 1024) }

    #[must_use]
    pub fn with_max_memory_gb(self, gb: usize) -> Self {
        self.with_max_memory(gb * 1024 * 1024 * 1024)
    }

    #[must_use]
    pub fn with_max_rows(mut self, rows: u64) -> Self {
        self.max_rows = Some(rows);
        self
    }

    #[must_use]
    pub fn with_max_batches(mut self, batches: u64) -> Self {
        self.max_batches = Some(batches);
        self
    }

    #[must_use]
    pub fn has_limits(&self) -> bool {
        self.max_memory_bytes.is_some() || self.max_rows.is_some() || self.max_batches.is_some()
    }
}

#[derive(Debug, Default)]
struct LimitState {
    total_rows:        u64,
    total_batches:     u64,
    total_memory:      usize,
    truncated:         bool,
    truncation_reason: Option<TruncationReason>,
}

impl LimitState {
    fn to_stats(&self) -> QueryStats {
        QueryStats {
            rows_returned:     self.total_rows,
            batches_returned:  self.total_batches,
            memory_bytes:      self.total_memory,
            truncated:         self.truncated,
            truncation_reason: self.truncation_reason,
        }
    }
}

#[pin_project]
pub struct LimitedStream<S> {
    #[pin]
    inner:   S,
    limits:  QueryLimits,
    state:   LimitState,
    stopped: bool,
}

impl<S> LimitedStream<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(inner: S, limits: QueryLimits) -> Self {
        Self { inner, limits, state: LimitState::default(), stopped: false }
    }

    pub fn stats(&self) -> QueryStats { self.state.to_stats() }
}

impl<S> Stream for LimitedStream<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.stopped {
            return Poll::Ready(None);
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let batch_rows = batch.num_rows() as u64;
                let batch_memory = batch.get_array_memory_size();

                let reason = if let Some(max_rows) = this.limits.max_rows {
                    if this.state.total_rows + batch_rows > max_rows {
                        Some(TruncationReason::RowLimit)
                    } else {
                        None
                    }
                } else {
                    None
                }
                .or_else(|| {
                    this.limits.max_memory_bytes.and_then(|max_memory| {
                        if this.state.total_memory + batch_memory > max_memory {
                            Some(TruncationReason::MemoryLimit)
                        } else {
                            None
                        }
                    })
                })
                .or_else(|| {
                    this.limits.max_batches.and_then(|max_batches| {
                        if this.state.total_batches + 1 > max_batches {
                            Some(TruncationReason::BatchLimit)
                        } else {
                            None
                        }
                    })
                });

                if let Some(reason) = reason {
                    this.state.truncated = true;
                    this.state.truncation_reason = Some(reason);
                    *this.stopped = true;
                    return Poll::Ready(None);
                }

                this.state.total_rows += batch_rows;
                this.state.total_batches += 1;
                this.state.total_memory += batch_memory;

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[pin_project]
pub struct LimitedResponse<S> {
    #[pin]
    stream: LimitedStream<S>,
}

impl<S> LimitedResponse<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(inner: S, limits: QueryLimits) -> Self {
        Self { stream: LimitedStream::new(inner, limits) }
    }

    pub fn stats(&self) -> QueryStats { self.stream.stats() }

    pub fn is_truncated(&self) -> bool { self.stream.state.truncated }

    pub fn truncation_reason(&self) -> Option<TruncationReason> {
        self.stream.state.truncation_reason
    }
}

impl<S> Stream for LimitedResponse<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures_util::StreamExt;

    use super::*;

    #[allow(clippy::cast_possible_wrap)] // Test values are small and deterministic.
    fn create_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values = (0..rows).map(|i| i as i64).collect::<Vec<_>>();
        let array = Int64Array::from(values);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn limited_response_truncates_on_row_limit() {
        let batches = vec![
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
        ];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_rows(150);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0usize;
        while let Some(next) = limited.next().await {
            drop(next.unwrap());
            count += 1;
        }

        assert_eq!(count, 1);
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::RowLimit));
        assert_eq!(limited.stats().rows_returned, 100);
    }

    #[tokio::test]
    async fn query_stats_summary_reports_truncation() {
        let stats = QueryStats {
            rows_returned:     200,
            batches_returned:  2,
            memory_bytes:      4096,
            truncated:         true,
            truncation_reason: Some(TruncationReason::BatchLimit),
        };

        let text = stats.summary();
        assert!(text.contains("200 rows"));
        assert!(text.contains("2 batches"));
        assert!(text.contains("TRUNCATED"));
        assert!(text.contains("batch limit exceeded"));
    }

    #[test]
    fn truncation_reason_display_and_stats_helpers() {
        assert_eq!(TruncationReason::MemoryLimit.to_string(), "memory limit exceeded");
        assert_eq!(TruncationReason::RowLimit.to_string(), "row limit exceeded");
        assert_eq!(TruncationReason::BatchLimit.to_string(), "batch limit exceeded");

        let full = QueryStats {
            rows_returned:     5,
            batches_returned:  1,
            memory_bytes:      128,
            truncated:         false,
            truncation_reason: None,
        };
        assert!(!full.is_truncated());
        assert!(!full.summary().contains("TRUNCATED"));
    }

    #[test]
    fn query_limits_builder_helpers() {
        let none = QueryLimits::none();
        assert!(!none.has_limits());

        let limits = QueryLimits::none().with_max_memory(1024).with_max_rows(9).with_max_batches(3);
        assert_eq!(limits.max_memory_bytes, Some(1024));
        assert_eq!(limits.max_rows, Some(9));
        assert_eq!(limits.max_batches, Some(3));
        assert!(limits.has_limits());

        assert_eq!(
            QueryLimits::none().with_max_memory_mb(2).max_memory_bytes,
            Some(2 * 1024 * 1024)
        );
        assert_eq!(
            QueryLimits::none().with_max_memory_gb(1).max_memory_bytes,
            Some(1024 * 1024 * 1024)
        );
    }

    #[tokio::test]
    async fn limited_response_truncates_on_memory_limit() {
        let first = create_test_batch(64);
        let first_size = first.get_array_memory_size();
        let batches = vec![Ok(first), Ok(create_test_batch(64))];
        let stream = futures_util::stream::iter(batches);
        // Allow one batch and truncate before second.
        let limits = QueryLimits::none().with_max_memory(first_size + 1);
        let mut limited = LimitedResponse::new(stream, limits);

        assert!(limited.next().await.unwrap().is_ok());
        assert!(limited.next().await.is_none());
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::MemoryLimit));
    }

    #[tokio::test]
    async fn limited_response_truncates_on_batch_limit_and_passes_errors() {
        let batches = vec![
            Ok(create_test_batch(1)),
            Ok(create_test_batch(1)),
            Err(crate::Error::Protocol("stop".into())),
        ];
        let stream = futures_util::stream::iter(batches);
        let mut limited = LimitedResponse::new(stream, QueryLimits::none().with_max_batches(1));

        assert!(limited.next().await.unwrap().is_ok());
        assert!(limited.next().await.is_none());
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::BatchLimit));

        let stream = futures_util::stream::iter(vec![Err(crate::Error::Protocol("err".into()))]);
        let mut passthrough = LimitedResponse::new(stream, QueryLimits::none().with_max_rows(100));
        let err = passthrough.next().await.unwrap().unwrap_err();
        assert!(matches!(err, crate::Error::Protocol(msg) if msg == "err"));
    }
}
