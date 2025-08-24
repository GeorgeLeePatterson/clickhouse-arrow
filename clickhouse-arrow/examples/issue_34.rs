#![expect(unused_crate_dependencies)]
mod common;

use arrow::array::RecordBatch;
use clickhouse_arrow::prelude::*;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let client = ArrowClient::builder()
        .with_destination("175.24.190.148:2345")
        .with_username("root")
        .with_password("clickhouse")
        .with_database("stock_L2_data")
        .build_arrow()
        .await?;
    let query = "
        SELECT *
        FROM stock_L2_data.gq_trans
        WHERE (1=1) AND (trading_date BETWEEN '2025-07-17' AND '2025-07-17')";
    let results = client.query(query, None).await?.collect::<Vec<_>>().await;
    let batches = results.into_iter().collect::<Result<Vec<_>>>()?;
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    eprintln!("Rows: {rows}");
    Ok(())
}
