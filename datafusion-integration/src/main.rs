use arrow::util::pretty;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::displayable;

use datafusion::prelude::*;
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx: SessionContext = SessionContext::new();
    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("./testing/data/csv/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;
    let sql1 = "SELECT c1,c3  FROM aggregate_test_100 WHERE c3 < 0 AND c1='a'";
    let r1 = run_pipeline(&ctx, sql1).await?;

    let sql2 = "SELECT c1,c4  FROM aggregate_test_100 WHERE c4 > 0 AND c1='b'";
    let r2 = run_pipeline(&ctx, sql2).await?;

    Ok(())
}

async fn run_pipeline(ctx: &SessionContext, sql: &str) -> Result<(), DataFusionError> {
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql).await?;
    // create datafusion physical plan (trait)
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan).await?;
    println!(
        "Detailed physical plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );

    let results = vayu::execute(vayu::SchedulerPipeline { plan }).unwrap();
    pretty::print_batches(&results)?;
    Ok(())
}
