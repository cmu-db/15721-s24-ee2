use arrow::util::pretty;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::displayable;

use datafusion::physical_plan::test::exec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use std::sync::Arc;
use vayu::VayuExecutionEngine;
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
    let mut executor = VayuExecutionEngine::new();
    let uuid = 1;
    let sql1 = "SELECT c1,c3,c13  FROM aggregate_test_100 WHERE c3 < 0 AND c1='a'";
    let plan1 = get_execution_plan_from_sql(&ctx, sql1).await?;
    let _ = run_pipeline(&mut executor, plan1, uuid).await?;

    let sql2 = "SELECT c1,c4,c13  FROM aggregate_test_100 WHERE c4 > 0 AND c1='b'";
    let plan2 = get_execution_plan_from_sql(&ctx, sql2).await?;
    let _ = run_pipeline(&mut executor, plan2, uuid).await?;

    let blob = executor.store.remove(uuid).unwrap();

    let results = blob.get_records();
    pretty::print_batches(&results)?;

    Ok(())
}
async fn get_execution_plan_from_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    // create datafusion logical plan
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql).await?;
    // create datafusion physical plan
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan).await?;
    // print datafusion physical plan
    // println!(
    //     "Detailed physical plan:\n{}",
    //     displayable(plan.as_ref()).indent(true)
    // );
    Ok(plan)
}

async fn run_pipeline(
    executor: &mut VayuExecutionEngine,
    plan: Arc<dyn ExecutionPlan>,
    uuid: i32,
) -> Result<(), DataFusionError> {
    let scheduler_pipeline =
        vayu::SchedulerPipeline::new(plan, vayu::SchedulerSinkType::RecordBatchStorage(uuid));

    let results = executor.execute(scheduler_pipeline).unwrap();
    match results {
        vayu::SchedulerSink::ReturnOutput(results) => pretty::print_batches(&results)?,
        vayu::SchedulerSink::RecordBatchStorage(uuid) => println!("uuid is {uuid}"),
    }
    Ok(())
}
