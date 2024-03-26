use arrow::util::pretty;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use std::sync::Arc;
mod util;
use ahash::RandomState;

use vayu::VayuExecutionEngine;
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx: SessionContext = SessionContext::new();
    // register csv file with the execution context
    ctx.register_csv(
        "a",
        &format!("./testing/data/csv/join_test_A.csv"),
        CsvReadOptions::new(),
    )
    .await?;
    ctx.register_csv(
        "b",
        &format!("./testing/data/csv/join_test_B.csv"),
        CsvReadOptions::new(),
    )
    .await?;
    let mut executor = VayuExecutionEngine::new();
    let uuid = 1;
    // let sql1 = "SELECT *  FROM aggregate_test_100 JOIN aggregate_test_100 ON c1";
    let sql1 = "SELECT *  FROM a,b WHERE a.a1 = b.b1+1 ";
    let plan1 = util::get_execution_plan_from_sql(&ctx, sql1).await?;
    let probe_plan = plan1.clone();
    let build_plan = util::get_hash_build_pipeline(plan1);
    let p = build_plan.as_any();
    let info = if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        let on_left = exec.on().iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        let reservation =
            MemoryConsumer::new("HashJoinInput").register(ctx.task_ctx().memory_pool());
        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        vayu::HashMapInfo {
            random_state,
            on_left,
            schema: exec.left().schema(),
            reservation: reservation,
        }
    } else {
        panic!("build operator expected");
    };
    let _ = run_pipeline(
        &mut executor,
        build_plan,
        vayu::SchedulerSinkType::HashMapStorage(uuid, info),
    )
    .await?;

    let _ = run_pipeline(
        &mut executor,
        probe_plan,
        vayu::SchedulerSinkType::ReturnOutput,
    )
    .await?;

    // let blob = executor.store.remove(2).unwrap();
    // let results = blob.get_records();

    // pretty::print_batches(&results)?;

    Ok(())
}
async fn test_scan_filter_project() -> Result<()> {
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
    let sql1 = "SELECT c1,c4,c13  FROM aggregate_test_100 WHERE c3 < 0 AND c1='a'";

    let plan1 = util::get_execution_plan_from_sql(&ctx, sql1).await?;

    let _ = run_pipeline(
        &mut executor,
        plan1,
        vayu::SchedulerSinkType::RecordBatchStorage(uuid),
    )
    .await?;

    let sql2 = "SELECT c1,c4,c13  FROM aggregate_test_100 WHERE c4 > 0 AND c1='b'";
    let plan2 = util::get_execution_plan_from_sql(&ctx, sql2).await?;
    let _ = run_pipeline(
        &mut executor,
        plan2,
        vayu::SchedulerSinkType::RecordBatchStorage(uuid),
    )
    .await?;

    let blob = executor.store.remove(uuid).unwrap();
    let results = blob.get_records();

    pretty::print_batches(&results)?;

    Ok(())
}

async fn run_pipeline(
    executor: &mut VayuExecutionEngine,
    plan: Arc<dyn ExecutionPlan>,
    sink: vayu::SchedulerSinkType,
) -> Result<(), DataFusionError> {
    let scheduler_pipeline = vayu::SchedulerPipeline::new(plan, sink);

    let results = executor.execute(scheduler_pipeline).unwrap();
    match results {
        vayu::SchedulerSink::ReturnOutput(results) => pretty::print_batches(&results)?,
        vayu::SchedulerSink::RecordBatchStorage(uuid) => println!("uuid is {uuid}"),
    }
    Ok(())
}
