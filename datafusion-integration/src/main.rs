use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use std::sync::Arc;
mod util;
use vayu::{sinks, VayuExecutionEngine};

#[tokio::main]
async fn main() -> Result<()> {
    // test_scan_filter_project().await?;
    test_hash_join().await?;
    Ok(())
}

async fn test_hash_join() -> Result<()> {
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
    // get executor
    let mut executor = VayuExecutionEngine::new();

    let uuid = 1;
    // get execution plan from th sql query
    let sql = "SELECT *  FROM a,b WHERE a.a1 = b.b1 ";
    let plan = util::get_execution_plan_from_sql(&ctx, sql).await?;

    // probe plan would be same as main plan
    let probe_plan = plan.clone();
    // get join node and build_plan
    // Note: build_plan does not have build/join node as that would be part of sink operator
    let (join_node, build_plan) = util::get_hash_build_pipeline(plan.clone());

    // run the build pipeline with the executor
    run_pipeline(
        &mut executor,
        build_plan,
        sinks::SchedulerSinkType::BuildAndStoreHashMap(uuid, join_node),
    )
    .await;

    // run the probe pipeline with the executor
    run_pipeline(
        &mut executor,
        probe_plan,
        sinks::SchedulerSinkType::PrintOutput,
    )
    .await;

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

    run_pipeline(
        &mut executor,
        plan1,
        sinks::SchedulerSinkType::StoreRecordBatch(uuid),
    )
    .await;

    let sql2 = "SELECT c1,c4,c13  FROM aggregate_test_100 WHERE c4 > 0 AND c1='b'";
    let plan2 = util::get_execution_plan_from_sql(&ctx, sql2).await?;
    run_pipeline(&mut executor, plan2, sinks::SchedulerSinkType::PrintOutput).await;
    Ok(())
}

async fn run_pipeline(
    executor: &mut VayuExecutionEngine,
    plan: Arc<dyn ExecutionPlan>,
    sink: sinks::SchedulerSinkType,
) {
    executor.execute(vayu::SchedulerPipeline::new(plan, sink));
}
