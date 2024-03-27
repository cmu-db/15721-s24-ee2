use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use std::sync::Arc;
mod util;
use vayu::{sinks, VayuExecutionEngine};

#[tokio::main]
async fn main() -> Result<()> {
    test_scan_filter_project().await?;
    test_hash_join().await?;
    test_store_record_batch().await?;
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
    let sql = "SELECT c1,c3 as neg,c4 as pos,c13  FROM aggregate_test_100 WHERE (c3 < 0 AND c1='a') OR ( c4 > 0 AND c1='b' ) ";

    let plan = util::get_execution_plan_from_sql(&ctx, sql).await?;

    run_pipeline(&mut executor, plan, sinks::SchedulerSinkType::PrintOutput).await;

    Ok(())
}

async fn test_store_record_batch() -> Result<()> {
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
    let sql = "SELECT c1,c3 as neg,c4 as pos,c13  FROM aggregate_test_100 WHERE c3 < 0 AND c1='a' ";

    let plan = util::get_execution_plan_from_sql(&ctx, sql).await?;
    let uuid = 42;
    run_pipeline(
        &mut executor,
        plan,
        sinks::SchedulerSinkType::StoreRecordBatch(uuid),
    )
    .await;
    let sql =
        "SELECT c1,c3 as neg,c4 as pos,c13  FROM aggregate_test_100 WHERE  c4 > 0 AND c1='b' ";

    let plan = util::get_execution_plan_from_sql(&ctx, sql).await?;
    let uuid = 42;
    run_pipeline(
        &mut executor,
        plan,
        sinks::SchedulerSinkType::StoreRecordBatch(uuid),
    )
    .await;

    // get executor to print the value
    executor.sink(uuid);

    Ok(())
}

async fn run_pipeline(
    executor: &mut VayuExecutionEngine,
    plan: Arc<dyn ExecutionPlan>,
    sink: sinks::SchedulerSinkType,
) {
    executor.execute(vayu::SchedulerPipeline::new(plan, sink));
}
