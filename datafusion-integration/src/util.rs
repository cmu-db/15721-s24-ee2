use core::panic;
use datafusion::datasource::physical_plan::CsvExec;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub fn get_hash_build_pipeline(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let p = plan.as_any();

    if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        return plan;
    }
    if let Some(_) = p.downcast_ref::<CsvExec>() {
        panic!("should never reach csvexec in get_hash_build_pipeline ");
        return plan;
    }
    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        return get_hash_build_pipeline(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<ProjectionExec>() {
        return get_hash_build_pipeline(exec.input().clone());
    }

    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        return get_hash_build_pipeline(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        return get_hash_build_pipeline(exec.input().clone());
    }
    return plan;
}

pub async fn get_execution_plan_from_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    // create datafusion logical plan
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql).await?;
    // create datafusion physical plan
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan).await?;
    // print datafusion physical plan
    println!(
        "Detailed physical plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    // panic!("hello");
    Ok(plan)
}
