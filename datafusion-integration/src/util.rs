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

/**
 * returns (join_node, build_plan)
 * Note: build_plan won't have join node
 */
pub fn get_hash_build_pipeline(
    plan: Arc<dyn ExecutionPlan>,
) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    let plan1 = plan.clone();
    let p = plan.as_any();

    if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        return (plan1, exec.left().clone());
    }
    if let Some(_) = p.downcast_ref::<CsvExec>() {
        panic!("should never reach csvexec in get_hash_build_pipeline ");
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
    panic!("No join node found");
}
