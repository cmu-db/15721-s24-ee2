use datafusion::error::DataFusionError::NotImplemented;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub async fn execute_physical_plan_cmu(_: PhysicalPlanNode) -> Result<Vec<RecordBatch>> {
    Err(NotImplemented(String::from(
        "implement execute_physical_plan_cmu",
    )))
}

pub async fn execute_physical_plan(
    plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
) -> Result<Vec<RecordBatch>> {
    collect(plan, ctx.task_ctx()).await
}
