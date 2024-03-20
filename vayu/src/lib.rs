mod pipeline_executor;
use arrow::array::RecordBatch;
use arrow::error::Result;
use pipeline::Pipeline;
use pipeline_executor::PipelineExecutor;
pub mod operators;
pub mod pipeline;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
pub fn execute(scheduler_pipeline: SchedulerPipeline) -> Result<Vec<RecordBatch>> {
    let plan = scheduler_pipeline.plan;
    let pipeline = Pipeline::new(plan);
    let mut pipeline_executor = PipelineExecutor::new(pipeline);
    let result = pipeline_executor.execute().unwrap();
    Ok(result)
}

pub struct SchedulerPipeline {
    pub plan: Arc<dyn ExecutionPlan>,
}
