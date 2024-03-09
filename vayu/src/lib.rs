use core::panic;
mod pipeline_executor;
use arrow::array::RecordBatch;
use arrow::error::Result;
use datafusion::physical_plan::{common, SendableRecordBatchStream};
use pipeline::Pipeline;
use pipeline_executor::PipelineExecutor;
pub mod operators;
pub mod pipeline;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
pub fn execute(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
    let pipeline = Pipeline::new(plan);
    let mut pipeline_executor = PipelineExecutor::new(pipeline);
    let result = pipeline_executor.execute().unwrap();
    Ok(result)
}
