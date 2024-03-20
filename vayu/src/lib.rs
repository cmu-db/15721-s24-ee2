mod pipeline_executor;
use arrow::array::RecordBatch;
use arrow::error::Result;
use pipeline::Pipeline;
use pipeline_executor::PipelineExecutor;
pub mod operators;
pub mod pipeline;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
pub fn execute(scheduler_pipeline: SchedulerPipeline) -> Result<SchedulerSink> {
    let plan = scheduler_pipeline.plan;
    let sink_type = scheduler_pipeline.sink;
    let pipeline = Pipeline::new(plan);
    let mut pipeline_executor = PipelineExecutor::new(pipeline);
    let result = pipeline_executor.execute().unwrap();
    match sink_type {
        SchedulerSinkType::ReturnOutput => Ok(SchedulerSink::ReturnOutput(result)),
        SchedulerSinkType::RecordBatchStorage => {
            // TODO: actually store the value
            Ok(SchedulerSink::RecordBatchStorage(1))
        }
    }
}

pub enum SchedulerSink {
    RecordBatchStorage(i32),
    ReturnOutput(Vec<RecordBatch>),
}
pub enum SchedulerSinkType {
    RecordBatchStorage,
    ReturnOutput,
}

pub struct SchedulerPipeline {
    pub plan: Arc<dyn ExecutionPlan>,
    pub sink: SchedulerSinkType,
}

impl SchedulerPipeline {
    pub fn new(plan: Arc<dyn ExecutionPlan>, sink: SchedulerSinkType) -> SchedulerPipeline {
        SchedulerPipeline { plan, sink }
    }
}
