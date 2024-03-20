mod pipeline_executor;
use arrow::array::RecordBatch;
use arrow::error::Result;
use pipeline::Pipeline;
use pipeline_executor::PipelineExecutor;
pub mod operators;
pub mod pipeline;
pub mod store;
use crate::store::Blob;
use crate::store::Store;
use datafusion::physical_plan::ExecutionPlan;
use std::result;
use std::sync::Arc;

pub struct VayuExecutionEngine {
    pub store: Store,
}

impl VayuExecutionEngine {
    pub fn new() -> VayuExecutionEngine {
        VayuExecutionEngine {
            store: Store::new(),
        }
    }

    pub fn execute(&mut self, scheduler_pipeline: SchedulerPipeline) -> Result<SchedulerSink> {
        let plan = scheduler_pipeline.plan;
        let sink_type = scheduler_pipeline.sink;
        // convert execution plan to a pipeline
        let pipeline = Pipeline::new(plan);
        // execute the plan to get the results
        let mut pipeline_executor = PipelineExecutor::new(pipeline);
        let result = pipeline_executor.execute().unwrap();

        // set to sink appropriately
        match sink_type {
            SchedulerSinkType::ReturnOutput => Ok(SchedulerSink::ReturnOutput(result)),
            SchedulerSinkType::RecordBatchStorage(uuid) => {
                // TODO: do inplace obviously!
                // this is highly inefficient but will fix after figuring out
                // whether to keep one store for all kind of data or multiple stores.
                let blob = self.store.remove(uuid);
                let mut blob = match blob {
                    Some(r) => r,
                    None => Blob::RecordBatchBlob(Vec::new()),
                };

                blob.append_records(result);

                self.store.insert(uuid, blob);
                // TODO: update not replace
                Ok(SchedulerSink::RecordBatchStorage(uuid))
            }
        }
    }
}
pub enum SchedulerSink {
    RecordBatchStorage(i32),
    ReturnOutput(Vec<RecordBatch>),
}
pub enum SchedulerSinkType {
    RecordBatchStorage(i32),
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
