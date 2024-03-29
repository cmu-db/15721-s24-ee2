mod pipeline_executor;
use arrow::array::RecordBatch;
use arrow::util::pretty;
use pipeline::Pipeline;
use pipeline_executor::PipelineExecutor;
pub mod operators;
pub mod pipeline;
pub mod sinks;

pub mod store;
use crate::sinks::SchedulerSinkType;
use crate::store::Blob::{HashMapBlob, RecordBatchBlob};
use crate::store::Store;
use core::panic;
use datafusion::physical_plan::ExecutionPlan;
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

    pub fn execute(&mut self, scheduler_pipeline: SchedulerPipeline) {
        let plan = scheduler_pipeline.plan;
        // convert execution plan to a pipeline

        let pipeline = Pipeline::new(plan, &mut self.store, 1);
        // execute the plan to get the results
        let mut pipeline_executor = PipelineExecutor::new(pipeline);
        let result = pipeline_executor.execute().unwrap();

        // do the sinking - very simple API
        // no need to create a seperate class and introduce indirection unless it moves out of hands
        // to call one function we would need 30+ lines otherwise
        let sink: SchedulerSinkType = scheduler_pipeline.sink;
        match sink {
            SchedulerSinkType::PrintOutput => {
                pretty::print_batches(&result).unwrap();
            }
            SchedulerSinkType::StoreRecordBatch(uuid) => {
                self.store.append(uuid, result);
            }
            SchedulerSinkType::BuildAndStoreHashMap(uuid, join_node) => {
                let mut sink = sinks::HashMapSink::new(uuid, join_node);
                let map = sink.build_map(result);
                self.store.insert(uuid, map.unwrap());
            }
        };
    }
    pub fn sink(&mut self, uuid: i32) {
        let blob = self.store.remove(uuid);
        match blob {
            Some(blob) => match blob {
                RecordBatchBlob(result) => {
                    pretty::print_batches(&result).unwrap();
                }
                HashMapBlob(results) => {
                    pretty::print_batches(&[results.batch().clone()]).unwrap();
                }
            },
            None => panic!("no blob for {uuid} found"),
        }
    }
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
