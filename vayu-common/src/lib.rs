use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use std::sync::Arc;
pub trait PhysicalOperator {
    fn name(&self) -> String;
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_data(&mut self) -> Option<RecordBatch>;
}
//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator + Send {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch>;
}

pub enum SchedulerSourceType {
    RecordBatchStream(SendableRecordBatchStream),
    RecordBatchStore(i32),
}

pub enum SchedulerSinkType {
    // StoreRecordBatch(i32),
    // BuildAndStoreHashMap(i32, Arc<dyn ExecutionPlan>),
    PrintOutput,
}
pub struct VayuPipeline {
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub sink: Option<SchedulerSinkType>,
}
pub struct Pipeline {
    pub source: Option<SendableRecordBatchStream>,
    pub vayu_pipeline: VayuPipeline,
}
pub struct PipelineState {
    uuid: i32,
}
impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            source: None,
            vayu_pipeline: VayuPipeline {
                operators: vec![],
                sink: None,
            },
        }
    }
}

pub struct Task {
    pub pipelines: Vec<Pipeline>,
}
impl Task {
    pub fn new() -> Self {
        Task { pipelines: vec![] }
    }
    pub fn add_pipeline(&mut self, pipeline: Pipeline) {
        self.pipelines.push(pipeline);
    }
}
