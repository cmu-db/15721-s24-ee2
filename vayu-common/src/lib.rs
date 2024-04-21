use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use std::sync::Arc;
pub mod store;
// use vayu::operators::aggregate::AggregateOperator;

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

#[derive(Clone)]
pub enum SchedulerSinkType {
    StoreRecordBatch(i32),
    // FinalAggregation(i32, AggregateOperator),
    BuildAndStoreHashMap(i32, Arc<dyn ExecutionPlan>),
    PrintOutput,
}

#[derive(Clone)]
pub enum FinalizeSinkType {
    PrintFromStore(i32),
    FinalAggregate(Arc<dyn ExecutionPlan>, i32),
}
#[derive(Clone)]
pub struct DatafusionPipeline {
    pub plan: Arc<dyn ExecutionPlan>,
    pub sink: Option<SchedulerSinkType>,
    pub id: i32,
}
#[derive(Clone)]
pub struct SchedulerPipeline {
    pub source: Option<Arc<dyn ExecutionPlan>>,
    pub pipeline: DatafusionPipeline,
    pub finalize: FinalizeSinkType,
}

pub struct DatafusionPipelineWithData {
    pub pipeline: DatafusionPipeline,
    pub data: RecordBatch,
}
pub struct VayuPipeline {
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub sink: Option<SchedulerSinkType>,
}

pub struct VayuPipelineWithData {
    pub pipeline: VayuPipeline,
    pub data: RecordBatch,
}
pub struct Task {
    pub pipelines: Vec<SchedulerPipeline>,
}

pub enum VayuMessage {
    Normal(DatafusionPipelineWithData),
    Finalize((FinalizeSinkType, i32)),
}
impl Task {
    pub fn new() -> Self {
        Task { pipelines: vec![] }
    }
    pub fn add_pipeline(&mut self, pipeline: SchedulerPipeline) {
        self.pipelines.push(pipeline);
    }
}
