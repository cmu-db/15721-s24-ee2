use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::Result;
use std::sync::Arc;

pub struct Pipeline {
    pub source_operator: Option<Box<dyn Source>>,
    pub sink_operator: Option<Box<dyn Sink>>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub state: PipelineState,
}
pub struct PipelineState {
    pub schema: Option<Arc<Schema>>,
}
impl Pipeline {
    pub fn new() -> Pipeline {
        Pipeline {
            source_operator: None,
            sink_operator: None,
            operators: vec![],
            state: PipelineState { schema: None },
        }
    }
}
pub trait PhysicalOperator {
    // fn is_sink(&self) -> bool;
    // fn is_source(&self) -> bool;
    fn name(&self) -> String;
}

//Operators that implement Sink trait consume data
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(&self, chunk: &mut RecordBatch) -> bool;
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_data(&self) -> Result<RecordBatch>;
}

//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&self, input: &RecordBatch) -> Result<RecordBatch>;
}
