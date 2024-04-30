use crate::common::enums::operator_result_type::{
    OperatorResultType, SinkResultType, SourceResultType,
};
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::helper::Entry;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use std::any::Any;
use std::sync::Arc;

pub trait PhysicalOperator {
    fn schema(&self) -> Arc<Schema>;
    //All physical operators should have a method that returns their type
    //This is useful when we want to downcast the dyn Trait to the actual type
    fn get_type(&self) -> PhysicalOperatorType;
}

//Operators that implement Sink trait consume data and typically are pipeline breakers
//that means that until all data chunks are consumed by the Sink then the pipeline cannot continue
//for example Sort is a Sink operator since it consumes data and waits until all the data is consumed in order to sort it
//All Sink operators should implement the sink method
//All Sink operators should also implement finalize which is needed to perform the final step in this operator
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType;
    fn as_any(&self) -> &dyn Any;

    fn finalize(&mut self) -> Entry;
}

//Operators that implement Source trait produces data
//for example Scan is a Source operator since it produces data
//All Source operators should implement the get_data method
pub trait Source: PhysicalOperator {
    fn get_data(&mut self) -> SourceResultType;
}

//Physical operators that implement the IntermediateOperator trait process data
//IntermediateOperators receive a chunk of data process it and return a modified chunk(record batch)
//for example Filter is an intermediate operator (filters some rows based on some expression)
//All IntermediateOperators should implement the execute method
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType;
}
