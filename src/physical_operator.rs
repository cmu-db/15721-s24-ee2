use crate::common::enums::operator_result_type::{
    OperatorResultType, SinkResultType, SourceResultType,
};
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::types::LogicalType;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

pub trait PhysicalOperator {
    //TODO getTypes etc
    fn schema(&self) -> Arc<Schema>;
    fn is_sink(&self) -> bool;
    // fn is_source(&self) -> bool;
}

//Operators that implement Sink trait consume data
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(&self, chunk: &mut RecordBatch) -> SinkResultType;
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_data(&self, chunk: &mut RecordBatch) -> SourceResultType;
}

//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&self, input: &RecordBatch, chunk: &RecordBatch) -> OperatorResultType;
}
