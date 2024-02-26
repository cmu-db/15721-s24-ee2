use crate::common::enums::operator_result_type::OperatorResultType;
use crate::common::types::data_chunk::DataChunk;
use crate::execution_context::ExecutionContext;
use crate::physical_operator::{IntermediateOperator};
use crate::physical_operator_states::{GlobalOperatorState, OperatorState};

pub struct FilterOperator {}

impl FilterOperator {
    pub fn new() -> Self {
        FilterOperator {}
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&self, context: &ExecutionContext, input: &DataChunk, chunk: &DataChunk, gstate: &GlobalOperatorState, state: &OperatorState) -> OperatorResultType {
        println!("FilterOperator::get_data");
        todo!()
    }
}
