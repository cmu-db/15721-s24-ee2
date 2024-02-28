use crate::common::enums::operator_result_type::OperatorResultType;
use crate::common::types::data_chunk::DataChunk;
use crate::common::types::LogicalType;
use crate::execution_context::ExecutionContext;
use crate::physical_operator::{IntermediateOperator, PhysicalOperator};
use crate::physical_operator_states::{GlobalOperatorState, OperatorState};

pub struct FilterOperator {}

impl FilterOperator {
    pub fn new() -> Self {
        FilterOperator {}
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(
        &self,
        context: &ExecutionContext,
        input: &DataChunk,
        chunk: &DataChunk,
        gstate: &GlobalOperatorState,
        state: &OperatorState,
    ) -> OperatorResultType {
        println!("FilterOperator::get_data");
        todo!()
    }

    fn get_operator_state(&self) -> Box<OperatorState> {
        todo!()
    }
}

impl PhysicalOperator for FilterOperator {
    fn get_types(&self) -> Vec<LogicalType> {
        todo!()
    }

    fn is_sink(&self) -> bool {
        false
    }
}
