use crate::common::enums::operator_result_type::SourceResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::types::data_chunk::DataChunk;
use crate::common::types::LogicalType;
use crate::execution_context::ExecutionContext;
use crate::physical_operator::{Source};
use crate::physical_operator_states::{GlobalSourceState, LocalSourceState, OperatorSourceInput, OperatorState};

pub struct ScanOperator { }

impl ScanOperator {
    pub fn new() -> ScanOperator {
        ScanOperator {}
    }
}

impl Source for ScanOperator {
    fn get_data(&self,
                // context: &ExecutionContext,
                chunk: &DataChunk, input: &OperatorSourceInput) -> SourceResultType {
        println!("ScanOperator::get_data");
        SourceResultType::Finished
    }

    fn get_local_source_state(&self, global_operator_state: &GlobalSourceState) -> Box<LocalSourceState> {
        todo!()
    }
}
