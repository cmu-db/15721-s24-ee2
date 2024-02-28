use crate::common::enums::operator_result_type::SourceResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::types::data_chunk::{CHUNK_SIZE, DataChunk};
use crate::common::types::LogicalType;
use crate::execution_context::ExecutionContext;
use crate::physical_operator::{PhysicalOperator, Source};
use crate::physical_operator_states::{
    GlobalSourceState, LocalSourceState, OperatorSourceInput, OperatorState,
};

pub struct ScanOperator {
}
static mut TIMES : usize = 1;
impl ScanOperator {
    pub fn new() -> ScanOperator {
        ScanOperator {}
    }
}

impl Source for ScanOperator {
    fn get_data(
        &self,
        // context: &ExecutionContext,
        chunk: &mut DataChunk,
        // input: &OperatorSourceInput,
    ) -> SourceResultType {
        unsafe {println!("ScanOperator::get_data {TIMES}");}
        chunk.reset();

        //will "read" 5 times from the source (e.g filesystem or storage team)
        //after reading 5 chunks will return finished
        unsafe {
            match TIMES {
                1..=5 => {
                    for i in 0..CHUNK_SIZE {
                        chunk.push(TIMES );
                    }
                }
                _ => {}
            }
            TIMES+=1;
        }

        match chunk.size() { 0 => {return SourceResultType::Finished}
        _ =>{ return SourceResultType::HaveMoreOutput}}
    }

    fn get_local_source_state(
        &self,
        global_operator_state: Option<&GlobalSourceState>,
    ) -> Box<LocalSourceState> {
        return Box::new(LocalSourceState{});
    }
}

impl PhysicalOperator for ScanOperator {
    fn get_types(&self) -> Vec<LogicalType> {
        todo!()
    }

    fn is_sink(&self) -> bool {
        false
    }
}
