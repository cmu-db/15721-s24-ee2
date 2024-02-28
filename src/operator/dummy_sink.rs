use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::types::data_chunk::DataChunk;
use crate::common::types::LogicalType;
use crate::execution_context::ExecutionContext;
use crate::physical_operator::{PhysicalOperator, Sink};
use crate::physical_operator_states::{LocalSinkState, OperatorSinkInput};

pub struct DummySinkOperator;

impl DummySinkOperator {
    pub fn new() -> Self{
        DummySinkOperator{}
    }
}

impl PhysicalOperator for DummySinkOperator {
    fn get_types(&self) -> Vec<LogicalType> {
        todo!()
    }

    fn is_sink(&self) -> bool {
        true
    }
}

impl Sink for DummySinkOperator{
    fn sink(&self, chunk: &mut DataChunk) -> SinkResultType {
        println!("Sinking");
        chunk.print();
        println!("");
        SinkResultType::NeedMoreInput
    }

    fn get_local_sink_state(&self) -> Box<LocalSinkState> {
        Box::new(LocalSinkState{})
    }
}