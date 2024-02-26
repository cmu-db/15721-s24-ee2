use crate::parallel::executor::Executor;
use crate::physical_operator::{IntermediateOperator, Sink, Source};
use crate::physical_operator_states::GlobalSourceState;

pub struct Pipeline {
    pub source_operator : Option<Box<dyn Source>>,
    pub sink_operator: Option<Box<dyn Sink>>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub source_state : Option<Box<GlobalSourceState>>,
    pub executor : *mut Executor,
}


impl Pipeline{
    pub fn new(executor : &mut Executor) -> Pipeline{
        Pipeline{
            executor,
            source_operator : None,
            sink_operator : None,
            operators : vec![],
            source_state : None,
        }
    }
}