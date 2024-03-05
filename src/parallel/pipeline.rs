use crate::physical_operator::{IntermediateOperator, Sink, Source};

pub struct Pipeline {
    pub source_operator: Option<Box<dyn Source>>,
    pub sink_operator: Option<Box<dyn Sink>>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
}

impl Pipeline {
    pub fn new() -> Pipeline {
        Pipeline {
            source_operator: None,
            sink_operator: None,
            operators: vec![],
        }
    }
}
