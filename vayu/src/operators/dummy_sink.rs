use crate::operators::operator_result_type::SinkResultType;
use crate::pipeline::{Sink, PhysicalOperator};
use datafusion::arrow::array::RecordBatch;
use std::sync::Arc;

pub struct DummySinkOperator;

impl DummySinkOperator {
    pub fn new() -> Self {
        DummySinkOperator {}
    }
}

impl Sink for DummySinkOperator {
    fn sink(&mut self, input: &Arc<RecordBatch>, results: &mut Vec<RecordBatch>) -> SinkResultType {
        println!("Sinking");
        println!("{:?}", *input);
        SinkResultType::Finished
    }

}

impl PhysicalOperator for DummySinkOperator {
    fn name(&self) -> String {
        String::from("dummy_sink")
    }
}
