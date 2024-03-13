use crate::common::enums::operator_result_type::SinkResultType;
use crate::physical_operator::{PhysicalOperator, Sink};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

pub struct DummySinkOperator;

impl DummySinkOperator {
    pub fn new() -> Self {
        DummySinkOperator {}
    }
}

impl Sink for DummySinkOperator {
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType {
        println!("Sinking");
        println!("{:?}", *input);
        SinkResultType::Finished
    }
}

impl PhysicalOperator for DummySinkOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn is_source(&self) -> bool {
        false
    }

    fn is_sink(&self) -> bool {
        true
    }
}
