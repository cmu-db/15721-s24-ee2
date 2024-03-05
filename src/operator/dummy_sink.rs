use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::types::LogicalType;
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

impl PhysicalOperator for DummySinkOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn is_sink(&self) -> bool {
        true
    }
}

impl Sink for DummySinkOperator {
    fn sink(&self, chunk: &mut RecordBatch) -> SinkResultType {
        println!("Sinking");
        SinkResultType::NeedMoreInput
    }
}
