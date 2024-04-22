use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::physical_operator::{PhysicalOperator, Sink};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::arrow::util::pretty;
use std::sync::Arc;

pub struct DummySinkOperator;

impl DummySinkOperator {
    pub fn new() -> Self {
        DummySinkOperator {}
    }
}

impl Sink for DummySinkOperator {
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType {
        pretty::print_batches(std::slice::from_ref(input.as_ref())).unwrap();
        SinkResultType::NeedMoreInput
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) {}
}

impl PhysicalOperator for DummySinkOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn get_type(&self) -> PhysicalOperatorType {
        todo!()
    }
}
