use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::helper::Entry;
use crate::physical_operator::{PhysicalOperator, Sink};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::arrow::util::pretty;
use std::sync::Arc;

pub struct PhysicalBatchCollector {
    result: Vec<RecordBatch>,
}

impl PhysicalBatchCollector {
    pub fn new() -> Self {
        PhysicalBatchCollector { result: vec![] }
    }

    pub fn print(&self) {
        for batch in &self.result {
            pretty::print_batches(std::slice::from_ref(batch)).unwrap();
        }
    }
}

impl Sink for PhysicalBatchCollector {
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType {
        self.result.push(input.as_ref().clone());
        SinkResultType::NeedMoreInput
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) -> Entry {
        Entry::empty
    }
}

impl PhysicalOperator for PhysicalBatchCollector {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn get_type(&self) -> PhysicalOperatorType {
        todo!()
    }
}
