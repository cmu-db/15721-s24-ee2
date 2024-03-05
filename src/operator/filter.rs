use crate::common::enums::operator_result_type::OperatorResultType;
use crate::common::types::LogicalType;
use crate::physical_operator::{IntermediateOperator, PhysicalOperator};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

pub struct FilterOperator {}

impl FilterOperator {
    pub fn new() -> Self {
        FilterOperator {}
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&self, input: &RecordBatch, chunk: &RecordBatch) -> OperatorResultType {
        println!("FilterOperator::get_data");
        todo!()
    }
}

impl PhysicalOperator for FilterOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn is_sink(&self) -> bool {
        false
    }
}
