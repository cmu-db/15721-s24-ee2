use crate::pipeline::{IntermediateOperator, PhysicalOperator};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::filter::batch_filter;
use datafusion::physical_plan::PhysicalExpr;

use std::sync::Arc;
pub struct FilterOperator {
    predicate: Arc<dyn PhysicalExpr>,
}
impl FilterOperator {
    pub fn new(predicate: Arc<dyn PhysicalExpr>) -> FilterOperator {
        FilterOperator { predicate }
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&self, input: &RecordBatch) -> Result<RecordBatch> {
        batch_filter(input, &self.predicate)
    }
}

impl PhysicalOperator for FilterOperator {
    fn name(&self) -> String {
        String::from("filter")
    }
}
