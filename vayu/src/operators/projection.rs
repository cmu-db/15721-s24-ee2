use crate::pipeline::{IntermediateOperator, PhysicalOperator};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;

pub struct ProjectionOperator {}
impl ProjectionOperator {
    pub fn new() -> ProjectionOperator {
        ProjectionOperator {}
    }
}

impl IntermediateOperator for ProjectionOperator {
    fn execute(&self, _: &RecordBatch) -> Result<RecordBatch> {
        todo!();
    }
}

impl PhysicalOperator for ProjectionOperator {
    fn name(&self) -> String {
        String::from("projection")
    }
}
