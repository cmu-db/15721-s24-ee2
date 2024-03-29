use arrow::datatypes::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::projection::batch_project;
use datafusion::physical_plan::PhysicalExpr;
use std::sync::Arc;
use vayu_common::{IntermediateOperator, PhysicalOperator};
pub struct ProjectionOperator {
    expr: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
}
impl ProjectionOperator {
    pub fn new(expr: Vec<Arc<dyn PhysicalExpr>>, schema: SchemaRef) -> ProjectionOperator {
        ProjectionOperator { expr, schema }
    }
}

impl IntermediateOperator for ProjectionOperator {
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        batch_project(input, self.expr.clone(), self.schema.clone())
    }
}

impl PhysicalOperator for ProjectionOperator {
    fn name(&self) -> String {
        String::from("projection")
    }
}
