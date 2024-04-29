use crate::common::enums::operator_result_type::OperatorResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::physical_operator::{IntermediateOperator, PhysicalOperator};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::physical_plan::PhysicalExpr;
use std::sync::Arc;

pub struct ProjectionOperator {
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    pub output_schema: Schema,
}
impl ProjectionOperator {
    pub fn new(
        input_schema: Arc<Schema>,
        expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    ) -> ProjectionOperator {
        let num_output_fields = expr.len();
        let mut fields = Vec::with_capacity(num_output_fields);
        for (e, name) in &expr {
            let field = Field::new(
                name,
                e.data_type(&input_schema).unwrap(),
                e.nullable(&input_schema).unwrap(),
            );
            fields.push(field);
        }

        let output_schema = Schema::new(fields);
        ProjectionOperator {
            expr,
            output_schema,
        }
    }
}

impl IntermediateOperator for ProjectionOperator {
    fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType {
        let mut arrays = Vec::new();
        arrays.reserve(self.expr.len());
        for (expr, _) in &self.expr {
            let new_column = expr.evaluate(input).unwrap();
            arrays.push(new_column.into_array(input.num_rows()).unwrap());
        }

        let output = RecordBatch::try_new(Arc::new(self.output_schema.clone()), arrays).unwrap();
        OperatorResultType::Finished(Arc::new(output))
    }
}

impl PhysicalOperator for ProjectionOperator {
    fn schema(&self) -> Arc<Schema> {
       Arc::new(self.output_schema.clone())
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Projection
    }
}
