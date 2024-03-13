use crate::common::enums::operator_result_type::OperatorResultType;
use crate::physical_operator::{IntermediateOperator, PhysicalOperator};
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;
use datafusion::physical_expr::PhysicalExpr;

pub struct FilterOperator {
    expression : Arc<dyn PhysicalExpr>,
}

impl FilterOperator {
    pub fn new(expression : Arc<dyn PhysicalExpr>) -> Self {
        FilterOperator {
            expression
        }
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType {
        let predicate_array = self.expression.evaluate(&input).unwrap()
            .into_array(input.num_rows()).unwrap();
        let predicate = predicate_array.as_boolean();
        let filtered_result = filter_record_batch(input, predicate).unwrap();
        OperatorResultType::Finished(Arc::new(filtered_result))
    }
}

impl PhysicalOperator for FilterOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn is_sink(&self) -> bool {
        false
    }

    fn is_source(&self) -> bool {
        false
    }
}
