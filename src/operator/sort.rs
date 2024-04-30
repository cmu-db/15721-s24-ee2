use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::helper::Entry;
use crate::physical_operator::{PhysicalOperator, Sink};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute;
use datafusion::arrow::compute::lexsort_to_indices;
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::Schema;
use datafusion::physical_expr::PhysicalSortExpr;
use std::sync::Arc;

pub struct SortOperator {
    expr: Vec<PhysicalSortExpr>,
    originals: Vec<RecordBatch>,
}

impl SortOperator {
    pub fn new(expr: Vec<PhysicalSortExpr>) -> Self {
        Self {
            expr,
            originals: Vec::new(),
        }
    }
}

impl Sink for SortOperator {
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType {
        //append data to the collector
        self.originals.push(input.as_ref().clone());
        return SinkResultType::NeedMoreInput;
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) -> Entry {
        let combined =
            compute::concat_batches(&self.originals[0].schema(), self.originals.iter()).unwrap();

        let sort_columns = self
            .expr
            .iter()
            .map(|expr| expr.evaluate_to_sort_column(&combined).unwrap())
            .collect::<Vec<_>>();

        let indices = lexsort_to_indices(&sort_columns, None).unwrap();

        let columns = combined
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<Result<_, _>>()
            .unwrap();

        let new_batch = RecordBatch::try_new(combined.schema(), columns).unwrap();
        Entry::batch(vec![Arc::new(new_batch)])
    }
}

impl PhysicalOperator for SortOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }
    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Sort
    }
}
