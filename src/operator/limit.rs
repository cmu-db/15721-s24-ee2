use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::helper::Entry;
use crate::physical_operator::{PhysicalOperator, Sink};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

//LimitOperatorData holds the RecordBatches sinked in LimitOperator
pub struct LimitOperator {
    num_records: usize,
    counter: usize,
    originals: Vec<Arc<RecordBatch>>,
}

impl LimitOperator {
    pub fn new(num_records: usize) -> Self {
        LimitOperator {
            num_records,
            counter: 0,
            originals: Vec::new(),
        }
    }
}

impl Sink for LimitOperator {
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType {
        //if we already have processed the required number of records then we return
        if self.counter >= self.num_records {
            return SinkResultType::Finished;
        }

        let prev_counter = self.counter;
        self.counter += input.num_rows();

        /*
        if we have more records than the limit expression
        we just need to slice the remaining rows from this chunk */
        if self.counter > self.num_records {
            let remaining_rows = self.num_records - prev_counter;
            let new_batch = input.slice(0, remaining_rows);
            self.originals.push(Arc::new(new_batch));
            return SinkResultType::Finished;
        }
        //exact num rows don't slice
        else if self.counter == self.num_records {
            self.originals.push(Arc::clone(input));
            return SinkResultType::Finished;
        }
        //we need more input to satisfy the limit
        else {
            self.originals.push(Arc::clone(input));
            return SinkResultType::NeedMoreInput;
        }
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) -> Entry {
        Entry::Batch(std::mem::take(&mut self.originals))
    }
}

impl PhysicalOperator for LimitOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Limit
    }
}
