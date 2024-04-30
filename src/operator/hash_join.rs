use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;
use std::vec;
use ahash;

use datafusion::arrow::array::{Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::physical_plan::PhysicalExpr;

use crate::common::enums::operator_result_type::{OperatorResultType, SinkResultType};
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::helper::Entry;
use crate::physical_operator::{IntermediateOperator, PhysicalOperator, Sink};
use datafusion::arrow::compute;
use ahash::RandomState;
use datafusion::common::hash_utils::create_hashes;
use crate::common::row_hashmap::{grouped_row_hashmap_add_batch, GroupedRowHashMap};

pub struct JoinLeftData {
    pub hash_table: GroupedRowHashMap,
    pub random_state: RandomState,
    pub originals: Vec<Arc<RecordBatch>>,
}

pub struct HashJoinBuildOperator {
    expr: Vec<Arc<dyn PhysicalExpr>>,
    pub schema: Arc<Schema>,
    pub hash_table: GroupedRowHashMap,
    pub originals: Vec<Arc<RecordBatch>>,
    batch_id: usize,
    random_state: RandomState
}

impl HashJoinBuildOperator {
    pub fn new(expr: Vec<Arc<dyn PhysicalExpr>>, schema: Arc<Schema>) -> Self {
        HashJoinBuildOperator {
            expr,
            schema,
            hash_table: Default::default(),
            originals: vec![],
            batch_id: 0,
            random_state: RandomState::generate_with(1, 2, 7, 8),
        }
    }
}

impl PhysicalOperator for HashJoinBuildOperator {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashJoinBuild
    }
}

impl Sink for HashJoinBuildOperator {
    fn sink(&mut self, batch: &Arc<RecordBatch>) -> SinkResultType {
        grouped_row_hashmap_add_batch(&mut self.hash_table, batch, self.batch_id, &self.expr, &self.random_state);
        //append data to the collector
        self.originals.push(Arc::clone(batch));
        //increase the row counter for next iteration
        self.batch_id += 1;

        SinkResultType::NeedMoreInput
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) -> Entry {
        let join_left_data = JoinLeftData {
            hash_table: std::mem::take(&mut self.hash_table),
            random_state: self.random_state.clone(),
            originals: std::mem::take(&mut self.originals),
        };
        Entry::HashMap(join_left_data)
    }
}

pub struct HashJoinProbeOperator {
    expr: Vec<Arc<dyn PhysicalExpr>>,
    left_side_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    store : Arc<RefCell<ahash::HashMap<usize, Entry>>>,
    uuid : usize,
    join_left_data : Entry,
}

impl HashJoinProbeOperator {
    pub fn new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        schema: Arc<Schema>,
        left_side_schema : Arc<Schema>,
        store : Arc<RefCell<ahash::HashMap<usize,Entry>>>,
        uuid : usize,
    ) -> Self {
        let num_output_fields = left_side_schema.fields.len() + schema.fields().len();
        let mut fields: Vec<Field> = Vec::with_capacity(num_output_fields);
        for f in left_side_schema.fields() {
            fields.push(f.deref().clone());
        }
        for f in schema.fields() {
            fields.push(f.deref().clone());
        }
        let output_schema = Arc::new(Schema::new(fields));
        HashJoinProbeOperator {
            expr,
            left_side_schema,
            output_schema,
            store,
            uuid,
            join_left_data : Entry::Empty,
        }
    }
}

impl IntermediateOperator for HashJoinProbeOperator {
    fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType {
        if matches!(&self.join_left_data, Entry::Empty) {
            self.join_left_data = self.store.borrow_mut().remove(&self.uuid).unwrap();
        }
        let join_left_data = match &self.join_left_data {
            Entry::HashMap(data) => {data}
            _ => {panic!()}
        };

        let num_rows = input.num_rows();
        let keys_values = self
            .expr
            .iter()
            .map(|f| {
                f.evaluate(input)
                    .unwrap()
                    .into_array(input.num_rows())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut hashes_buffer: Vec<u64> = Vec::new();
        hashes_buffer.resize(num_rows, 0);
        let hash_values =
            create_hashes(&keys_values, &join_left_data.random_state, &mut hashes_buffer).unwrap();

        let num_left_batches = join_left_data.originals.len();
        let mut left_indices_vec: Vec<Vec<u64>> = Vec::new();
        left_indices_vec.resize(num_left_batches, Vec::new());
        let mut right_indices_vec: Vec<u64> = Vec::new();
        for right_row_idx in 0..num_rows {
            let hashmap_key = hash_values[right_row_idx];
            match join_left_data.hash_table.get(&hashmap_key) {
                Some(left_row_list) => {
                    for (batch_id, left_row_idx) in &left_row_list.row_list {
                        left_indices_vec[*batch_id].push(*left_row_idx);
                        right_indices_vec.push(right_row_idx as u64);
                    }
                }
                None => {}
            }
        }
        if right_indices_vec.is_empty() {
            return OperatorResultType::Finished(Arc::new(RecordBatch::new_empty(self.output_schema.clone())));
        }

        let mut columns: Vec<Arc<dyn Array>> =
            Vec::with_capacity(self.output_schema.fields().len());

        let num_left_columns = self.left_side_schema.fields().len();
        for left_col_idx in 0..num_left_columns {
            let mut col_fragments = Vec::new();
            for batch_id in 0..num_left_batches {
                if left_indices_vec[batch_id].is_empty() {
                    continue;
                }
                //let left_indices = UInt64Array::from(std::mem::take(&mut left_indices_vec[batch_id]));
                let left_indices = UInt64Array::from(left_indices_vec[batch_id].clone());
                let col_fragment = compute::take(
                    join_left_data.originals[batch_id].column(left_col_idx),
                    &left_indices, None
                ).unwrap();
                col_fragments.push(col_fragment);
            }
            let col_fragments_ref = col_fragments.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            let left_col = compute::concat(&col_fragments_ref[..]).unwrap();
            columns.push(left_col);
        }

        let right_indices = UInt64Array::from(right_indices_vec);
        for right_col in input.columns() {
            let new_col = compute::take(right_col, &right_indices, None).unwrap();
            columns.push(new_col);
        }

        let output = RecordBatch::try_new(self.output_schema.clone(), columns).unwrap();

        OperatorResultType::Finished(Arc::new(output))
    }
}

impl PhysicalOperator for HashJoinProbeOperator {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.output_schema)
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashJoinProbe
    }
}
