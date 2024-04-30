use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::helper::Entry;
use crate::physical_operator::{PhysicalOperator, Sink};
use ahash::random_state::RandomState;
use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::hash_utils::create_hashes;
use datafusion::physical_expr::{AggregateExpr, PhysicalExpr};
use std::collections::HashMap;
use std::iter::repeat;
use std::sync::Arc;

pub struct HashAggregateOperator {
    //used for sum,min,max etc
    aggregate_expr: Vec<Arc<dyn AggregateExpr>>,

    //the group by expr e.g. GROUP BY colA, colB
    group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,

    out_schema: Arc<Schema>,

    //keeps the original batches before grouping
    originals: Vec<RecordBatch>,
    batch_id: usize,

    //Key = hash(col1,col2,col3...)
    //values = row_ids
    hash_table: HashMap<u64, GroupedRowList>,
    random_state: RandomState,
}

struct GroupedRowList {
    row_lists: Vec<Vec<u64>>,
    repr_row_id: (usize, u64),
}

impl HashAggregateOperator {
    pub fn new(
        input_schema: Arc<Schema>,
        aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
        group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    ) -> Self {
        let mut fields = Vec::with_capacity(group_by_expr.len() + aggregate_expr.len());
        for (expr, name) in &group_by_expr {
            fields.push(Field::new(
                name,
                expr.data_type(&input_schema).unwrap(),
                false,
            ));
        }
        for expr in &aggregate_expr {
            fields.push(expr.field().unwrap());
        }
        let out_schema = Arc::new(Schema::new(fields));
        Self {
            aggregate_expr,
            group_by_expr,
            out_schema,
            originals: Vec::new(),
            batch_id: 0,
            hash_table: Default::default(),
            random_state: RandomState::generate_with(1, 2, 3, 4),
        }
    }
}

impl Sink for HashAggregateOperator {
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType {
        let num_rows = input.num_rows();
        let keys_values = self
            .group_by_expr
            .iter()
            .map(|(f, _name)| {
                f.evaluate(input)
                    .unwrap()
                    .into_array(input.num_rows())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut hashes_buffer: Vec<u64> = Vec::new();
        hashes_buffer.resize(num_rows, 0);
        let hash_values =
            create_hashes(&keys_values, &self.random_state, &mut hashes_buffer).unwrap();

        for row_idx in 0..num_rows {
            //grab the hash which is the key
            let hashmap_key = hash_values[row_idx];
            // the value in the hash table is the row_id
            let hashmap_value = row_idx as u64;

            //if the key does not exist create a new array
            if !self.hash_table.contains_key(&hashmap_key) {
                let mut row_lists = Vec::with_capacity(self.batch_id + 1);
                row_lists.resize(self.batch_id + 1, Vec::new());
                row_lists[self.batch_id].push(hashmap_value);
                self.hash_table.insert(
                    hashmap_key,
                    GroupedRowList {
                        row_lists,
                        repr_row_id: (self.batch_id, hashmap_value),
                    },
                );
            } else {
                //else if the key already exists in the map then just append the value to the
                let grouped_row_lists = self.hash_table.get_mut(&hashmap_key).unwrap();
                let row_lists = &mut grouped_row_lists.row_lists;
                row_lists.resize(self.batch_id + 1, Vec::new());
                row_lists[self.batch_id].push(hashmap_value);
            }
        }

        //append data to the collector
        self.originals.push(input.as_ref().clone());
        //increase the row counter for next iteration
        self.batch_id += 1;

        return SinkResultType::NeedMoreInput;
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) -> Entry {
        let num_output_rows = self.hash_table.len();
        let mut columns = Vec::with_capacity(self.group_by_expr.len() + self.aggregate_expr.len());

        // group_by cols
        {
            let representative_rows_vec = self
                .hash_table
                .iter()
                .map(|(_, grouped_row_lists)| {
                    let (batch_id, row_id) = grouped_row_lists.repr_row_id;
                    self.originals[batch_id].slice(row_id as usize, 1)
                })
                .collect::<Vec<_>>();
            let representative_batch =
                compute::concat_batches(&self.originals[0].schema(), &representative_rows_vec)
                    .unwrap();
            for (group_by, _name) in &self.group_by_expr {
                let group_by_column = group_by
                    .evaluate(&representative_batch)
                    .and_then(|v| v.into_array(num_output_rows))
                    .unwrap();
                columns.push(group_by_column);
            }
        }

        // aggregate cols
        let mut accumulators = self
            .aggregate_expr
            .iter()
            .map(|agg_expr| {
                repeat(0)
                    .take(num_output_rows)
                    .map(|_| agg_expr.create_accumulator())
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        for (batch_id, batch) in self.originals.iter().enumerate() {
            for (agg_id, agg_expr) in self.aggregate_expr.iter().enumerate() {
                let input_exprs = agg_expr.expressions();
                let input_cols = input_exprs
                    .iter()
                    .map(|e| {
                        e.evaluate(batch)
                            .and_then(|v| v.into_array(batch.num_rows()))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();

                for (group_id, (_, grouped_row_lists)) in self.hash_table.iter().enumerate() {
                    if grouped_row_lists.row_lists.len() <= batch_id {
                        continue;
                    }
                    let row_list = grouped_row_lists.row_lists[batch_id].clone();
                    if row_list.is_empty() {
                        continue;
                    }
                    let row_list_array = UInt64Array::from(row_list);
                    let filtered_input_cols = input_cols
                        .iter()
                        .map(|c| compute::take(c, &row_list_array, None))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    accumulators[agg_id][group_id]
                        .update_batch(&filtered_input_cols)
                        .unwrap();
                }
            }
        }

        for accums in &mut accumulators {
            let output_col_vec = accums
                .iter_mut()
                .map(|acc| acc.evaluate().unwrap().to_array())
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let output_col_ref = output_col_vec
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>();
            let output_col = compute::concat(&output_col_ref[..]).unwrap();
            columns.push(output_col);
        }

        let batch = (RecordBatch::try_new(self.out_schema.clone(), columns).unwrap());
        Entry::batch(vec![batch])
    }
}

impl PhysicalOperator for HashAggregateOperator {
    fn schema(&self) -> Arc<Schema> {
        self.out_schema.clone()
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashAggregate
    }
}
