use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::row_hashmap::{grouped_row_hashmap_add_batch, GroupedRowHashMap};
use crate::common::split_large_batch;
use crate::helper::Entry;
use crate::physical_operator::{PhysicalOperator, Sink};
use ahash::random_state::RandomState;
use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::physical_expr::{AggregateExpr, PhysicalExpr};
use std::sync::Arc;

pub struct HashAggregateOperator {
    //used for sum,min,max etc
    aggregate_expr: Vec<Arc<dyn AggregateExpr>>,

    //the group by expr e.g. GROUP BY colA, colB
    group_by_expr: Vec<Arc<dyn PhysicalExpr>>,

    out_schema: Arc<Schema>,

    //keeps the original batches before grouping
    originals: Vec<RecordBatch>,
    batch_id: usize,

    //Key = hash(col1,col2,col3...)
    //values = row_ids
    hash_table: GroupedRowHashMap,
    random_state: RandomState,
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
        let group_by_expr_only = group_by_expr.iter().map(|(f, _name)| Arc::clone(f)).collect::<Vec<_>>();
        Self {
            aggregate_expr,
            group_by_expr: group_by_expr_only,
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
        
        grouped_row_hashmap_add_batch(&mut self.hash_table, input, self.batch_id, &self.group_by_expr, &self.random_state);

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
                    let (batch_id, row_id) = grouped_row_lists.row_list[0];
                    self.originals[batch_id].slice(row_id as usize, 1)
                })
                .collect::<Vec<_>>();
            let representative_batch =
                compute::concat_batches(&self.originals[0].schema(), &representative_rows_vec)
                    .unwrap();
            for group_by in &self.group_by_expr {
                let group_by_column = group_by
                    .evaluate(&representative_batch)
                    .and_then(|v| v.into_array(num_output_rows))
                    .unwrap();
                columns.push(group_by_column);
            }
        }

        // aggregate cols
        let num_batches = self.originals.len();
        // Each agg_expr emits one output column
        for agg_expr in self.aggregate_expr.iter() {
            // Create input_cols for each batch
            let input_exprs = agg_expr.expressions();
            let input_cols = self.originals.iter().map(|batch| {
                input_exprs.iter().map(|e| {
                    e.evaluate(batch).and_then(|v| v.into_array(batch.num_rows()))
                }).collect::<Result<Vec<_>,_>>().unwrap()
            }).collect::<Vec<_>>();

            let mut output_col_vec = Vec::with_capacity(num_output_rows);
            // Each grouped_row_list emits one output row
            for (_, grouped_row_list) in self.hash_table.iter() {
                // Create accumulator
                let mut accumulator = agg_expr.create_accumulator().unwrap();

                // Process batch
                let mut row_list_start_idx: usize = 0;
                for batch_id in 0..num_batches {
                    // Calculate list of row_ids for this batch_id from grouped_row_list
                    let mut row_list = Vec::new();
                    for idx in row_list_start_idx..(grouped_row_list.row_list.len() + 1) {
                        if idx == grouped_row_list.row_list.len() {
                            row_list_start_idx = idx;
                            break;
                        }
                        let (b_id, r_id) = grouped_row_list.row_list[idx];
                        if b_id > batch_id {
                            row_list_start_idx = idx;
                            break;
                        }
                        assert_eq!(b_id, batch_id);
                        row_list.push(r_id);
                    }
                    if row_list.is_empty() {
                        continue;
                    }

                    // accumulate
                    let row_list_array = UInt64Array::from(row_list);
                    let filtered_input_cols = input_cols[batch_id]
                        .iter()
                        .map(|c| compute::take(c, &row_list_array, None))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    accumulator.update_batch(&filtered_input_cols).unwrap();
                }

                // emit output row
                output_col_vec.push(
                    accumulator.evaluate().unwrap().to_array().unwrap()
                );
            }

            // emit column
            let output_col_ref = output_col_vec
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>();
            let output_col = compute::concat(&output_col_ref[..]).unwrap();
            columns.push(output_col);
        }

        let batch = RecordBatch::try_new(self.out_schema.clone(), columns).unwrap();
        let batch_vec = split_large_batch(&batch, 1024);
        Entry::Batch(batch_vec)
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
