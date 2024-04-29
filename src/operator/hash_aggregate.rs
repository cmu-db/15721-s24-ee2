use crate::common::enums::operator_result_type::SinkResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::physical_operator::{PhysicalOperator, Sink};
use ahash::random_state::RandomState;
use datafusion::arrow::array::{RecordBatch, UInt64Array};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::hash_utils::create_hashes;
use datafusion::physical_expr::{AggregateExpr, PhysicalExpr};
use std::collections::HashMap;
use std::sync::Arc;

pub struct AggregatedData {
    pub data: Option<RecordBatch>,
}
pub struct HashAggregateOperator {
    pub aggregated_data: AggregatedData,

    //used for sum,min,max etc
    aggregate_expr: Vec<Arc<dyn AggregateExpr>>,

    //the group by expr e.g. GROUP BY colA, colB
    group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,

    //keeps the original batches before grouping
    originals: Vec<RecordBatch>,
    originals_counter: usize,

    //Key = hash(col1,col2,col3...)
    //values = row_id
    hash_table: HashMap<u64, Vec<u64>>,
    random_state: RandomState,
}

impl HashAggregateOperator {
    pub fn new(
        aggregate_expr: Vec<Arc<dyn AggregateExpr>>,
        group_by_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    ) -> Self {
        Self {
            aggregated_data: AggregatedData { data: None },
            aggregate_expr,
            group_by_expr,
            originals: Vec::new(),
            originals_counter: 0,
            hash_table: Default::default(),
            random_state: RandomState::generate_with(1, 2, 3, 4),
        }
    }
}

impl Sink for HashAggregateOperator {
    fn sink(&mut self, input: &Arc<RecordBatch>) -> SinkResultType {
        let num_rows = input.num_rows();
        let keys_values = self.group_by_expr.iter().map(|(f, _name)| {
            f.evaluate(input).unwrap()
                .into_array(input.num_rows()).unwrap()
        }).collect::<Vec<_>>();
        let mut hashes_buffer: Vec<u64> = Vec::new();
        hashes_buffer.resize(num_rows, 0);
        let hash_values = create_hashes(&keys_values, &self.random_state, &mut hashes_buffer).unwrap();

        for row_idx in 0..num_rows {
            //grab the hash which is the key
            let hashmap_key = hash_values[row_idx];
            // the value in the hash table is the row_id
            let hashmap_value = (self.originals_counter + row_idx) as u64;

            //if the key does not exist create a new array
            if !self.hash_table.contains_key(&hashmap_key) {
                self.hash_table.insert(hashmap_key, vec![hashmap_value]);
            } else {
                //else if the key already exists in the map then just append the value to the
                //existing vector
                self.hash_table
                    .get_mut(&hashmap_key)
                    .unwrap()
                    .push(hashmap_value);
            }
        }

        //append data to the collector
        self.originals.push(input.as_ref().clone());
        //increase the row counter for next iteration
        self.originals_counter += input.num_rows();

        return SinkResultType::NeedMoreInput;
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) {
        //combine the record batches
        let combined =
            compute::concat_batches(&self.originals[0].schema(), self.originals.iter()).unwrap();

        //vector having the columns for the output (aggregate columns and group by cols)
        let mut columns = Vec::with_capacity(self.group_by_expr.len() + self.aggregate_expr.len());

        //grab the distinct values for group by cols
        for (expr, _) in &self.group_by_expr {
            let new_col = expr.evaluate(&combined).unwrap();
            let mut indices = Vec::with_capacity(self.hash_table.len());
            for group in self.hash_table.values() {
                indices.push(group[0]);
            }

            let indices = UInt64Array::from(indices);

            let agg_col = compute::take(
                &new_col.into_array(combined.num_rows()).unwrap(),
                &indices,
                None,
            )
            .unwrap();
            columns.push(agg_col);
        }

        for agg_expr in &self.aggregate_expr {
            let input_exprs = agg_expr.expressions();
            let all_input_cols = input_exprs
                .iter()
                .map(|e| {
                    e.evaluate(&combined)
                        .and_then(|v| v.into_array(combined.num_rows()))
                })
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let mut output_col = Vec::with_capacity(self.hash_table.len());
            for (_, group) in &self.hash_table {
                let group = UInt64Array::from(group.clone());
                let input_cols = all_input_cols
                    .iter()
                    .map(|c| compute::take(c, &group, None))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let mut accum = agg_expr.create_accumulator().unwrap();
                accum.update_batch(&input_cols).unwrap();
                let out_value = accum.evaluate().unwrap().to_array().unwrap();
                output_col.push(out_value);
            }
            let output_col_ref = output_col.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            columns.push(compute::concat(&output_col_ref[..]).unwrap());
        }

        let mut fields = Vec::with_capacity(self.group_by_expr.len() + self.aggregate_expr.len());
        for (expr, name) in &self.group_by_expr {
            fields.push(Field::new(
                name,
                expr.data_type(&self.originals[0].schema()).unwrap(),
                false,
            ));
        }
        for expr in &self.aggregate_expr {
            fields.push(expr.field().unwrap());
        }
        let out_schema = Schema::new(fields);
        self.aggregated_data.data =
            Some(RecordBatch::try_new(Arc::new(out_schema), columns).unwrap());
    }
}

impl PhysicalOperator for HashAggregateOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashAggregate
    }
}
