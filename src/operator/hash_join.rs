use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::{Array,Int32Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{Field, DataType, Schema};
use datafusion::physical_plan::PhysicalExpr;

use crate::common::enums::operator_result_type::{SinkResultType,OperatorResultType};
use crate::physical_operator::{IntermediateOperator, PhysicalOperator, Sink};
use std::hash::Hasher;
use std::collections::hash_map::DefaultHasher;
use datafusion::arrow::compute;

pub struct JoinLeftData{
	pub hash_table : HashMap<u64,Vec<u64>>,
	pub schema: Arc<Schema>,
	pub originals : Vec<RecordBatch>
}

#[derive(Clone)]
pub struct HashJoinBuildOperator{
    expr: Vec<Arc<dyn PhysicalExpr>>,
	pub schema : Arc<Schema>, 
	pub hash_table : HashMap<u64,Vec<u64>>,
	pub originals : Vec<RecordBatch>,
	originals_counter: usize
}

impl HashJoinBuildOperator
{
	pub fn new(expr: Vec<Arc<dyn PhysicalExpr>> , schema : Arc<Schema>) -> Self{
		HashJoinBuildOperator{
			expr,
			schema,
			hash_table : HashMap::new(),
			originals : vec![],
			originals_counter: 0
		}
	}
}

impl PhysicalOperator for HashJoinBuildOperator
{
	fn schema(&self) -> Arc<Schema> {
		Arc::clone(&self.schema)
	}

	fn is_sink(&self) -> bool{
		true
	}

	fn is_source (&self ) -> bool{
		false
	}
}

impl Sink for HashJoinBuildOperator
{
    fn sink(&mut self, batch : &Arc<RecordBatch>) -> SinkResultType {
		let num_rows = batch.num_rows();
		let join_keys = self.expr.iter().map(
			|f| f.evaluate(batch).unwrap().into_array(batch.num_rows()).unwrap()).collect::<Vec<_>>();

		for row_idx in 0..num_rows {
			let mut hasher = DefaultHasher::new();
			for col_idx in 0..join_keys.len() {
				match join_keys[col_idx].data_type() {
					DataType::Int32 => {
						let col: &Int32Array = join_keys[col_idx].as_any().downcast_ref().unwrap();
						let value = col.value(row_idx);
						hasher.write_i32(value);
					}
					_ => {todo!();}
				}
			}
			let hash_key = hasher.finish();
			let hash_value = (self.originals_counter + row_idx) as u64;
			if !self.hash_table.contains_key(&hash_key) {
				self.hash_table.insert(hash_key, vec![hash_value]);
			}
			else {
				self.hash_table.get_mut(&hash_key).unwrap().push(hash_value);
			}
		}
		self.originals.push(batch.as_ref().clone());
		self.originals_counter += batch.num_rows();

		SinkResultType::Finished
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

}


pub struct HashJoinProbeOperator{
	expr: Vec<Arc<dyn PhysicalExpr>>,
	schema : Arc<Schema>,
	join_left_data : JoinLeftData,
	output_schema: Arc<Schema>
}

impl HashJoinProbeOperator{
	pub fn new(expr: Vec<Arc<dyn PhysicalExpr>>, schema : Arc<Schema>, join_left_data : JoinLeftData) -> Self{
		//let output_schema = Arc::new(Schema::try_merge(
		//	vec![*join_left_data.schema, *schema]
		//).unwrap());
		let num_output_fields = join_left_data.schema.fields.len() + schema.fields().len();
		let mut fields: Vec<Field> = Vec::with_capacity(num_output_fields);
		for f in join_left_data.schema.fields() {
			fields.push(f.deref().clone());
		}
		for f in schema.fields() {
			fields.push(f.deref().clone());
		}
		let output_schema = Arc::new(Schema::new(fields));
		HashJoinProbeOperator{
			expr,
			schema,
			join_left_data,
			output_schema
		}
	}
}

impl IntermediateOperator for HashJoinProbeOperator {
	fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType{

		let num_rows = input.num_rows();
		let join_keys = self.expr.iter().map(
			|f| f.evaluate(input).unwrap().into_array(input.num_rows()).unwrap()).collect::<Vec<_>>();
        
		let left_large_batch = compute::concat_batches(
			&self.join_left_data.schema, self.join_left_data.originals.iter()).unwrap();

		let mut left_indices_vec: Vec<u64> = Vec::new();
		let mut right_indices_vec: Vec<u64> = Vec::new();
		for right_row_idx in 0..num_rows {
			let mut hasher = DefaultHasher::new();
			for col_idx in 0..join_keys.len() {
				match join_keys[col_idx].data_type() {
					DataType::Int32 => {
						let col: &Int32Array = join_keys[col_idx].as_any().downcast_ref().unwrap();
						let value = col.value(right_row_idx);
						hasher.write_i32(value);
					}
					_ => {todo!();}
				}
			}
			let hash_key = hasher.finish();
			match self.join_left_data.hash_table.get(&hash_key) {
				Some(left_row_idxs) => {
					for left_row_idx in left_row_idxs {
						// TODO
						left_indices_vec.push(*left_row_idx);
						right_indices_vec.push(right_row_idx as u64);
					}
				}
				None => {}
			}
		}
		
		let left_indices = UInt64Array::from(left_indices_vec);
		let right_indices = UInt64Array::from(right_indices_vec);

		let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(self.output_schema.fields().len());
		for left_col in left_large_batch.columns() {
			let new_col = compute::take(left_col, &left_indices, None).unwrap();
			columns.push(new_col);
		}
		for right_col in input.columns() {
			let new_col = compute::take(right_col, &right_indices, None).unwrap();
			columns.push(new_col);
		}

		let output = RecordBatch::try_new(self.output_schema.clone(), columns).unwrap();

		OperatorResultType::Finished(Arc::new(output))
	}
}

impl PhysicalOperator for HashJoinProbeOperator{
	fn schema(&self) -> Arc<Schema> {
		Arc::clone(&self.schema)
	}

	fn is_sink(&self) -> bool{
		false
	}

	fn is_source (&self ) -> bool{
		true
	}
}
