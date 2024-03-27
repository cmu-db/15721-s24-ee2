use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Schema};
use datafusion::physical_plan::PhysicalExpr;

use crate::common::enums::operator_result_type::{SinkResultType,OperatorResultType};
use crate::physical_operator::{IntermediateOperator, PhysicalOperator, Sink};
use std::hash::{DefaultHasher, Hasher};

pub struct JoinLeftData{
	pub hash_table : HashMap<u64,u64>,
	pub hasher : DefaultHasher,
	pub original : Vec<RecordBatch>
}

#[derive(Clone)]
pub struct HashJoinBuildOperator{
    expr: Vec<Arc<dyn PhysicalExpr>>,
	schema : Arc<Schema>, 
	pub hash_table : HashMap<u64,u64>,
	pub hasher : DefaultHasher,
	pub original : Vec<RecordBatch>,
}

impl HashJoinBuildOperator
{
	pub fn new(expr: Vec<Arc<dyn PhysicalExpr>> , schema : Arc<Schema>) -> Self{
		HashJoinBuildOperator{
			expr,
			schema,
			hash_table : HashMap::new(),
			hasher : DefaultHasher::new(),
			original : vec![],
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

		let keys_values = self.expr.iter().map(|f| f.evaluate(batch).unwrap().into_array(batch.num_rows())).collect::<Vec<_>>();
        let key = keys_values[0].as_ref().unwrap();
		self.original.push(batch.as_ref().clone());
		match &key.data_type() {
			DataType::Int32 => {let x : &Int32Array = key.as_any().downcast_ref().unwrap();
				for (row_id, val ) in x.iter().enumerate(){
					match val {
						Some(val) => {
							self.hasher.write_i32(val);
							self.hash_table.insert(self.hasher.finish(),row_id.try_into().unwrap());
						}
						_ => {}
					}
				}
			}
			_ =>{}
		}

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
}

impl HashJoinProbeOperator{
	pub fn new(expr: Vec<Arc<dyn PhysicalExpr>>, schema : Arc<Schema>, join_left_data : JoinLeftData) -> Self{
		HashJoinProbeOperator{
			expr,
			schema,
			join_left_data,
		}
	}
}

impl IntermediateOperator for HashJoinProbeOperator {
	fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType{

		let keys_values = self.expr.iter().map(|f| f.evaluate(input).unwrap().into_array(input.num_rows())).collect::<Vec<_>>();
        let key = keys_values[0].as_ref().unwrap();
		let id : &Int32Array = self.join_left_data.original[0].column_by_name("id").unwrap().as_any().downcast_ref().unwrap();
		let value : &Int32Array = self.join_left_data.original[0].column_by_name("value").unwrap().as_any().downcast_ref().unwrap();
		let names : &StringArray = input.column_by_name("name").unwrap().as_any().downcast_ref().unwrap();
		match &key.data_type() {
			DataType::Int32 => {let x : &Int32Array = key.as_any().downcast_ref().unwrap();
				for (row_id, val ) in x.iter().enumerate(){
					match val {
						Some(val) => {
							self.join_left_data.hasher.write_i32(val);
							let hash = self.join_left_data.hasher.finish();
							println!("Hash = {} for value = {} ", hash, val);
							let left_row_id = self.join_left_data.hash_table.get(&hash);
							//we found a match
							match left_row_id{
								Some(left_row_id) => {
								println!("{}, {}, {}", id.value(left_row_id.as_usize()), value.value(left_row_id.as_usize()), names.value(row_id));
								}
								None => {println!("No match found ");}
							}
						}
						_ => {}
					}
				}
			}
			_ =>{}
		}

		OperatorResultType::Finished(input.clone())
		// todo!();

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
