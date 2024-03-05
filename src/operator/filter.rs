use crate::common::enums::operator_result_type::OperatorResultType;
use crate::common::types::LogicalType;
use crate::physical_operator::{IntermediateOperator, PhysicalOperator};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

pub struct FilterOperator {
    expression : Box<dyn PhysicalExpr>,
}

impl FilterOperator {
    pub fn new(expression : Box<dyn PhysicalExpr>) -> Self {
        FilterOperator {
            expression
        }
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&self, input: &RecordBatch, chunk: &RecordBatch) -> OperatorResultType {
        let result = self.expression.evaluate(&input);

        match result {
            Ok(res) => {
                let ColumnarValue::Array(arr)  = res else{
                    panic!()
                };
                let arr = arr.as_boolean();
                // println!("count 91: {}", v.iter().filter(|&n| *n == 91).count());
                let size = arr.true_count();
                let mut v = Vec::new();
                for col in input.columns(){
                    let mut current = Vec::new();
                    current.reserve(size);
                    for i in input.num_rows(){
                        if arr.value(i) == true {
                            current.push(col.slice(i,1));
                        }
                    }
                    v.push(current);
                }

                let new_batch = RecordBatch::try_new(input.schema(), v);
            }
            Err(e) => {
                eprintln!("{e}");
                panic!();
            }
        }


        OperatorResultType::Finished
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
