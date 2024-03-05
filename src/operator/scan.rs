use crate::common::enums::operator_result_type::SourceResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::types::LogicalType;
use crate::physical_operator::{PhysicalOperator, Source};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::sqlparser::keywords::Keyword::TIME;
use std::sync::Arc;

pub struct ScanOperator {
    schema: Arc<Schema>,
}
impl ScanOperator {
    pub fn new(schema: Arc<Schema>) -> ScanOperator {
        ScanOperator { schema }
    }
}

impl Source for ScanOperator {
    fn get_data(&self, chunk: &mut RecordBatch) -> SourceResultType {
        match chunk.num_rows() {
            0 => return SourceResultType::Finished,
            _ => return SourceResultType::HaveMoreOutput,
        }
    }
}

impl PhysicalOperator for ScanOperator {
    fn schema(&self) -> Arc<Schema> {
        todo!()
    }
    fn is_sink(&self) -> bool {
        false
    }
}
