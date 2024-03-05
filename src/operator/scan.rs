use crate::common::enums::operator_result_type::SourceResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::types::LogicalType;
use crate::physical_operator::{PhysicalOperator, Source};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::csv::{Reader, ReaderBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::sql::sqlparser::keywords::Keyword::TIME;
use std::fs::File;
use std::sync::Arc;

pub struct ScanOperator {
    schema: Arc<Schema>,
    file: Reader<File>,
}
impl ScanOperator {
    pub fn new(schema: Arc<Schema>, file_path: &str) -> ScanOperator {
        let file = File::open(&file_path).unwrap();
        ScanOperator {
            schema: Arc::clone(&schema),
            file: ReaderBuilder::new(Arc::clone(&schema)).build(file).unwrap(),
        }
    }
}

impl Source for ScanOperator {
    fn get_data(&mut self, chunk: &mut RecordBatch) -> SourceResultType {
        let batch = self.file.next();
        match batch {
            None => return SourceResultType::Finished,
            Some(batch) => match batch {
                Ok(batch) => {
                    *chunk = batch;
                    return SourceResultType::HaveMoreOutput;
                }
                Err(e) => {
                    eprintln!("{e}");
                    panic!()
                }
            },
        }
    }
}

impl PhysicalOperator for ScanOperator {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
    fn is_sink(&self) -> bool {
        false
    }

    fn is_source(&self) -> bool {
        true
    }
}
