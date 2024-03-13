use crate::common::enums::operator_result_type::SourceResultType;
use crate::physical_operator::{PhysicalOperator, Source};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::{Reader, ReaderBuilder};
use datafusion::arrow::datatypes::Schema;
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
    fn get_data(&mut self) -> SourceResultType {
        let batch = self.file.next();
        match batch {
            None => return SourceResultType::Finished(Arc::new(RecordBatch::new_empty(self.schema.clone()))),
            Some(batch) => match batch {
                Ok(batch) => {
                    return SourceResultType::HaveMoreOutput(Arc::new(batch));
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
