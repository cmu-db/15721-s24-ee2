use crate::operators::operator_result_type::SourceResultType;
use crate::pipeline::{PhysicalOperator, Source};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::{Reader, ReaderBuilder};
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use futures::StreamExt;
use std::fs::File;
use std::sync::Arc;

enum ScanReader {
    Parquet(ParquetRecordBatchReader),
    Csv(Reader<File>),
    Error
}

pub struct ScanOperator {
    schema: Arc<Schema>,
    scan_reader: ScanReader
}
impl ScanOperator {
    pub fn new(schema: Arc<Schema>, file_path: &str) -> ScanOperator {
        let file = File::open(&file_path).unwrap();
        let scan_reader = if file_path.ends_with("parquet") {
            ScanReader::Parquet(ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap())
        }
        else if file_path.ends_with("csv") {
            ScanReader::Csv(ReaderBuilder::new(Arc::clone(&schema)).build(file).unwrap())
        }
        else {
            ScanReader::Error
        };
        ScanOperator {
            schema: Arc::clone(&schema),
            scan_reader
        }
    }
}

impl Source for ScanOperator {
    fn get_data(&mut self) -> SourceResultType {
        let batch = match &mut self.scan_reader {
            ScanReader::Parquet(reader) => {
                reader.next()
            },
            ScanReader::Csv(reader) => {
                reader.next()
            },
            ScanReader::Error => {panic!()}
        };
        match batch {
            Some(batch) => {
                return SourceResultType::HaveMoreOutput(Arc::new(batch.unwrap()));
            },
            None => {
                return SourceResultType::Finished(Arc::new(RecordBatch::new_empty(self.schema.clone())))
            }
        }
    }
}

impl PhysicalOperator for ScanOperator {
    fn name(&self) -> String {
        String::from("scan")
    }
}

pub struct DataFusionScanOperator {
    stream: SendableRecordBatchStream,
    schema: Arc<Schema>
}

impl DataFusionScanOperator {
    pub fn new(stream: SendableRecordBatchStream, schema: Arc<Schema>) -> DataFusionScanOperator {
        DataFusionScanOperator {
            stream,
            schema
        }
    }
}

impl Source for DataFusionScanOperator {
    fn get_data(&mut self) -> SourceResultType {
        let data = futures::executor::block_on(self.stream.next());
        match data {
            Some(d) => {
                return SourceResultType::HaveMoreOutput(Arc::new(d.unwrap()))
            }
            None => {
                return SourceResultType::Finished(Arc::new(RecordBatch::new_empty(self.schema.clone())))
            }
        }
    }
}

impl PhysicalOperator for DataFusionScanOperator {
    fn name(&self) -> String {
        String::from("dfscan")
    }
}
