use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::physical_plan::{common, SendableRecordBatchStream};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use std::sync::Arc;
use tokio::task;

pub struct Pipeline {
    pub source_operator: Option<Box<dyn Source>>,
    pub sink_operator: Option<Box<dyn Sink>>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
}
impl Pipeline {
    pub fn new() -> Pipeline {
        Pipeline {
            source_operator: None,
            sink_operator: None,
            operators: vec![],
        }
    }
}
pub trait PhysicalOperator {
    // fn is_sink(&self) -> bool;
    // fn is_source(&self) -> bool;
}

//Operators that implement Sink trait consume data
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(&self, chunk: &mut RecordBatch) -> bool;
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_data(&self) -> Result<RecordBatch>;
}

//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&self, input: &RecordBatch, chunk: &RecordBatch) -> bool;
}

pub struct ScanOperator {
    csvconfig: CsvConfig,
    fileconfig: FileScanConfig,
}
impl ScanOperator {
    pub fn new(csvconfig: CsvConfig, fileconfig: FileScanConfig) -> ScanOperator {
        ScanOperator {
            csvconfig,
            fileconfig,
        }
    }
}

impl Source for ScanOperator {
    fn get_data(&self) -> Result<RecordBatch> {
        let conf = Arc::new(self.csvconfig.clone());
        let opener = CsvOpener::new(conf, FileCompressionType::UNCOMPRESSED);
        let stream = FileStream::new(&self.fileconfig, 0, opener, &ExecutionPlanMetricsSet::new())?;
        let temp = Box::pin(stream) as SendableRecordBatchStream;
        let temp_chunks = task::block_in_place(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(common::collect(temp))
        })
        .unwrap();
        Ok(temp_chunks[0].clone())
    }
}

impl PhysicalOperator for ScanOperator {}
