use crate::pipeline::{PhysicalOperator, Source};
use arrow::csv::ReaderBuilder;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::physical_plan::{common, SendableRecordBatchStream};

use arrow::datatypes::Schema;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::Reader;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::stream::StreamExt;
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use tokio::task;
pub struct ScanOperator {
    stream: SendableRecordBatchStream,
}
impl ScanOperator {
    pub fn new(stream: SendableRecordBatchStream) -> ScanOperator {
        ScanOperator { stream }
    }
}

impl Source for ScanOperator {
    fn get_data(&mut self) -> Option<RecordBatch> {
        let block = task::block_in_place(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(self.stream.next())
        });
        let t = block.transpose();
        t.unwrap()
    }
}

impl PhysicalOperator for ScanOperator {
    fn name(&self) -> String {
        String::from("scan")
    }
}
