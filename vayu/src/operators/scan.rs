use crate::pipeline::{PhysicalOperator, Source};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::physical_plan::{common, SendableRecordBatchStream};

use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task;
pub struct ScanOperator {
    csvconfig: CsvConfig,
    pub fileconfig: FileScanConfig,
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

impl PhysicalOperator for ScanOperator {
    fn name(&self) -> String {
        String::from("scan")
    }
}
