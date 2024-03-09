use crate::pipeline::{IntermediateOperator, PhysicalOperator};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::physical_plan::{common, SendableRecordBatchStream};

use crate::pipeline::DummyFeeder;
use arrow::compute::filter_record_batch;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::physical_plan::{
    functions, ColumnStatistics, Partitioning, PhysicalExpr, Statistics, WindowExpr,
};
use futures::Stream;
use futures::StreamExt;
use std::sync::Arc;
use tokio::task;
pub struct FilterOperator {
    exec: SendableRecordBatchStream,
    dummy_input: DummyFeeder,
}
impl FilterOperator {
    pub fn new(exec: SendableRecordBatchStream, dummy_input: DummyFeeder) -> FilterOperator {
        FilterOperator { exec, dummy_input }
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        self.dummy_input.batch = Some(input.clone());
        let data = futures::executor::block_on(self.exec.next());
        data.unwrap()
    }
}

impl PhysicalOperator for FilterOperator {
    fn name(&self) -> String {
        String::from("filter")
    }
}
