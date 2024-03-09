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
use datafusion::physical_plan::filter::batch_filter;
use datafusion::physical_plan::{
    functions, ColumnStatistics, Partitioning, PhysicalExpr, Statistics, WindowExpr,
};
use futures::Stream;
use futures::StreamExt;
use std::sync::Arc;
use tokio::task;
pub struct FilterOperator {
    predicate: Arc<dyn PhysicalExpr>,
}
impl FilterOperator {
    pub fn new(predicate: Arc<dyn PhysicalExpr>) -> FilterOperator {
        FilterOperator { predicate }
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&self, input: &RecordBatch) -> Result<RecordBatch> {
        batch_filter(input, &self.predicate)
    }
}

impl PhysicalOperator for FilterOperator {
    fn name(&self) -> String {
        String::from("filter")
    }
}
