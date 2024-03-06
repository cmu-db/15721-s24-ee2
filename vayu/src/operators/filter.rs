use crate::pipeline::{IntermediateOperator, PhysicalOperator};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::physical_plan::{common, SendableRecordBatchStream};

use arrow::compute::filter_record_batch;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result;
use datafusion::physical_plan::{
    functions, ColumnStatistics, Partitioning, PhysicalExpr, Statistics, WindowExpr,
};
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
        let output = filter_record_batch(
            &input,
            as_boolean_array(
                &self
                    .predicate
                    .evaluate(&input)
                    .unwrap()
                    .into_array(1024)
                    .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        Ok(output)
    }
}

impl PhysicalOperator for FilterOperator {
    fn name(&self) -> String {
        String::from("filter")
    }
}
