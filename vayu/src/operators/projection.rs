use crate::pipeline::{IntermediateOperator, PhysicalOperator};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::physical_plan::projection::ProjectionExec;
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
pub struct ProjectionOperator {
    projection: ProjectionExec,
}
impl ProjectionOperator {
    pub fn new(projection: ProjectionExec) -> ProjectionOperator {
        ProjectionOperator { projection }
    }
}

impl IntermediateOperator for ProjectionOperator {
    fn execute(&self, input: &RecordBatch) -> Result<RecordBatch> {
        todo!();
    }
}

impl PhysicalOperator for ProjectionOperator {
    fn name(&self) -> String {
        String::from("projection")
    }
}
