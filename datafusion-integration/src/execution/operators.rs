use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::error::DataFusionError::NotImplemented;
use datafusion::error::Result;
use datafusion::physical_plan::{common, SendableRecordBatchStream};
// use datafusion::execution::SendableRecordBatchStream;
use arrow::compute::filter_record_batch;
use datafusion::common::cast::as_boolean_array;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use tokio::task;
// use datafusion::common::str_to_byte;
use datafusion_proto::physical_plan::from_proto::{
    parse_physical_expr, parse_protobuf_file_scan_config,
};
use datafusion_proto::protobuf::{FileScanExecConf, PhysicalExprNode};
use std::sync::Arc;
fn str_to_byte(s: &String, descriptions: &str) -> Result<u8> {
    if s.len() != 1 {
        return Err(NotImplemented(String::from("size not one")));
    }
    Ok(s.as_bytes()[0])
}

pub fn filter(input: RecordBatch, expr: &PhysicalExprNode) -> Result<RecordBatch> {
    let ctx = SessionContext::new();
    let predicate = parse_physical_expr(expr, &ctx, &input.schema()).unwrap();
    println!("{}", predicate);
    let output = filter_record_batch(
        &input,
        as_boolean_array(
            &predicate
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
