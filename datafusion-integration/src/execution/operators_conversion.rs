use arrow::csv::ReaderBuilder;
use arrow::datatypes::Schema;
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileStream;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use datafusion::error::DataFusionError::NotImplemented;
use datafusion::error::Result;
use std::fs::File;

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
use vayu::operators::filter::FilterOperator;
use vayu::operators::projection::ProjectionOperator;

use vayu::operators::scan::ScanOperator;
fn str_to_byte(s: &String, descriptions: &str) -> Result<u8> {
    if s.len() != 1 {
        return Err(NotImplemented(String::from("size not one")));
    }
    Ok(s.as_bytes()[0])
}

pub fn scan(
    base_conf: Option<FileScanExecConf>,
    header: bool,
    delimiter: &String,
    quote: &String,
) -> Result<ScanOperator> {
    let ctx = SessionContext::new();
    let base_config: datafusion::datasource::physical_plan::FileScanConfig =
        parse_protobuf_file_scan_config(base_conf.as_ref().unwrap(), &ctx)?;
    let schema = base_config.file_schema.clone();
    let delimiter = str_to_byte(delimiter, "delimiter")?;
    let quote = str_to_byte(quote, "quote")?;

    let object_store = ctx
        .runtime_env()
        .object_store(&base_config.object_store_url)?;

    let file_projection = base_config.projection.as_ref().map(|p| {
        p.iter()
            .filter(|col_idx| **col_idx < base_config.file_schema.fields().len())
            .copied()
            .collect()
    });
    let conf: CsvConfig = CsvConfig::new(
        128,
        schema.clone(),
        file_projection,
        header,
        delimiter,
        quote,
        object_store,
    );
    let conf = Arc::new(conf.clone());
    let opener = CsvOpener::new(conf, FileCompressionType::UNCOMPRESSED);
    let stream = FileStream::new(&base_config, 0, opener, &ExecutionPlanMetricsSet::new()).unwrap();
    let stream1 = Box::pin(stream) as SendableRecordBatchStream;

    Ok(ScanOperator::new(stream1, schema))
}

pub fn filter(schema: &Schema, expr: &PhysicalExprNode) -> Result<FilterOperator> {
    let ctx = SessionContext::new();
    let predicate = parse_physical_expr(expr, &ctx, schema).unwrap();
    Ok(FilterOperator::new(predicate))
}

pub fn projection(schema: &Schema, projection: &PhysicalExprNode) -> Result<FilterOperator> {
    let ctx = SessionContext::new();

    let exprs = projection.expr.iter().zip(projection.expr_name.iter());
    let exprs = exprs
        .map(|(expr, name)| Ok((parse_physical_expr(expr, &ctx, schema)?, name.to_string())))
        .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;
    Ok(ProjectionOperator::new(exprs))
}
