use datafusion::arrow::array::{Array, BooleanArray, Datum, Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::DataType::Int64;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use ee2::operator::dummy_sink::DummySinkOperator;
use ee2::operator::filter::FilterOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::sync::Arc;

fn main() {
    println!("We made it :) ");
}
