use datafusion::arrow::datatypes::{DataType, Field, Schema};
use ee2::operator::dummy_sink::DummySinkOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::sync::Arc;

fn main() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema),
        "/home/claspias/dev/research/15721-s24-ee2/data/data.csv",
    )));

    let sink: Option<Box<dyn Sink>> = Some(Box::new(DummySinkOperator::new()));

    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.sink_operator = sink;
    pipeline.execute();

    println!("We made it :) ");
}
