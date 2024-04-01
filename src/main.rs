use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{col,lit};
use datafusion::physical_expr::create_physical_expr;
use ee2::operator::dummy_sink::DummySinkOperator;
use ee2::operator::filter::FilterOperator;
use ee2::operator::projection::ProjectionOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::sync::Arc;

fn main() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, true)
    ]);
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema.clone()),
        "data/data.parquet",
    )));

    let expr2 = col("id");
    let physical_expr2 = create_physical_expr(
        &expr2, 
        &DFSchema::try_from(schema.clone()).unwrap(), 
        &ExecutionProps::new()
    ).unwrap();
    let project_expr = Vec::from([(physical_expr2, String::from("new_id"))]);
    let project = Box::new(ProjectionOperator::new(
        Arc::new(schema.clone()), project_expr
    ));

    let project_output_schema = project.output_schema.clone();
    let expr = col("new_id").lt_eq(lit(5));
    let physical_expr = create_physical_expr(
        &expr, 
        &DFSchema::try_from(project_output_schema).unwrap(), 
        &ExecutionProps::new()
    ).unwrap();

    let filter = Box::new(FilterOperator::new(
        physical_expr
    ));

    let sink: Option<Box<dyn Sink>> = Some(Box::new(DummySinkOperator::new()));

    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.operators.push(project);
    pipeline.operators.push(filter);
    pipeline.sink_operator = sink;

    pipeline.execute();

    println!("We made it :) ");
}
