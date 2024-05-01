use ahash::{HashMap, HashMapExt};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::col;
use datafusion::physical_expr::create_physical_expr;
use ee2::operator::physical_batch_collector::PhysicalBatchCollector;
use ee2::operator::projection::ProjectionOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::cell::RefCell;
use std::sync::Arc;

#[test]
fn filter_and_projection() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, true),
    ]);

    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        "data/data.parquet",
        Arc::new(schema.clone()),
        None,
    )));

    let expr2 = col("id");
    let physical_expr2 = create_physical_expr(
        &expr2,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();
    let project_expr = Vec::from([(physical_expr2, String::from("new_id"))]);
    let project = Box::new(ProjectionOperator::new(
        Arc::new(schema.clone()),
        project_expr,
    ));

    let store = Arc::new(RefCell::new(HashMap::new()));
    let pipeline_number = 0;

    //Physical Batch collector does not store the data in the store
    //It is just a helper sink operator for printing the data

    let sink: Option<Box<dyn Sink>> = Some(Box::new(PhysicalBatchCollector::new()));
    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.operators.push(project);
    pipeline.sink_operator = sink;

    //execute the pipeline
    pipeline.execute(pipeline_number, Arc::clone(&store));
    let sink = pipeline.sink_operator.take().unwrap();
    let sink = sink
        .as_any()
        .downcast_ref::<PhysicalBatchCollector>()
        .unwrap();
    let batches = &sink.result.clone();

    //assert that there is only one column with the projected name
    for batch in batches {
        assert_eq!(batch.num_columns(), 1);
        assert_ne!(batch.schema().fields().find("new_id"), None);
    }
}
