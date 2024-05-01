use ahash::{HashMap, HashMapExt};
use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::col;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::aggregates::AggregateFunction;
use ee2::helper::Entry;
use ee2::operator::hash_aggregate::HashAggregateOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::cell::RefCell;
use std::sync::Arc;
use std::vec;

#[test]
fn hash_aggregate() {
    //define schema of table to read
    let schema = Schema::new(vec![
        Field::new("fruit", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
        Field::new("quantity", DataType::Int64, false),
    ]);

    //create scan operator with the schema
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        "data/products.parquet",
        Arc::new(schema.clone()),
        None,
    )));

    //create the group by condition
    //group based on grade
    let expr = col("price") * col("quantity");
    let aggregate_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let expr = col("fruit");
    let group_by_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let aggregate_expr = create_aggregate_expr(
        &AggregateFunction::Sum,
        false,
        &[aggregate_expr.clone()],
        &[],
        &schema,
        "total_price",
    )
    .unwrap();

    let v = vec![(group_by_expr, String::from("fruit"))];
    //create the join build operator
    let sink: Option<Box<dyn Sink>> = Some(Box::new(HashAggregateOperator::new(
        Arc::new(schema.clone()),
        vec![aggregate_expr],
        v,
    )));

    // source operator -> Scan
    // Sink operator -> HashJoinBuild
    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.sink_operator = sink;

    //Store the result with id = 1;
    let store = Arc::new(RefCell::new(HashMap::new()));
    let pipeline_number = 0;

    //execute the pipeline
    pipeline.execute(pipeline_number, Arc::clone(&store));

    //retrieve the result from the store
    let result = store.borrow_mut().remove(&pipeline_number).unwrap();
    match result {
        Entry::Batch(mut batches) => {
            let mut batch = Arc::unwrap_or_clone(batches.pop().unwrap());
            let aggregated_column = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mut v = vec![];
            for value in aggregated_column {
                v.push(value.unwrap());
            }
            v.sort();
            assert_eq!(v, vec![45, 50, 144]);
        }
        _ => {
            panic!()
        }
    }
}
