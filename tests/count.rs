use ahash::{HashMap, HashMapExt};
use datafusion::arrow::array::Int64Array;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::col;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::aggregates::AggregateFunction;
use ee2::helper::{tpch_schema, Entry};
use ee2::operator::hash_aggregate::HashAggregateOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::cell::RefCell;
use std::sync::Arc;
use std::vec;

#[test]
fn count_customer_with_scale_factor_1() {
    //grab the schema for customer table
    let schema = tpch_schema("customer");

    //create a scan operator
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        "data/tpch/customer.parquet",
        Arc::new(schema.clone()),
        None,
    )));

    //create the group by condition
    //group based on c_custkey
    let expr = col("c_custkey");
    let aggregate_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    //create the aggregate expression and rename the column to "num"
    let aggregate_expr = create_aggregate_expr(
        &AggregateFunction::Count,
        false,
        &[aggregate_expr.clone()],
        &[],
        &schema,
        "num",
    )
    .unwrap();

    let v = vec![];
    //create the hash aggregate operator as a sink
    let sink: Option<Box<dyn Sink>> = Some(Box::new(HashAggregateOperator::new(
        Arc::new(schema),
        vec![aggregate_expr],
        v,
    )));

    //now create the first pipeline with
    // source operator -> Scan
    // Sink operator -> Hash Aggregate
    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.sink_operator = sink;

    //Store the result with id = 1;
    let store = Arc::new(RefCell::new(HashMap::new()));
    let pipeline_number = 1;

    //execute the pipeline
    pipeline.execute(pipeline_number, Arc::clone(&store));

    //retrieve the result from the store
    let result = store.borrow_mut().remove(&pipeline_number).unwrap();
    match result {
        Entry::Batch(mut batches) => {
            if batches.len() != 1 {
                panic!("Count * should return one row and one column (one record batch)");
            }

            let mut batch = Arc::unwrap_or_clone(batches.pop().unwrap());
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.num_rows(), 1);
            let removed_column = batch.remove_column(0);
            assert_eq!(
                removed_column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap(),
                &Int64Array::from(vec![150000])
            );
        }
        _ => {
            panic!("Wrong type entry in the store for hash aggregate")
        }
    }
}
