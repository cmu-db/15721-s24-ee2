use datafusion::arrow::util::pretty;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::aggregates::AggregateFunction;
use ee2::helper::tpch_schema;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::sync::Arc;
use std::vec;
use ee2::operator::hash_aggregate::HashAggregateOperator;
use std::time::Instant;
use datafusion::common::ScalarValue::Date32;
use ee2::operator::filter::FilterOperator;

fn main() {
    //define schema of table to read
    let schema = tpch_schema("lineitem");

    //create scan operator with the schema
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema.clone()),
        "data/tpch/lineitem.parquet",
    )));

    let expr = col("l_shipdate").lt_eq(lit(Date32(Some(10471))));
    let physical_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
        .unwrap();

    //create filter operator with the above expression student_id <= 3
    let filter = Box::new(FilterOperator::new(physical_expr));

    let expr = col("l_returnflag");
    let group_by_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    ).unwrap();

    let expr2 = col("l_linestatus");
    let group_by_expr2= create_physical_expr(
        &expr2,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    ).unwrap();


    //sum(l_quantity) as sum_qty
    let expr = col("l_quantity");
    let aggr= create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let aggregate_expr1 = create_aggregate_expr(
        &AggregateFunction::Sum,
        false,
        &[aggr.clone()],
        &[],
        &schema,
        "sum_qty",
    )
        .unwrap();

    //sum(l_extendedprice) as sum_base_price
    let expr = col("l_extendedprice");
    let aggr= create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
        .unwrap();
    let aggregate_expr2 = create_aggregate_expr(
        &AggregateFunction::Sum,
        false,
        &[aggr.clone()],
        &[],
        &schema,
        "sum_base_price",
    )
        .unwrap();

    let v = vec![(group_by_expr, String::from("l_returnflag")), (group_by_expr2, String::from("l_linestatus"))];
    let sink: Option<Box<dyn Sink>> = Some(Box::new(HashAggregateOperator::new(
        vec![aggregate_expr1, aggregate_expr2],
        v
    )));

    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.operators.push(filter);
    pipeline.sink_operator = sink;



    let start = Instant::now();
    pipeline.execute();
    let duration = start.elapsed();
    let build = pipeline.sink_operator.take().unwrap();
    let build = build
        .as_any()
        .downcast_ref::<HashAggregateOperator>()
        .unwrap();

    let grouped_data = &build.aggregated_data;
    let _ = pretty::print_batches(std::slice::from_ref(grouped_data.data.as_ref().unwrap()));

    println!("Duration of query is {:?}", duration);
}
