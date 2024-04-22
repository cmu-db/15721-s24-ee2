use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::col;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::aggregates::AggregateFunction;
use ee2::helper::tpch_schema;
use ee2::operator::dummy_sink::DummySinkOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{Sink, Source};
use std::sync::Arc;
use std::vec;

fn main() {
    //define schema of table to read
    let region_schema = tpch_schema("nation");
    //create scan operator with the schema
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(region_schema.clone()),
        "data/tpch/nation.parquet",
    )));

    // //create the group by condition
    // //group based on grade
    // let expr = col("price") * col("quantity");
    // let aggregate_expr = create_physical_expr(
    //     &expr,
    //     &DFSchema::try_from(schema.clone()).unwrap(),
    //     &ExecutionProps::new(),
    // )
    // .unwrap();
    //
    // let expr = col("fruit");
    // let group_by_expr = create_physical_expr(
    //     &expr,
    //     &DFSchema::try_from(schema.clone()).unwrap(),
    //     &ExecutionProps::new(),
    // )
    // .unwrap();
    //
    // let aggregate_expr = create_aggregate_expr(
    //     &AggregateFunction::Sum,
    //     false,
    //     &[aggregate_expr.clone()],
    //     &[],
    //     &schema,
    //     "total_price",
    // )
    // .unwrap();
    //
    // let v = vec![(group_by_expr, String::from("fruit"))];
    // //create the join build operator
    let sink: Option<Box<dyn Sink>> = Some(Box::new(DummySinkOperator::new()));
    //
    // //now create the first pipeline with
    // // source operator -> Scan
    // // Sink operator -> HashJoinBuild
    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.sink_operator = sink;
    pipeline.execute();
    //
    // let build = pipeline.sink_operator.take().unwrap();
    // let build = build
    //     .as_any()
    //     .downcast_ref::<HashAggregateOperator>()
    //     .unwrap();
    //
    // let grouped_data = &build.aggregated_data;
    // let _ = pretty::print_batches(std::slice::from_ref(
    //     grouped_data.data.as_ref().unwrap(),
    // ));
}
