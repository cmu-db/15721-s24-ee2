use datafusion::arrow::util::pretty;
use datafusion::common::DFSchema;
use datafusion::common::ScalarValue::Date32;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use ee2::df2pipeline::{self, df2pipeline};
use ee2::helper::tpch_schema;
use ee2::operator::filter::FilterOperator;
use ee2::operator::hash_aggregate::HashAggregateOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::{self, Pipeline};
use ee2::physical_operator::{Sink, Source};
use std::sync::Arc;
use std::time::Instant;
use std::vec;
fn main() {
    //define schema of table to read
    let sql = "select
    count(l_returnflag), count(l_linestatus)
 from
     lineitem
 where 
     l_returnflag = 'R'";
    let ctx = SessionContext::default();

    let _ = futures::executor::block_on(ctx.register_parquet(
        "lineitem",
        "data/tpch/lineitem.parquet",
        ParquetReadOptions::default(),
    ))
    .unwrap();
    let plan = futures::executor::block_on(df2pipeline::get_execution_plan_from_sql(&ctx, sql));
    let mut pipeline = df2pipeline(plan);

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
