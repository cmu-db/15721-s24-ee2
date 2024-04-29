use std::collections::HashMap;
use std::time::Instant;
use datafusion::arrow::util::pretty;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use ee2::logical_to_physical;
use ee2::operator::hash_aggregate::HashAggregateOperator;
use ee2::operator::physical_batch_collector::PhysicalBatchCollector;

#[tokio::main]
async fn main() {

    let mut tables = HashMap::new();
    tables.insert("customer".to_string(), "data/tpch/customer.parquet".to_string());
    tables.insert("lineitem".to_string(), "data/tpch/lineitem.parquet".to_string());
    tables.insert("nation".to_string(), "data/tpch/nation.parquet".to_string());
    tables.insert("orders".to_string(), "data/tpch/orders.parquet".to_string());
    // ctx.register_parquet("part", "data/tpch/part.parquet", ParquetReadOptions::default()).await.unwrap();
    // ctx.register_parquet("partsupp", "data/tpch/partsupp.parquet", ParquetReadOptions::default()).await.unwrap();
    // ctx.register_parquet("region", "data/tpch/region.parquet", ParquetReadOptions::default()).await.unwrap();
    // ctx.register_parquet("supplier", "data/tpch/supplier.parquet", ParquetReadOptions::default()).await.unwrap();

    let ctx = SessionContext::new();
    ctx.register_parquet("customer", "data/tpch/customer.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("lineitem", "data/tpch/lineitem.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("nation", "data/tpch/nation.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("orders", "data/tpch/orders.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("part", "data/tpch/part.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("partsupp", "data/tpch/partsupp.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("region", "data/tpch/region.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("supplier", "data/tpch/supplier.parquet", ParquetReadOptions::default()).await.unwrap();

    // let mut sql = String::new();
    // print!(">>> ");

    let sql = String::from("Select sum(l_quantity) from lineitem;");
    let res = ctx.sql(sql.as_str()).await;
    let df;
    match res{
        Ok(dataframe) => {df = dataframe}
        Err(_) => {panic!("Error occurred. Try again");}
    }
    let plan = df.into_unoptimized_plan();
    println!("Optimized logical plan : {:?}", plan);

    let mut logical_to_physical= logical_to_physical::LogicalToPhysical::new(tables.clone());
    logical_to_physical.create_physical(&plan);
    let mut pipelines = logical_to_physical.pipelines;
    let mut pipeline = pipelines.pop().unwrap();
    match pipeline.sink_operator {
        None => {
            pipeline.sink_operator = Some(Box::new(PhysicalBatchCollector::new()));
        }
        Some(_) => {}
    }


    let start = Instant::now();
    pipeline.execute();
    let duration = start.elapsed();

    //print results
    if(pipeline.sink_operator.is_none()){
        let collector = pipeline.sink_operator.take().unwrap();
        let collector = collector
            .as_any()
            .downcast_ref::<PhysicalBatchCollector>()
            .unwrap();
        collector.print();
    }
    else{
        let mut agg = pipeline.sink_operator.take().unwrap();
        let mut agg = agg
            .as_any()
            .downcast_ref::<HashAggregateOperator>()
            .unwrap();

        pretty::print_batches(std::slice::from_ref(agg.aggregated_data.data.as_ref().unwrap()));
    }


    println!("Duration of query is {:?}", duration);

}
