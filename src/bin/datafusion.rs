use std::io;
use std::time::Instant;
use datafusion::physical_plan::accept;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use ee2::helper;
use ee2::operator::physical_batch_collector::PhysicalBatchCollector;
use std::io::Write;
use datafusion::arrow::util::pretty;
use ee2::operator::hash_aggregate::HashAggregateOperator;
use ee2::operator::sort::SortOperator;

#[tokio::main]
async fn main() {

    let ctx = SessionContext::new();
    ctx.register_parquet("customer", "data/tpch/customer.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("lineitem", "data/tpch/lineitem.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("nation", "data/tpch/nation.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("orders", "data/tpch/orders.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("part", "data/tpch/part.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("partsupp", "data/tpch/partsupp.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("region", "data/tpch/region.parquet", ParquetReadOptions::default()).await.unwrap();
    ctx.register_parquet("supplier", "data/tpch/supplier.parquet", ParquetReadOptions::default()).await.unwrap();

    loop {
        let mut sql = String::new();
        print!(">>> ");
        io::stdout().flush().unwrap();
        let _ = std::io::stdin().read_line(&mut sql).unwrap();

        let res = ctx.sql(sql.as_str()).await;
        let df;
        match res{
            Ok(dataframe) => {df = dataframe}
            Err(_) => {println!("Error occurred. Try again"); continue;}
        }
        let plan = df.create_physical_plan().await.unwrap();
        println!("Physical plan : {:#?}", plan);

        let mut visitor = helper::PhysicalToPhysicalVisitor::new();
        let _ = accept(plan.as_ref(), &mut visitor);

        let mut pipeline = visitor.pipeline;
        match pipeline.sink_operator {
            None => {
                pipeline.sink_operator = Some(Box::new(PhysicalBatchCollector::new()));
            }
            Some(_) => {}
        }

        //run the query
        let start = Instant::now();
        pipeline.execute();
        let duration = start.elapsed();

        let sink = pipeline.sink_operator.take().unwrap();

        if let Some(sort) = sink.as_any().downcast_ref::<SortOperator>(){
            let data = &sort.sorted_data.data;
            match data {
                None => {}
                Some(sorted_data) => {
                    let _ = pretty::print_batches(std::slice::from_ref(sorted_data));
                }
            }
        }
        else if let Some(collector) = sink.as_any().downcast_ref::<PhysicalBatchCollector>(){
                collector.print();
        }
        else if let Some(aggregate) = sink.as_any().downcast_ref::<HashAggregateOperator>(){
            let grouped_data = &aggregate.aggregated_data;
            let _ = pretty::print_batches(std::slice::from_ref(grouped_data.data.as_ref().unwrap()));
            //todo group by
        }
        else {
            panic!("not implemented")
        }

        println!("Duration of query is {:?}", duration);
    }

}