use std::fmt::format;
use std::fs::read_to_string;
use datafusion::physical_plan::accept;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use ee2::helper;
use ee2::operator::physical_batch_collector::PhysicalBatchCollector;
use ee2::physical_operator::Sink;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() {
    let config = SessionConfig::new()
        .set(
            "datafusion.execution.target_partitions",
            ScalarValue::UInt64(Some(1)),
        )
        .set_bool("datafusion.execution.coalesce_batches", false)
        .set_bool("datafusion.optimizer.repartition_aggregations", false)
        .set_bool("datafusion.optimizer.repartition_joins", false);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet(
        "customer",
        "data/tpch/customer.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "lineitem",
        "data/tpch/lineitem.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "nation",
        "data/tpch/nation.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "orders",
        "data/tpch/orders.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "part",
        "data/tpch/part.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "partsupp",
        "data/tpch/partsupp.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "region",
        "data/tpch/region.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "supplier",
        "data/tpch/supplier.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    loop {
        let mut sql = String::new();
        print!(">>> ");
        io::stdout().flush().unwrap();
        let _ = std::io::stdin().read_line(&mut sql).unwrap();

        //remove whitespace
        let sql = String::from(sql.trim_start());
        let sql = String::from(sql.trim_end());
        let sql = String::from(sql.trim_end_matches('\n'));
        let mut sql = String::from(sql.trim_end_matches(';'));

        // native mode
        let mut native_mode = false;
        if sql.as_str().starts_with("native ") {
            native_mode = true;
            sql = String::from(&sql[7..]);
            sql = String::from(sql.trim_start());
        }

        if sql.as_str().starts_with("tpch"){
            sql = execute_tpch_query(sql) ;
        }

        let res = ctx.sql(sql.as_str()).await;
        let df;
        match res {
            Ok(dataframe) => df = dataframe,
            Err(_) => {
                println!("Error occurred. Try again");
                continue;
            }
        }
        let plan = df.create_physical_plan().await.unwrap();
        // println!("Physical plan : {:#?}", plan);

        if native_mode {
            let start = Instant::now();
            let result = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap();
            let duration = start.elapsed();
            let mut printer = PhysicalBatchCollector::new();
            for batch in result {
                printer.sink(&Arc::new(batch));
            }
            printer.print();
            println!("Duration of query is {:?} (datafusion native mode)", duration);
            continue;
        }

        let mut visitor = helper::PhysicalToPhysicalVisitor::new();
        let _ = accept(plan.as_ref(), &mut visitor);

        let pipelines = visitor.pipelines;
        let mut pipeline_number = 0;
        let mut total_duration: Duration = Duration::new(0, 0);
        for mut pipeline in pipelines {
            //println!("{:?}",physical_operator_to_string(&pipeline.source_operator.unwrap().get_type()));
            //println!("{:?}",physical_operator_to_string(&pipeline.sink_operator.unwrap().get_type()));
            //continue;

            if pipeline.sink_operator.is_none() {
                pipeline.sink_operator = Some(Box::new(PhysicalBatchCollector::new()));
            }

            //run the query
            let start = Instant::now();
            pipeline.execute(pipeline_number, Arc::clone(&visitor.store));
            let duration = start.elapsed();
            total_duration += duration;

            let sink = pipeline.sink_operator.take().unwrap();

            if let Some(collector) = sink.as_any().downcast_ref::<PhysicalBatchCollector>() {
                collector.print();
            }
            pipeline_number += 1;
        }
        println!("Duration of query is {:?}", total_duration);
    }
}

fn read_lines(filename: &str) -> String {
    let mut result = Vec::new();
    for line in read_to_string(filename).unwrap().lines() {
        let mut str = String::from(line);
        if str.ends_with("\n"){
            str.pop();
        }
        str.push(' ');
        result.push(str);
    }
    let mut query = String::new();
    for line in result{
        query+=line.as_str();
    }

    query
}

fn execute_tpch_query(str : String) -> String {
    read_lines(format!("data/tpch/queries/{}.sql",str.as_str()).as_str())
}
