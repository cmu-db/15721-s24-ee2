use datafusion::datasource::physical_plan::CsvExec;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use crate::helper::tpch_schema;
use crate::operator::filter::FilterOperator;
use crate::operator::hash_aggregate::HashAggregateOperator;
use crate::operator::scan::ScanOperator;
use crate::parallel::pipeline::Pipeline;
use crate::physical_operator::{Sink, Source};
use datafusion::execution::context::SessionState;
use std::vec;

pub fn df2pipeline(plan: Arc<dyn ExecutionPlan>) -> Pipeline {
    let p = plan.as_any();

    let batch_size = 1024;
    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let context = ctx.task_ctx();

    // set batch size here
    // println!("batch size {context.se}");

    if let Some(_) = p.downcast_ref::<ParquetExec>() {
        let schema = tpch_schema("lineitem");

        //create scan operator with the schema
        let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
            Arc::new(schema.clone()),
            "data/tpch/lineitem.parquet",
        )));
        return Pipeline {
            source_operator: scan,
            operators: vec![],
            sink_operator: None,
        };
    }
    if let Some(exec) = p.downcast_ref::<AggregateExec>() {
        let mut pipeline = df2pipeline(exec.input().clone());
        let g = exec.group_expr();
        let sink: Option<Box<dyn Sink>> = Some(Box::new(HashAggregateOperator::new(
            exec.aggr_expr().to_vec(),
            g.expr().to_vec(),
        )));

        println!("adding aggregate");
        pipeline.sink_operator = sink;
        return pipeline;
    }
    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        let mut pipeline = df2pipeline(exec.input().clone());
        let tt = Box::new(FilterOperator::new(exec.predicate().clone()));
        println!("adding filter");
        pipeline.operators.push(tt);
        return pipeline;
    }
    // if let Some(exec) = p.downcast_ref::<ProjectionExec>() {
    //     let mut pipeline = df2pipeline(exec.input().clone());
    //     println!("adding projection");
    //     let expr = exec.expr().iter().map(|x| x.0.clone()).collect();
    //     let schema = exec.schema().clone();
    //     let tt = Box::new(ProjectionOperator::new(expr, schema));
    //     pipeline.operators.push(tt);
    //     return pipeline;
    // }

    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        return df2pipeline(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        return df2pipeline(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<CoalescePartitionsExec>() {
        return df2pipeline(exec.input().clone());
    }
    panic!("should never reach the end");
}

pub async fn get_execution_plan_from_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Arc<dyn ExecutionPlan> {
    // create datafusion logical plan
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql)
        .await
        .unwrap();
    // create datafusion physical plan
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan)
        .await
        .unwrap();
    // print datafusion physical plan
    println!(
        "Detailed physical plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    // panic!("hello");
    plan
}
