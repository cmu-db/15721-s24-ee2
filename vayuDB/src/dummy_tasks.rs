use core::panic;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::SessionContext;
use std::path::Path;
use std::sync::Arc;
use vayu::df2vayu;
use vayu::operators::join;
use vayu_common::DatafusionPipelineWithSource;
use vayu_common::Task;

pub async fn test_filter_project_aggregate() -> Result<Task> {
    // create local execution context
    let ctx: SessionContext = SessionContext::new();
    // register csv file with the execution context

    ctx.register_csv(
        "aggregate_test_100",
        &format!("./testing/data/csv/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;
    let sql = "SELECT count(c1),sum(c3),sum(c4),count(c13)  FROM aggregate_test_100 WHERE (c3 < 0 AND c1='a') OR ( c4 > 0 AND c1='b' ) ";
    let plan = get_execution_plan_from_sql(&ctx, sql).await?;
    let source = df2vayu::get_source_node(plan.clone());
    let mut task = Task::new();

    let pipeline = DatafusionPipelineWithSource {
        source,
        plan,
        sink: Some(vayu_common::SchedulerSinkType::PrintOutput),
    };
    task.add_pipeline(pipeline);

    return Ok(task);
}

pub async fn test_hash_join() -> Result<Task> {
    let ctx: SessionContext = SessionContext::new();
    // register csv file with the execution context
    ctx.register_csv(
        "a",
        &format!("./testing/data/csv/join_test_A.csv"),
        CsvReadOptions::new(),
    )
    .await?;
    ctx.register_csv(
        "b",
        &format!("./testing/data/csv/join_test_B.csv"),
        CsvReadOptions::new(),
    )
    .await?;

    // get execution plan from th sql query
    let sql = "SELECT *  FROM a,b WHERE a.a1 = b.b1 ";
    let plan = get_execution_plan_from_sql(&ctx, sql).await?;
    let mut task = Task::new();

    let uuid = 42;
    let (join_node, build_plan) = df2vayu::get_hash_build_pipeline(plan.clone(), uuid);

    let build_source_pipeline = df2vayu::get_source_node(build_plan.clone());
    let sink = vayu_common::SchedulerSinkType::BuildAndStoreHashMap(uuid, join_node);
    let build_pipeline = DatafusionPipelineWithSource {
        source: build_source_pipeline,
        plan: build_plan,
        sink: Some(sink),
    };
    task.add_pipeline(build_pipeline);
    // TODO: set this uuid in probe also
    let probe_plan = plan.clone();
    let probe_source_node = df2vayu::get_source_node(probe_plan.clone());

    let probe_pipeline = DatafusionPipelineWithSource {
        source: probe_source_node,
        plan: probe_plan,
        sink: Some(vayu_common::SchedulerSinkType::PrintOutput),
    };
    task.add_pipeline(probe_pipeline);

    Ok(task)
}

pub async fn get_execution_plan_from_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    // create datafusion logical plan
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql)
        .await
        .unwrap();
    // create datafusion physical plan
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan)
        .await
        .unwrap();
    // print datafusion physical plan
    // println!(
    //     "Detailed physical plan:\n{}",
    //     displayable(plan.as_ref()).indent(true)
    // );
    // panic!("hello");
    Ok(plan)
}
