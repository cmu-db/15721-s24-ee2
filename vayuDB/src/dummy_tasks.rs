use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::CsvReadOptions;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use vayu::df2vayu;
use vayu::df2vayu::df2vayu;
use vayu_common::Pipeline;
use vayu_common::Task;

pub async fn scan_filter_project() -> Result<Task> {
    // create local execution context
    let ctx: SessionContext = SessionContext::new();
    // register csv file with the execution context

    ctx.register_csv(
        "aggregate_test_100",
        &format!("./testing/data/csv/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;
    let sql = "SELECT c1,c3 as neg,c4 as pos,c13  FROM aggregate_test_100 WHERE (c3 < 0 AND c1='a') OR ( c4 > 0 AND c1='b' ) ";
    let plan = get_execution_plan_from_sql(&ctx, sql).await?;
    let pipeline = df2vayu::new_from_df(plan, vayu_common::SchedulerSinkType::PrintOutput);
    let mut task = Task::new();
    task.add_pipeline(pipeline);
    return Ok(task);
}

pub async fn get_execution_plan_from_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>> {
    // create datafusion logical plan
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql).await?;
    // create datafusion physical plan
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan).await?;
    // print datafusion physical plan
    println!(
        "Detailed physical plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    // panic!("hello");
    Ok(plan)
}
