use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::displayable;

use datafusion::prelude::*;
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();
    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("./testing/data/csv/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;
    // sql query
    let sql = "SELECT c1,c12  FROM aggregate_test_100 WHERE c12 < 0.3 AND c1='b'";
    // create datafusion logical plan
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql).await?;
    // create datafusion physical plan (trait)
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan).await?;
    println!(
        "Detailed physical plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );

    // let stream = plan.execute(0, Arc::new(TaskContext::from(&ctx)));
    // execute the pipeline
    // let results = vayu::execute(stream.unwrap()).unwrap();
    let results = vayu::execute(plan).unwrap();
    pretty::print_batches(&results)?;
    Ok(())
}

// // get results from datfusion execution engine
// let results = cmu_execution::execute_physical_plan(plan.clone(), ctx).await?;

// println!(
//     "physical plan:\n {}",
//     displayable(plan.as_ref()).indent(true)
// );
