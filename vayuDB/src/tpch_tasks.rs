use crate::dummy_tasks::get_execution_plan_from_sql;
use datafusion::common::exec_err;
use datafusion::common::plan_err;
use datafusion::common::Result;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use datafusion_benchmarks::tpch::RunOpt;
use datafusion_benchmarks::CommonOpt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::exit;
use vayu::df2vayu;
use vayu::operators::aggregate::AggregateOperator;
use vayu_common::DatafusionPipeline;
use vayu_common::SchedulerPipeline;
use vayu_common::SchedulerSinkType;
use vayu_common::Task;
fn get_tpch_data_path() -> Result<String> {
    let path = std::env::var("TPCH_DATA").unwrap_or_else(|_| "benchmarks/data".to_string());
    if !Path::new(&path).exists() {
        return exec_err!(
            "Benchmark data not found (set TPCH_DATA env var to override): {}",
            path
        );
    }
    Ok(path)
}
// export TPCH_DATA=/Users/kothari/Desktop/course/15721/15721-s24-ee2/arrow-datafusion/benchmarks/data/tpch_sf1

/// Get the SQL statements from the specified query file
pub fn get_query_sql(query: usize) -> Result<Vec<String>> {
    if query > 0 && query < 23 {
        let possibilities = vec![
            format!("queries/q{query}.sql"),
            format!("/Users/kothari/Desktop/course/15721/15721-s24-ee2/arrow-datafusion/benchmarks/queries/q{query}.sql"),
        ];
        let mut errors = vec![];
        for filename in possibilities {
            match fs::read_to_string(&filename) {
                Ok(contents) => {
                    return Ok(contents
                        .split(';')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect());
                }
                Err(e) => errors.push(format!("{filename}: {e}")),
            };
        }
        plan_err!("invalid query. Could not find query: {:?}", errors)
    } else {
        plan_err!("invalid query. Expected value between 1 and 22")
    }
}

pub async fn test_tpchq1() -> Result<Task> {
    let ctx = SessionContext::default();
    let path = get_tpch_data_path()?;
    let common = CommonOpt {
        iterations: 1,
        partitions: Some(2),
        batch_size: 81920,
        debug: false,
    };
    let opt = RunOpt {
        query: Some(1),
        common,
        path: PathBuf::from(path.to_string()),
        file_format: "parquet".to_string(),
        mem_table: false,
        output_path: None,
        disable_statistics: false,
    };
    opt.register_tables(&ctx).await.unwrap();
    let queries = get_query_sql(1).unwrap();
    // println!("{:?}", queries);
    let sql = queries.get(0).unwrap();

    let plan = get_execution_plan_from_sql(&ctx, sql).await.unwrap();
    let final_aggregate = plan.clone();
    // let final_aggregate = AggregateOperator::new(final_aggregate);

    let plan = plan.children().get(0).unwrap().clone();
    println!(
        "=== Physical plan ===\n{}\n",
        displayable(plan.as_ref()).indent(true)
    );
    let source = Some(df2vayu::get_source_node(plan.clone()));
    let mut task = Task::new();

    let uuid = 55;
    let pipeline = DatafusionPipeline {
        plan,
        sink: Some(SchedulerSinkType::StoreRecordBatch(uuid)),
        id: 1,
    };

    let finalize = vayu_common::FinalizeSinkType::FinalAggregate(final_aggregate, uuid);
    let pipeline = SchedulerPipeline {
        source,
        pipeline,
        finalize,
    };

    task.add_pipeline(pipeline);

    return Ok(task);
}
