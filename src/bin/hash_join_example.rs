use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::create_physical_expr;
use ee2::operator::filter::FilterOperator;
use ee2::operator::hash_join::{HashJoinBuildOperator, HashJoinProbeOperator, JoinLeftData};
use ee2::operator::physical_batch_collector::PhysicalBatchCollector;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{IntermediateOperator, Sink, Source};
use std::sync::Arc;

fn main() {
    //define schema of table to read
    let schema = Schema::new(vec![
        Field::new("student_id", DataType::Int32, false),
        Field::new("student_name", DataType::Utf8, false),
    ]);

    //create scan operator with the schema
    //and file path
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema.clone()),
        "data/students.csv",
    )));

    //create an expression that keeps only the rows with student_id <= 3
    let expr = col("student_id").lt_eq(lit(3));
    let physical_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    //create filter operator with the above expression student_id <= 3
    let filter = Box::new(FilterOperator::new(physical_expr));

    //create the join condition
    // we will join based on student_id
    let expr = col("student_id");
    let join_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let v = vec![join_expr];
    //create the join build operator
    let sink: Option<Box<dyn Sink>> =
        Some(Box::new(HashJoinBuildOperator::new(v, Arc::new(schema))));

    //now create the first pipeline with
    // source operator -> Scan
    // Intermediate operator ->Filter
    // Sink operator -> HashJoinBuild
    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    pipeline.operators.push(filter);
    pipeline.sink_operator = sink;
    pipeline.execute();

    let build = pipeline.sink_operator.take().unwrap();
    let build: HashJoinBuildOperator = build
        .as_any()
        .downcast_ref::<HashJoinBuildOperator>()
        .unwrap()
        .to_owned();

    //create a second pipeline by scanning the data from another file and performing the probe
    //define schema of table to read
    let schema = Schema::new(vec![
        Field::new("s_id", DataType::Int32, false),
        Field::new("class", DataType::Utf8, false),
    ]);

    //create scan operator with the above schema
    //and file path
    let scan2: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema.clone()),
        "data/classes.csv",
    )));

    //create the expression to probe
    // we are going to probe based on the s_id
    let expr = col("s_id");
    let join_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let v = vec![join_expr];
    let build_data = JoinLeftData {
        hash_table: build.hash_table,
        originals: build.originals,
        schema: build.schema,
    };
    //create the HashJoinProbe operator
    let probe: Box<dyn IntermediateOperator> =
        Box::new(HashJoinProbeOperator::new(v, Arc::new(schema), build_data));

    //create dummy sink printing operator
    let sink: Option<Box<dyn Sink>> = Some(Box::new(PhysicalBatchCollector::new()));

    //create the second pipeline
    // Source -> Scan
    // Operator -> HashJoinBuild
    // Sink -> Dummy Sink (just printing)
    let mut pipeline2 = Pipeline::new();
    pipeline2.source_operator = scan2;
    pipeline2.operators.push(probe);
    pipeline2.sink_operator = sink;
    pipeline2.execute();
}
