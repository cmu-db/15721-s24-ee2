use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::{create_physical_expr, PhysicalSortExpr};
use ee2::operator::filter::FilterOperator;
use ee2::operator::hash_join::{HashJoinBuildOperator, HashJoinProbeOperator, JoinLeftData};
use ee2::operator::scan::ScanOperator;
use ee2::operator::sort::SortOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{IntermediateOperator, Sink, Source};
use std::sync::Arc;

fn main() {
    //define schema of table to read
    let schema1 = Schema::new(vec![
        Field::new("student_id", DataType::Int32, false),
        Field::new("student_name", DataType::Utf8, false),
    ]);

    //create scan operator with the schema
    //and file path
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema1.clone()),
        "data/students.csv",
    )));

    //create an expression that keeps only the rows with student_id <= 3
    let expr = col("student_id").lt_eq(lit(3));
    let physical_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema1.clone()).unwrap(),
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
        &DFSchema::try_from(schema1.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let v = vec![join_expr];
    //create the join build operator
    let sink: Option<Box<dyn Sink>> = Some(Box::new(HashJoinBuildOperator::new(
        v,
        Arc::new(schema1.clone()),
    )));

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
    let schema2 = Schema::new(vec![
        Field::new("s_id", DataType::Int32, false),
        Field::new("class", DataType::Utf8, false),
    ]);

    //create scan operator with the above schema
    //and file path
    let scan2: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema2.clone()),
        "data/more_classes.csv",
    )));

    //create the expression to probe
    // we are going to probe based on the s_id
    let expr = col("s_id");
    let join_expr = create_physical_expr(
        &expr,
        &DFSchema::try_from(schema2.clone()).unwrap(),
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
    let probe: Box<dyn IntermediateOperator> = Box::new(HashJoinProbeOperator::new(
        v,
        Arc::new(schema2.clone()),
        build_data,
    ));

    let merged_schema = Schema::try_merge(vec![schema1, schema2]).unwrap();
    let col_ref = col("class");
    let col_ref = create_physical_expr(
        &col_ref,
        &DFSchema::try_from(merged_schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let col_ref2 = col("student_name");
    let col_ref2 = create_physical_expr(
        &col_ref2,
        &DFSchema::try_from(merged_schema.clone()).unwrap(),
        &ExecutionProps::new(),
    )
    .unwrap();

    let expr: PhysicalSortExpr = PhysicalSortExpr {
        expr: col_ref,
        options: SortOptions {
            descending: false,
            nulls_first: false,
        },
    };

    let expr2: PhysicalSortExpr = PhysicalSortExpr {
        expr: col_ref2,
        options: SortOptions {
            descending: false,
            nulls_first: false,
        },
    };
    //create sort sink
    let sink: Option<Box<dyn Sink>> = Some(Box::new(SortOperator::new(vec![expr2, expr])));

    //create the second pipeline
    // Source -> Scan
    // Operator -> HashJoinBuild
    // Sink -> Limit
    let mut pipeline2 = Pipeline::new();
    pipeline2.source_operator = scan2;
    pipeline2.operators.push(probe);
    pipeline2.sink_operator = sink;
    pipeline2.execute();

    let sort_operator = pipeline2.sink_operator.take().unwrap();
    let sort_opeartor = sort_operator
        .as_any()
        .downcast_ref::<SortOperator>()
        .unwrap();
    let data = &sort_opeartor.sorted_data.data;
    match data {
        None => {}
        Some(sorted_data) => {
            pretty::print_batches(std::slice::from_ref(sorted_data)).unwrap();
        }
    }
}
