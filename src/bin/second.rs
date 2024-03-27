use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{col,lit};
use datafusion::physical_expr::create_physical_expr;
use ee2::operator::dummy_sink::DummySinkOperator;
use ee2::operator::hash_join::{HashJoinBuildOperator, HashJoinProbeOperator, JoinLeftData};
use ee2::operator::filter::FilterOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::pipeline::Pipeline;
use ee2::physical_operator::{IntermediateOperator, Sink, Source};
use std::sync::Arc;

fn main() {
    //define schema of table to read 
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, true)
    ]);

    //create scan operator with the above schema 
    //and file path
    let scan: Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema.clone()),
        "data/data.csv",
    )));


    //create an expression that keeps only the rows with id < 5
    let expr = col("id").lt_eq(lit(5));
    let physical_expr = create_physical_expr(
        &expr, 
        &DFSchema::try_from(schema.clone()).unwrap(), 
        &ExecutionProps::new()
    ).unwrap();

    //create filter operator with the above expression
    let filter = Box::new(FilterOperator::new(
        physical_expr
    ));

    //create dummy sink printing operator
    // let sink: Option<Box<dyn Sink>> = Some(Box::new(DummySinkOperator::new()));
    
    let expr = col("id");
    let join_expr = create_physical_expr(
        &expr, 
        &DFSchema::try_from(schema.clone()).unwrap(), 
        &ExecutionProps::new()
    ).unwrap();

    let v = vec![join_expr];
    let sink : Option<Box<dyn Sink>> = Some(Box::new(HashJoinBuildOperator::new(v, Arc::new(schema))));

    let mut pipeline = Pipeline::new();
    pipeline.source_operator = scan;
    // pipeline.operators.push(project);
    pipeline.operators.push(filter);
    pipeline.sink_operator = sink;
    pipeline.execute();

    let build = pipeline.sink_operator.take().unwrap();
    let build :HashJoinBuildOperator = build.as_any().downcast_ref::<HashJoinBuildOperator>().unwrap().to_owned();
    println!("hash table is {:?}",build.hash_table);


    //create a second pipeline by scanning the data from another file and performing the probe

    //define schema of table to read 
    let schema = Schema::new(vec![
        Field::new("n_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false)
    ]);

    //create scan operator with the above schema 
    //and file path
    let scan2 : Option<Box<dyn Source>> = Some(Box::new(ScanOperator::new(
        Arc::new(schema.clone()),
        "data/data2.csv",
    )));


    let expr = col("n_id");
    let join_expr = create_physical_expr(
        &expr, 
        &DFSchema::try_from(schema.clone()).unwrap(), 
        &ExecutionProps::new()
    ).unwrap();

    let v = vec![join_expr];
    let build_data = JoinLeftData{
        hash_table: build.hash_table, originals: build.originals, schema: build.schema
    };
    let probe : Box<dyn IntermediateOperator> = Box::new(HashJoinProbeOperator::new(v, Arc::new(schema), build_data));

    //create dummy sink printing operator
    let sink: Option<Box<dyn Sink>> = Some(Box::new(DummySinkOperator::new()));

    let mut pipeline2 = Pipeline::new();
    pipeline2.source_operator = scan2;
    pipeline2.operators.push(probe);
    pipeline2.sink_operator = sink;
    pipeline2.execute();

    println!("We made it :) ");
}
