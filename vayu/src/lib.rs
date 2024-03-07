use core::panic;

use arrow::array::RecordBatch;
use arrow::error::Result;
use pipeline::Pipeline;
pub mod operators;
pub mod pipeline;
pub fn execute(pipeline: Pipeline) -> Result<Vec<RecordBatch>> {
    // panic if no source operator
    if pipeline.source_operator.is_none() {
        panic!("no source operator");
    }

    let mut source = pipeline.source_operator.unwrap();

    println!(
        "source is {} operators len is {}",
        source.name(),
        pipeline.operators.len()
    );

    let mut result: Vec<RecordBatch> = vec![];
    let ref_pipeline = &*pipeline.operators;
    loop {
        let data1 = source.get_data();
        if data1.is_none() {
            break;
        }
        let mut data = data1.unwrap();
        for x in ref_pipeline {
            println!("running operator {}", x.name());
            data = x.execute(&data).unwrap();
        }
        result.push(data);
    }
    Ok(result)
}
