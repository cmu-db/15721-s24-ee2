use core::panic;

use arrow::error::Result;
use arrow::{array::RecordBatch, util::pretty};
use pipeline::Pipeline;
pub mod operators;
pub mod pipeline;
pub fn execute(pipeline: Pipeline) -> Result<Vec<RecordBatch>> {
    // pipeline
    println!("source is {}", pipeline.source_operator.is_some());
    println!("operators len is {}", pipeline.operators.len());
    let mut data = if let Some(so) = pipeline.source_operator {
        let data = so.get_data().unwrap();
        println!("Pipeline source {}", so.name());
        data
    } else {
        panic!("no source operator")
    };
    for x in pipeline.operators {
        let mut data1 = x.execute(&data).unwrap();
        println!("{}", x.name());
        data = data1;
    }
    Ok(vec![data])
}
