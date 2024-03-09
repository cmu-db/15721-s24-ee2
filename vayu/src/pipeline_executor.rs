use crate::pipeline::Pipeline;
use arrow::array::RecordBatch;
use arrow::error::Result;
use datafusion::physical_plan::{common, SendableRecordBatchStream};
use futures::StreamExt;
use std::task::{Context, Poll};
use tokio::runtime::Runtime; // 0.3.5
pub struct PipelineExecutor {
    pipeline: Pipeline,
}

impl PipelineExecutor {
    pub fn new(pipeline: Pipeline) -> Self {
        PipelineExecutor { pipeline }
    }
    pub fn execute(&mut self) -> Result<Vec<RecordBatch>> {
        let exhausted_source = false;
        let mut results: Vec<RecordBatch> = vec![];
        if self.pipeline.source.is_none() {
            panic!("no source");
        }
        let source = self.pipeline.source.as_mut().unwrap();

        println!("source is present");
        loop {
            let data = futures::executor::block_on(source.next());
            match data {
                Some(block) => results.push(block.unwrap()),
                None => break,
            }
        }
        Ok(results)
    }
}
