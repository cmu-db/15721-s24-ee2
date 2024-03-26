use crate::pipeline::IntermediateOperator;
use crate::pipeline::Pipeline;
use arrow::array::RecordBatch;
use arrow::error::Result;
use futures::StreamExt;
pub struct PipelineExecutor {
    pipeline: Pipeline,
}

impl PipelineExecutor {
    pub fn new(pipeline: Pipeline) -> Self {
        PipelineExecutor { pipeline }
    }
    pub fn execute(&mut self) -> Result<Vec<RecordBatch>> {
        let mut results: Vec<RecordBatch> = vec![];
        if self.pipeline.source.is_none() {
            panic!("no source");
        }
        let source = self.pipeline.source.as_mut().unwrap();

        println!("source is present");
        loop {
            // read from source until finished.
            // todo: each source record batch can be processed in seperate thread.
            let data = futures::executor::block_on(source.next());
            match data {
                Some(data) => {
                    let output =
                        Self::execute_push_internal(&mut self.pipeline.operators, data.unwrap());
                    results.push(output)
                }
                // no data left to be processed
                None => break,
            }
        }
        Ok(results)
    }
    /**
     * takes a record batch and passes it through all the operators
     * and returns the final  record batch. synchronous code. faster.
     * no operator can be blocked (for now).
     */
    fn execute_push_internal(
        operators: &mut Vec<Box<dyn IntermediateOperator>>,
        mut data: RecordBatch,
    ) -> RecordBatch {
        for mut x in operators {
            println!(
                "running operator {} size {}x{}",
                x.name(),
                data.num_rows(),
                data.num_columns()
            );
            data = x.execute(&data).unwrap();
        }
        data
    }
}
