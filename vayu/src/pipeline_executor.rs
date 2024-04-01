use std::sync::Arc;

use crate::pipeline::IntermediateOperator;
use crate::pipeline::Pipeline;
use crate::operators::operator_result_type::*;
use arrow::array::RecordBatch;
use arrow::error::Result;
pub struct PipelineExecutor {
    pipeline: Pipeline,
}

impl PipelineExecutor {
    pub fn new(pipeline: Pipeline) -> Self {
        PipelineExecutor { pipeline }
    }
    pub fn execute(&mut self) -> Result<Vec<RecordBatch>> {
        struct StackEntry {
            index: usize,
            input: Arc<RecordBatch>,
        }

        let mut stack: Vec<StackEntry> = Vec::new();
        let mut results: Vec<RecordBatch> = vec![];

        loop {
            let source_operator = self.pipeline.source_operator.as_mut().unwrap();
            let source_result = source_operator.get_data();
            match source_result {
                SourceResultType::HaveMoreOutput(batch) => {
                    stack.push(StackEntry {
                        index: 0,
                        input: batch
                    });
                }
                SourceResultType::Finished(_) => {
                    break;
                }
            }

            loop {
                if stack.is_empty() {
                    break;
                }

                let StackEntry{ index, input } = stack.pop().unwrap();
                if index >= self.pipeline.operators.len() {
                    let sink = self.pipeline.sink_operator.as_mut().unwrap();
                    sink.sink(&input, &mut results);
                } else {
                    let op = self.pipeline.operators[index].as_mut();
                    let intermediate_result = op.execute(&input);
                    match intermediate_result {
                        OperatorResultType::HaveMoreOutput(batch) => {
                            stack.push(StackEntry { index, input });
                            stack.push(StackEntry { index: index + 1, input: batch});
                        },
                        OperatorResultType::Finished(batch) => {
                            stack.push(StackEntry { index: index + 1, input: batch});
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}
