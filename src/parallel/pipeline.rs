use crate::common::enums::operator_result_type::{OperatorResultType, SinkResultType, SourceResultType};
use crate::physical_operator::{IntermediateOperator, Sink, Source};
use datafusion::arrow::array::RecordBatch;
use std::sync::Arc;

pub struct Pipeline {
    pub source_operator: Option<Box<dyn Source>>,
    pub sink_operator: Option<Box<dyn Sink>>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
}

impl Pipeline {
    pub fn new() -> Pipeline {
        Pipeline {
            source_operator: None,
            sink_operator: None,
            operators: vec![],
        }
    }

    pub fn execute(&mut self) -> () {
        struct StackEntry {
            index: usize,
            input: Arc<RecordBatch>,
        }

        let mut stack: Vec<StackEntry> = Vec::new();

        loop {
            let source_operator = self.source_operator.as_mut().unwrap();
            let source_result = source_operator.get_data();
            match source_result {
                SourceResultType::HaveMoreOutput(batch) => {
                    stack.push(StackEntry {
                        index: 0,
                        input: batch
                    });
                }
                SourceResultType::Finished => {
                    break;
                }
            }

            loop {
                if stack.is_empty() {
                    break;
                }

                let StackEntry{ index, input } = stack.pop().unwrap();
                if index >= self.operators.len() {
                    let sink = self.sink_operator.as_mut().unwrap();
                    let res = sink.sink(&input);
                    match res {
                        SinkResultType::NeedMoreInput => {}
                        SinkResultType::Finished => {return;}
                    }
                } else {
                    let op = self.operators[index].as_mut();
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
    }
}
