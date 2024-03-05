use crate::common::enums::operator_result_type::{OperatorResultType, SourceResultType};
use crate::physical_operator::{IntermediateOperator, PhysicalOperator, Sink, Source};
use datafusion::arrow::array::RecordBatch;
use std::rc::Rc;

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
        struct temp {
            index: usize,
            input: Rc<RecordBatch>,
        }

        let mut stack: Vec<temp> = Vec::new();

        loop {
            let mut source_operator = self.source_operator.as_mut().unwrap();
            let mut input_batch = RecordBatch::new_empty(source_operator.schema());
            let source_result = source_operator.get_data(&mut input_batch);

            match source_result {
                SourceResultType::HaveMoreOutput => {}
                SourceResultType::Finished => {
                    break;
                }
            }
            stack.push(temp {
                index: 0,
                input: Rc::new(input_batch),
            });

            loop {
                if stack.is_empty() {
                    break;
                }

                let temp { index, input } = stack.pop().unwrap();
                if index >= self.operators.len() {
                    let sink = self.sink_operator.as_ref().unwrap();
                    let sink_result = sink.sink(&*input);
                } else {
                    let op = &self.operators[index];
                    let mut output_batch = Rc::new(RecordBatch::new_empty(op.schema()));
                    let intermediate_result = op.execute(&*input, &mut output_batch);

                    match intermediate_result {
                        OperatorResultType::HaveMoreOutput => stack.push(temp { index, input }),
                        OperatorResultType::Finished => {}
                    }
                    stack.push(temp {
                        index: index + 1,
                        input: output_batch,
                    })
                }
            }
        }
    }
}
