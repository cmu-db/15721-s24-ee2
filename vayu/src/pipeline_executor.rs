use arrow::array::RecordBatch;
use arrow::error::Result;
use futures::StreamExt;
use vayu_common::IntermediateOperator;
use vayu_common::Pipeline;
use vayu_common::SchedulerSourceType;
pub struct PipelineExecutor {
    pipeline: Pipeline,
}

impl PipelineExecutor {
    pub fn new(pipeline: Pipeline) -> Self {
        PipelineExecutor { pipeline }
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
        for x in operators {
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
