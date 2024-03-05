use crate::parallel::pipeline::Pipeline;

pub struct PipelineExecutor {}

impl PipelineExecutor {
    pub fn execute(pipeline: &mut Pipeline) {
        pipeline.execute();
    }
}
