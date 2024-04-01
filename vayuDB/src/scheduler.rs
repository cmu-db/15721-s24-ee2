use crate::dummy_tasks::{test_filter_project, test_hash_join};
use std::task::Poll;
pub struct Scheduler {
    next_pipeline: usize,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler { next_pipeline: 0 }
    }
    pub fn get_pipeline(&mut self) -> Poll<vayu_common::DatafusionPipelineWithSource> {
        if true {
            let mut task = futures::executor::block_on(test_filter_project()).unwrap();
            let pipeline = task.pipelines.remove(0);
            Poll::Ready(pipeline)
        } else {
            let mut task = futures::executor::block_on(test_hash_join()).unwrap();
            let pipeline = task.pipelines.remove(self.next_pipeline);
            self.next_pipeline = 1 - self.next_pipeline;
            Poll::Ready(pipeline)
        }
    }
}
