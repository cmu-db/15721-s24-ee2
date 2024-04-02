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
        if self.next_pipeline % 3 == 0 {
            // filter and project
            self.next_pipeline += 1;
            let mut task = futures::executor::block_on(test_filter_project()).unwrap();
            let pipeline = task.pipelines.remove(0);
            return Poll::Ready(pipeline);
        } else if self.next_pipeline % 3 == 1 {
            // hash build
            self.next_pipeline += 1;
            let mut task = futures::executor::block_on(test_hash_join()).unwrap();
            let pipeline = task.pipelines.remove(0);
            return Poll::Ready(pipeline);
        } else if self.next_pipeline % 3 == 2 {
            // hash probe
            self.next_pipeline += 1;
            let mut task = futures::executor::block_on(test_hash_join()).unwrap();
            let pipeline = task.pipelines.remove(1);
            return Poll::Ready(pipeline);
        } else {
            panic!("magic its magic")
        }
    }
}
