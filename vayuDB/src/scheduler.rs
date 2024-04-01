use vayu::pipeline;

use crate::dummy_tasks::test_hash_join;
use std::task::Poll;
pub struct Scheduler {
    next_task: usize,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler { next_task: 0 }
    }
    pub fn get_task(&mut self) -> Poll<vayu_common::DatafusionPipelineWithSource> {
        let mut task = futures::executor::block_on(test_hash_join()).unwrap();
        let pipeline = task.pipelines.remove(self.next_task);
        self.next_task = (self.next_task + 1) % 2;
        Poll::Ready(pipeline)
    }
}
