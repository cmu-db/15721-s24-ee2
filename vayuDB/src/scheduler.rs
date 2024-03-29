use crate::dummy_tasks::scan_filter_project;
use std::task::Poll;
pub struct Scheduler {
    tasks: Vec<vayu_common::Task>,
    next_task: usize,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            tasks: vec![],
            next_task: 0,
        }
    }
    pub fn get_task(&mut self) -> Poll<vayu_common::Task> {
        let t = futures::executor::block_on(scan_filter_project()).unwrap();
        Poll::Ready(t)
    }
}
