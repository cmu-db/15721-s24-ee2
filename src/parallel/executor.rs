use crate::common::constants::PendingExecutionResult;
use crate::parallel::pipeline::Pipeline;
use crate::parallel::task::Task;
use crate::physical_operator::Sink;
use std::rc::Rc;

pub struct Executor {
    //this one might need to be changed
    pub physical_plan: Option<Box<dyn Sink>>,

    //we might need thread safe and interior mutability
    pub pipelines: Vec<Rc<Pipeline>>,

    //the task to process if any
    pub task: Option<Rc<dyn Task>>,
}
impl Executor {
    pub fn execute_task(&mut self) -> PendingExecutionResult {
        // if let Some(current_task) = &self.task{
        //     let result = current_task.execute();
        // }

        PendingExecutionResult::Blocked
    }
}
