pub enum TaskExecutionResult {
    TaskFinished,
    TaskNotFinished,
    TaskError,
    TaskBlocked,
}

pub trait Task {
    fn execute(&self) -> TaskExecutionResult;
}
