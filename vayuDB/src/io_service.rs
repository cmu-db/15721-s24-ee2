use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::sync::Arc;
use std::task::Poll;
pub struct IOService {
    stream: Option<SendableRecordBatchStream>,
    uuid: i32,
}

impl IOService {
    pub fn new() -> Self {
        Self {
            stream: None,
            uuid: 0,
        }
    }
    pub fn submit_request(&mut self, source: Arc<dyn ExecutionPlan>) -> i32 {
        let context = SessionContext::new().task_ctx();
        let stream = source.execute(0, context).unwrap();
        self.stream = Some(stream);
        self.uuid = 1;
        self.uuid
    }
    pub fn poll_response(&mut self) -> Poll<(i32, RecordBatch)> {
        if self.stream.is_none() {
            return Poll::Pending;
        }
        let stream = self.stream.take();

        let data = futures::executor::block_on(stream.unwrap().next())
            .unwrap()
            .unwrap();
        Poll::Ready((self.uuid, data))
    }
}
