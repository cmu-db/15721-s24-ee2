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
        println!("submit_request");
        let context: Arc<datafusion::execution::TaskContext> = SessionContext::new().task_ctx();
        let stream = source.execute(0, context).unwrap();
        self.stream = Some(stream);
        self.uuid = 1;
        self.uuid
    }
    pub fn poll_response(&mut self) -> Poll<(i32, Option<RecordBatch>)> {
        // println!("poll_response");

        if self.stream.is_none() {
            return Poll::Pending;
        }
        println!("poll_response2");
        // let stream = self.stream.take();

        let data = futures::executor::block_on(self.stream.as_mut().unwrap().next());

        match data {
            Some(data) => {
                let data = data.unwrap();
                println!("DATA in poll response size {}", data.num_rows());
                Poll::Ready((self.uuid, Some(data)))
            }
            None => {
                println!("setting stream to null because got no data from this stream");
                self.stream = None;
                Poll::Ready((self.uuid, None))
            }
        }
    }
}
