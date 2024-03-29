use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use std::marker::Send;
use std::task::Poll;
pub struct IOService {
    stream: Option<SendableRecordBatchStream>,
}

impl IOService {
    pub fn new() -> Self {
        Self { stream: None }
    }
    pub fn submit_request(&mut self, stream: SendableRecordBatchStream) -> i32 {
        self.stream = Some(stream);
        1
    }
    pub fn poll_response(&mut self) -> Poll<(i32, RecordBatch)> {
        let stream = self.stream.take();

        let data = futures::executor::block_on(stream.unwrap().next())
            .unwrap()
            .unwrap();
        Poll::Ready((1, data))
    }
}
