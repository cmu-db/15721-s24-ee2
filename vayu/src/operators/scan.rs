use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::StreamExt;
use tokio::task;
use vayu_common::{PhysicalOperator, Source};
pub struct ScanOperator {
    stream: SendableRecordBatchStream,
}
impl ScanOperator {
    pub fn new(stream: SendableRecordBatchStream) -> ScanOperator {
        ScanOperator { stream }
    }
}
// right now scan operator is blocking by design
// i don't wish to pass the execution access to tokio runtime.
// eventually we would have the main process call async functions to get data and then this data would
// be passed to the worker who would process the data parallely without any runtime.
// basically scan operator would not be called on worker thread which are only for CPU tasks
// zero context switches let's goooo!!!!1
impl Source for ScanOperator {
    fn get_data(&mut self) -> Option<RecordBatch> {
        let block = task::block_in_place(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(self.stream.next())
        });
        let t = block.transpose();
        t.unwrap()
    }
}

impl PhysicalOperator for ScanOperator {
    fn name(&self) -> String {
        String::from("scan")
    }
}
