use crate::pipeline::{PhysicalOperator, Source};
use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::StreamExt;

use tokio::task;
pub struct ScanOperator {
    stream: SendableRecordBatchStream,
}
impl ScanOperator {
    pub fn new(stream: SendableRecordBatchStream) -> ScanOperator {
        ScanOperator { stream }
    }
}

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
