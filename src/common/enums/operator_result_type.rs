use datafusion::arrow::array::RecordBatch;
use std::sync::Arc;

pub enum OperatorResultType {
    // Have more output for the current batch
    HaveMoreOutput(Arc<RecordBatch>),
    // Finished for the current batch
    Finished(Arc<RecordBatch>),
}

pub enum SourceResultType {
    HaveMoreOutput(Arc<RecordBatch>),
    Finished,
}

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}
