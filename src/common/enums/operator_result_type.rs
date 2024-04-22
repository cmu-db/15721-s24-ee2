use datafusion::arrow::array::RecordBatch;
use std::sync::Arc;

pub enum OperatorResultType {
    HaveMoreOutput(Arc<RecordBatch>),
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