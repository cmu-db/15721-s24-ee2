pub struct DummySource {
    pub batch: RecordBatch,
}

impl DummySource {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // use functions from futures crate convert the batch into a stream
        let fut = futures::future::ready(Ok(self.batch.clone()));
        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.batch.schema(),
            stream,
        )))
    }
}
