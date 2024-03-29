# Vayu

## Introduction 

This crates has basic implementation of a database using vayu execution engine. 

## Common Vayu Structures 
```
pub enum SchedulerSinkType {
    PrintOutput,
}
pub struct VayuPipeline {
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub sink: Option<SchedulerSinkType>,
}
pub struct Pipeline {
    pub source: Option<SendableRecordBatchStream>,
    pub vayu_pipeline: VayuPipeline,
}

pub struct Task {
    pub pipelines: Vec<Pipeline>,
}
```


## How to add a custom scheduler/io_engine ? 
1. Scheduler
```
pub fn new() -> Self;
pub fn get_task(&mut self) -> Poll<vayu_common::Task>;
```
2. IO
    IO takes an SendableRecordBatchStream as an input and returns RecordBatch.
    Note: for now each source operator has to return data in one Record batch only. If polling again returns Some(RecordBatch), database would panic.
    
```
pub fn new() -> Self;
pub fn submit_request(&mut self, stream: SendableRecordBatchStream) -> i32;
pub fn poll_response(&mut self) -> Poll<(i32, RecordBatch)>;
```