# Vayu

## Introduction 

This crates has basic implementation of a database using vayu execution engine. 

## Common Vayu Structures 
```
pub struct DatafusionPipelineWithSource {
    pub source: Arc<dyn ExecutionPlan>,
    pub plan: Arc<dyn ExecutionPlan>,
    pub sink: SchedulerSinkType,
}
```


## How to add a custom scheduler/io_engine ? 
1. Scheduler
```
pub fn new() -> Self
pub fn get_pipeline(&mut self) -> Poll<vayu_common::DatafusionPipelineWithSource> 
pub fn ack_pipeline(&mut self, pipeline_id: i32);
```

2. IO
    IO takes an ExecutionPlan as an input and returns RecordBatch.
    Note: for now each source operator has to return data in one Record batch only.
    
```
pub fn new() -> Self
pub fn submit_request(&mut self, source: Arc<dyn ExecutionPlan>) -> i32
pub fn poll_response(&mut self) -> Poll<(i32, RecordBatch)>
```

rest would be done by vayu execution engine

TODO:
[] make each worker and main thread run on separate CPU
[] script which checks if only those processes are running on assigned cores 
[] script to check context switch and system calls from each threads (perf)
[] support to handler multiple pipeline breakers in one task for eg multiple joins
[] benchmarking 
[] tests