# Vayu Execution Engine

Vayu is a push-based execution engine based on thread-per-core architecture with no-locking, no context-switches and non-blocking.
It is using SQL parser, query optimizer from datafusion.
The optimized physical plan is taken as input and scheduler breaks it into multiple fragments based on pipeline breaker.

## Architecture

1 + 1 + N architecture 

1 core will run the kernel, handle hardware interrupts and run other daemon processes.

1 core will run the main thread of the execution engine. This thread will -
1. poll the pipeline from the scheduler
2. send the I/O requests to the io_service
3. poll the data from io_service
4. send the pipeline+data to a worker thread 
5. acknowledge the completion of the pipeline to the scheduler 

All the the events are non-blocking and short. 
Due to recent advances in non blocking IO such as io_uring this is possible. 

N X 1 core will run worker thread which would
1. ask the main thread for a pipeline
2. pass it through vayu execution engine
3. print/store the result in a global store

main thread and all the worker threads are pinned to the core. 

## Motivation - Why this architecture?
Scheduler naturally breaks a task into pipelines which can(and should) run till completion without blocking. 
Using a tokio runtime would provide no benefits as each pipeline is non-blocking. 
That lead us to design a database architecture which is entirely non-blocking, lockfree and uses thread-per-core design.


## Operators
1. Scan 
2. Filter 
3. Projection 
4. Hash Join
5. Hash Aggregate (in progress)

## How to run?
`cargo run` in vayuDB crate with run a database with filter-project query and a hash join query. 
The hash join query consists of two pipelines, which may be run on two different workers.
Scheduler will keep on sending the tasks.


## Common Vayu Structures 
```
pub struct DatafusionPipelineWithSource {
    pub source: Arc<dyn ExecutionPlan>,
    pub plan: Arc<dyn ExecutionPlan>,
    pub sink: SchedulerSinkType,
}
```


## Scheduler and IO Service? 
1. Scheduler
```
pub fn new() -> Self
pub fn get_pipeline(&mut self) -> Poll<vayu_common::DatafusionPipelineWithSource> 
pub fn ack_pipeline(&mut self, pipeline_id: i32);
```

2. IO
```
pub fn new() -> Self
pub fn submit_request(&mut self, source: Arc<dyn ExecutionPlan>) -> i32
pub fn poll_response(&mut self) -> Poll<(i32, RecordBatch)>
```

## TODO:
- [ ] make each worker and main thread run on separate CPU
- [ ] script which checks if only those - processes are running on assigned cores 
- [ ] script to check context switch and system calls from each threads (perf)
- [ ] support to handler multiple pipeline breakers in one task for eg multiple joins
- [ ] benchmarking 
- [ ] tests