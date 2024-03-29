use crossbeam_channel::{bounded, Receiver, Sender};
use datafusion::arrow::array::RecordBatch;
use std::collections::HashMap;
use std::task::Poll;
use std::thread;
use vayu_common::VayuPipeline;
mod dummy_tasks;
use std::time::Instant;
mod io_service;
mod scheduler;
struct PipelineWithData {
    pub pipeline: VayuPipeline,
    pub data: RecordBatch,
}
fn round_robin(worker_id: usize, num_threads: usize) -> usize {
    (worker_id + 1) % num_threads
}
fn start_worker(receiver: Receiver<PipelineWithData>) {
    // TODO: set cpu affinity
    let mut executor = vayu::VayuExecutionEngine::new();
    // Receive structs sent over the channel
    let mut now = Instant::now();
    while let Ok(my_struct) = receiver.recv() {
        println!("got data in {}", now.elapsed().as_micros());
        now = Instant::now();
        let pipeline = my_struct.pipeline;
        let data = my_struct.data;
        println!("Received struct: {:?}", data.num_rows());
        executor.execute(pipeline, data);
        println!("executed data in {}", now.elapsed().as_micros());
        now = Instant::now();
    }
}
fn main() {
    let num_threads = 2;
    let mut senders: Vec<Sender<PipelineWithData>> = Vec::new();
    for thread_num in 0..num_threads {
        // create a bounded channel to send data from main thread to worker thread
        // TODO: it is mpmc right, use some optimized spsc lockfree queue
        let (sender, receiver) = bounded(100);
        println!("going inside {thread_num}");
        // store the sender for future use
        senders.push(sender);

        // start worker thread which will keep looking for new entries in the bounded channel
        thread::spawn(move || {
            start_worker(receiver);
        });
    }
    println!("senders length is {}", senders.len());
    let mut scheduler = scheduler::Scheduler::new();
    let mut io_service = io_service::IOService::new();

    // TODO: create task_queue - buffer tasks
    let mut worker_id = 0;
    let mut request_pipeline_map: HashMap<i32, VayuPipeline> = HashMap::<i32, VayuPipeline>::new();
    loop {
        // poll scheduler for a new task
        let task = scheduler.get_task();
        if let Poll::Ready(task) = task {
            // TODO: add support for multiple dependent pipeline
            assert!(task.pipelines.len() == 1);
            for pipeline in task.pipelines {
                // get the source operator
                let stream = pipeline.source.unwrap();
                // submit the request to io service
                let request_num = io_service.submit_request(stream);
                // insert the pipeline into the local map
                request_pipeline_map.insert(request_num, pipeline.vayu_pipeline);
            }
        }
        // poll io_service for a response
        let response = io_service.poll_response();
        if let Poll::Ready((request_num, data)) = response {
            // get the pipeline from the local map

            let pipeline = request_pipeline_map.remove(&request_num).unwrap();
            // send over channel
            senders[worker_id]
                .send(PipelineWithData { pipeline, data })
                .expect("Failed to send struct");

            // ASSUMPTION: we will get data in one record batch for one pipeline
            // assign the next pipeline to some other worker
            worker_id = round_robin(worker_id, num_threads);
        }
    }
}
