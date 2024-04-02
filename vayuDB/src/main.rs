use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::task::Poll;
use std::thread;
use vayu_common::{DatafusionPipeline, DatafusionPipelineWithData};
mod dummy_tasks;
mod io_service;
mod scheduler;
use std::sync::mpsc;
fn round_robin(worker_id: usize, num_threads: usize) -> usize {
    (worker_id + 1) % num_threads
}
fn start_worker(receiver: Receiver<DatafusionPipelineWithData>, sender: Sender<bool>) {
    // TODO: set cpu affinity
    let mut executor = vayu::VayuExecutionEngine::new();
    // Receive structs sent over the channel
    sender.send(true).unwrap();
    while let Ok(pipeline) = receiver.recv() {
        println!("got a pipeline for the thread, executing ...");
        executor.execute(pipeline);
        sender.send(true).unwrap();
    }
}
fn main() {
    let (informer_sender, informer_receiver) = bounded(0);
    let num_threads = 1;
    let mut senders: Vec<Sender<DatafusionPipelineWithData>> = Vec::new();
    for thread_num in 0..num_threads {
        // create a bounded channel to send data from main thread to worker thread
        // TODO: it is mpmc right, use some optimized spsc lockfree queue
        let (sender, receiver) = bounded(100);
        println!("spawning a new thread {thread_num}");
        // store the sender for future use
        senders.push(sender);
        let informer_sender_clone = informer_sender.clone();
        // start worker thread which will keep looking for new entries in the bounded channel
        thread::spawn(move || {
            start_worker(receiver, informer_sender_clone);
        });
    }
    println!("number of workers {}", senders.len());
    let mut scheduler = scheduler::Scheduler::new();
    let mut io_service = io_service::IOService::new();

    // TODO: create task_queue - buffer tasks
    let mut worker_id = 0;
    let mut request_pipeline_map: HashMap<i32, DatafusionPipeline> =
        HashMap::<i32, DatafusionPipeline>::new();

    let mut non_assigned_pipelines = 0;
    loop {
        // poll scheduler for a new task
        if non_assigned_pipelines < 10 {
            let pipeline = scheduler.get_pipeline();

            if let Poll::Ready(pipeline) = pipeline {
                non_assigned_pipelines += 1;
                // TODO: add support for multiple dependent pipeline
                println!("got a pipeline from scheduler");
                assert!(pipeline.sink.is_some());
                // submit the source request to io service
                let request_num = io_service.submit_request(pipeline.source);
                println!("sent the request to the io_service");

                // insert the pipeline into the local map
                request_pipeline_map.insert(
                    request_num,
                    DatafusionPipeline {
                        plan: pipeline.plan,
                        sink: pipeline.sink,
                    },
                );
            }
        }
        if let Ok(value) = informer_receiver.recv() {
            // poll io_service for a response
            let response = io_service.poll_response();
            if let Poll::Ready((request_num, data)) = response {
                // get the pipeline from the local map
                println!("got a response from the io_service");

                let pipeline = request_pipeline_map.remove(&request_num).unwrap();
                // send over channel
                let msg = DatafusionPipelineWithData { pipeline, data };
                senders[worker_id].send(msg).expect("Failed to send struct");
                println!("sent the pipeline and the data to the worker");
                non_assigned_pipelines -= 1;
                // ASSUMPTION: we will get data in one record batch for one pipeline
                // assign the next pipeline to some other worker
                worker_id = round_robin(worker_id, num_threads);
            }
        } else {
            panic!("what is this?")
        }
    }
}
