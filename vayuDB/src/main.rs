use crossbeam_channel::{bounded, Receiver, Sender};
use futures::stream::Skip;
use std::collections::HashMap;
use std::task::Poll;
use std::thread;
use vayu_common::{DatafusionPipeline, DatafusionPipelineWithData};
mod dummy_tasks;
mod io_service;
mod scheduler;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::thread::scope;
use lockfree;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use vayu_common;
use vayu_common::store::Store;
fn round_robin(worker_id: usize, num_threads: usize) -> usize {
    (worker_id + 1) % num_threads
}
fn start_worker(
    receiver: Receiver<DatafusionPipelineWithData>,
    sender: Sender<i32>,
    global_store: Arc<Mutex<Store>>,
) {
    // TODO: set cpu affinity
    let mut executor = vayu::VayuExecutionEngine::new(global_store);
    // Receive structs sent over the channel
    sender.send(0).unwrap();
    while let Ok(pipeline) = receiver.recv() {
        println!("got a pipeline for the thread, executing ...");
        executor.execute(pipeline);
        sender.send(0).unwrap();
    }
}

fn main() {
    // can use these crates if they have a lockfree concurrent hashmap
    // let global_store: SkipMap<i32, i32> = SkipMap::new();
    // let global_store: lockfree::map::Map<i32, i32> = lockfree::map::Map::default();
    // let global_store: leapfrog::LeapMap<i32, i32> = leapfrog::LeapMap::new();

    // number of threads to run parallely
    let num_threads = 2;
    // global store to store intermediate hash maps ( and other stuff )
    // TODO: change this to use spinlock and eventually something better
    let global_store: Arc<Mutex<Store>> = Arc::new(Mutex::new(Store::new()));

    // this MPSC queue would be used by workers to inform -
    // 1. pipeline <id> has finished
    // 2. request for new work
    let (informer_sender, informer_receiver): (Sender<i32>, Receiver<i32>) = bounded(0);

    // vector to store main_thread->worker channels
    let mut senders: Vec<Sender<DatafusionPipelineWithData>> = Vec::new();

    for thread_num in 0..num_threads {
        // channel to send pipeline and data from main thread the to worker thread
        // TODO: it is mpmc right now, use some optimized spsc lockfree queue
        let (sender, receiver) = bounded(1);
        senders.push(sender);

        println!("spawning a new thread {thread_num}");

        let global_store_clone = Arc::clone(&global_store);
        let informer_sender_clone = informer_sender.clone();

        // start worker thread which will keep looking for new entries in the channel
        thread::spawn(move || {
            start_worker(receiver, informer_sender_clone, global_store_clone);
        });
    }

    println!("total number of workers {}", senders.len());

    let mut scheduler = scheduler::Scheduler::new();
    let mut io_service = io_service::IOService::new();

    // TODO: create task_queue - buffer tasks
    let mut worker_id = 0;
    let mut request_pipeline_map: HashMap<i32, DatafusionPipeline> = HashMap::new();

    // right now a pipeline would be assigned to a worker only when it is free
    // but we will poll some extra pipelines from the scheduler and send it to the io service
    // so that we can start working on it once any worker is free
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
                println!("got a response from the io_service");

                // TODO: handle when a source gives multiple record batches
                // get the pipeline from the local map
                let pipeline = request_pipeline_map.remove(&request_num).unwrap();

                // send over channel
                let msg = DatafusionPipelineWithData { pipeline, data };
                senders[worker_id].send(msg).expect("Failed to send struct");
                println!("sent the pipeline and the data to the worker");

                non_assigned_pipelines -= 1;
                // assign the next pipeline to some other worker
                worker_id = round_robin(worker_id, num_threads);
            }
        } else {
            panic!("what is this?")
        }
    }
}
