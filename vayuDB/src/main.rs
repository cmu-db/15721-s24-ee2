use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::task::Poll;
use std::thread;
use vayu_common::{DatafusionPipeline, DatafusionPipelineWithData};
mod dummy_tasks;
mod io_service;
mod scheduler;
mod tpch_tasks;
use std::collections::LinkedList;
use std::sync::Arc;
use std::sync::Mutex;
use vayu_common;

use vayu_common::store::Store;
fn start_worker(
    receiver: Receiver<DatafusionPipelineWithData>,
    sender: Sender<(usize, i32)>,
    global_store: Arc<Mutex<Store>>,
    thread_id: usize,
) {
    let mut executor = vayu::VayuExecutionEngine::new(global_store);
    // Receive structs sent over the channel
    while let Ok(pipeline) = receiver.recv() {
        let pipeline_id = pipeline.pipeline.id;
        println!("{thread_id}:got a pipeline for the thread, executing ...");
        executor.execute(pipeline);
        println!("{thread_id}:done executing ...");
        sender.send((thread_id, pipeline_id)).unwrap();
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
    let (informer_sender, informer_receiver): (Sender<(usize, i32)>, Receiver<(usize, i32)>) =
        bounded(0);

    // vector to store main_thread->worker channels
    let mut senders: Vec<Sender<DatafusionPipelineWithData>> = Vec::new();
    let mut free_threads: LinkedList<usize> = LinkedList::new();

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
            // TODO: set cpu affinity
            start_worker(
                receiver,
                informer_sender_clone,
                global_store_clone,
                thread_num,
            );
        });
        free_threads.push_back(thread_num);
    }

    println!("total number of workers {}", senders.len());

    let mut scheduler = scheduler::Scheduler::new();
    let mut io_service = io_service::IOService::new();

    // TODO: create task_queue - buffer tasks
    let mut request_pipeline_map: HashMap<i32, DatafusionPipeline> = HashMap::new();

    // right now a pipeline would be assigned to a worker only when it is free
    // but we will poll some extra pipelines from the scheduler and send it to the io service
    // so that we can start working on it once any worker is free
    let mut next_id = 0;

    if request_pipeline_map.len() == 0 {
        // poll scheduler for a new task
        let pipeline = scheduler.get_pipeline(next_id);
        if let Poll::Ready(pipeline) = pipeline {
            // TODO: add support for multiple dependent pipeline
            println!("got a pipeline from scheduler");

            // submit the source request to io service
            let request_num = io_service.submit_request(pipeline.source);
            println!("sent the request to the io_service");

            // insert the pipeline into the local map
            request_pipeline_map.insert(
                request_num,
                DatafusionPipeline {
                    plan: pipeline.plan,
                    sink: pipeline.sink,
                    id: next_id,
                },
            );
            next_id += 1;
        }
    }
    loop {
        if let Ok((thread_id, finished_pipeline_id)) = informer_receiver.try_recv() {
            println!("got ack from thread {}", thread_id);
            if finished_pipeline_id != -1 {
                scheduler.ack_pipeline(finished_pipeline_id);
            }
            // add in the queue
            free_threads.push_back(thread_id);
        }
        if let Some(&thread_id) = free_threads.front() {
            // println!("free thread available");
            // poll io_service for a response
            let response = io_service.poll_response();
            if let Poll::Ready((request_num, data)) = response {
                if data.is_none() {
                    let pipeline = request_pipeline_map.remove(&request_num);
                } else {
                    let data = data.unwrap();
                    free_threads.pop_front();
                    println!("got a response from the io_service");

                    // TODO: handle when a source gives multiple record batches
                    // get the pipeline from the local map
                    let pipeline = request_pipeline_map.remove(&request_num);

                    assert!(pipeline.is_some());
                    let pipeline = pipeline.unwrap();
                    let pipeline2 = DatafusionPipeline {
                        plan: pipeline.plan.clone(),
                        sink: pipeline.sink.clone(),
                        id: pipeline.id,
                    };
                    request_pipeline_map.insert(request_num, pipeline2);
                    // send over channel
                    let msg = DatafusionPipelineWithData { pipeline, data };
                    senders[thread_id].send(msg).expect("Failed to send struct");
                    println!("sent the pipeline and the data to the worker");
                }
                // assign the next pipeline to some other worker
                // worker_id = round_robin(worker_id, num_threads);
            }
        }
    }
}
