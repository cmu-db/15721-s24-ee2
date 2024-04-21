use crossbeam_channel::{bounded, Receiver, Sender};
use datafusion::arrow::array::ArrowNativeTypeOp;
use std::collections::HashMap;
use std::task::Poll;
use std::thread;
use vayu_common::SchedulerPipeline;
use vayu_common::{DatafusionPipeline, DatafusionPipelineWithData};
mod dummy_tasks;
mod io_service;
mod scheduler;
mod tpch_tasks;
use std::collections::LinkedList;
use std::sync::Arc;
use std::sync::Mutex;
use vayu_common;
use vayu_common::VayuMessage;

use vayu_common::store::Store;

use crate::scheduler::Scheduler;
fn start_worker(
    receiver: Receiver<VayuMessage>,
    sender: Sender<(usize, i32)>,
    global_store: Arc<Mutex<Store>>,
    thread_id: usize,
) {
    let mut executor = vayu::VayuExecutionEngine::new(global_store);
    // Receive structs sent over the channel
    while let Ok(message) = receiver.recv() {
        match message {
            VayuMessage::Normal(pipeline) => {
                let pipeline_id = pipeline.pipeline.id;
                println!("{thread_id}:got a pipeline for the thread, executing ...");
                executor.execute(pipeline);
                println!("{thread_id}:done executing ...");
                sender.send((thread_id, pipeline_id)).unwrap();
            }
            VayuMessage::Finalize((sink, pipeline_id)) => {
                println!("{thread_id}:got a finalize pipeline for the thread, executing ...");
                executor.finalize(sink);
                println!("{thread_id}:done executing ...");
                sender.send((thread_id, pipeline_id)).unwrap();
            }
        }
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
    let mut senders: Vec<Sender<VayuMessage>> = Vec::new();
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
    let mut request_pipeline_map: HashMap<i32, (SchedulerPipeline, i32)> = HashMap::new();
    let mut completed_pipeline_list: LinkedList<SchedulerPipeline> = LinkedList::new();

    // right now a pipeline would be assigned to a worker only when it is free
    // but we will poll some extra pipelines from the scheduler and send it to the io service
    // so that we can start working on it once any worker is free
    let mut next_id = 0;

    // poll scheduler for a new task
    let pipeline = scheduler.get_pipeline(next_id);
    if let Poll::Ready(mut pipeline) = pipeline {
        // TODO: add support for multiple dependent pipeline
        println!("got a pipeline from scheduler");

        let source = pipeline.source.take().unwrap();
        // submit the source request to io service
        let request_num = io_service.submit_request(source);
        println!("sent the request to the io_service");

        // insert the pipeline into the local map
        request_pipeline_map.insert(request_num, (pipeline, 0));
        next_id += 1;
    }

    loop {
        if let Ok((thread_id, request_num)) = informer_receiver.try_recv() {
            println!("got ack from thread {}", thread_id);
            if request_num != -1 {
                let pipeline = request_pipeline_map.remove(&request_num);
                match pipeline {
                    Some((pipeline, processing_count)) => {
                        let processing_count = processing_count - 1;
                        println!("current processing count is {processing_count}");
                        if processing_count == 0 {
                            completed_pipeline_list.push_back(pipeline);
                        } else {
                            request_pipeline_map.insert(request_num, (pipeline, processing_count));
                        }
                    }
                    None => {
                        println!("inform scheduler we are done");
                        // scheduler.ack_pipeline(request_num);
                    }
                }
            }

            // add in the queue
            free_threads.push_back(thread_id);
        }
        if free_threads.len() == 0 {
            continue;
        }
        // check from finalize queue
        if !completed_pipeline_list.is_empty() {
            println!("removing item from completed list");

            let thread_id = free_threads.pop_front().unwrap();
            // pipeline.
            //data is finished
            let pipeline = completed_pipeline_list.pop_front().unwrap();
            let msg = VayuMessage::Finalize((pipeline.finalize, pipeline.pipeline.id));
            senders[thread_id].send(msg).expect("Failed to send struct");
            println!("finalize:sent the pipeline and the data to the worker");
            continue;
        }
        // println!("free thread available");
        // poll io_service for a response
        let response = io_service.poll_response();
        if let Poll::Ready((request_num, data)) = response {
            if data.is_none() {
                let mv = request_pipeline_map.get(&request_num);

                assert!(mv.is_some());
                let (_, processing_count) = mv.unwrap();
                println!("no more data left. processing count is {processing_count}");
                if processing_count.is_zero() {
                    let pipeline = request_pipeline_map.remove(&request_num);
                    assert!(pipeline.is_some());
                    let (pipeline, _) = pipeline.unwrap();
                    completed_pipeline_list.push_back(pipeline);
                }
            } else {
                let data = data.unwrap();
                let thread_id = free_threads.pop_front().unwrap();
                println!("got a response from the io_service");

                // get the pipeline from the local map
                let pipeline = request_pipeline_map.remove(&request_num);

                assert!(pipeline.is_some());
                let (pipeline, processing_count) = pipeline.unwrap();

                request_pipeline_map.insert(request_num, (pipeline.clone(), processing_count + 1));

                // send over channel
                let msg = VayuMessage::Normal(DatafusionPipelineWithData {
                    pipeline: pipeline.pipeline,
                    data,
                });
                senders[thread_id].send(msg).expect("Failed to send struct");
                println!("sent the pipeline and the data to the worker");
            }
            // assign the next pipeline to some other worker
            // worker_id = round_robin(worker_id, num_threads);
        }
    }
}
