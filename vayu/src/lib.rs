use arrow::array::RecordBatch;
use arrow::util::pretty;
use vayu_common::DatafusionPipelineWithData;
use vayu_common::IntermediateOperator;
use vayu_common::VayuPipeline;
pub mod operators;
use crate::operators::aggregate::AggregateOperator;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use std::sync::{Arc, Mutex};
pub mod sinks;
use vayu_common::store::Store;
pub mod df2vayu;
mod dummy;
pub struct VayuExecutionEngine {
    // this is per node store
    // pub store: Store,
    // this is global store
    pub global_store: Arc<Mutex<Store>>,
    // Note: only one of them will survive lets see which
}

fn get_batches_from_record_batch_blob(vblob: Vec<vayu_common::store::Blob>) -> Vec<RecordBatch> {
    let mut batches = vec![];
    for val in vblob {
        match val {
            vayu_common::store::Blob::RecordBatchBlob(batch) => {
                batches.push(batch);
            }
            vayu_common::store::Blob::HashMapBlob(_) => {
                panic!("not done")
            }
        }
    }
    batches
}
fn get_record_batch_blob_from_batches(batch: Vec<RecordBatch>) -> Vec<vayu_common::store::Blob> {
    let mut vblob = vec![];
    for val in batch {
        vblob.push(vayu_common::store::Blob::RecordBatchBlob(val));
    }
    vblob
}
impl VayuExecutionEngine {
    pub fn new(global_store: Arc<Mutex<Store>>) -> VayuExecutionEngine {
        VayuExecutionEngine {
            //    store: Store::new(),
            global_store,
        }
    }
    pub fn finalize(&mut self, sink: vayu_common::FinalizeSinkType) {
        println!("running finalize");

        match sink {
            vayu_common::FinalizeSinkType::PrintFromStore(uuid) => {
                println!("running print from store {uuid}");
                let mut store = self.global_store.lock().unwrap();
                println!("{:?}", store.store.keys());

                let blob = store.remove(uuid);

                drop(store);
                let result = blob.unwrap();
                let batches = get_batches_from_record_batch_blob(result);
                // pretty::print_batches(&batches).unwrap();
            }
            vayu_common::FinalizeSinkType::FinalAggregate(plan, uuid) => {
                println!("running FinalAggregate from store {uuid}");
                let mut store = self.global_store.lock().unwrap();
                println!("{:?}", store.store.keys());

                let blob = store.remove(uuid);

                drop(store);
                let result = blob.unwrap();
                let batches = get_batches_from_record_batch_blob(result);

                let mut operator = df2vayu::aggregate(plan);
                let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();

                let result = operator.execute(&batch).unwrap();
                pretty::print_batches(&[result.clone()]).unwrap();
            }
            vayu_common::FinalizeSinkType::BuildAndStoreHashMap(uuid, join_node) => {
                let mut sink = sinks::HashMapSink::new(uuid, join_node);

                let mut store = self.global_store.lock().unwrap();
                println!("{:?}", store.store.keys());

                let blob = store.remove(uuid).unwrap();
                drop(store);
                let batches = get_batches_from_record_batch_blob(blob);

                let hashmap = sink.build_map(batches);
                let mut store = self.global_store.lock().unwrap();
                store.insert(uuid, hashmap.unwrap());
                drop(store);
                println!("storing the map {uuid}");
            }
        }
    }
    pub fn sink(&mut self, sink: vayu_common::SchedulerSinkType, result: Vec<RecordBatch>) {
        println!(
            "runningsink size {}x{}",
            result[0].num_rows(),
            result[0].num_columns()
        );
        match sink {
            vayu_common::SchedulerSinkType::PrintOutput => {
                pretty::print_batches(&result).unwrap();
            }
            vayu_common::SchedulerSinkType::StoreRecordBatch(uuid) => {
                println!("storing at store {uuid}");
                let mut store = self.global_store.lock().unwrap();
                let t = get_record_batch_blob_from_batches(result);
                store.append(uuid, t);

                println!("{:?}", store.store.keys());
                drop(store);
            }
        };
    }
    pub fn execute(&mut self, pipeline: DatafusionPipelineWithData) {
        let data = pipeline.data;

        let pipeline = pipeline.pipeline;

        let sink = pipeline.sink;

        let mut store = self.global_store.lock().unwrap();
        let mut pipeline: VayuPipeline = df2vayu::df2vayu(pipeline.plan, &mut store, pipeline.id);
        drop(store);

        pipeline.sink = sink;

        self.execute_internal(pipeline, data);
    }

    pub fn execute_internal(&mut self, mut pipeline: VayuPipeline, mut data: RecordBatch) {
        println!("operators size {}", pipeline.operators.len());
        for x in &mut pipeline.operators {
            println!(
                "running operator {} size {}x{}",
                x.name(),
                data.num_rows(),
                data.num_columns()
            );
            data = x.execute(&data).unwrap();
            println!("done execute");
        }
        println!("running sink now");
        assert!(pipeline.sink.is_some());
        self.sink(pipeline.sink.unwrap(), vec![data]);
    }

    // pub fn print_blob(&mut self, uuid: i32) {
    //     let blob = self.store.remove(uuid);
    //     match blob {
    //         Some(blob) => match blob {
    //             RecordBatchBlob(result) => {
    //                 pretty::print_batches(&result).unwrap();
    //             }
    //             HashMapBlob(results) => {
    //                 pretty::print_batches(&[results.batch().clone()]).unwrap();
    //             }
    //         },
    //         None => panic!("no blob for {uuid} found"),
    //     }
    // }
}
