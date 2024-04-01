use arrow::array::RecordBatch;
use arrow::util::pretty;
use vayu_common::DatafusionPipelineWithData;
use vayu_common::VayuPipeline;
pub mod operators;
pub mod pipeline;
pub mod sinks;
pub mod store;
use crate::store::Store;
pub mod df2vayu;
pub struct VayuExecutionEngine {
    pub store: Store,
}

impl VayuExecutionEngine {
    pub fn new() -> VayuExecutionEngine {
        VayuExecutionEngine {
            store: Store::new(),
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
            // vayu_common::SchedulerSinkType::StoreRecordBatch(uuid) => {
            //     self.store.append(uuid, result);
            // }
            vayu_common::SchedulerSinkType::BuildAndStoreHashMap(uuid, join_node) => {
                let mut sink = sinks::HashMapSink::new(uuid, join_node);
                let map = sink.build_map(result);
                println!("BuildAndStoreHashMap storing in uuid{uuid}");
                self.store.insert(uuid, map.unwrap());
            }
        };
    }
    pub fn execute(&mut self, pipeline: DatafusionPipelineWithData) {
        let data = pipeline.data;
        let sink = pipeline.pipeline.sink;
        let mut pipeline: VayuPipeline = df2vayu::df2vayu(pipeline.pipeline.plan, &mut self.store);
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
