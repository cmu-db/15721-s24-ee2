use std::ptr;
use crate::common::enums::operator_result_type::{OperatorResultType, SinkResultType, SourceResultType};
use crate::common::types::data_chunk::{DataChunk, CHUNK_SIZE};
use crate::parallel::pipeline::Pipeline;
use crate::physical_operator::{PhysicalOperator, Source};
use crate::physical_operator_states::{LocalSinkState, LocalSourceState, OperatorSinkInput, OperatorState};
use std::ops::{Index, IndexMut};


pub enum PipelineExecuteResult{
    Finished,
    NotFinished,
    Interrupted
}

enum ChunkPosition{
    FinalChunk,
    Index(u64),
}

impl IndexMut<ChunkPosition> for PipelineExecutor {
    fn index_mut(&mut self, index: ChunkPosition) -> &mut Self::Output {
        // println!("Accessing {index:?}-side of balance mutably");
        match index {
            ChunkPosition::FinalChunk => &mut self.final_chunk,
            ChunkPosition::Index(i) => &mut self.pipeline.operators.get(i).as_ref().unwrap(),
        }
    }
}

impl Index<ChunkPosition> for PipelineExecutor {
    type Output = DataChunk;
    fn index(&self, index: ChunkPosition) -> &Self::Output {
        // println!("Accessing {index:?}-side of balance mutably");
        match index {
            ChunkPosition::FinalChunk => &self.final_chunk,
            ChunkPosition::Index(i) => &self.pipeline.operators.get(i).as_ref().unwrap(),
        }
    }
}

pub struct PipelineExecutor<'a>{

    //TODO
    pipeline : &'a Pipeline,

    intermediate_chunks : Vec<Box<DataChunk>>,

    intermediate_states : Vec<Box<OperatorState>>,

    local_source_state : Option<Box<LocalSourceState>>,

    local_sink_state : Option<Box<LocalSinkState>>,

    final_chunk : DataChunk,

    finished_processing_idx : i32,

    exhausted_source : bool ,

    remaining_sink_chunk : bool,

    next_batch_blocked : bool,

    requires_batch_index : bool,


}

impl PipelineExecutor<'_>{

    pub fn new(pipeline: & Pipeline)-> PipelineExecutor{
        let mut pipeline_executor = PipelineExecutor{
            pipeline,
            intermediate_chunks: Vec::new(),
            intermediate_states: Vec::new(),
            local_source_state: None,
            local_sink_state: None,
            final_chunk : DataChunk::new(),
            finished_processing_idx: -1,
            exhausted_source: false,
            remaining_sink_chunk: false,
            next_batch_blocked: false,
            requires_batch_index : false,
        };

        assert!(pipeline_executor.pipeline.source_state.is_some());

        if pipeline_executor.pipeline.sink_operator.is_some(){
            //get the state from operator
            pipeline_executor.local_sink_state = Some(pipeline_executor.pipeline.sink_operator.as_ref().unwrap().get_local_sink_state());

            //TODO
            // pipeline_executor.requires_batch_index = pipeline_executor.pipeline.sink_operator.as_ref().unwrap().requires_batch_index() && pipeline_executor.pipeline.source_operator.as_ref().unwrap().supports_batch_index();
        }

        pipeline_executor.local_source_state = Some(pipeline_executor.pipeline.source_operator.as_ref().unwrap().get_local_source_state(pipeline_executor.pipeline.source_state.as_ref().unwrap()));        //

        pipeline_executor.intermediate_states.reserve(pipeline_executor.pipeline.operators.len());
        pipeline_executor.intermediate_chunks.reserve(pipeline_executor.pipeline.operators.len());

        for i in 0..pipeline.operators.len(){

            let mut chunk = Box::new(DataChunk::new());
            match i { 0 => { chunk.initialize(&pipeline_executor.pipeline.source_operator.as_ref().unwrap().get_types(), CHUNK_SIZE);}
                _ => {chunk.initialize(&pipeline_executor.pipeline.operators.get(i-1).as_ref().unwrap().get_types(), CHUNK_SIZE);}
            }
            pipeline_executor.intermediate_chunks.push(chunk);

            let current_op = pipeline_executor.pipeline.operators.get(i).unwrap();
            let op_state = current_op.get_operator_state();
            pipeline_executor.intermediate_states.push(op_state);

            //TODO
        }

        //TODO
        // pipeline_executor.initialize_chunk(&mut pipeline_executor.final_chunk);

        pipeline_executor

    }
    //Execute a pipeline
    //this makes calls to execute_push_internal or fetch_from_source
    pub fn execute(&mut self, max_chunks : usize ) -> PipelineExecuteResult{

        assert!(self.pipeline.sink_operator.is_some());

        // let mut source_chunk = match self.pipeline.operators.is_empty() {
        //     true => {&mut self.final_chunk}
        //     false => {&mut self.intermediate_chunks.get(0).unwrap()}
        // };

        for i in 0..max_chunks{

            let result : OperatorResultType;

            if self.exhausted_source {
                //TODO
            } else if self.remaining_sink_chunk{
                // result = self.execute_push_internal(&self.final_chunk, 0);
                self.remaining_sink_chunk = false;
            } else if !self.exhausted_source || self.next_batch_blocked {
                let mut source_result : SourceResultType;

                // if !self.next_batch_blocked{
                //
                //     source_chunk.reset();
                //
                //     source_result = self.fetch_from_source(&source_chunk);
                //
                //     if let SourceResultType::Blocked = source_result {
                //         return PipelineExecuteResult::Interrupted;
                //     }
                //
                //     if let SourceResultType::Finished = source_result{
                //         self.exhausted_source = true;
                //     }
                // }


                // if self.exhausted_source && source_chunk.size() == 0{
                //     continue;
                // }
                //
                // result = self.execute_push_internal(&source_chunk, 0);

            }
        }

        // PipelineExecuteResult::Finished
        todo!()
    }

    //this will call execute2 and/or sink
    fn execute_push_internal(&mut self, input : &DataChunk, initial_idx : u64) -> OperatorResultType{

        //assert that a sink operator exists in this pipeline
        assert!(self.pipeline.sink_operator.is_some());


        // if the input is empty then we need more input
        if input.size() == 0 {
            return OperatorResultType::NeedMoreInput;
        }


        // this loop will continuously push the input chunk through the pipeline as long as:
        // - the OperatorResultType for the Execute is HAVE_MORE_OUTPUT
        // - the Sink doesn't block

        loop {
            let mut result : OperatorResultType;

            //input is the final_chunk no executing is needed , the chunk just needs to be sinked
            if !ptr::eq(input ,&self.final_chunk){
                self.final_chunk.reset();
                result = self.execute2(&input, &self.final_chunk, initial_idx);
                if let OperatorResultType::Finished = result {
                    return OperatorResultType::Finished;
                }
            }

            else{
                result = OperatorResultType::NeedMoreInput;
            }

            let sink_chunk = &mut self.final_chunk;

            if sink_chunk.size() > 0 {
                assert!(self.pipeline.sink_operator.is_some());
                //mabye we need to cast here
                //or create a function that return the state
                // assert!(self.pipeline.sink_operator.sink_state.is_some())


                //maybe create a function to get a reference to the state
                // let sink_input = OperatorSinkInput{ global_state : &self.pipeline.sink_operator.unwrap().get_sink_state(), local_state: &self.local_sink_state };

                // let sink_result = self.sink(&sink_chunk, &sink_input);

                // if let SinkResultType::Blocked = sink_result {
                //     return OperatorResultType::Blocked;
                // }
                // else if let SinkResultType::Finished = sink_result {
                //     self.finish_processing();
                //     return OperatorResultType::Finished;
                // }
            }

            if let OperatorResultType::NeedMoreInput = result {
                return OperatorResultType::NeedMoreInput;
            }
        }

    }

    fn execute2(&self, input : &DataChunk, result : &DataChunk, initial_idx : u64)-> OperatorResultType{
        todo!()
    }

    fn finish_processing(&mut self){
        todo!()
    }

    fn sink(&self, chunk : &DataChunk, input : &OperatorSinkInput) -> SinkResultType {
        todo!()
    }

    //calls  get data from this class that calls the get_data from operator
    fn fetch_from_source(&self, result : &DataChunk)-> SourceResultType {

        todo!()
    }

    //should call get_data from source operator
    fn get_data(&self){
        // self.pipeline.source_operator.unwrap().get_data(results);
        todo!()
    }

    fn initialize_chunk(&mut self, chunk: &mut DataChunk) {
        match self.pipeline.operators.is_empty() {
            true => {chunk.initialize(&self.pipeline.source_operator.as_ref().unwrap().get_types(), CHUNK_SIZE);}
            false => {chunk.initialize(&self.pipeline.operators.last().unwrap().get_types(),CHUNK_SIZE);}
        }
    }


}

