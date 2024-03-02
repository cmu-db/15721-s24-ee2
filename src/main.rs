use ee2::common::types::data_chunk::DataChunk;
use ee2::operator::dummy_sink::DummySinkOperator;
use ee2::operator::filter::FilterOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::executor::Executor;
use ee2::parallel::pipeline::Pipeline;
use ee2::parallel::pipeline_executor::{PipelineExecuteResult, PipelineExecutor};
use ee2::physical_operator::{Sink, Source};

fn main() {
    let mut executor = Executor{
        physical_plan : None,
        pipelines : vec![],
        task : None
    };

    let mut pipeline = Pipeline::new(&mut executor);

    let source : Option<Box<dyn Source>>= Some(Box::new(ScanOperator::new()));
    let sink : Option<Box<dyn Sink>> = Some(Box::new(DummySinkOperator::new()));

    pipeline.source_operator = source;
    pipeline.sink_operator = sink;


    let mut pipeline_executor = PipelineExecutor::new(&pipeline);

    let res = pipeline_executor.execute(100000);

    println!("");
    match res {
        PipelineExecuteResult::Finished => {println!("finished");}
        PipelineExecuteResult::NotFinished => {println!("not finished");}
        PipelineExecuteResult::Interrupted => {println!("interrupted");}
    }


    println!("We made it :) ");
}
