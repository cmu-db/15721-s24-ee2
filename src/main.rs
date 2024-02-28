use ee2::common::types::data_chunk::DataChunk;
use ee2::operator::filter::FilterOperator;
use ee2::operator::scan::ScanOperator;
use ee2::parallel::executor::Executor;
use ee2::parallel::pipeline::Pipeline;
use ee2::parallel::pipeline_executor::PipelineExecutor;
use ee2::physical_operator::Source;

fn main() {
    let mut executor = Executor{
        physical_plan : None,
        pipelines : vec![],
        task : None
    };

    let mut pipeline = Pipeline::new(&mut executor);

    let source : Option<Box<dyn Source>>= Some(Box::new(ScanOperator::new()));

    pipeline.source_operator = source;

    let mut pipeline_executor = PipelineExecutor::new(&pipeline);

    pipeline_executor.execute(100000);

    println!("We made it :) ");
}
