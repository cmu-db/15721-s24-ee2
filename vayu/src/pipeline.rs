use crate::operators::dummy_sink::DummySinkOperator;
use crate::operators::filter::FilterOperator;
use crate::operators::hash_join::{HashJoinProbeOperator,HashJoinBuildOperator};
use crate::operators::projection::ProjectionOperator;
use crate::operators::operator_result_type::*;
use crate::operators::scan::{DataFusionScanOperator, ScanOperator};
use crate::sinks::SchedulerSinkType;
use crate::store::Store;
use arrow::array::BooleanBufferBuilder;
use core::panic;
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::physical_plan::CsvExec;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::hash_join::BuildSide;
use datafusion::physical_plan::joins::hash_join::BuildSideReadyState;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
pub struct Pipeline {
    pub source_operator: Option<Box<dyn Source>>,
    pub sink_operator: Option<Box<dyn Sink>>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub state: PipelineState,
}
pub struct PipelineState {
    uuid: i32,
}
impl Pipeline {
    pub fn new(plan: Arc<dyn ExecutionPlan>, store: &mut Store, sink_type: SchedulerSinkType, uuid: i32) -> Pipeline {
        let mut pipeline = Pipeline {
            source_operator: None,
            sink_operator: None,
            operators: vec![],
            state: PipelineState { uuid },
        };
        make_pipeline(&mut pipeline, plan, store);
        match sink_type {
            SchedulerSinkType::PrintOutput => {
                pipeline.sink_operator = Some(Box::new(DummySinkOperator::new()));
            }
            SchedulerSinkType::StoreRecordBatch(uuid) => {
                todo!();
            }
            SchedulerSinkType::BuildAndStoreHashMap(uuid, join_node) => {
                todo!();
            }
        }
        pipeline
    }
}
fn make_pipeline(pipeline: &mut Pipeline, plan: Arc<dyn ExecutionPlan>, store: &mut Store) {
    let p = plan.as_any();
    let config = SessionConfig::new().with_batch_size(32);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let context = ctx.task_ctx();

    // set batch size here
    // println!("batch size {context.se}");
    if let Some(_) = p.downcast_ref::<CsvExec>() {
        let stream = plan.execute(0, context).unwrap();
        let schema = plan.schema();
        println!("adding source");
        pipeline.source_operator = Some(Box::new(DataFusionScanOperator::new(stream, schema)));
        return;
    }
    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        make_pipeline(pipeline, exec.input().clone(), store);
        let tt = Box::new(FilterOperator::new(exec.predicate().clone()));
        println!("adding filter");

        pipeline.operators.push(tt);
        return;
    }
    if let Some(exec) = p.downcast_ref::<ProjectionExec>() {
        make_pipeline(pipeline, exec.input().clone(), store);
        println!("adding projection");
        let expr = exec.expr().iter().map(|x| x.clone()).collect();
        let schema = exec.schema().clone();
        let tt = Box::new(ProjectionOperator::new(schema, expr));
        pipeline.operators.push(tt);
        return;
    }
    /*if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        // this function will only be called for probe side
        // build side wont have hashjoinexec in make_pipeline call
        make_pipeline(pipeline, exec.right().clone(), store);
        println!("adding hashprobe");
        let mut hashjoinstream = exec.get_hash_join_stream(0, context).unwrap();
        // using uuid but this value would be present in HashProbeExec itself
        let build_map = store.remove(pipeline.state.uuid).unwrap();
        let left_data = Arc::new(build_map.get_map());
        let visited_left_side = BooleanBufferBuilder::new(0);
        hashjoinstream.build_side = BuildSide::Ready(BuildSideReadyState {
            left_data,
            visited_left_side,
        });
        let tt = Box::new(HashProbeOperator::new(hashjoinstream));
        pipeline.operators.push(tt);
        return;
    }*/
    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        make_pipeline(pipeline, exec.input().clone(), store);
        return;
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        make_pipeline(pipeline, exec.input().clone(), store);
        return;
    }
    panic!("should never reach the end");
}
pub trait PhysicalOperator {
    fn name(&self) -> String;
}

//Operators that implement Sink trait consume data
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(&mut self, input: &Arc<RecordBatch>, results: &mut Vec<RecordBatch>) -> SinkResultType;
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_data(&mut self) -> SourceResultType;
}

//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType;
}
