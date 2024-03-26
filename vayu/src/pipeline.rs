use crate::operators::filter::FilterOperator;
use crate::operators::join::HashProbeOperator;
use crate::operators::projection::ProjectionOperator;
use crate::store::Store;
use arrow::array::BooleanBufferBuilder;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::physical_plan::CsvExec;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream;
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
    pub source: Option<SendableRecordBatchStream>,
    pub sink: Option<SendableRecordBatchStream>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub state: PipelineState,
    pub do_build_phase: bool,
}
pub struct PipelineState {
    pub schema: Option<Arc<Schema>>,
}
impl Pipeline {
    pub fn new(plan: Arc<dyn ExecutionPlan>, store: &mut Store, do_build_phase: bool) -> Pipeline {
        let mut pipeline = Pipeline {
            source: None,
            sink: None,
            operators: vec![],
            state: PipelineState { schema: None },
            do_build_phase,
        };
        make_pipeline(&mut pipeline, plan, store);
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
        println!("adding source");
        pipeline.source = Some(stream);
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
        let expr = exec.expr().iter().map(|x| x.0.clone()).collect();
        let schema = exec.schema().clone();
        let tt = Box::new(ProjectionOperator::new(expr, schema));
        pipeline.operators.push(tt);
        return;
    }
    if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        if pipeline.do_build_phase {
            make_pipeline(pipeline, exec.left().clone(), store);
            println!("doing hashbuild");
            // in build phase you dont do anything
            // building hashmap is done after completing the pipeline
        } else {
            make_pipeline(pipeline, exec.right().clone(), store);
            println!("adding hashprobe");
            let mut hashjoinstream = exec.get_hash_join_stream(0, context).unwrap();
            let build_map = store.remove(1).unwrap();
            let left_data = Arc::new(build_map.get_map());
            let visited_left_side = BooleanBufferBuilder::new(0);
            hashjoinstream.build_side = BuildSide::Ready(BuildSideReadyState {
                left_data,
                visited_left_side,
            });
            // println!("{:?}", left_data);
            let tt = Box::new(HashProbeOperator::new(hashjoinstream));
            pipeline.operators.push(tt);
        }
        return;
    }
    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        make_pipeline(pipeline, exec.input().clone(), store);
        return;
        // pipeline.operators.push(FilterOperator::new())
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        make_pipeline(pipeline, exec.input().clone(), store);
        return;
        // pipeline.operators.push(FilterOperator::new())
    }
}
pub trait PhysicalOperator {
    fn name(&self) -> String;
}

//Operators that implement Sink trait consume data
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(&self, chunk: &mut RecordBatch) -> bool;
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_data(&mut self) -> Option<RecordBatch>;
}

//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch>;
}
