use crate::operators::filter::FilterOperator;
use crate::operators::join::HashProbeOperator;
use crate::operators::projection::ProjectionOperator;
use crate::Pipeline;
use crate::Store;
use arrow::array::BooleanBufferBuilder;
use datafusion::datasource::physical_plan::CsvExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::hash_join::BuildSide;
use datafusion::physical_plan::joins::hash_join::BuildSideReadyState;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use vayu_common::SchedulerSinkType;
use vayu_common::SchedulerSourceType;

pub fn new_from_df(
    plan: Arc<dyn ExecutionPlan>,
    // store: &mut Store,
    sink: SchedulerSinkType,
) -> Pipeline {
    let mut pipeline = df2vayu(plan);
    pipeline.vayu_pipeline.sink = Some(sink);
    return pipeline;
}
pub fn df2vayu(plan: Arc<dyn ExecutionPlan>) -> Pipeline {
    let p = plan.as_any();
    let batch_size = 1024;
    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let context = ctx.task_ctx();

    // set batch size here
    // println!("batch size {context.se}");
    if let Some(_) = p.downcast_ref::<CsvExec>() {
        let stream = plan.execute(0, context).unwrap();
        println!("adding source");
        let mut pipeline = Pipeline::new();
        pipeline.source = Some(stream);
        return pipeline;
    }
    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        let mut pipeline = df2vayu(exec.input().clone());
        let tt = Box::new(FilterOperator::new(exec.predicate().clone()));
        println!("adding filter");

        pipeline.vayu_pipeline.operators.push(tt);
        return pipeline;
    }
    if let Some(exec) = p.downcast_ref::<ProjectionExec>() {
        let mut pipeline = df2vayu(exec.input().clone());
        println!("adding projection");
        let expr = exec.expr().iter().map(|x| x.0.clone()).collect();
        let schema = exec.schema().clone();
        let tt = Box::new(ProjectionOperator::new(expr, schema));
        pipeline.vayu_pipeline.operators.push(tt);
        return pipeline;
    }
    // if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
    //     // this function will only be called for probe side
    //     // build side wont have hashjoinexec in make_pipeline call
    //     let mut pipeline = df2vayu(exec.right().clone(), store);
    //     println!("adding hashprobe");
    //     let mut hashjoinstream = exec.get_hash_join_stream(0, context).unwrap();
    //     // using uuid but this value would be present in HashProbeExec itself
    //     let build_map = store.remove(1).unwrap();
    //     let left_data = Arc::new(build_map.get_map());
    //     let visited_left_side = BooleanBufferBuilder::new(0);
    //     hashjoinstream.build_side = BuildSide::Ready(BuildSideReadyState {
    //         left_data,
    //         visited_left_side,
    //     });
    //     let tt = Box::new(HashProbeOperator::new(hashjoinstream));
    //     pipeline.operators.push(tt);
    //     return pipeline;
    // }
    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        return df2vayu(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        return df2vayu(exec.input().clone());
    }
    panic!("should never reach the end");
}
