use crate::operators::aggregate::AggregateOperator;
use crate::operators::filter::FilterOperator;
use crate::operators::join::HashProbeOperator;
use crate::operators::projection::ProjectionOperator;
use crate::Store;
use ahash::random_state::RandomSource;
use ahash::RandomState;
use arrow::array::BooleanBufferBuilder;
use arrow::compute::kernels::concat_elements;
use datafusion::datasource::physical_plan::CsvExec;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::aggregates::StreamType;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
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
use vayu_common::VayuPipeline;

pub fn df2vayu(plan: Arc<dyn ExecutionPlan>, store: &mut Store, pipeline_id: i32) -> VayuPipeline {
    let p = plan.as_any();
    let batch_size = 1024;
    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let context = ctx.task_ctx();

    // set batch size here
    // println!("batch size {context.se}");
    if let Some(_) = p.downcast_ref::<CsvExec>() {
        return VayuPipeline {
            operators: vec![],
            sink: None,
        };
    }
    if let Some(_) = p.downcast_ref::<ParquetExec>() {
        return VayuPipeline {
            operators: vec![],
            sink: None,
        };
    }
    if let Some(exec) = p.downcast_ref::<AggregateExec>() {
        let mut pipeline = df2vayu(exec.input().clone(), store, pipeline_id);
        // check if no group by present
        if !exec.group_by().expr().is_empty() {
            panic!("group by present- not handled");
        }

        let tt = Box::new(AggregateOperator::new(exec));
        println!("adding aggregate");
        pipeline.operators.push(tt);
        return pipeline;
    }
    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        let mut pipeline = df2vayu(exec.input().clone(), store, pipeline_id);
        let tt = Box::new(FilterOperator::new(exec.predicate().clone()));
        println!("adding filter");
        pipeline.operators.push(tt);
        return pipeline;
    }
    if let Some(exec) = p.downcast_ref::<ProjectionExec>() {
        let mut pipeline = df2vayu(exec.input().clone(), store, pipeline_id);
        println!("adding projection");
        let expr = exec.expr().iter().map(|x| x.0.clone()).collect();
        let schema = exec.schema().clone();
        let tt = Box::new(ProjectionOperator::new(expr, schema));
        pipeline.operators.push(tt);
        return pipeline;
    }
    if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        // this function will only be called for probe side
        // build side wont have hashjoinexec in make_pipeline call

        // let dummy = exec.left().execute(0, context.clone());
        let mut pipeline = df2vayu(exec.right().clone(), store, pipeline_id);
        println!("adding hashprobe");

        let mut hashjoinstream = exec.get_hash_join_stream(0, context).unwrap();
        println!("got joinstream");

        // using uuid but this value would be present in HashProbeExec itself
        // TODO: remove from the correct key
        let build_map = store.remove(42).unwrap();
        let left_data = Arc::new(build_map.get_map());
        let visited_left_side = BooleanBufferBuilder::new(0);
        hashjoinstream.build_side = BuildSide::Ready(BuildSideReadyState {
            left_data,
            visited_left_side,
        });
        let tt = Box::new(HashProbeOperator::new(hashjoinstream));
        pipeline.operators.push(tt);
        return pipeline;
    }
    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        return df2vayu(exec.input().clone(), store, pipeline_id);
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        return df2vayu(exec.input().clone(), store, pipeline_id);
    }
    if let Some(exec) = p.downcast_ref::<CoalescePartitionsExec>() {
        return df2vayu(exec.input().clone(), store, pipeline_id);
    }
    panic!("should never reach the end");
}

/**
 * returns (join_node, build_plan)
 * Note: build_plan won't have join node
 */
pub fn get_hash_build_pipeline(
    plan: Arc<dyn ExecutionPlan>,
    uuid: i32,
) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    let plan1 = plan.clone();
    let p = plan.as_any();

    if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        return (plan1, exec.left().clone());
    }
    if let Some(_) = p.downcast_ref::<CsvExec>() {
        panic!("should never reach csvexec in get_hash_build_pipeline ");
    }
    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        return get_hash_build_pipeline(exec.input().clone(), uuid);
    }
    if let Some(exec) = p.downcast_ref::<ProjectionExec>() {
        return get_hash_build_pipeline(exec.input().clone(), uuid);
    }
    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        return get_hash_build_pipeline(exec.input().clone(), uuid);
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        return get_hash_build_pipeline(exec.input().clone(), uuid);
    }
    panic!("No join node found");
}

/**
 * returns (join_node, build_plan)
 * Note: build_plan won't have join node
 */
pub fn get_source_node(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let p = plan.as_any();

    if let Some(exec) = p.downcast_ref::<HashJoinExec>() {
        return get_source_node(exec.right().clone());
    }
    if let Some(_) = p.downcast_ref::<CsvExec>() {
        return plan;
    }
    if let Some(_) = p.downcast_ref::<ParquetExec>() {
        return plan;
    }
    if let Some(exec) = p.downcast_ref::<AggregateExec>() {
        return get_source_node(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<CoalescePartitionsExec>() {
        return get_source_node(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        return get_source_node(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<ProjectionExec>() {
        return get_source_node(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        return get_source_node(exec.input().clone());
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        return get_source_node(exec.input().clone());
    }
    panic!("No source node found");
}

pub fn aggregate(exec: Arc<dyn ExecutionPlan>) -> AggregateOperator {
    let p = exec.as_any();
    let final_aggregate = if let Some(exec) = p.downcast_ref::<AggregateExec>() {
        if !exec.group_by().expr().is_empty() {
            panic!("group by present- not handled");
        }
        let tt = AggregateOperator::new(exec);
        tt
    } else {
        panic!("not an aggregate");
    };
    final_aggregate
}
