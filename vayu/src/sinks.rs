use datafusion::physical_plan::joins::hash_join;
use datafusion::physical_plan::ExecutionPlan;

use crate::RecordBatch;
use ahash::RandomState;
use arrow::datatypes::Schema;
use vayu_common::store::Blob;

use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
pub struct HashMapSink {
    pub on_left: Vec<PhysicalExprRef>,
    pub schema: Arc<Schema>,
    pub uuid: i32,
}

impl HashMapSink {
    pub fn new(uuid: i32, plan: Arc<dyn ExecutionPlan>) -> Self {
        let p = plan.as_any();
        let exec = p.downcast_ref::<HashJoinExec>();
        if exec.is_none() {
            panic!("not a join node");
        }
        let exec = exec.unwrap();
        let on_left = exec.on().iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        HashMapSink {
            on_left,
            schema: exec.left().schema(),
            uuid,
        }
    }
    pub fn build_map(&mut self, result: Vec<RecordBatch>) -> Option<Blob> {
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let ctx: SessionContext = SessionContext::new();
        let reservation =
            MemoryConsumer::new("HashJoinInput").register(ctx.task_ctx().memory_pool());

        let hash_map = hash_join::create_hash_build_map(
            result,
            random_state,
            self.on_left.clone(),
            self.schema.clone(),
            reservation,
        )
        .unwrap();
        Some(Blob::HashMapBlob(hash_map))
    }
}
