use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::joins::hash_join::{
    create_hashes_outer, HashJoinStream, HashJoinStreamState, JoinLeftData, ProcessProbeBatchState,
};
use datafusion::physical_plan::joins::utils::StatefulStreamResult;
use vayu_common::{IntermediateOperator, PhysicalOperator};

pub struct HashProbeOperator {
    probe: HashJoinStream,
    build_map: Option<JoinLeftData>,
}
impl HashProbeOperator {
    pub fn new(build_uuid: i32, probe: HashJoinStream) -> Self {
        Self {
            probe,
            build_map: None,
        }
    }
}

impl IntermediateOperator for HashProbeOperator {
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        let mut hashes_buffer: Vec<u64> = vec![];
        let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let probe = &mut self.probe;

        // These things are being done in datafusion::fetch_probe_batch
        create_hashes_outer(
            &input,
            probe.on_right.clone(),
            &mut hashes_buffer,
            &random_state,
        )?;
        probe.hashes_buffer = hashes_buffer.clone();
        probe.state = HashJoinStreamState::ProcessProbeBatch(ProcessProbeBatchState {
            batch: input.clone(),
            offset: (0, None),
            joined_probe_idx: None,
        });

        // probe the build side
        let output = probe.process_probe_batch().unwrap();
        let o = match output {
            StatefulStreamResult::Ready(t) => t.unwrap(),
            StatefulStreamResult::Continue => {
                panic!("got continue in hash probe")
            }
        };
        Ok(o)
    }
}

impl PhysicalOperator for HashProbeOperator {
    fn name(&self) -> String {
        String::from("hash probe")
    }
}
