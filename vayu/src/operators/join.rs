use std::hash;

use crate::pipeline::{IntermediateOperator, PhysicalOperator};
use arrow::util::pretty;
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::joins::hash_join::create_hashes_outer;
use datafusion::physical_plan::joins::hash_join::HashJoinStream;
use datafusion::physical_plan::joins::hash_join::HashJoinStreamState;
use datafusion::physical_plan::joins::hash_join::JoinLeftData;
use datafusion::physical_plan::joins::hash_join::ProcessProbeBatchState;
use datafusion::physical_plan::joins::utils::StatefulStreamResult;
pub struct HashProbeOperator {
    probe: HashJoinStream,
}

impl HashProbeOperator {
    pub fn new(probe: HashJoinStream) -> Self {
        Self { probe }
    }
}

impl IntermediateOperator for HashProbeOperator {
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        let mut hashes_buffer: Vec<u64> = vec![];
        let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);

        create_hashes_outer(
            &input,
            self.probe.on_right.clone(),
            &mut hashes_buffer,
            &random_state,
        )?;
        // self.probe.build_side = BuildSide::Ready(BuildSideReadyState {
        //     left_data,
        //     BooleanBufferBuilder::new(0),
        // });
        let probe = &mut self.probe;
        probe.hashes_buffer = hashes_buffer.clone();
        probe.state = HashJoinStreamState::ProcessProbeBatch(ProcessProbeBatchState {
            batch: input.clone(),
            offset: (0, None),
            joined_probe_idx: None,
        });
        println!("vayu {:?}", hashes_buffer);

        println!("probe side data input");
        pretty::print_batches(&[input.clone()])?;

        let output = probe.process_probe_batch().unwrap();
        println!("probe side data output");

        let o = match output {
            StatefulStreamResult::Ready(t) => t.unwrap(),
            StatefulStreamResult::Continue => {
                panic!("got continue in hash probe")
            }
        };
        pretty::print_batches(&[o.clone()])?;
        Ok(o)
        // batch_filter(input, &self.predicate)
    }
}

impl PhysicalOperator for HashProbeOperator {
    fn name(&self) -> String {
        String::from("hash probe")
    }
}
