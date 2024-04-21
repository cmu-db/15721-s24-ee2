// use crate::dummy_tasks::test_hash_join;
use crate::tpch_tasks::test_tpchq1;
use datafusion_benchmarks::tpch;
use std::{hash::Hash, task::Poll};
use vayu_common::SchedulerPipeline;
#[derive(PartialEq)]
enum HashJoinState {
    CanSendBuild,
    BuildSent(i32),
    CanSendProbe,
    ProbeSent(i32),
}
pub struct Scheduler {
    turn: usize,
    // stored_id: i32,
    state: HashJoinState,
    probe_pipeline: Option<SchedulerPipeline>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            turn: 0,
            state: HashJoinState::CanSendBuild,
            probe_pipeline: None,
        }
    }

    pub fn get_pipeline(&mut self, id: i32) -> Poll<vayu_common::SchedulerPipeline> {
        let mut task = futures::executor::block_on(test_tpchq1()).unwrap();
        let pipeline = task.pipelines.remove(0);
        return Poll::Ready(pipeline);

        // let mut task = futures::executor::block_on(test_filter_project_aggregate()).unwrap();
        // let pipeline = task.pipelines.remove(0);
        // return Poll::Ready(pipeline);

        self.turn = 1 - self.turn;
        // if self.turn == 0 && self.state == HashJoinState::CanSendBuild {
        //     let mut task = futures::executor::block_on(test_hash_join()).unwrap();
        //     self.probe_pipeline = Some(task.pipelines.remove(1));
        //     let build_pipeline = task.pipelines.remove(0);

        //     self.state = HashJoinState::BuildSent(id);
        //     return Poll::Ready(build_pipeline);
        // } else if self.turn == 0 && self.state == HashJoinState::CanSendProbe {
        //     self.state = HashJoinState::ProbeSent(id);
        //     assert!(self.probe_pipeline.is_some());
        //     let probe_pipeline = self.probe_pipeline.take().unwrap();
        //     return Poll::Ready(probe_pipeline);
        // } else {
        //     let mut task = futures::executor::block_on(test_filter_project_aggregate()).unwrap();
        //     let pipeline = task.pipelines.remove(0);
        //     return Poll::Ready(pipeline);
        //     // return Poll::Pending;
        // }
    }
    pub fn ack_pipeline(&mut self, ack_id: i32) {
        match self.state {
            HashJoinState::BuildSent(id) => {
                if id == ack_id {
                    self.state = HashJoinState::CanSendProbe;
                }
            }
            HashJoinState::ProbeSent(id) => {
                if id == ack_id {
                    self.state = HashJoinState::CanSendBuild;
                }
            }
            _ => {}
        }

        // if pipeline_id == self.build_id {
        //     self.next_pipeline = 2;
        // }
    }
}
