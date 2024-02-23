// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::displayable;

// use datafusion::physical_plan::{
//     functions, udaf, AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr, Statistics,
// };

use datafusion::{dataframe, prelude::*};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf;

mod cmu_execution;
// use cmu_execution;
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();
    // path to testdata
    let testdata =
        "/Users/kothari/Desktop/course/15721/15721-s24-ee2/datafusion-integration/testing/data";
    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;

    // sql query
    let sql = "SELECT c1,c12  FROM aggregate_test_100  WHERE c12 < 0.03";

    // create datafusion logical plan
    let logical_plan = SessionState::create_logical_plan(&ctx.state(), sql).await?;

    // create datafusion physical plan (trait)
    let plan = SessionState::create_physical_plan(&ctx.state(), &logical_plan).await?;
    println!(
        "physical plan:\n {}",
        displayable(plan.as_ref()).indent(true)
    );

    // convert to node based datafusion physical plan
    let codec: DefaultPhysicalExtensionCodec = DefaultPhysicalExtensionCodec {};
    let plan1: protobuf::PhysicalPlanNode =
        protobuf::PhysicalPlanNode::try_from_physical_plan(plan.clone(), &codec).expect("to proto");

    // get results from datfusion execution engine
    let results = cmu_execution::execute_physical_plan(plan.clone(), ctx).await?;

    // get results from cmu execution engine (let's name it guys)
    // let results = cmu_execution::execute_physical_plan_cmu(plan1).await?;

    pretty::print_batches(&results)?;

    Ok(())
}