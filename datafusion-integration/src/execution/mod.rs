use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError::NotImplemented;
use datafusion::error::Result;
use datafusion::physical_plan::collect;

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::PhysicalPlanNode;
mod operators;
mod operators_conversion;
mod pipeline;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use pipeline::Source;
use std::os::unix::net::SocketAddr;
use std::sync::Arc;

use self::pipeline::{Pipeline, ScanOperator};
pub async fn execute_physical_plan_cmu(plan: PhysicalPlanNode) -> Result<Vec<RecordBatch>> {
    let mut pipeline: pipeline::Pipeline = Pipeline::new();
    // println!("{:?}", plan.physical_plan_type);
    print_tree(plan)
}

pub async fn get_pipeline(plan: PhysicalPlanNode) -> pipeline::Pipeline {
    let mut pipeline: pipeline::Pipeline = Pipeline::new();
    make_pipeline(&mut pipeline, plan.clone());
    pipeline
}

fn print_tree(node: PhysicalPlanNode) -> Result<Vec<RecordBatch>> {
    match node.physical_plan_type {
        Some(PhysicalPlanType::Filter(filter)) => {
            println!("Filter");
            let input = print_tree(*filter.input.unwrap()).unwrap();
            // util::filter(filter.expr);
            let t = input.iter();
            let expr = &filter.expr.unwrap();
            t.map(|x| operators::filter(x.clone(), expr)).collect()
        }
        Some(PhysicalPlanType::CoalesceBatches(t)) => {
            println!("CoalesceBatches");
            return print_tree(*t.input.unwrap());
        }
        Some(PhysicalPlanType::Repartition(t)) => {
            println!("Repartition");
            return print_tree(*t.input.unwrap());
        }
        Some(PhysicalPlanType::CsvScan(scan)) => {
            let t = 10;

            operators::scan(
                scan.base_conf,
                scan.has_header,
                &scan.delimiter,
                &scan.quote,
            )
        }

        _ => {
            println!("unknown");
            Err(NotImplemented(String::from(
                "node type not added to print_tree",
            )))
        }
    }
}
fn make_pipeline(pipeline: &mut pipeline::Pipeline, node: PhysicalPlanNode) {
    match node.physical_plan_type {
        Some(PhysicalPlanType::Filter(filter)) => {
            println!("Filter");
            make_pipeline(pipeline, *filter.input.unwrap());
        }
        Some(PhysicalPlanType::CoalesceBatches(t)) => {
            println!("CoalesceBatches");
            make_pipeline(pipeline, *t.input.unwrap());
        }
        Some(PhysicalPlanType::Repartition(t)) => {
            println!("Repartition");
            make_pipeline(pipeline, *t.input.unwrap());
        }
        Some(PhysicalPlanType::CsvScan(scan)) => {
            let so = operators_conversion::scan(
                scan.base_conf,
                scan.has_header,
                &scan.delimiter,
                &scan.quote,
            )
            .unwrap();
            pipeline.source_operator = Some(Box::new(so) as Box<dyn Source>)
            // TODO: add ScanOperator to pipeline
        }

        _ => {
            println!("unknown");
        }
    }
}

pub async fn execute_physical_plan(
    plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
) -> Result<Vec<RecordBatch>> {
    collect(plan, ctx.task_ctx()).await
}
