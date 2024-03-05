use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError::NotImplemented;
use datafusion::error::Result;
use datafusion::physical_plan::collect;

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::PhysicalPlanNode;
mod operators;
use std::sync::Arc;
pub async fn execute_physical_plan_cmu(plan: PhysicalPlanNode) -> Result<Vec<RecordBatch>> {
    // println!("{:?}", plan.physical_plan_type);
    print_tree(plan)
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
        Some(PhysicalPlanType::CsvScan(scan)) => operators::scan(
            scan.base_conf,
            scan.has_header,
            &scan.delimiter,
            &scan.quote,
        ),

        _ => {
            println!("unknown");
            Err(NotImplemented(String::from(
                "node type not added to print_tree",
            )))
        }
    }
}

pub async fn execute_physical_plan(
    plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
) -> Result<Vec<RecordBatch>> {
    collect(plan, ctx.task_ctx()).await
}
