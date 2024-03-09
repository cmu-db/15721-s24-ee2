use datafusion::logical_expr::builder::project;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::PhysicalPlanNode;
mod operators_conversion;
use vayu::pipeline::Source;
use vayu::pipeline::{self, IntermediateOperator};

use std::sync::Arc;
use vayu::pipeline::Pipeline;
pub async fn get_pipeline(plan: std::sync::Arc<dyn ExecutionPlan>) -> pipeline::Pipeline {
    let mut pipeline: pipeline::Pipeline = Pipeline::new();
    make_pipeline(&mut pipeline, plan.clone());
    pipeline
}

fn make_pipeline(pipeline: &mut pipeline::Pipeline, plan: std::sync::Arc<dyn ExecutionPlan>) {
    match node.physical_plan_type {
        Some(PhysicalPlanType::Filter(filter)) => {
            println!("Filter");
            let expr = &filter.expr.unwrap();
            make_pipeline(pipeline, *filter.input.unwrap());
            let schema = match pipeline.state.schema.as_ref() {
                Some(schema) => schema.clone(),
                None => panic!("schema not found"),
            };
            let fo = operators_conversion::filter(&schema, expr).unwrap();
            pipeline
                .operators
                .push(Box::new(fo) as Box<dyn IntermediateOperator>);
            println!("len in func  {}", pipeline.operators.len());
        }
        Some(PhysicalPlanType::CoalesceBatches(t)) => {
            println!("CoalesceBatches");
            make_pipeline(pipeline, *t.input.unwrap());
        }
        Some(PhysicalPlanType::Repartition(t)) => {
            println!("Repartition");
            make_pipeline(pipeline, *t.input.unwrap());
        }
        Some(PhysicalPlanType::Projection(t)) => {
            println!("Projection");
            let schema = match pipeline.state.schema.as_ref() {
                Some(schema) => schema.clone(),
                None => panic!("schema not found"),
            };
            make_pipeline(pipeline, *t.input.unwrap());
            let po = operators_conversion::projection(&schema, projection);
        }
        Some(PhysicalPlanType::CsvScan(scan)) => {
            let so = operators_conversion::scan(
                scan.base_conf,
                scan.has_header,
                &scan.delimiter,
                &scan.quote,
            )
            .unwrap();
            let schema = so.schema.clone();

            pipeline.source_operator = Some(Box::new(so) as Box<dyn Source>);
            pipeline.state.schema = Some(schema);
        }

        _ => {
            panic!("unknown physical operator");
        }
    }
}
