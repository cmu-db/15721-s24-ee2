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
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::physical_plan::{CsvConfig, CsvOpener};
use std::os::unix::net::SocketAddr;
use std::sync::Arc;
use vayu::pipeline::Source;
use vayu::pipeline::{self, IntermediateOperator};

use vayu::operators::scan::ScanOperator;
use vayu::pipeline::Pipeline;

pub async fn get_pipeline(plan: PhysicalPlanNode) -> pipeline::Pipeline {
    let mut pipeline: pipeline::Pipeline = Pipeline::new();
    make_pipeline(&mut pipeline, plan.clone());
    pipeline
}

fn make_pipeline(pipeline: &mut pipeline::Pipeline, node: PhysicalPlanNode) {
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
        Some(PhysicalPlanType::CsvScan(scan)) => {
            let so = operators_conversion::scan(
                scan.base_conf,
                scan.has_header,
                &scan.delimiter,
                &scan.quote,
            )
            .unwrap();
            let schema = so.fileconfig.file_schema.clone();

            pipeline.source_operator = Some(Box::new(so) as Box<dyn Source>);
            pipeline.state.schema = Some(schema);
            // TODO: add ScanOperator to pipeline
        }

        _ => {
            println!("unknown");
        }
    }
}
