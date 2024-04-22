use arrow::datatypes::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::aggregates::aggregate_expressions;
use datafusion::physical_plan::aggregates::create_accumulators;
use datafusion::physical_plan::aggregates::finalize_aggregation;
use datafusion::physical_plan::aggregates::no_grouping::aggregate_batch;
use datafusion::physical_plan::aggregates::no_grouping::AggregateStream;
use datafusion::physical_plan::aggregates::AccumulatorItem;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::aggregates::StreamType;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::projection::batch_project;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalExpr;
use std::sync::Arc;
use vayu_common::{IntermediateOperator, PhysicalOperator};
pub struct AggregateOperator {
    schema: SchemaRef,
    mode: AggregateMode,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    accumulators: Vec<AccumulatorItem>,
}
impl AggregateOperator {
    pub fn new(agg: &AggregateExec) -> Self {
        let agg_filter_expr = agg.filter_expr.clone();
        let new_mode = *agg.mode();
        println!("mode:{:?}", new_mode);

        let aggregate_expressions =
            aggregate_expressions(&agg.aggr_expr(), &agg.mode(), 0).unwrap();
        let filter_expressions = match new_mode {
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned => {
                agg_filter_expr
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; agg.aggr_expr().len()]
            }
        };
        let accumulators = create_accumulators(&agg.aggr_expr).unwrap();
        AggregateOperator {
            schema: Arc::clone(&agg.schema()),
            mode: new_mode,
            aggregate_expressions,
            filter_expressions,
            accumulators,
        }
    }
}

impl IntermediateOperator for AggregateOperator {
    fn execute(&mut self, input: &RecordBatch) -> Result<RecordBatch> {
        let result = aggregate_batch(
            &self.mode,
            input.clone(),
            &mut self.accumulators,
            &self.aggregate_expressions,
            &self.filter_expressions,
        );

        // only finally
        let result = finalize_aggregation(&mut self.accumulators, &self.mode);
        // println!("{:?}", result);
        // println!("{:?}", self.schema);
        println!("mode {:?}", self.mode);
        let result = result.and_then(|columns| {
            RecordBatch::try_new(self.schema.clone(), columns).map_err(Into::into)
        });
        result
    }
}

impl PhysicalOperator for AggregateOperator {
    fn name(&self) -> String {
        String::from("aggregate")
    }
}
