use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::datatypes::Schema;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Expr::AggregateFunction;
use datafusion::physical_plan::aggregates;
use datafusion::logical_expr::expr::AggregateFunctionDefinition;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use crate::operator::projection::ProjectionOperator;
use crate::operator::scan::ScanOperator;
use crate::parallel::pipeline::Pipeline;
use crate::physical_operator::{IntermediateOperator, Sink, Source};
use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;
use crate::operator::hash_aggregate::HashAggregateOperator;

pub struct LogicalToPhysical{
    pub pipelines: Vec<Pipeline>,
    tables: HashMap<String,String>,
}

impl LogicalToPhysical{
    pub fn new(tables: HashMap<String,String>) -> Self{
        Self{
            pipelines : vec![],
            tables,
        }
    }
}
impl LogicalToPhysical{
    pub fn create_physical(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::Projection(projection) => {
                self.create_physical(projection.input.as_ref());

                let schema = Schema::from(projection.input.schema().as_ref());
                let mut projected_columns = vec![];

                for expr in &projection.expr{
                    let projected_name = LogicalToPhysical::extract_name(expr);
                    let physical_expr = create_physical_expr(
                        &expr,
                        projection.input.schema(),
                        &ExecutionProps::new(),
                    ).unwrap();
                    projected_columns.push((physical_expr,projected_name));
                }

                let projection = ProjectionOperator::new(Arc::new(schema), projected_columns);
                let projection : Box<dyn IntermediateOperator> = Box::new(projection);
                self.pipelines.last_mut().unwrap().operators.push(projection);
            }
            LogicalPlan::Filter(_) => {
                todo!()
            }
            LogicalPlan::Aggregate(aggregate) => {
                self.create_physical(aggregate.input.as_ref());

                let schema = Schema::from(aggregate.input.schema().as_ref());
                let mut aggregate_expressions = vec![];
                let group_by_expressions= vec![];

                for expr in &aggregate.aggr_expr{
                    let s = Schema::from(aggregate.schema.as_ref());
                    let aggregate_expr = create_aggregate_expr_and_maybe_filter(expr, aggregate.input.schema().as_ref(),&schema,&ExecutionProps::new()).unwrap().0;
                    aggregate_expressions.push(aggregate_expr);
                }

                let hash_aggregate = HashAggregateOperator::new(aggregate_expressions,group_by_expressions);
                let hash_aggreate : Box<dyn Sink> = Box::new(hash_aggregate);
                self.pipelines.last_mut().unwrap().sink_operator = Some(hash_aggreate);

                //add dependency to the pipeline
                self.pipelines.push(Pipeline::new());
                let last_pipeline = self.pipelines.last_mut().unwrap();

            }
            LogicalPlan::Sort(_) => {
                todo!()
            }
            LogicalPlan::Join(_) => {
                todo!()
            }
            LogicalPlan::TableScan(scan) => {
                match self.tables.get::<String>(&scan.table_name.table().to_string()) {
                    None => {panic!("Table not found")}
                    Some(path) => {
                        let schema = Schema::from(scan.projected_schema.as_ref());
                        let scan = ScanOperator::new(Arc::new(schema), path);
                        let scan : Box<dyn Source> = Box::new(scan);
                        let scan = Some(scan);
                        self.pipelines.push(Pipeline::new());
                        self.pipelines.last_mut().unwrap().source_operator = scan;
                    }
                }
            }
            LogicalPlan::Limit(_) => {
                todo!()
            }
            LogicalPlan::Distinct(_) => {todo!()}
            _=> {panic!("Unimplemented")}
        }
    }

    fn extract_name(expr : &Expr) -> String {
        match expr {
            Expr::Column(col) => {
                col.name.clone()
            }
            Expr::Alias(alias) => {
                alias.name.clone()
            }
            _ => {unimplemented!("Unimplemented expression in process_expression")}
        }
    }

    // fn extract_aggregate_function(expr : &Expr) -> &aggregates::AggregateFunction {
    //     if let AggregateFunction(function) = expr{
    //         match &function.func_def {
    //             AggregateFunctionDefinition::BuiltIn(fun) => {
    //                 return fun
    //             }
    //             _=> {panic!("Not supported function")}
    //         }
    //     }
    //     panic!()
    //
    // }

}
