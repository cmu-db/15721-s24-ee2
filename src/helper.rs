use ahash::HashMap;
use std::cell::RefCell;
use std::sync::Arc;
use std::vec;
// This file contains some helper functions for tpch queries
use crate::operator::filter::FilterOperator;
use crate::operator::hash_aggregate::HashAggregateOperator;
use crate::operator::hash_join::{HashJoinBuildOperator, HashJoinProbeOperator, JoinLeftData};
use crate::operator::limit::LimitOperator;
use crate::operator::placeholder::PlaceholderOperator;
use crate::operator::projection::ProjectionOperator;
use crate::operator::scan::{ScanIntermediatesOperator, ScanOperator};
use crate::operator::sort::SortOperator;
use crate::parallel::pipeline::Pipeline;
use crate::physical_operator::{IntermediateOperator, Sink, Source};
use ahash::HashMapExt;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};

//return the schema of the region table in TPCH
pub fn tpch_schema(table: &str) -> Schema {
    match table {
        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int32, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int32, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int32, false),
            Field::new("l_partkey", DataType::Int32, false),
            Field::new("l_suppkey", DataType::Int32, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int32, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int32, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int32, false),
            Field::new("o_custkey", DataType::Int32, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_priority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_suppkey", DataType::Int32, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int32, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int32, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int32, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),
        _ => {
            panic!("No such schema available for {table}")
        }
    }
}
//the entries coming from different operators for store
pub enum Entry {
    Empty,
    Batch(Vec<Arc<RecordBatch>>),
    HashMap(JoinLeftData),
}

pub struct PhysicalToPhysicalVisitor {
    pub pipelines: Vec<Pipeline>,
    pub next_pipeline_number: usize,
    pub store: Arc<RefCell<HashMap<usize, Entry>>>,
    pub stack: Vec<usize>,
}
impl PhysicalToPhysicalVisitor {
    pub fn new() -> Self {
        PhysicalToPhysicalVisitor {
            pipelines: vec![],
            next_pipeline_number: 0,
            store: Arc::new(RefCell::new(HashMap::new())),
            stack: vec![],
        }
    }
}

impl ExecutionPlanVisitor for PhysicalToPhysicalVisitor {
    type Error = ();
    fn pre_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        Ok(true)
    }

    fn post_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let node = plan.as_any();

        if let Some(operator) = node.downcast_ref::<datafusion::datasource::physical_plan::parquet::ParquetExec>() {
            let filepath = operator.base_config().file_groups[0][0].object_meta.location.as_ref();
            //add the / for the root (for some reason it's absent)
            let filepath = format!("/{filepath}");
            let scan = ScanOperator::new(filepath.as_str(), operator.schema(), operator.predicate().cloned());
            let scan : Box<dyn Source> = Box::new(scan);
            let scan = Some(scan);
            self.pipelines.push(Pipeline::new());
            self.pipelines.last_mut().unwrap().source_operator = scan;
            self.stack.push(self.next_pipeline_number);
            self.next_pipeline_number +=1;
        }
        else if let Some(filter)  = node.downcast_ref::<datafusion::physical_plan::filter::FilterExec>(){
            let predicate = filter.predicate().clone();
            let filter_operator = Box::new(FilterOperator::new(predicate, filter.schema()));
            self.pipelines.last_mut().unwrap().operators.push(filter_operator);
        }
        else if let Some(projection)  = node.downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>(){
            let schema = match &self.pipelines.last().unwrap().operators.last() {
                None => {
                    self.pipelines.last().unwrap().source_operator.as_ref().unwrap().as_ref().schema()
                }
                Some(opearator) => {
                    opearator.schema()
                }
            };
            let projection = ProjectionOperator::new(schema, projection.expr().to_vec());
            let projection : Box<dyn IntermediateOperator> = Box::new(projection);
            self.pipelines.last_mut().unwrap().operators.push(projection);
        }
        else if let Some(_coalesce)  = node.downcast_ref::<datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec>(){
            //println!("Visiting a coalesce batches");
        }
        else if let Some(_coalesce)  = node.downcast_ref::<datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec>(){
            //println!("Visiting a coalesce partitions");
        }
        else if let Some(_repartition)  = node.downcast_ref::<datafusion::physical_plan::repartition::RepartitionExec>(){
            //println!("Visiting a repartition");
        }
        else if let Some(operator)  = node.downcast_ref::<datafusion::physical_plan::aggregates::AggregateExec>(){
            let schema = match &self.pipelines.last().unwrap().operators.last() {
                None => {
                    self.pipelines.last().unwrap().source_operator.as_ref().unwrap().as_ref().schema()
                }
                Some(opearator) => {
                    opearator.schema()
                }
            };
            match operator.mode() {
                AggregateMode::Partial => {return Ok(true);}
                _ => {}
            }
            let aggr_expr : Vec<_> = operator.aggr_expr().iter().cloned().collect();
            let group_by: Vec<_> = operator.group_by().expr().iter().cloned().collect();
            let aggregate_op : Box<dyn Sink>= Box::new(HashAggregateOperator::new(schema, aggr_expr, group_by));
            self.pipelines.last_mut().unwrap().sink_operator = Some(aggregate_op);

            self.pipelines.push(Pipeline::new());
            let scan : Box <dyn Source> = Box::new(ScanIntermediatesOperator::new(self.next_pipeline_number-1, operator.schema(), Arc::clone(&self.store)));
            self.stack.pop().unwrap();
            self.stack.push(self.next_pipeline_number);
            self.next_pipeline_number +=1;
            self.pipelines.last_mut().unwrap().source_operator = Some(scan);

        }
        else if let Some(hash_join)  = node.downcast_ref::<datafusion::physical_plan::joins::HashJoinExec>(){
            let mut build_expr = Vec::new();
            let mut probe_expr = Vec::new();
            for expr in hash_join.on(){
               build_expr.push(expr.0.clone());
               probe_expr.push(expr.1.clone());
            }
            let current = self.stack.pop().unwrap();
            let build_side_num = self.stack.pop().unwrap();
            self.stack.push(current);


            let hash_build: Box<dyn Sink> = Box::new(HashJoinBuildOperator::new(build_expr, hash_join.left.schema()));
            self.pipelines[build_side_num].sink_operator = Some(hash_build);

            let hash_probe_operator: Box<dyn IntermediateOperator> = Box::new(HashJoinProbeOperator::new(probe_expr,hash_join.right.schema(), hash_join.left.schema(), Arc::clone(&self.store), build_side_num));
            self.pipelines.last_mut().unwrap().operators.push(hash_probe_operator);
        }
        else if let Some(operator)  = node.downcast_ref::<datafusion::physical_plan::sorts::sort::SortExec>(){
            let sort_expr  : Vec<_> = operator.expr().iter().cloned().collect();
            let sort : Box <dyn Sink> = Box::new(SortOperator::new(sort_expr));
            self.pipelines.last_mut().unwrap().sink_operator = Some(sort);

            self.pipelines.push(Pipeline::new());
            let scan : Box <dyn Source> = Box::new(ScanIntermediatesOperator::new(self.next_pipeline_number-1, operator.schema(), Arc::clone(&self.store)));
            self.stack.pop().unwrap();
            self.stack.push(self.next_pipeline_number);
            self.next_pipeline_number +=1;
            self.pipelines.last_mut().unwrap().source_operator = Some(scan);
        }

        else if let Some(operator) = node.downcast_ref::<PlaceholderRowExec>(){
            let placeholer = PlaceholderOperator::new(operator.schema());
            let placeholer : Box<dyn Source> = Box::new(placeholer);
            let placeholer = Some(placeholer);
            self.pipelines.push(Pipeline::new());
            self.pipelines.last_mut().unwrap().source_operator = placeholer;
            self.stack.push(self.next_pipeline_number);
            self.next_pipeline_number +=1;
        }
        else if let Some(operator)  = node.downcast_ref::<datafusion::physical_plan::limit::GlobalLimitExec>(){
            let num_fetch = operator.fetch().unwrap();
            let limit : Box <dyn Sink> = Box::new(LimitOperator::new(num_fetch));
            self.pipelines.last_mut().unwrap().sink_operator = Some(limit);

            self.pipelines.push(Pipeline::new());
            let scan : Box <dyn Source> = Box::new(ScanIntermediatesOperator::new(self.next_pipeline_number-1, operator.schema(), Arc::clone(&self.store)));
            self.stack.pop().unwrap();
            self.stack.push(self.next_pipeline_number);
            self.next_pipeline_number +=1;
            self.pipelines.last_mut().unwrap().source_operator = Some(scan);
        }

        else {
            println!("node is {:#?}",plan);
            panic!("Visit not implemented for this node");
        }
        return Ok(true);
    }
}
