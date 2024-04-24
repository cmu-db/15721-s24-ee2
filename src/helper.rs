// This file contains some helper functions for tpch queries
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};
use crate::operator::scan::ScanOperator;
use crate::parallel::pipeline::Pipeline;
use crate::physical_operator::Source;

//return the schema of the region table in TPCH
pub fn tpch_schema(table: &str) -> Schema {
    match table {
        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int32, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int32, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15,2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int32, false),
            Field::new("l_partkey", DataType::Int32, false),
            Field::new("l_suppkey", DataType::Int32, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15,2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15,2), false),
            Field::new("l_discount", DataType::Decimal128(15,2), false),
            Field::new("l_tax", DataType::Decimal128(15,2), false),
            Field::new("l_returnflag", DataType::Utf8,false),
            Field::new("l_linestatus", DataType::Utf8,false),
            Field::new("l_shipdate", DataType::Date32,false),
            Field::new("l_commitdate", DataType::Date32,false),
            Field::new("l_receiptdate", DataType::Date32,false),
            Field::new("l_shipinstruct", DataType::Utf8,false),
            Field::new("l_shipmode", DataType::Utf8,false),
            Field::new("l_comment", DataType::Utf8,false),
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
            Field::new("o_totalprice", DataType::Decimal128(15,2), false),
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
            Field::new("p_retailprice", DataType::Decimal128(15,2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_suppkey", DataType::Int32, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15,2), false),
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
            Field::new("s_acctbal", DataType::Decimal128(15,2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),
        _ => {
            panic!("No such schema available for {table}")
        }
    }
}
pub struct MyVisitor{
    pub pipeline: Pipeline,
}
impl MyVisitor{
    pub fn new() -> Self{
        MyVisitor{
            pipeline : Pipeline::new()
        }
    }
}


impl ExecutionPlanVisitor for MyVisitor {
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
            let scan = ScanOperator::new(operator.schema(), filepath.as_str());
            let scan : Box<dyn Source> = Box::new(scan);
            let scan = Some(scan);
            self.pipeline.source_operator = scan;
        }
        else if let Some(_filter)  = node.downcast_ref::<datafusion::physical_plan::filter::FilterExec>(){
            println!("Visiting a filter");
        }
        else if let Some(_projection)  = node.downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>(){
            println!("Visiting a projection");
        }
        else if let Some(_coalesce)  = node.downcast_ref::<datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec>(){
            println!("Visiting a coalesce");
        }
        else {
            panic!("Visit not implemented for this node");
        }
        return Ok(true);
    }
}