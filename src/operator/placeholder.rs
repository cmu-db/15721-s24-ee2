use std::cell::RefCell;
use crate::common::enums::operator_result_type::SourceResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::physical_operator::{PhysicalOperator, Source};
use datafusion::arrow::array::{ArrayRef, AsArray, BooleanArray, NullArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowFilter
};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::utils::reassign_predicate_columns;
use datafusion::physical_plan::PhysicalExpr;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::sync::Arc;
use ahash::RandomState;

pub struct PlaceholderOperator {
    output_schema : Arc<Schema>,
    state: bool,
}
impl PlaceholderOperator {
    pub fn new(output_schema: Arc<Schema>) -> Self{
        Self{
            output_schema,
            state: false,
        }
    }
}

impl Source for PlaceholderOperator {
    fn get_data(&mut self) -> SourceResultType {
        if self.state {
            return SourceResultType::Finished;
        }
        else {
                let n_field = self.output_schema.fields.len();
                let data = RecordBatch::try_new_with_options(
                    Arc::new(Schema::new(
                        (0..n_field)
                            .map(|i| {
                                Field::new(format!("placeholder_{i}"), DataType::Null, true)
                            })
                            .collect::<Fields>(),
                    )),
                    (0..n_field)
                        .map(|_i| {
                            let ret: ArrayRef = Arc::new(NullArray::new(1));
                            ret
                        })
                        .collect(),
                    // Even if column number is empty we can generate single row.
                    &RecordBatchOptions::new().with_row_count(Some(1)),
                ).unwrap();
            self.state = true;
            return SourceResultType::HaveMoreOutput(Arc::new(data));
        }
    }
}

impl PhysicalOperator for PlaceholderOperator {
    fn schema(&self) -> Arc<Schema> {
        self.output_schema.clone()
    }
    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Placeholder
    }
}