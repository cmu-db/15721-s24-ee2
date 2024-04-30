use crate::common::enums::operator_result_type::SourceResultType;
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::helper::Entry;
use crate::physical_operator::{PhysicalOperator, Source};
use ahash::HashMap;
use datafusion::arrow::array::{AsArray, BooleanArray, RecordBatch};
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowPredicate, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowFilter,
};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::utils::reassign_predicate_columns;
use datafusion::physical_plan::PhysicalExpr;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

pub struct ScanOperator {
    reader: ParquetRecordBatchReader,
    pub projected_schema: SchemaRef,
}
impl ScanOperator {
    pub fn new(
        file_path: &str,
        projected_schema: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> ScanOperator {
        let file = File::open(&file_path).unwrap();
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        // projection mask
        let file_schema = builder.schema();
        let mut project_schema_indices = Vec::with_capacity(file_schema.fields().len());
        for (file_idx, file_field) in file_schema.fields().iter().enumerate() {
            if let Some((_, table_field)) = projected_schema.fields().find(file_field.name()) {
                match can_cast_types(file_field.data_type(), table_field.data_type()) {
                    true => {
                        project_schema_indices.push(file_idx);
                    }
                    false => {
                        assert!(false);
                    } // Projection type mismatch
                }
            }
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), project_schema_indices);

        // optional filter
        if let Some(predicate) = predicate {
            let exprs = datafusion::physical_expr::split_conjunction(&predicate);
            let mut filters: Vec<Box<dyn ArrowPredicate>> = vec![];
            for expr in exprs {
                let mut project_indices_set: BTreeSet<usize> = BTreeSet::default();
                required_column_indices_of_expr(file_schema, expr, &mut project_indices_set);
                let project_indices = project_indices_set.into_iter().collect();
                let filter = MyArrowPredicate::try_new(
                    expr.clone(),
                    project_indices,
                    file_schema,
                    builder.metadata(),
                );
                filters.push(Box::new(filter));
            }
            builder = builder.with_row_filter(RowFilter::new(filters));
        }

        let reader = builder.with_projection(mask).build().unwrap();
        ScanOperator {
            reader,
            projected_schema,
        }
    }
}

impl Source for ScanOperator {
    fn get_data(&mut self) -> SourceResultType {
        let batch = self.reader.next();
        match batch {
            Some(batch) => {
                return SourceResultType::HaveMoreOutput(Arc::new(batch.unwrap()));
            }
            None => return SourceResultType::Finished,
        }
    }
}

impl PhysicalOperator for ScanOperator {
    fn schema(&self) -> Arc<Schema> {
        self.projected_schema.clone()
    }
    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Scan
    }
}

// Modified from datafusion/src/datasource/physical_plan/parquet/row_filter.rs
struct MyArrowPredicate {
    physical_expr: Arc<dyn PhysicalExpr>,
    projection_mask: ProjectionMask,
    projection: Vec<usize>,
}

impl MyArrowPredicate {
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        project_indices: Vec<usize>,
        schema: &Schema,
        metadata: &ParquetMetaData,
    ) -> Self {
        let schema = Arc::new(schema.project(&project_indices).unwrap());
        let physical_expr = reassign_predicate_columns(expr, &schema, true).unwrap();

        // ArrowPredicate::evaluate is passed columns in the order they appear in the file
        // If the predicate has multiple columns, we therefore must project the columns based
        // on the order they appear in the file
        let projection = match project_indices.len() {
            0 | 1 => vec![],
            _ => MyArrowPredicate::remap_projection(&project_indices),
        };

        Self {
            physical_expr,
            projection,
            projection_mask: ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                project_indices,
            ),
        }
    }

    fn remap_projection(src: &[usize]) -> Vec<usize> {
        let len = src.len();

        // Compute the column mapping from projected order to file order
        // i.e. the indices required to sort projected schema into the file schema
        //
        // e.g. projection: [5, 9, 0] -> [2, 0, 1]
        let mut sorted_indexes: Vec<_> = (0..len).collect();
        sorted_indexes.sort_unstable_by_key(|x| src[*x]);

        // Compute the mapping from schema order to projected order
        // i.e. the indices required to sort file schema into the projected schema
        //
        // Above we computed the order of the projected schema according to the file
        // schema, and so we can use this as the comparator
        //
        // e.g. sorted_indexes [2, 0, 1] -> [1, 2, 0]
        let mut projection: Vec<_> = (0..len).collect();
        projection.sort_unstable_by_key(|x| sorted_indexes[*x]);
        projection
    }
}

impl ArrowPredicate for MyArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        let batch = match self.projection.is_empty() {
            true => batch,
            false => batch.project(&self.projection)?,
        };

        // scoped timer updates on drop
        match self
            .physical_expr
            .evaluate(&batch)
            .and_then(|v| v.into_array(batch.num_rows()))
        {
            Ok(array) => {
                let bool_arr = array.as_boolean().to_owned();
                Ok(bool_arr)
            }
            Err(e) => Err(ArrowError::ComputeError(format!(
                "Error evaluating filter predicate: {e:?}"
            ))),
        }
    }
}

fn required_column_indices_of_expr(
    file_schema: &SchemaRef,
    node: &Arc<dyn PhysicalExpr>,
    indices: &mut BTreeSet<usize>,
) {
    if let Some(column) = node
        .as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::Column>()
    {
        if let Ok(idx) = file_schema.index_of(column.name()) {
            indices.insert(idx);
        } else {
            panic!();
        }
    }
    for child in node.as_ref().children() {
        required_column_indices_of_expr(file_schema, &child, indices);
    }
}

pub struct ScanIntermediatesOperator {
    pub schema: SchemaRef,
    pub uuid: usize,
    pub store: Arc<RefCell<HashMap<usize, Entry>>>,
    entry: Entry,
}

impl ScanIntermediatesOperator {
    pub fn new(uuid: usize, schema: SchemaRef, store: Arc<RefCell<HashMap<usize, Entry>>>) -> Self {
        Self {
            uuid,
            schema,
            store,
            entry: Entry::empty,
        }
    }
}

impl PhysicalOperator for ScanIntermediatesOperator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::ScanIntermediates
    }
}

impl Source for ScanIntermediatesOperator {
    fn get_data(&mut self) -> SourceResultType {
        match &mut self.entry {
            Entry::batch(_) => {}
            Entry::empty => {
                let mut binding = self.store.borrow_mut();
                self.entry = binding.remove(&self.uuid).unwrap();
            }
            _ => panic!("bug"),
        }
        if let Entry::batch(ref mut batch) = self.entry {
            let b = batch.pop();
            match b {
                None => SourceResultType::Finished,
                Some(batch) => {
                    return SourceResultType::HaveMoreOutput(batch);
                }
            }
        } else {
            panic!("Bug in source operator")
        }
    }
}
