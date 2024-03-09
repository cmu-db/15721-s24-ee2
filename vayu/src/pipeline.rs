use crate::operators::filter::FilterOperator;
use crate::operators::scan::ScanOperator;
use arrow::datatypes::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::physical_plan::CsvExec;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use futures::stream::Stream;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
#[derive(Debug)]
pub struct DummyFeeder {
    pub batch: Option<RecordBatch>,
}

impl Stream for DummyFeeder {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &self.batch {
            Some(value) => Poll::Ready(Some(Ok(value.clone()))),
            None => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        (100, Some(100))
    }
}

impl RecordBatchStream for DummyFeeder {
    fn schema(&self) -> SchemaRef {
        self.batch.as_ref().unwrap().schema().clone()
    }
}
#[derive(Debug)]
pub struct DummyExec {
    schema: SchemaRef,
}

impl DisplayAs for DummyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DummyExec")
            }
        }
    }
}

impl ExecutionPlan for DummyExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("NoOpExecutionPlan::with_new_children");
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // use functions from futures crate convert the batch into a stream
        Ok(Box::pin(DummyFeeder { batch: None }))
    }
}

pub struct Pipeline {
    pub source: Option<SendableRecordBatchStream>,
    pub sink: Option<SendableRecordBatchStream>,
    pub operators: Vec<Box<dyn IntermediateOperator>>,
    pub state: PipelineState,
}
pub struct PipelineState {
    pub schema: Option<Arc<Schema>>,
}
impl Pipeline {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Pipeline {
        let mut pipeline = Pipeline {
            source: None,
            sink: None,
            operators: vec![],
            state: PipelineState { schema: None },
        };
        make_pipeline(&mut pipeline, plan);
        pipeline
    }
}

fn make_pipeline(pipeline: &mut Pipeline, plan: Arc<dyn ExecutionPlan>) {
    let p = plan.as_any();
    let ctx: Arc<SessionContext> = Arc::new(SessionContext::new());
    let context = ctx.task_ctx();
    if let Some(exec) = p.downcast_ref::<CsvExec>() {
        let mut stream = plan.execute(0, context).unwrap();
        pipeline.source = Some(stream);
        return;
    }

    if let Some(exec) = p.downcast_ref::<FilterExec>() {
        make_pipeline(pipeline, exec.input().clone());
        let tt = Box::new(FilterOperator::new(exec.predicate().clone()));
        pipeline.operators.push(tt);

        return;
    }
    if let Some(exec) = p.downcast_ref::<RepartitionExec>() {
        make_pipeline(pipeline, exec.input().clone());
        return;
        // pipeline.operators.push(FilterOperator::new())
    }
    if let Some(exec) = p.downcast_ref::<CoalesceBatchesExec>() {
        make_pipeline(pipeline, exec.input().clone());
        return;
        // pipeline.operators.push(FilterOperator::new())
    }
}
pub trait PhysicalOperator {
    fn name(&self) -> String;
}

//Operators that implement Sink trait consume data
pub trait Sink: PhysicalOperator {
    // Sink method is called constantly with new input, as long as new input is available
    fn sink(&self, chunk: &mut RecordBatch) -> bool;
}

//Operators that implement Source trait emit data
pub trait Source: PhysicalOperator {
    fn get_data(&mut self) -> Option<RecordBatch>;
}

//Physical operators that implement the Operator trait process data
pub trait IntermediateOperator: PhysicalOperator {
    //takes an input chunk and outputs another chunk
    //for example in Projection Operator we appply the expression to the input chunk and produce the output chunk
    fn execute(&self, input: &RecordBatch) -> Result<RecordBatch>;
}
