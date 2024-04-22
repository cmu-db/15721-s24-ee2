use datafusion::physical_plan::ExecutionPlan;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, vec};

use arrow::array::{ArrayRef, UInt64Builder};
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::{arrow_datafusion_err, not_impl_err, DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::stream::Stream;

use futures::{FutureExt, StreamExt};
#[derive(Debug)]
pub struct DummyExec {
    cache: PlanProperties,
    statistics: Statistics,
    schema: Arc<Schema>,
}

impl DummyExec {
    pub fn new(cache: PlanProperties, statistics: Statistics, schema: Arc<Schema>) -> Self {
        DummyExec {
            cache,
            statistics,
            schema,
        }
    }
}
impl ExecutionPlan for DummyExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = DummyStream {
            schema: self.schema.clone(),
        };
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.statistics.clone())
    }
}

impl DisplayAs for DummyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "dummmyyy")?;
        Ok(())
    }
}
struct DummyStream {
    schema: Arc<Schema>,
}
impl Stream for DummyStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        return Poll::Pending;
    }
}

impl RecordBatchStream for DummyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
