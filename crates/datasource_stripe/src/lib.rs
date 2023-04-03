pub mod errors;

use crate::errors::{Result, StripeError};
use async_channel::Receiver;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::arrow::array::{
    BooleanBuilder, StringArray, StringBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::ipc::reader::StreamReader as ArrowStreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datasource_common::util;
use futures::{Stream, StreamExt};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{self, Write};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use stripe::{Client, ListSubscriptions, Subscription};

#[derive(Debug)]
struct StripeApiObject {
    object: &'static str,
    schema: Arc<ArrowSchema>,
}

static STRIPE_OBJECT_SUBSCRIPTIONS: Lazy<StripeApiObject> = Lazy::new(|| StripeApiObject {
    object: "subscriptions",
    schema: Arc::new(ArrowSchema::new(vec![
        Field::new("id".to_string(), DataType::Utf8, false),
        Field::new("customer_id".to_string(), DataType::Utf8, false),
    ])),
});

impl StripeApiObject {
    fn from_str(s: &str) -> Result<&'static StripeApiObject> {
        Ok(match s {
            "subscriptions" => &STRIPE_OBJECT_SUBSCRIPTIONS,
            other => return Err(StripeError::InvalidApiObject(other.to_string())),
        })
    }
}

pub struct StripeAccessor {
    client: Arc<Client>,
}

impl StripeAccessor {
    pub async fn connect(api_key: &str) -> Result<Self> {
        let client = Arc::new(Client::new(api_key).with_app_info(
            "GlareDB".to_string(),
            Some(buildenv::git_tag().to_string()),
            Some("https://glaredb.com".to_string()),
        ));

        Ok(StripeAccessor { client })
    }

    pub async fn into_table_provider(self, obj: &str) -> Result<StripeTableProvider> {
        let obj = StripeApiObject::from_str(obj)?;
        Ok(StripeTableProvider {
            obj,
            client: self.client,
        })
    }
}

pub struct StripeTableProvider {
    client: Arc<Client>,
    obj: &'static StripeApiObject,
}

#[async_trait]
impl TableProvider for StripeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.obj.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DatafusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(proj) => Arc::new(self.schema().project(proj)?),
            None => self.schema(),
        };

        Ok(Arc::new(StripeExec {
            client: self.client.clone(),
            arrow_schema: projected_schema,
            obj: self.obj,
        }))
    }
}

struct StripeExec {
    client: Arc<Client>,
    arrow_schema: Arc<ArrowSchema>,
    obj: &'static StripeApiObject,
}

impl ExecutionPlan for StripeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for StripeExec".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(
            StripeStream::new(self.schema(), self.client.clone(), self.obj)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StripeExec: obj={}", self.obj.object,)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl fmt::Debug for StripeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StripeExec")
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

struct StripeStream {
    arrow_schema: Arc<ArrowSchema>,
    inner: Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>,
}

impl StripeStream {
    fn new(
        schema: Arc<ArrowSchema>,
        client: Arc<Client>,
        obj: &'static StripeApiObject,
    ) -> Result<Self> {
        let stream_schema = schema.clone();
        let stream = match obj.object {
            "subscriptions" => stream! {
                let mut inner = match Self::subscriptions_stream(client.as_ref(), stream_schema).await {
                    Ok(inner) => inner,
                    Err(e) => {
                        yield Err(DataFusionError::External(Box::new(e)));
                        return;
                    }
                };
                while let Some(result) = inner.next().await {
                    yield result;
                }
            },
            other => unimplemented!(),
        };

        Ok(StripeStream {
            arrow_schema: schema,
            inner: stream.boxed(),
        })
    }

    async fn subscriptions_stream(
        client: &Client,
        schema: Arc<ArrowSchema>,
    ) -> Result<Pin<Box<dyn Stream<Item = DatafusionResult<RecordBatch>> + Send>>> {
        let params = ListSubscriptions::new();
        let subs = Subscription::list(client, &params)
            .await?
            .paginate(params)
            .stream(client)
            .chunks(100)
            .map(move |results| {
                let subs = match results.into_iter().collect::<Result<Vec<_>, _>>() {
                    Ok(subs) => subs,
                    Err(e) => return Err(DataFusionError::External(Box::new(e))),
                };

                let mut id = StringBuilder::with_capacity(subs.len(), 16);
                let mut customer_id = StringBuilder::with_capacity(subs.len(), 16);
                for sub in subs {
                    id.append_value(sub.id);
                    customer_id.append_value(sub.customer.id());
                }

                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(id.finish()), Arc::new(customer_id.finish())],
                )
                .map_err(DataFusionError::from)
            });

        Ok(subs.boxed())
    }
}

impl Stream for StripeStream {
    type Item = DatafusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for StripeStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }
}
