use std::fmt::Debug;
use std::io::Cursor;

use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::{Field, Schema};
use rayexec_bullet::ipc::stream::{StreamReader, StreamWriter};
use rayexec_bullet::ipc::IpcConfig;
use rayexec_error::{OptionExt, RayexecError, Result, ResultExt};
use rayexec_io::http::reqwest::header::{HeaderValue, CONTENT_TYPE};
use rayexec_io::http::reqwest::{Method, Request, StatusCode};
use rayexec_io::http::{read_text, HttpClient, HttpResponse};
use rayexec_proto::prost::Message;
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use url::{Host, Url};
use uuid::Uuid;

use crate::database::DatabaseContext;
use crate::execution::intermediate::{IntermediatePipelineGroup, StreamId};
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedStatement;
use crate::proto::DatabaseProtoConv;

pub const API_VERSION: usize = 0;

pub const REMOTE_ENDPOINTS: Endpoints = Endpoints {
    healthz: "/healthz",
    rpc_hybrid_plan: "/rpc/v0/hybrid/plan",
    rpc_hybrid_execute: "/rpc/v0/hybrid/execute",
    rpc_hybrid_push: "/rpc/v0/hybrid/push_batch",
    rpc_hybrid_finalize: "/rpc/v0/hybrid/finalize",
    rpc_hybrid_pull: "/rpc/v0/hybrid/pull_batch",
};

#[derive(Debug)]
pub struct Endpoints {
    pub healthz: &'static str,
    pub rpc_hybrid_plan: &'static str,
    pub rpc_hybrid_execute: &'static str,
    pub rpc_hybrid_push: &'static str,
    pub rpc_hybrid_finalize: &'static str,
    pub rpc_hybrid_pull: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HybridPlanRequest {
    /// The sql statement we're planning.
    ///
    /// This includes partially bound items that reference the things in the
    /// resolved context.
    pub statement: ResolvedStatement,
    pub resolve_context: ResolveContext,
}

impl DatabaseProtoConv for HybridPlanRequest {
    type ProtoType = rayexec_proto::generated::hybrid::PlanRequest;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        let statement =
            serde_json::to_vec(&self.statement).context("failed to encode statement")?;
        Ok(Self::ProtoType {
            resolved_statement_json: statement,
            resolve_context: Some(self.resolve_context.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let statement = serde_json::from_slice(&proto.resolved_statement_json)
            .context("failed to decode statement")?;
        Ok(Self {
            statement,
            resolve_context: ResolveContext::from_proto_ctx(
                proto.resolve_context.required("resolve_context")?,
                context,
            )?,
        })
    }
}

#[derive(Debug)]
pub struct HybridPlanResponse {
    /// Id for the query.
    pub query_id: Uuid,
    /// Pipelines that should be executed on the client.
    pub pipelines: IntermediatePipelineGroup,
    /// Output schema for the query.
    pub schema: Schema,
}

impl DatabaseProtoConv for HybridPlanResponse {
    type ProtoType = rayexec_proto::generated::hybrid::PlanResponse;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            query_id: Some(self.query_id.to_proto()?),
            pipelines: Some(self.pipelines.to_proto_ctx(context)?),
            schema: Some(self.schema.to_proto()?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            query_id: Uuid::from_proto(proto.query_id.required("query_id")?)?,
            pipelines: IntermediatePipelineGroup::from_proto_ctx(
                proto.pipelines.required("pipelines")?,
                context,
            )?,
            schema: Schema::from_proto(proto.schema.required("schema")?)?,
        })
    }
}

#[derive(Debug)]
pub struct HybridExecuteRequest {
    pub query_id: Uuid,
}

impl ProtoConv for HybridExecuteRequest {
    type ProtoType = rayexec_proto::generated::hybrid::ExecuteRequest;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            query_id: Some(self.query_id.to_proto()?),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            query_id: Uuid::from_proto(proto.query_id.required("query_id")?)?,
        })
    }
}

#[derive(Debug)]
pub struct HybridExecuteResponse {}

impl ProtoConv for HybridExecuteResponse {
    type ProtoType = rayexec_proto::generated::hybrid::ExecuteResponse;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug)]
pub struct HybridPushRequest {
    pub stream_id: StreamId,
    pub partition: usize,
    pub batch: IpcBatch,
}

impl ProtoConv for HybridPushRequest {
    type ProtoType = rayexec_proto::generated::hybrid::PushRequest;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            stream_id: Some(self.stream_id.to_proto()?),
            partition: self.partition as u32,
            batch: Some(self.batch.to_proto()?),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            stream_id: StreamId::from_proto(proto.stream_id.required("stream_id")?)?,
            partition: proto.partition as usize,
            batch: IpcBatch::from_proto(proto.batch.required("batch")?)?,
        })
    }
}

#[derive(Debug)]
pub struct HybridPushResponse {}

impl ProtoConv for HybridPushResponse {
    type ProtoType = rayexec_proto::generated::hybrid::PushResponse;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug)]
pub struct HybridFinalizeRequest {
    pub stream_id: StreamId,
    pub partition: usize,
}

impl ProtoConv for HybridFinalizeRequest {
    type ProtoType = rayexec_proto::generated::hybrid::FinalizeRequest;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            stream_id: Some(self.stream_id.to_proto()?),
            partition: self.partition as u32,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            stream_id: StreamId::from_proto(proto.stream_id.required("stream_id")?)?,
            partition: proto.partition as usize,
        })
    }
}

#[derive(Debug)]
pub struct HybridFinalizeResponse {}

impl ProtoConv for HybridFinalizeResponse {
    type ProtoType = rayexec_proto::generated::hybrid::FinalizeResponse;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug)]
pub struct HybridPullRequest {
    pub stream_id: StreamId,
    pub partition: usize,
}

impl ProtoConv for HybridPullRequest {
    type ProtoType = rayexec_proto::generated::hybrid::PullRequest;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            stream_id: Some(self.stream_id.to_proto()?),
            partition: self.partition as u32,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            stream_id: StreamId::from_proto(proto.stream_id.required("stream_id")?)?,
            partition: proto.partition as usize,
        })
    }
}

#[derive(Debug)]
pub struct HybridPullResponse {
    pub status: PullStatus,
}

impl ProtoConv for HybridPullResponse {
    type ProtoType = rayexec_proto::generated::hybrid::PullResponse;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            status: Some(self.status.to_proto()?),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            status: PullStatus::from_proto(proto.status.required("status")?)?,
        })
    }
}

#[derive(Debug)]
pub enum PullStatus {
    Batch(IpcBatch),
    Pending,
    Finished,
}

impl ProtoConv for PullStatus {
    type ProtoType = rayexec_proto::generated::hybrid::PullStatus;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::hybrid::pull_status::Value;
        use rayexec_proto::generated::hybrid::PullStatusBatch;

        let value = match self {
            Self::Batch(batch) => Value::Batch(PullStatusBatch {
                batch: Some(batch.to_proto()?),
            }),
            Self::Pending => Value::Pending(Default::default()),
            Self::Finished => Value::Finished(Default::default()),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::hybrid::pull_status::Value;

        Ok(match proto.value.required("value")? {
            Value::Batch(batch) => {
                Self::Batch(IpcBatch::from_proto(batch.batch.required("batch")?)?)
            }
            Value::Pending(_) => Self::Pending,
            Value::Finished(_) => Self::Finished,
        })
    }
}

/// Wrapper around a batch that implements IPC encoding/decoding when converting
/// to protobuf.
#[derive(Debug)]
pub struct IpcBatch(pub Batch);

// TODO: Don't allocate vectors in this.
impl ProtoConv for IpcBatch {
    type ProtoType = rayexec_proto::generated::array::IpcStreamBatch;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        let buf = Vec::new();

        // Field names don't matter. A full schema is included just for
        // compatability with arrow ipc, but we only care about the data types.
        let schema = Schema::new(
            self.0
                .columns()
                .iter()
                .map(|c| Field::new("", c.datatype().clone(), true)),
        );

        let mut writer = StreamWriter::try_new(buf, &schema, IpcConfig {})?;
        writer.write_batch(&self.0)?;

        let buf = writer.into_writer();

        Ok(Self::ProtoType { ipc: buf })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        let mut reader = StreamReader::try_new(Cursor::new(proto.ipc), IpcConfig {})?;
        let batch = reader
            .try_next_batch()?
            .ok_or_else(|| RayexecError::new("Missing IPC batch"))?;

        if reader.try_next_batch()?.is_some() {
            return Err(RayexecError::new("Received too many IPC batches"));
        }

        Ok(Self(batch))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HybridConnectConfig {
    pub remote: Url,
}

impl HybridConnectConfig {
    pub fn try_from_connection_string(conn_str: &str) -> Result<Self> {
        // TODO: I don't know yet.
        let url = if Host::parse(conn_str).is_ok() {
            Url::parse(&format!("http://{conn_str}:80"))
        } else {
            Url::parse(conn_str)
        }
        .context("failed to parse connection string")?;

        Ok(Self { remote: url })
    }
}

// TODO: Borrowed bytes
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestEnvelope {
    #[serde(with = "serde_bytes")]
    pub encoded_msg: Vec<u8>,
}

// TODO: Borrowed bytes
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    #[serde(with = "serde_bytes")]
    pub encoded_msg: Vec<u8>,
}

#[derive(Debug)]
pub struct HybridClient<C: HttpClient> {
    url: Url,
    client: C,
}

impl<C: HttpClient> HybridClient<C> {
    pub fn new(client: C, conf: HybridConnectConfig) -> Self {
        HybridClient {
            url: conf.remote,
            client,
        }
    }

    pub async fn ping(&self) -> Result<()> {
        let url = self
            .url
            .join(REMOTE_ENDPOINTS.healthz)
            .context("failed to parse healthz url")?;
        let resp = self
            .client
            .do_request(Request::new(Method::GET, url))
            .await
            .context("failed to send request")?;

        if resp.status() != StatusCode::OK {
            return Err(RayexecError::new(format!(
                "Expected 200 from healthz, got {}",
                resp.status().as_u16()
            )));
        }

        Ok(())
    }

    // TODO: Is passing context here weird? Needed for properly encoding bind
    // data, and decoding the pipelines we get back.
    pub async fn remote_plan(
        &self,
        stmt: ResolvedStatement,
        resolve_context: ResolveContext,
        context: &DatabaseContext,
    ) -> Result<HybridPlanResponse> {
        let url = self
            .url
            .join(REMOTE_ENDPOINTS.rpc_hybrid_plan)
            .context("failed to parse plan endpoint")?;

        let msg = HybridPlanRequest {
            statement: stmt,
            resolve_context,
        };

        let encoded_msg = msg.to_proto_ctx(context)?.encode_to_vec();

        let mut req = Request::new(Method::POST, url);
        Self::put_json_body(&mut req, &RequestEnvelope { encoded_msg })?;

        let resp = self
            .client
            .do_request(req)
            .await
            .context("failed to send request")?;

        if resp.status() != StatusCode::OK {
            let text = read_text(resp).await?;
            return Err(RayexecError::new(text));
        }

        let resp: ResponseEnvelope = serde_json::from_slice(resp.bytes().await?.as_ref())
            .context("failed to deserialize response")?;

        let resp = HybridPlanResponse::from_proto_ctx(
            Message::decode(resp.encoded_msg.as_slice()).context("failed to decode message")?,
            context,
        )?;

        Ok(resp)
    }

    pub async fn remote_execute(&self, query_id: Uuid) -> Result<()> {
        let url = self
            .url
            .join(REMOTE_ENDPOINTS.rpc_hybrid_execute)
            .context("failed to parse plan endpoint")?;

        let msg = HybridExecuteRequest { query_id };

        let _resp: HybridExecuteResponse = self.do_request(msg, url).await?;

        Ok(())
    }

    pub async fn push(&self, stream_id: StreamId, partition: usize, batch: Batch) -> Result<()> {
        let url = self
            .url
            .join(REMOTE_ENDPOINTS.rpc_hybrid_push)
            .context("failed to parse push endpoint")?;

        let msg = HybridPushRequest {
            stream_id,
            partition,
            batch: IpcBatch(batch),
        };

        let _resp: HybridPushResponse = self.do_request(msg, url).await?;

        Ok(())
    }

    pub async fn finalize(&self, stream_id: StreamId, partition: usize) -> Result<()> {
        let url = self
            .url
            .join(REMOTE_ENDPOINTS.rpc_hybrid_finalize)
            .context("failed to parse finalize endpoint")?;

        let msg = HybridFinalizeRequest {
            stream_id,
            partition,
        };

        let _resp: HybridFinalizeResponse = self.do_request(msg, url).await?;

        Ok(())
    }

    pub async fn pull(&self, stream_id: StreamId, partition: usize) -> Result<PullStatus> {
        let url = self
            .url
            .join(REMOTE_ENDPOINTS.rpc_hybrid_pull)
            .context("failed to parse pull endpoint")?;

        let msg = HybridPullRequest {
            stream_id,
            partition,
        };

        let resp: HybridPullResponse = self.do_request(msg, url).await?;

        Ok(resp.status)
    }

    async fn do_request<M, R>(&self, msg: M, url: Url) -> Result<R>
    where
        M: ProtoConv,
        M::ProtoType: Message,
        R: ProtoConv,
        R::ProtoType: Message + Default,
    {
        let encoded_msg = msg.to_proto()?.encode_to_vec();

        let mut req = Request::new(Method::POST, url);
        Self::put_json_body(&mut req, &RequestEnvelope { encoded_msg })?;

        let resp = self
            .client
            .do_request(req)
            .await
            .context("failed to send request")?;

        if resp.status() != StatusCode::OK {
            // TODO: Structured return error.
            let text = read_text(resp).await?;
            return Err(RayexecError::new(text));
        }

        let resp: ResponseEnvelope = serde_json::from_slice(resp.bytes().await?.as_ref())
            .context("failed to deserialize response")?;

        let resp = R::from_proto(
            Message::decode(resp.encoded_msg.as_slice()).context("failed to decode message")?,
        )?;

        Ok(resp)
    }

    fn put_json_body(req: &mut Request, body: &impl Serialize) -> Result<()> {
        let body = serde_json::to_vec(body).context("failed to serialize body")?;
        *req.body_mut() = Some(body.into());
        req.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(())
    }
}
