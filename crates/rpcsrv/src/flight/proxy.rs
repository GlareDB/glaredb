use std::borrow::Cow;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action,
    ActionType,
    Criteria,
    Empty,
    FlightData,
    FlightDescriptor,
    FlightInfo,
    HandshakeRequest,
    HandshakeResponse,
    PutResult,
    SchemaResult,
    Ticket,
};
use base64::prelude::*;
use futures::stream::BoxStream;
use futures::StreamExt;
use proxyutil::cloudauth::{AuthParams, CloudAuthenticator, ProxyAuthenticator, ServiceProtocol};
use proxyutil::metadata_constants::{DB_NAME_KEY, ORG_KEY};
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use super::handler::{FLIGHTSQL_DATABASE_HEADER, FLIGHTSQL_GCS_BUCKET_HEADER};
use crate::errors::{Result, RpcsrvError};
use crate::proxy::{ProxiedRequestStream, ProxyHandler};
use crate::util::ConnKey;

pub type CloudFlightProxyHandler = ProxyHandler<CloudAuthenticator, FlightServiceClient<Channel>>;

impl CloudFlightProxyHandler {
    async fn connect(&self, meta: &mut MetadataMap) -> Result<FlightServiceClient<Channel>> {
        let params = Self::auth_params_from_metadata(meta)?;
        let details = self.authenticator.authenticate(params).await?;
        meta.insert(
            FLIGHTSQL_GCS_BUCKET_HEADER,
            details.gcs_storage_bucket.try_into()?,
        );
        meta.insert(FLIGHTSQL_DATABASE_HEADER, details.database_id.try_into()?);
        let key = ConnKey {
            ip: details.ip.clone(),
            port: details.port.clone(),
        };
        // Already have a grpc connection,
        if let Some(conn) = self.conns.get(&key) {
            let conn = conn.clone();
            return Ok(conn);
        }

        let url = format!("http://{}:{}", key.ip, key.port);
        let channel = Endpoint::new(url)?
            .tcp_keepalive(Some(Duration::from_secs(600)))
            .tcp_nodelay(true)
            .keep_alive_while_idle(true)
            .connect()
            .await?;

        let client = FlightServiceClient::new(channel);

        self.conns.insert(key, client.clone());

        Ok(client)
    }

    fn auth_params_from_metadata(meta: &MetadataMap) -> Result<AuthParams> {
        let basic = "Basic ";
        let authorization = meta
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("authorization field not present"))?
            .to_str()
            .map_err(|_| Status::internal("authorization not parsable"))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {authorization}"
            )))?;
        }
        let base64 = &authorization[basic.len()..];
        let bytes = BASE64_STANDARD
            .decode(base64)
            .map_err(|_| Status::internal("authorization not decodable"))?;
        let s =
            String::from_utf8(bytes).map_err(|_| Status::internal("authorization not parsable"))?;
        let parts: Vec<_> = s.split(':').collect();
        let (user, password) = match parts.as_slice() {
            [user, pass] => (user.to_string(), pass.to_string()),
            _ => Err(Status::invalid_argument(
                "Invalid authorization header".to_string(),
            ))?,
        };

        fn get_val<'b>(key: &'static str, meta: &'b MetadataMap) -> Result<&'b str> {
            let val = meta
                .get(key)
                .ok_or(RpcsrvError::MissingAuthKey(key))?
                .to_str()?;
            Ok(val)
        }

        let db_name = get_val(DB_NAME_KEY, meta)?;
        let org = get_val(ORG_KEY, meta)?;

        Ok(AuthParams {
            user: Cow::Owned(user),
            password: Cow::Owned(password),
            db_name: Cow::Borrowed(db_name),
            org: Cow::Borrowed(org),
            service: ServiceProtocol::RpcSrv,
        })
    }
}

#[tonic::async_trait]
impl FlightService for CloudFlightProxyHandler {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        mut request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        let req = request.into_inner();
        let req = ProxiedRequestStream::new(req);
        let res = client.handshake(req).await?;
        let res = res.into_inner();
        Ok(Response::new(res.boxed()))
    }

    async fn list_flights(
        &self,
        mut request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        let res = client.list_flights(request).await?;
        let res = res.into_inner();
        Ok(Response::new(res.boxed()))
    }

    async fn get_flight_info(
        &self,
        mut request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        client.get_flight_info(request).await
    }

    async fn get_schema(
        &self,
        mut request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        client.get_schema(request).await
    }

    async fn do_get(
        &self,
        mut request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        let res = client.do_get(request).await?;
        let res = res.into_inner();

        Ok(Response::new(res.boxed()))
    }

    async fn do_put(
        &self,
        mut request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        let req = request.into_inner();
        let req = ProxiedRequestStream::new(req);
        let res = client.do_put(req).await?;
        let res = res.into_inner();
        Ok(Response::new(res.boxed()))
    }

    async fn do_action(
        &self,
        mut request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        let res = client.do_action(request).await?;
        let res = res.into_inner();
        Ok(Response::new(res.boxed()))
    }

    async fn list_actions(
        &self,
        mut request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        let res = client.list_actions(request).await?;
        let res = res.into_inner();
        Ok(Response::new(res.boxed()))
    }

    async fn do_exchange(
        &self,
        mut request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let meta = request.metadata_mut();
        let mut client = self.connect(meta).await?;
        let req = request.into_inner();
        let req = ProxiedRequestStream::new(req);
        let res = client.do_exchange(req).await?;
        let res = res.into_inner();
        Ok(Response::new(res.boxed()))
    }
}
