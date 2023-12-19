use crate::errors::{Result, RpcsrvError};
use crate::proxy::{ProxiedRequestStream, ProxyHandler};
use crate::util::ConnKey;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use base64::prelude::*;

use futures::stream::BoxStream;
use futures::StreamExt;
use proxyutil::cloudauth::{
    AuthParams, CloudAuthenticator, DatabaseDetails, ProxyAuthenticator, ServiceProtocol,
};
use proxyutil::metadata_constants::{DB_NAME_KEY, ORG_KEY};
use std::borrow::Cow;
use std::time::Duration;
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Endpoint},
    Status,
};
use tonic::{Request, Response, Streaming};

pub type CloudFlightProxyHandler = ProxyHandler<CloudAuthenticator, FlightServiceClient<Channel>>;

impl CloudFlightProxyHandler {
    async fn connect(
        &self,
        meta: &MetadataMap,
    ) -> Result<(DatabaseDetails, FlightServiceClient<Channel>)> {
        let params = Self::auth_params_from_metadata(meta)?;
        let details = self.authenticator.authenticate(params).await?;

        let key = ConnKey {
            ip: details.ip.clone(),
            port: details.port.clone(),
        };
        // Already have a grpc connection,
        if let Some(conn) = self.conns.get(&key) {
            let conn = conn.clone();
            return Ok((details, conn));
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

        Ok((details, client))
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
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let req = request.into_inner();
        let req = ProxiedRequestStream::new(req);
        let res = client.handshake(req).await?;
        let res = res.into_inner();
        let mut res = Response::new(res.boxed());
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let res = client.list_flights(request).await?;
        let res = res.into_inner();
        let mut res = Response::new(res.boxed());
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let mut res = client.get_flight_info(request).await?;
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let mut res = client.get_schema(request).await?;
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let res = client.do_get(request).await?;
        let res = res.into_inner();
        let mut res = Response::new(res.boxed());
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let req = request.into_inner();
        let req = ProxiedRequestStream::new(req);
        let res = client.do_put(req).await?;
        let res = res.into_inner();
        let mut res = Response::new(res.boxed());
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let res = client.do_action(request).await?;
        let res = res.into_inner();
        let mut res = Response::new(res.boxed());
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let res = client.list_actions(request).await?;
        let res = res.into_inner();
        let mut res = Response::new(res.boxed());
        res.extensions_mut().insert(details);
        Ok(res)
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let meta = request.metadata();
        let (details, mut client) = self.connect(meta).await?;
        let req = request.into_inner();
        let req = ProxiedRequestStream::new(req);
        let res = client.do_exchange(req).await?;
        let res = res.into_inner();
        let mut res = Response::new(res.boxed());
        res.extensions_mut().insert(details);
        Ok(res)
    }
}
