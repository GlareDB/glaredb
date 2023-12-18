use crate::errors::{Result, RpcsrvError};
use crate::proxy::ProxyHandler;
use crate::util::ConnKey;
use arrow_flight::flight_service_client::FlightServiceClient;
use base64::prelude::*;

use dashmap::DashMap;
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

pub type FlightProxyHandler = ProxyHandler<CloudAuthenticator, FlightServiceClient<Channel>>;

impl FlightProxyHandler {
    pub fn new(authenticator: CloudAuthenticator) -> Self {
        FlightProxyHandler {
            authenticator,
            conns: DashMap::new(),
        }
    }

    /// Connect to a compute node.
    ///
    /// This will read authentication params from the metadata map, get
    /// deployment info from Cloud, then return a connection to the requested
    /// deployment+compute node.
    ///
    /// Database details will be returned alongside the client.
    async fn connect(
        &self,
        meta: &MetadataMap,
    ) -> Result<(DatabaseDetails, FlightServiceClient<Channel>)> {
        let params = Self::auth_params_from_metadata(meta)?;

        // TODO: We'll want to figure out long-lived auth sessions to avoid
        // needing to hit Cloud for every request (e.g. JWT). This isn't a
        // problem for pgsrv since a connections map one-to-one with sessions,
        // and we only need to authenticate at the beginning of the connection.
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

        // TLS is terminated at rpcsrv
        //
        // | Local |-gRPC-client <-- TLS --> gRPC-server-| rpcsrv | --> compute
        //
        // TODO: establish mTLS
        let url = format!("http://{}:{}", key.ip, key.port);
        let channel = Endpoint::new(url)?
            .tcp_keepalive(Some(Duration::from_secs(600)))
            .tcp_nodelay(true)
            .keep_alive_while_idle(true)
            .connect()
            .await?;
        let client = FlightServiceClient::new(channel);

        // May have raced, but that's not a concern here.
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

// #[async_trait]
// impl FlightSqlService for FlightProxyHandler {
//     type FlightService = ();

//     async fn do_handshake(
//         &self,
//         request: Request<Streaming<HandshakeRequest>>,
//     ) -> Result<
//         Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
//         Status,
//     > {
//         let metadata = request.metadata();
//         let (details, _) = self.connect(metadata).await?;
//         let payload = serde_json::to_vec(&details).unwrap();
//         let result = HandshakeResponse {
//             protocol_version: 0,
//             payload: payload.into(),
//         };
//         let result = Ok(result);
//         let output = futures::stream::iter(vec![result]);
//         let resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
//             Response::new(Box::pin(output));

//         Ok(resp)
//     }

//     async fn do_get_fallback(
//         &self,
//         request: Request<Ticket>,
//         message: Any,
//     ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
//         println!("do_get_fallback");
//         let metadata = request.metadata();
//         let (_, mut conn) = self.connect(metadata).await?;
//         let res = conn.do_get(request);

//         todo!()
//     }

//     async fn get_flight_info_statement(
//         &self,
//         _query: CommandStatementQuery,
//         _request: Request<FlightDescriptor>,
//     ) -> Result<Response<FlightInfo>, Status> {
//         println!("get_flight_info_statement");
//         todo!()
//     }

//     async fn get_flight_info_sql_info(
//         &self,
//         query: CommandGetSqlInfo,
//         req: Request<FlightDescriptor>,
//     ) -> Result<Response<FlightInfo>, Status> {
//         println!("get_flight_info_sql_info: {:?}", req);
//         println!("get_flight_info_sql_info: {:?}", query);
//         let (_, mut conn) = self.connect(req.metadata()).await?;
//         let res = conn.get_flight_info(req);
//         res.await
//     }

//     async fn do_action_create_prepared_statement(
//         &self,
//         query: ActionCreatePreparedStatementRequest,
//         req: Request<Action>,
//     ) -> Result<ActionCreatePreparedStatementResult, Status> {
//         todo!()
//         // res.await
//     }

//     async fn do_action_close_prepared_statement(
//         &self,
//         _query: ActionClosePreparedStatementRequest,
//         _req: Request<Action>,
//     ) -> Result<(), Status> {
//         todo!()
//     }

//     async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
// }
