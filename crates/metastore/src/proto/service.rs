#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchCatalogRequest {
    /// ID of the database catalog to fetch.
    #[prost(string, tag = "1")]
    pub db_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchCatalogResponse {
    #[prost(message, optional, tag = "1")]
    pub catalog: ::core::option::Option<super::catalog::DatabaseCatalog>,
}
/// The result of a catalog mutation request (add, drop, alter).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogMutateStatus {
    /// Status of the mutation.
    #[prost(enumeration = "catalog_mutate_status::Status", tag = "1")]
    pub status: i32,
    /// The current state of the catalog as witnessed by metastore.
    ///
    /// If the mutation was accepted, this catalog will included that mutation. If
    /// the mutation was rejected, this catalog will not have that mutation
    /// applied. In either case, this catalog should replace any stale catalog.
    #[prost(message, optional, tag = "2")]
    pub catalog: ::core::option::Option<super::catalog::DatabaseCatalog>,
}
/// Nested message and enum types in `CatalogMutateStatus`.
pub mod catalog_mutate_status {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum Status {
        Unknown = 0,
        /// Mutation applied.
        Applied = 1,
        /// Mutation rejected.
        Rejected = 2,
    }
    impl Status {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Status::Unknown => "UNKNOWN",
                Status::Applied => "APPLIED",
                Status::Rejected => "REJECTED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "APPLIED" => Some(Self::Applied),
                "REJECTED" => Some(Self::Rejected),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEntryRequest {
    #[prost(oneof = "create_entry_request::Request", tags = "1, 2")]
    pub request: ::core::option::Option<create_entry_request::Request>,
}
/// Nested message and enum types in `CreateEntryRequest`.
pub mod create_entry_request {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "1")]
        Schema(super::CreateSchema),
        #[prost(message, tag = "2")]
        View(super::CreateView),
    }
}
/// Create a new schema.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSchema {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Do not error if the schema already exists.
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
}
/// Create a new view.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateView {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub sql: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEntryResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<CatalogMutateStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropEntryRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropEntryResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<CatalogMutateStatus>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterEntryRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AlterEntryResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<CatalogMutateStatus>,
}
/// Generated client implementations.
pub mod metastore_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct MetastoreServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MetastoreServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MetastoreServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MetastoreServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            MetastoreServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Fetch the catalog for some database.
        ///
        /// The returned catalog will be the latest catalog that this metastore node
        /// knows about.
        pub async fn fetch_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchCatalogRequest>,
        ) -> Result<tonic::Response<super::FetchCatalogResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/metastore.service.MetastoreService/FetchCatalog",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Create an entry in the catalog.
        pub async fn create_entry(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateEntryRequest>,
        ) -> Result<tonic::Response<super::CreateEntryResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/metastore.service.MetastoreService/CreateEntry",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Drop an entry from the catalog.
        pub async fn drop_entry(
            &mut self,
            request: impl tonic::IntoRequest<super::DropEntryRequest>,
        ) -> Result<tonic::Response<super::DropEntryResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/metastore.service.MetastoreService/DropEntry",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Alter an entry in the catalog.
        pub async fn alter_entry(
            &mut self,
            request: impl tonic::IntoRequest<super::AlterEntryRequest>,
        ) -> Result<tonic::Response<super::AlterEntryResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/metastore.service.MetastoreService/AlterEntry",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod metastore_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with MetastoreServiceServer.
    #[async_trait]
    pub trait MetastoreService: Send + Sync + 'static {
        /// Fetch the catalog for some database.
        ///
        /// The returned catalog will be the latest catalog that this metastore node
        /// knows about.
        async fn fetch_catalog(
            &self,
            request: tonic::Request<super::FetchCatalogRequest>,
        ) -> Result<tonic::Response<super::FetchCatalogResponse>, tonic::Status>;
        /// Create an entry in the catalog.
        async fn create_entry(
            &self,
            request: tonic::Request<super::CreateEntryRequest>,
        ) -> Result<tonic::Response<super::CreateEntryResponse>, tonic::Status>;
        /// Drop an entry from the catalog.
        async fn drop_entry(
            &self,
            request: tonic::Request<super::DropEntryRequest>,
        ) -> Result<tonic::Response<super::DropEntryResponse>, tonic::Status>;
        /// Alter an entry in the catalog.
        async fn alter_entry(
            &self,
            request: tonic::Request<super::AlterEntryRequest>,
        ) -> Result<tonic::Response<super::AlterEntryResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MetastoreServiceServer<T: MetastoreService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MetastoreService> MetastoreServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MetastoreServiceServer<T>
    where
        T: MetastoreService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/metastore.service.MetastoreService/FetchCatalog" => {
                    #[allow(non_camel_case_types)]
                    struct FetchCatalogSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::FetchCatalogRequest>
                    for FetchCatalogSvc<T> {
                        type Response = super::FetchCatalogResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchCatalogRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).fetch_catalog(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FetchCatalogSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.service.MetastoreService/CreateEntry" => {
                    #[allow(non_camel_case_types)]
                    struct CreateEntrySvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::CreateEntryRequest>
                    for CreateEntrySvc<T> {
                        type Response = super::CreateEntryResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateEntryRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_entry(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateEntrySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.service.MetastoreService/DropEntry" => {
                    #[allow(non_camel_case_types)]
                    struct DropEntrySvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::DropEntryRequest>
                    for DropEntrySvc<T> {
                        type Response = super::DropEntryResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropEntryRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).drop_entry(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropEntrySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.service.MetastoreService/AlterEntry" => {
                    #[allow(non_camel_case_types)]
                    struct AlterEntrySvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::AlterEntryRequest>
                    for AlterEntrySvc<T> {
                        type Response = super::AlterEntryResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AlterEntryRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).alter_entry(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AlterEntrySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: MetastoreService> Clone for MetastoreServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: MetastoreService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MetastoreService> tonic::server::NamedService for MetastoreServiceServer<T> {
        const NAME: &'static str = "metastore.service.MetastoreService";
    }
}
