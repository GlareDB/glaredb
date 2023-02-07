#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitializeCatalogRequest {
    /// ID of the catalog to initialize.
    #[prost(bytes = "vec", tag = "1")]
    pub db_id: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitializeCatalogResponse {
    #[prost(enumeration = "initialize_catalog_response::Status", tag = "1")]
    pub status: i32,
}
/// Nested message and enum types in `InitializeCatalogResponse`.
pub mod initialize_catalog_response {
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
        /// Catalog initialized.
        Initialized = 1,
        /// Catalog already loaded.
        AlreadyLoaded = 2,
    }
    impl Status {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Status::Unknown => "UNKNOWN",
                Status::Initialized => "INITIALIZED",
                Status::AlreadyLoaded => "ALREADY_LOADED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "INITIALIZED" => Some(Self::Initialized),
                "ALREADY_LOADED" => Some(Self::AlreadyLoaded),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchCatalogRequest {
    /// ID of the database catalog to fetch.
    #[prost(bytes = "vec", tag = "1")]
    pub db_id: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchCatalogResponse {
    #[prost(message, optional, tag = "1")]
    pub catalog: ::core::option::Option<super::catalog::CatalogState>,
}
/// Possible mutations to make.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Mutation {
    #[prost(oneof = "mutation::Mutation", tags = "1, 2, 3, 4, 5, 6")]
    pub mutation: ::core::option::Option<mutation::Mutation>,
}
/// Nested message and enum types in `Mutation`.
pub mod mutation {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Mutation {
        #[prost(message, tag = "1")]
        DropSchema(super::DropSchema),
        #[prost(message, tag = "2")]
        DropObject(super::DropObject),
        #[prost(message, tag = "3")]
        CreateSchema(super::CreateSchema),
        #[prost(message, tag = "4")]
        CreateView(super::CreateView),
        #[prost(message, tag = "5")]
        CreateConnection(super::CreateConnection),
        #[prost(message, tag = "6")]
        CreateExternalTable(super::CreateExternalTable),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropSchema {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropObject {
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSchema {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateView {
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub sql: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateConnection {
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub options: ::core::option::Option<super::catalog::ConnectionOptions>,
    /// next: 5
    #[prost(bool, tag = "4")]
    pub if_not_exists: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateExternalTable {
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub connection_id: u32,
    #[prost(message, optional, tag = "4")]
    pub options: ::core::option::Option<super::catalog::TableOptions>,
    /// next: 6
    #[prost(bool, tag = "5")]
    pub if_not_exists: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MutateRequest {
    /// Mutate the catalog for this database.
    #[prost(bytes = "vec", tag = "1")]
    pub db_id: ::prost::alloc::vec::Vec<u8>,
    /// Catalog version we're trying to execution mutations against. Mutations will
    /// be rejected if this version doesn't match Metastore's version of the
    /// catalog.
    #[prost(uint64, tag = "2")]
    pub catalog_version: u64,
    /// Mutations to attempt to execute against the catalog.
    #[prost(message, repeated, tag = "3")]
    pub mutations: ::prost::alloc::vec::Vec<Mutation>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MutateResponse {
    /// Status of the mutation.
    #[prost(enumeration = "mutate_response::Status", tag = "1")]
    pub status: i32,
    /// The current state of the catalog as witnessed by metastore.
    ///
    /// If the mutation was accepted, this catalog will included that mutation. If
    /// the mutation was rejected, this catalog will not have that mutation
    /// applied. In either case, this catalog should replace any stale catalog.
    #[prost(message, optional, tag = "2")]
    pub catalog: ::core::option::Option<super::catalog::CatalogState>,
}
/// Nested message and enum types in `MutateResponse`.
pub mod mutate_response {
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
        /// Initialize a database catalog.
        ///
        /// Idempotent, safe to call multiple times.
        pub async fn initialize_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::InitializeCatalogRequest>,
        ) -> Result<tonic::Response<super::InitializeCatalogResponse>, tonic::Status> {
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
                "/metastore.service.MetastoreService/InitializeCatalog",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Fetch the catalog for some database.
        ///
        /// The returned catalog will be the latest catalog that this metastore node
        /// knows about.
        /// TODO: Could be streaming.
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
        /// Mutate a database's catalog.
        pub async fn mutate_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::MutateRequest>,
        ) -> Result<tonic::Response<super::MutateResponse>, tonic::Status> {
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
                "/metastore.service.MetastoreService/MutateCatalog",
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
        /// Initialize a database catalog.
        ///
        /// Idempotent, safe to call multiple times.
        async fn initialize_catalog(
            &self,
            request: tonic::Request<super::InitializeCatalogRequest>,
        ) -> Result<tonic::Response<super::InitializeCatalogResponse>, tonic::Status>;
        /// Fetch the catalog for some database.
        ///
        /// The returned catalog will be the latest catalog that this metastore node
        /// knows about.
        /// TODO: Could be streaming.
        async fn fetch_catalog(
            &self,
            request: tonic::Request<super::FetchCatalogRequest>,
        ) -> Result<tonic::Response<super::FetchCatalogResponse>, tonic::Status>;
        /// Mutate a database's catalog.
        async fn mutate_catalog(
            &self,
            request: tonic::Request<super::MutateRequest>,
        ) -> Result<tonic::Response<super::MutateResponse>, tonic::Status>;
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
                "/metastore.service.MetastoreService/InitializeCatalog" => {
                    #[allow(non_camel_case_types)]
                    struct InitializeCatalogSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::InitializeCatalogRequest>
                    for InitializeCatalogSvc<T> {
                        type Response = super::InitializeCatalogResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InitializeCatalogRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).initialize_catalog(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = InitializeCatalogSvc(inner);
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
                "/metastore.service.MetastoreService/MutateCatalog" => {
                    #[allow(non_camel_case_types)]
                    struct MutateCatalogSvc<T: MetastoreService>(pub Arc<T>);
                    impl<
                        T: MetastoreService,
                    > tonic::server::UnaryService<super::MutateRequest>
                    for MutateCatalogSvc<T> {
                        type Response = super::MutateResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::MutateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).mutate_catalog(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = MutateCatalogSvc(inner);
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
