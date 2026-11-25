/// Generated client implementations.
pub mod kv_events_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value
    )]
    use tonic::codegen::http::Uri;
    use tonic::codegen::*;
    /// KV cache events of the scheduler, as its ZMQ publisher emits them
    /// (`python/sglang/srt/disaggregation/kv_events.py`), with the same sequence
    /// numbers.
    #[derive(Debug, Clone)]
    pub struct KvEventsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl KvEventsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> KvEventsClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
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
        ) -> KvEventsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                    http::Request<tonic::body::Body>,
                    Response = http::Response<
                        <T as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody,
                    >,
                >,
            <T as tonic::codegen::Service<http::Request<tonic::body::Body>>>::Error:
                Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            KvEventsClient::new(InterceptedService::new(inner, interceptor))
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
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// One stream per data-parallel rank; each Rust listener serves its own rank.
        ///
        /// The first response is `start`; every later one is a `batch`, in
        /// sequence-number order, each number once. Without `resume`, the stream
        /// starts with the next batch the server forwards. With `resume`, it first
        /// replays the engine's buffered batches from the cursor (the scheduler keeps
        /// `buffer_steps` of them), then continues live.
        ///
        /// Each batch is one message, however large; raise the client's receive
        /// limit above gRPC's default 4 MiB for long prompts.
        ///
        /// INVALID_ARGUMENT: this listener does not serve the rank.
        /// FAILED_PRECONDITION: the scheduler publishes no KV cache events over ZMQ.
        /// OUT_OF_RANGE: the cursor's batches are no longer buffered, or its
        /// `stream_id` is not the current one.
        /// DATA_LOSS: batches after the last one sent can no longer be recovered
        /// from the engine's buffer.
        /// UNAVAILABLE: the engine's replay socket did not answer, the relay
        /// stopped, or the server is shutting down.
        pub async fn subscribe_kv_events(
            &mut self,
            request: impl tonic::IntoRequest<super::SubscribeKvEventsRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::SubscribeKvEventsResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/sglang.kv.v1.KvEvents/SubscribeKvEvents");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "sglang.kv.v1.KvEvents",
                "SubscribeKvEvents",
            ));
            self.inner.server_streaming(req, path, codec).await
        }
        /// Streams the blocks that this listener's rank holds in its device cache
        /// (medium `GPU`), as the stores that recreate them: applied in order to an
        /// empty state, they give the state after every batch before `end.resume`.
        /// A block follows its parent. A block whose parent is not held is left out,
        /// so a client that loaded the snapshot lacks it, also once the parent is
        /// stored again. The last response is `end`; a stream that ends without it
        /// is not a snapshot. To follow the cache from the snapshot, subscribe with
        /// `end.resume`, or subscribe before the call and drop the batches below
        /// `end.resume`: the snapshot holds every batch forwarded before the call.
        ///
        /// INVALID_ARGUMENT: this listener does not serve the rank.
        /// FAILED_PRECONDITION: the scheduler publishes no KV cache events over ZMQ,
        /// or `grpc_snapshot` is false in its KV events config.
        /// UNAVAILABLE: the snapshot is behind the stream (retry), or the server is
        /// shutting down.
        pub async fn get_kv_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::GetKvSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::GetKvSnapshotResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/sglang.kv.v1.KvEvents/GetKvSnapshot");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("sglang.kv.v1.KvEvents", "GetKvSnapshot"));
            self.inner.server_streaming(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod kv_events_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with KvEventsServer.
    #[async_trait]
    pub trait KvEvents: std::marker::Send + std::marker::Sync + 'static {
        /// Server streaming response type for the SubscribeKvEvents method.
        type SubscribeKvEventsStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::SubscribeKvEventsResponse, tonic::Status>,
            > + std::marker::Send
            + 'static;
        /// One stream per data-parallel rank; each Rust listener serves its own rank.
        ///
        /// The first response is `start`; every later one is a `batch`, in
        /// sequence-number order, each number once. Without `resume`, the stream
        /// starts with the next batch the server forwards. With `resume`, it first
        /// replays the engine's buffered batches from the cursor (the scheduler keeps
        /// `buffer_steps` of them), then continues live.
        ///
        /// Each batch is one message, however large; raise the client's receive
        /// limit above gRPC's default 4 MiB for long prompts.
        ///
        /// INVALID_ARGUMENT: this listener does not serve the rank.
        /// FAILED_PRECONDITION: the scheduler publishes no KV cache events over ZMQ.
        /// OUT_OF_RANGE: the cursor's batches are no longer buffered, or its
        /// `stream_id` is not the current one.
        /// DATA_LOSS: batches after the last one sent can no longer be recovered
        /// from the engine's buffer.
        /// UNAVAILABLE: the engine's replay socket did not answer, the relay
        /// stopped, or the server is shutting down.
        async fn subscribe_kv_events(
            &self,
            request: tonic::Request<super::SubscribeKvEventsRequest>,
        ) -> std::result::Result<tonic::Response<Self::SubscribeKvEventsStream>, tonic::Status>;
        /// Server streaming response type for the GetKvSnapshot method.
        type GetKvSnapshotStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::GetKvSnapshotResponse, tonic::Status>,
            > + std::marker::Send
            + 'static;
        /// Streams the blocks that this listener's rank holds in its device cache
        /// (medium `GPU`), as the stores that recreate them: applied in order to an
        /// empty state, they give the state after every batch before `end.resume`.
        /// A block follows its parent. A block whose parent is not held is left out,
        /// so a client that loaded the snapshot lacks it, also once the parent is
        /// stored again. The last response is `end`; a stream that ends without it
        /// is not a snapshot. To follow the cache from the snapshot, subscribe with
        /// `end.resume`, or subscribe before the call and drop the batches below
        /// `end.resume`: the snapshot holds every batch forwarded before the call.
        ///
        /// INVALID_ARGUMENT: this listener does not serve the rank.
        /// FAILED_PRECONDITION: the scheduler publishes no KV cache events over ZMQ,
        /// or `grpc_snapshot` is false in its KV events config.
        /// UNAVAILABLE: the snapshot is behind the stream (retry), or the server is
        /// shutting down.
        async fn get_kv_snapshot(
            &self,
            request: tonic::Request<super::GetKvSnapshotRequest>,
        ) -> std::result::Result<tonic::Response<Self::GetKvSnapshotStream>, tonic::Status>;
    }
    /// KV cache events of the scheduler, as its ZMQ publisher emits them
    /// (`python/sglang/srt/disaggregation/kv_events.py`), with the same sequence
    /// numbers.
    #[derive(Debug)]
    pub struct KvEventsServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> KvEventsServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
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
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for KvEventsServer<T>
    where
        T: KvEvents,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/sglang.kv.v1.KvEvents/SubscribeKvEvents" => {
                    #[allow(non_camel_case_types)]
                    struct SubscribeKvEventsSvc<T: KvEvents>(pub Arc<T>);
                    impl<T: KvEvents>
                        tonic::server::ServerStreamingService<super::SubscribeKvEventsRequest>
                        for SubscribeKvEventsSvc<T>
                    {
                        type Response = super::SubscribeKvEventsResponse;
                        type ResponseStream = T::SubscribeKvEventsStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SubscribeKvEventsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as KvEvents>::subscribe_kv_events(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = SubscribeKvEventsSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/sglang.kv.v1.KvEvents/GetKvSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct GetKvSnapshotSvc<T: KvEvents>(pub Arc<T>);
                    impl<T: KvEvents>
                        tonic::server::ServerStreamingService<super::GetKvSnapshotRequest>
                        for GetKvSnapshotSvc<T>
                    {
                        type Response = super::GetKvSnapshotResponse;
                        type ResponseStream = T::GetKvSnapshotStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetKvSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as KvEvents>::get_kv_snapshot(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = GetKvSnapshotSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    let mut response = http::Response::new(tonic::body::Body::default());
                    let headers = response.headers_mut();
                    headers.insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    headers.insert(
                        http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    Ok(response)
                }),
            }
        }
    }
    impl<T> Clone for KvEventsServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "sglang.kv.v1.KvEvents";
    impl<T> tonic::server::NamedService for KvEventsServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
