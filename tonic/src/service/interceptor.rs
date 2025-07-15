//! gRPC interceptors which are a kind of middleware.
//!
//! See [`Interceptor`] for more details.

use crate::{request::SanitizeHeaders, Status};
use pin_project::pin_project;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower_layer::Layer;
use tower_service::Service;

/// A gRPC interceptor.
///
/// gRPC interceptors are similar to middleware but have less flexibility. An interceptor allows
/// you to do two main things, one is to add/remove/check items in the `MetadataMap` of each
/// request. Two, cancel a request with a `Status`.
///
/// Any function that satisfies the bound `FnMut(Request<()>) -> Result<Request<()>, Status>` can be
/// used as an `Interceptor`.
///
/// An interceptor can be used on both the server and client side through the `tonic-build` crate's
/// generated structs.
///
/// See the [interceptor example][example] for more details.
///
/// If you need more powerful middleware, [tower] is the recommended approach. You can find
/// examples of how to use tower with tonic [here][tower-example].
///
/// Additionally, interceptors is not the recommended way to add logging to your service. For that
/// a [tower] middleware is more appropriate since it can also act on the response. For example
/// tower-http's [`Trace`](https://docs.rs/tower-http/latest/tower_http/trace/index.html)
/// middleware supports gRPC out of the box.
///
/// [tower]: https://crates.io/crates/tower
/// [example]: https://github.com/hyperium/tonic/tree/master/examples/src/interceptor
/// [tower-example]: https://github.com/hyperium/tonic/tree/master/examples/src/tower
pub trait Interceptor {
    /// Intercept a request before it is sent, optionally cancelling it.
    fn call(&mut self, request: crate::Request<()>) -> Result<crate::Request<()>, Status>;
}

impl<F> Interceptor for F
where
    F: FnMut(crate::Request<()>) -> Result<crate::Request<()>, Status>,
{
    fn call(&mut self, request: crate::Request<()>) -> Result<crate::Request<()>, Status> {
        self(request)
    }
}

/// Async version of `Interceptor`.
pub trait AsyncInterceptor {
    /// The Future returned by the interceptor.
    type Future: Future<Output = Result<crate::Request<()>, Status>>;
    /// Intercept a request before it is sent, optionally cancelling it.
    fn call(&mut self, request: crate::Request<()>) -> Self::Future;
}

impl<F, U> AsyncInterceptor for F
where
    F: FnMut(crate::Request<()>) -> U,
    U: Future<Output = Result<crate::Request<()>, Status>>,
{
    type Future = U;

    fn call(&mut self, request: crate::Request<()>) -> Self::Future {
        self(request)
    }
}

/// A gRPC interceptor that can be used as a [`Layer`],
///
/// See [`Interceptor`] for more details.
#[derive(Debug, Clone, Copy)]
pub struct InterceptorLayer<I> {
    interceptor: I,
}

impl<I> InterceptorLayer<I> {
    /// Create a new interceptor layer.
    ///
    /// See [`Interceptor`] for more details.
    pub fn new(interceptor: I) -> Self {
        Self { interceptor }
    }
}

impl<S, I> Layer<S> for InterceptorLayer<I>
where
    I: Clone,
{
    type Service = InterceptedService<S, I>;

    fn layer(&self, service: S) -> Self::Service {
        InterceptedService::new(service, self.interceptor.clone())
    }
}

/// A gRPC async interceptor that can be used as a [`Layer`],
///
/// See [`AsyncInterceptor`] for more details.
#[derive(Debug, Clone, Copy)]
pub struct AsyncInterceptorLayer<I> {
    interceptor: I,
}

impl<I> AsyncInterceptorLayer<I> {
    /// Create a new async interceptor layer.
    ///
    /// See [`AsyncInterceptor`] for more details.
    pub fn new(interceptor: I) -> Self {
        Self { interceptor }
    }
}

impl<S, I> Layer<S> for AsyncInterceptorLayer<I>
where
    S: Clone,
    I: AsyncInterceptor + Clone,
{
    type Service = AsyncInterceptedService<S, I>;

    fn layer(&self, service: S) -> Self::Service {
        AsyncInterceptedService::new(service, self.interceptor.clone())
    }
}

/// A service wrapped in an interceptor middleware.
///
/// See [`Interceptor`] for more details.
#[derive(Clone, Copy)]
pub struct InterceptedService<S, I> {
    inner: S,
    interceptor: I,
}

impl<S, I> InterceptedService<S, I> {
    /// Create a new `InterceptedService` that wraps `S` and intercepts each request with the
    /// function `F`.
    pub fn new(service: S, interceptor: I) -> Self {
        Self {
            inner: service,
            interceptor,
        }
    }
}

impl<S, I> fmt::Debug for InterceptedService<S, I>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InterceptedService")
            .field("inner", &self.inner)
            .field("f", &format_args!("{}", std::any::type_name::<I>()))
            .finish()
    }
}

// Components and attributes of a request, without metadata or extensions.
#[derive(Debug)]
struct DecomposedRequest<ReqBody> {
    uri: http::Uri,
    method: http::Method,
    version: http::Version,
    msg: ReqBody,
}

/// Decompose the request into its contents and properties, and create a new request without a body.
///
/// It is bad practice to modify the body (i.e. Message) of the request via an interceptor.
/// To avoid exposing the body of the request to the interceptor function, we first remove it
/// here, allow the interceptor to modify the metadata and extensions, and then recreate the
/// HTTP request with the original message body with the `recompose` function. Also note that Tonic
/// requests do not preserve the URI, HTTP version, and HTTP method of the HTTP request, so we
/// extract them here and then add them back in `recompose`.
fn decompose<ReqBody>(
    req: http::Request<ReqBody>,
) -> (DecomposedRequest<ReqBody>, crate::Request<()>) {
    let uri = req.uri().clone();
    let method = req.method().clone();
    let version = req.version();

    let req = crate::Request::from_http(req);
    let (metadata, extensions, msg) = req.into_parts();
    let req_without_body = crate::Request::from_parts(metadata, extensions, ());
    let decomposed_req = DecomposedRequest {
        uri,
        method,
        version,
        msg,
    };

    (decomposed_req, req_without_body)
}

/// Combine the modified metadata and extensions with the original message body and attributes.
fn recompose<ReqBody>(
    dreq: DecomposedRequest<ReqBody>,
    modified_req: crate::Request<()>,
) -> http::Request<ReqBody> {
    let (metadata, extensions, _) = modified_req.into_parts();
    let req = crate::Request::from_parts(metadata, extensions, dreq.msg);

    req.into_http(dreq.uri, dreq.method, dreq.version, SanitizeHeaders::No)
}

impl<S, I, ReqBody, ResBody> Service<http::Request<ReqBody>> for InterceptedService<S, I>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    I: Interceptor,
{
    type Response = http::Response<ResponseBody<ResBody>>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let (dreq, req_without_body) = decompose(req);

        match self.interceptor.call(req_without_body) {
            Ok(modified_req) => {
                let modified_req_with_body = recompose(dreq, modified_req);
                ResponseFuture::future(self.inner.call(modified_req_with_body))
            }
            Err(status) => ResponseFuture::status(status),
        }
    }
}

// required to use `InterceptedService` with `Router`
impl<S, I> crate::server::NamedService for InterceptedService<S, I>
where
    S: crate::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

/// A service wrapped in an interceptor middleware.
///
/// See [`AsyncInterceptor`] for more details.
#[derive(Clone, Copy)]
pub struct AsyncInterceptedService<S, I> {
    inner: S,
    interceptor: I,
}

impl<S, I> AsyncInterceptedService<S, I> {
    /// Create a new `AsyncInterceptedService` that wraps `S` and intercepts each request with the
    /// function `F`.
    pub fn new(service: S, interceptor: I) -> Self {
        Self {
            inner: service,
            interceptor,
        }
    }
}

impl<S, I> fmt::Debug for AsyncInterceptedService<S, I>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncInterceptedService")
            .field("inner", &self.inner)
            .field("f", &format_args!("{}", std::any::type_name::<I>()))
            .finish()
    }
}

impl<S, I, ReqBody, ResBody> Service<http::Request<ReqBody>> for AsyncInterceptedService<S, I>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone,
    I: AsyncInterceptor + Clone,
{
    type Response = http::Response<ResponseBody<ResBody>>;
    type Error = S::Error;
    type Future = AsyncResponseFuture<S, I::Future, ReqBody, ResBody>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        let clone = self.inner.clone();
        let inner = std::mem::replace(&mut self.inner, clone);
        AsyncResponseFuture::new(req, &mut self.interceptor, inner)
    }
}

// required to use `AsyncInterceptedService` with `Router`
impl<S, I> crate::server::NamedService for AsyncInterceptedService<S, I>
where
    S: crate::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

/// Response future for [`InterceptedService`].
#[pin_project]
#[derive(Debug)]
pub struct ResponseFuture<F> {
    #[pin]
    kind: Kind<F>,
}

impl<F> ResponseFuture<F> {
    fn future(future: F) -> Self {
        Self {
            kind: Kind::Future(future),
        }
    }

    fn status(status: Status) -> Self {
        Self {
            kind: Kind::Status(Some(status)),
        }
    }
}

#[pin_project(project = KindProj)]
#[derive(Debug)]
enum Kind<F> {
    Future(#[pin] F),
    Status(Option<Status>),
}

impl<F, E, B> Future for ResponseFuture<F>
where
    F: Future<Output = Result<http::Response<B>, E>>,
{
    type Output = Result<http::Response<ResponseBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::Future(future) => future.poll(cx).map_ok(|res| res.map(ResponseBody::wrap)),
            KindProj::Status(status) => {
                let (parts, ()) = status.take().unwrap().into_http::<()>().into_parts();
                let response = http::Response::from_parts(parts, ResponseBody::<B>::empty());
                Poll::Ready(Ok(response))
            }
        }
    }
}

#[pin_project(project = PinnedOptionProj)]
#[derive(Debug)]
enum PinnedOption<F> {
    Some(#[pin] F),
    None,
}

/// Response future for [`AsyncInterceptedService`].
///
/// Handles the call to the async interceptor, then calls the inner service and wraps the result in
/// [`ResponseFuture`].
#[pin_project(project = AsyncResponseFutureProj)]
#[derive(Debug)]
pub struct AsyncResponseFuture<S, I, ReqBody, ResBody>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    I: Future<Output = Result<crate::Request<()>, Status>>,
{
    #[pin]
    interceptor_fut: PinnedOption<I>,
    #[pin]
    inner_fut: PinnedOption<ResponseFuture<S::Future>>,
    inner: S,
    dreq: Option<DecomposedRequest<ReqBody>>,
}

impl<S, I, ReqBody, ResBody> AsyncResponseFuture<S, I, ReqBody, ResBody>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    I: Future<Output = Result<crate::Request<()>, Status>>,
{
    fn new<A: AsyncInterceptor<Future = I>>(
        req: http::Request<ReqBody>,
        interceptor: &mut A,
        inner: S,
    ) -> Self {
        let (dreq, req_without_body) = decompose(req);
        let interceptor_fut = interceptor.call(req_without_body);

        AsyncResponseFuture {
            interceptor_fut: PinnedOption::Some(interceptor_fut),
            inner_fut: PinnedOption::None,
            inner,
            dreq: Some(dreq),
        }
    }

    /// Calls the inner service with the intercepted request (which has been modified by the
    /// async interceptor func).
    fn create_inner_fut(
        this: &mut AsyncResponseFutureProj<'_, S, I, ReqBody, ResBody>,
        intercepted_req: Result<crate::Request<()>, Status>,
    ) -> ResponseFuture<S::Future> {
        match intercepted_req {
            Ok(req) => {
                let Some(dreq) = std::mem::take(this.dreq) else {
                    unreachable!(); // SAFETY: the caller ensures that this is only called once
                };

                ResponseFuture::future(this.inner.call(recompose(dreq, req)))
            }
            Err(status) => ResponseFuture::status(status),
        }
    }
}

impl<S, I, ReqBody, ResBody> Future for AsyncResponseFuture<S, I, ReqBody, ResBody>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    I: Future<Output = Result<crate::Request<()>, Status>>,
{
    type Output = Result<http::Response<ResponseBody<ResBody>>, S::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        // The struct was initialized (via `new`) with interceptor func future, which we poll here.
        if let PinnedOptionProj::Some(f) = this.interceptor_fut.as_mut().project() {
            match f.poll(cx) {
                Poll::Ready(intercepted_req) => {
                    let inner_fut = AsyncResponseFuture::<S, I, ReqBody, ResBody>::create_inner_fut(
                        &mut this,
                        intercepted_req,
                    );
                    // Set the inner service future and clear the interceptor future.
                    this.inner_fut.set(PinnedOption::Some(inner_fut));
                    this.interceptor_fut.set(PinnedOption::None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // At this point, inner_fut should always be Some.
        match this.inner_fut.project() {
            PinnedOptionProj::None => unreachable!(),
            PinnedOptionProj::Some(f) => f.poll(cx),
        }
    }
}

/// Response body for [`InterceptedService`].
#[pin_project]
#[derive(Debug)]
pub struct ResponseBody<B> {
    #[pin]
    kind: ResponseBodyKind<B>,
}

#[pin_project(project = ResponseBodyKindProj)]
#[derive(Debug)]
enum ResponseBodyKind<B> {
    Empty,
    Wrap(#[pin] B),
}

impl<B> ResponseBody<B> {
    fn new(kind: ResponseBodyKind<B>) -> Self {
        Self { kind }
    }

    fn empty() -> Self {
        Self::new(ResponseBodyKind::Empty)
    }

    fn wrap(body: B) -> Self {
        Self::new(ResponseBodyKind::Wrap(body))
    }
}

impl<B: http_body::Body> http_body::Body for ResponseBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.project().kind.project() {
            ResponseBodyKindProj::Empty => Poll::Ready(None),
            ResponseBodyKindProj::Wrap(body) => body.poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.kind {
            ResponseBodyKind::Empty => true,
            ResponseBodyKind::Wrap(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match &self.kind {
            ResponseBodyKind::Empty => http_body::SizeHint::with_exact(0),
            ResponseBodyKind::Wrap(body) => body.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn doesnt_remove_headers_from_requests() {
        let svc = tower::service_fn(|request: http::Request<()>| async move {
            assert_eq!(
                request
                    .headers()
                    .get("user-agent")
                    .expect("missing in leaf service"),
                "test-tonic"
            );

            Ok::<_, Status>(http::Response::new(()))
        });

        let svc = InterceptedService::new(svc, |request: crate::Request<()>| {
            assert_eq!(
                request
                    .metadata()
                    .get("user-agent")
                    .expect("missing in interceptor"),
                "test-tonic"
            );

            Ok(request)
        });

        let request = http::Request::builder()
            .header("user-agent", "test-tonic")
            .body(())
            .unwrap();

        svc.oneshot(request).await.unwrap();
    }

    #[tokio::test]
    async fn handles_intercepted_status_as_response() {
        let message = "Blocked by the interceptor";
        let expected = Status::permission_denied(message).into_http::<()>();

        let svc = tower::service_fn(|_: http::Request<()>| async {
            Ok::<_, Status>(http::Response::new(()))
        });

        let svc = InterceptedService::new(svc, |_: crate::Request<()>| {
            Err(Status::permission_denied(message))
        });

        let request = http::Request::builder().body(()).unwrap();
        let response = svc.oneshot(request).await.unwrap();

        assert_eq!(expected.status(), response.status());
        assert_eq!(expected.version(), response.version());
        assert_eq!(expected.headers(), response.headers());
    }

    #[tokio::test]
    async fn doesnt_change_http_method() {
        let svc = tower::service_fn(|request: http::Request<()>| async move {
            assert_eq!(request.method(), http::Method::OPTIONS);

            Ok::<_, hyper::Error>(hyper::Response::new(()))
        });

        let svc = InterceptedService::new(svc, Ok);

        let request = http::Request::builder()
            .method(http::Method::OPTIONS)
            .body(())
            .unwrap();

        svc.oneshot(request).await.unwrap();
    }
}
