//! # API
//!
//! The api module provides the entry point to the public API. Requests are routed
//! to the appropriate handler for processing, returning a response that can
//! be serialized to a JSON object or directly to HTTP.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use common::api::{Client, Body, Headers};
//!
//! // Create a client
//! let client = Client::new(provider);
//!
//! // Simple request without headers
//! let response = client.request(my_request).owner("alice").await?;
//!
//! // Request with headers
//! let response = client.request(my_request).owner("alice").headers(my_headers).await?;
//! ```

use std::fmt::Debug;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;

use http::StatusCode;
// use tracing::instrument;

/// Build an API `Client` to execute the request.
///
/// The client is the main entry point for making API requests. It holds
/// the provider configuration and provides methods to create the request
/// builder.
#[derive(Clone, Debug)]
pub struct Client<P: Send + Sync> {
    /// The provider to use while handling of the request.
    pub provider: P,
}

impl<P: Send + Sync> Client<P> {
    /// Create a new `Client`.
    #[must_use]
    pub const fn new(provider: P) -> Self {
        Self { provider }
    }
}

impl<P: Send + Sync> Client<P> {
    /// Create a new `Request` with no headers.
    pub const fn request<B: Body, U, E>(
        &'_ self, body: B,
    ) -> RequestBuilder<'_, P, NoOwner, Empty, B, U, E> {
        RequestBuilder::new(self, body)
    }
}

/// A type-safe request builder that uses the type system to ensure required
/// fields are set before execution.
///
/// The builder uses types to track its state:
/// - `O`: Owner state (`NoOwner` or `OwnerSet`)
/// - `H`: Header state (`NoHeader` or `HeaderSet`)
/// - `U`: Expected response type
/// - `E`: Expected error type
#[derive(Debug)]
pub struct RequestBuilder<'a, P, O, H, B, U, E>
where
    P: Send + Sync,
    B: Body,
    H: Headers,
{
    client: &'a Client<P>,
    owner: O,
    headers: H,
    body: B,
    _phantom: PhantomData<(U, E)>,
}

/// The request has no owner set.
#[doc(hidden)]
pub struct NoOwner;
/// The request has a owner set.
#[doc(hidden)]
pub struct OwnerSet<'a>(&'a str);

impl<'a, P, B, U, E> RequestBuilder<'a, P, NoOwner, Empty, B, U, E>
where
    P: Send + Sync,
    B: Body,
{
    /// Create a new `Request` instance.
    pub const fn new(client: &'a Client<P>, body: B) -> Self {
        Self { client, owner: NoOwner, headers: Empty, body, _phantom: PhantomData }
    }
}

impl<'a, P, H, B, U, E> RequestBuilder<'a, P, NoOwner, H, B, U, E>
where
    P: Send + Sync,
    B: Body,
    H: Headers,
{
    /// Set the headers for the request.
    #[must_use]
    pub fn owner<'o>(self, owner: &'o str) -> RequestBuilder<'a, P, OwnerSet<'o>, H, B, U, E> {
        RequestBuilder {
            client: self.client,
            headers: self.headers,
            owner: OwnerSet(owner),
            body: self.body,
            _phantom: PhantomData,
        }
    }
}

impl<'a, P, O, B, U, E> RequestBuilder<'a, P, O, Empty, B, U, E>
where
    P: Send + Sync,
    B: Body,
{
    /// Set request headers.
    #[must_use]
    pub fn headers<H: Headers>(self, headers: H) -> RequestBuilder<'a, P, O, H, B, U, E> {
        RequestBuilder {
            client: self.client,
            owner: self.owner,
            headers,
            body: self.body,
            _phantom: PhantomData,
        }
    }
}

impl<'a, P, H, B, U, E> IntoFuture for RequestBuilder<'a, P, OwnerSet<'a>, H, B, U, E>
where
    P: Send + Sync,
    H: Headers + 'a,
    B: Body + 'a,
    U: Send + 'a,
    E: Send,
    Request<B, H>: Handler<U, P, Error = E>,
{
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;
    type Output = Result<Response<U>, E>;

    fn into_future(self) -> Self::IntoFuture {
        let request = Request { body: self.body, headers: self.headers };
        Box::pin(request.handle(self.owner.0, &self.client.provider))
    }
}

/// A request to process.
#[derive(Clone, Debug)]
pub struct Request<B, H = Empty>
where
    B: Body,
    H: Headers,
{
    /// The request to process.
    pub body: B,

    /// Headers associated with this request.
    pub headers: H,
}

impl<B: Body> From<B> for Request<B> {
    fn from(body: B) -> Self {
        Self { body, headers: Empty }
    }
}

/// Top-level response data structure common to all handler.
#[derive(Clone, Debug)]
pub struct Response<O, H = Empty>
where
    H: Headers,
{
    /// Response HTTP status code.
    pub status: StatusCode,

    /// Response HTTP headers, if any.
    pub headers: Option<H>,

    /// The endpoint-specific response.
    pub body: O,
}

impl<T> From<T> for Response<T> {
    fn from(body: T) -> Self {
        Self { status: StatusCode::OK, headers: None, body }
    }
}

impl<T> Deref for Response<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

/// Request handler.
///
/// The primary role of this trait is to provide a common interface for
/// requests so they can be handled by [`handle`] method.
pub trait Handler<U, P> {
    /// The error type returned by the handler.
    type Error;

    /// Routes the message to the concrete handler used to process the message.
    fn handle(
        self, owner: &str, provider: &P,
    ) -> impl Future<Output = Result<Response<U>, Self::Error>> + Send;
}

/// The `Body` trait is used to restrict the types able to implement
/// request body. It is implemented by all `xxxRequest` types.
pub trait Body: Clone + Debug + Send + Sync {}

/// The `Headers` trait is used to restrict the types able to implement
/// request headers.
pub trait Headers: Clone + Debug + Send + Sync {}

/// Implement empty headers for use by handlers that do not require headers.
#[derive(Clone, Debug)]
pub struct Empty;
impl Headers for Empty {}
