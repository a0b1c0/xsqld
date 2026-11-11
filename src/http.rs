//! HTTP: the Hrana port (`POST /v2/pipeline`, `GET /dump`, `GET /health`, `GET /metrics`) and the admin port
//! (`POST /v1/namespaces/<ns>/create`, `DELETE /v1/namespaces/<ns>`, `GET /health`, `GET /metrics`). Status codes and
//! error bodies follow sqld 0.24.32 where clients look at them.

use std::io::{self, Write};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use serde::Deserialize;

use crate::db::{AdminError, Db, Registry};
use crate::dump;
use crate::exec;
use crate::metrics;
use crate::proto::{PipelineReq, PipelineResp, StreamRequest, StreamResponse, StreamResult};
use crate::reader::{self, ReadOutcome};
use crate::sql;
use crate::writer::{Job, Outcome};

pub type App = Arc<Registry>;

/// `dump_url`: connect through response headers must finish within this
const DUMP_URL_HEAD_TIMEOUT: Duration = Duration::from_secs(30);
/// `dump_url`: longest gap between two body chunks before the download is abandoned
const DUMP_URL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub fn user_router(app: App) -> Router {
    let limits = Limits::new(&app.cfg, app.cfg.max_request_bytes);
    Router::new()
        .route("/v2/pipeline", post(pipeline))
        .route("/dump", get(dump))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(limits.max_body))
        .layer(axum::middleware::from_fn_with_state(limits, guard))
        .with_state(app)
}

pub fn admin_router(app: App) -> Router {
    let limits = Limits::new(&app.cfg, 64 * 1024);
    Router::new()
        .route("/v1/namespaces/{ns}/create", post(create))
        .route("/v1/namespaces/{ns}", delete(remove))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(limits.max_body))
        .layer(axum::middleware::from_fn_with_state(limits, guard))
        .with_state(app)
}

/// Serves one port until `stop` turns true, then waits for the connections in flight. At most `--max-connections`
/// connections are open (more wait in the listen backlog); a connection must send a complete request head within
/// `--header-read-timeout-ms`, which also closes idle keep-alive connections.
pub async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    cfg: Arc<crate::config::Config>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let stopped = async move {
        let _ = stop.wait_for(|s| *s).await;
    };
    tokio::pin!(stopped);
    let slots = Arc::new(tokio::sync::Semaphore::new(cfg.max_connections));
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut http = hyper::server::conn::http1::Builder::new();
    http.timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(cfg.header_read_timeout());
    loop {
        let slot = tokio::select! {
            s = slots.clone().acquire_owned() => s.expect("the semaphore is never closed"),
            _ = &mut stopped => break,
        };
        let stream = tokio::select! {
            r = listener.accept() => match r {
                Ok((stream, _)) => stream,
                Err(e) => {
                    // e.g. out of file descriptors: back off instead of spinning
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = &mut stopped => break,
        };
        let _ = stream.set_nodelay(true);
        let service = hyper_util::service::TowerToHyperService::new(router.clone());
        let conn = graceful.watch(http.serve_connection(hyper_util::rt::TokioIo::new(stream), service));
        tokio::spawn(async move {
            let _slot = slot;
            let _ = conn.await;
        });
    }
    graceful.shutdown().await;
}

#[derive(Clone)]
struct Limits {
    cfg: Arc<crate::config::Config>,
    requests: Arc<tokio::sync::Semaphore>,
    body_timeout: Duration,
    max_body: usize,
}

impl Limits {
    fn new(cfg: &Arc<crate::config::Config>, max_body: usize) -> Self {
        Self {
            cfg: cfg.clone(),
            requests: Arc::new(tokio::sync::Semaphore::new(cfg.max_concurrent_requests)),
            body_timeout: cfg.body_read_timeout(),
            max_body,
        }
    }
}

/// When a request arrived (its head was read): the request's time limit counts from here, body reading included
#[derive(Clone, Copy)]
struct Arrived(Instant);

/// Every request: none for a Host that is not a local name and none from a web page (403, see [`refused`]), at most
/// `--max-concurrent-requests` at once (more get 503 at once; `/health` is always answered, the watchdog must not take
/// an overloaded server for a dead one), and the whole body within `--body-read-timeout-ms` (408 otherwise)
async fn guard(State(limits): State<Limits>, req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let arrived = Instant::now();
    if let Some(why) = refused(&limits.cfg, req.headers()) {
        return error(StatusCode::FORBIDDEN, why);
    }
    let _permit = if req.uri().path() == "/health" {
        None
    } else {
        match limits.requests.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "Too many requests in flight"),
        }
    };
    let (mut parts, body) = req.into_parts();
    let body = match tokio::time::timeout(limits.body_timeout, axum::body::to_bytes(body, limits.max_body)).await {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            let too_large = std::error::Error::source(&e).is_some_and(|s| s.is::<http_body_util::LengthLimitError>());
            return if too_large {
                text(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Failed to buffer the request body: length limit exceeded",
                )
            } else {
                text(StatusCode::BAD_REQUEST, format!("Failed to read the request body: {e}"))
            };
        }
        Err(_) => return text(StatusCode::REQUEST_TIMEOUT, "Request body not received in time"),
    };
    parts.extensions.insert(Arrived(arrived));
    next.run(axum::extract::Request::from_parts(parts, Body::from(body)))
        .await
}

/// Why a request is not answered, if it is not. The server has no authentication and picks the database by the Host
/// header, so a web page open on this machine must not get through:
/// - after DNS rebinding the page talks to this server as its own origin, and a same-origin GET carries no header
///   that gives a browser away; but its Host is the page's domain, which is not a local name
///   (`--allowed-host-suffixes`);
/// - a page can send a cross-site request to a local name (`http://tenant.localhost:8080`), a "simple" POST
///   (`text/plain`, no preflight) included; browsers mark those with `Origin` (every POST) and `Sec-Fetch-Site`.
///
/// Local clients send neither header. `Sec-Fetch-Mode` alone says nothing: Node's `fetch` sends it on every request.
fn refused(cfg: &crate::config::Config, headers: &HeaderMap) -> Option<String> {
    if let Some(host) = headers.get(header::HOST) {
        match host.to_str() {
            Ok(h) if cfg.host_allowed(h) => {}
            _ => {
                return Some(format!(
                    "Host `{}` is not accepted (see --allowed-host-suffixes)",
                    String::from_utf8_lossy(host.as_bytes())
                ));
            }
        }
    }
    (headers.contains_key(header::ORIGIN) || headers.contains_key("sec-fetch-site"))
        .then(|| "Requests from web browsers are not accepted".to_owned())
}

/// 200 like sqld (whose body is empty; clients look at the status only), plus what is running
async fn health() -> Response {
    axum::Json(serde_json::json!({
        "version": crate::config::VERSION,
        "commit": crate::config::COMMIT,
    }))
    .into_response()
}

async fn metrics(State(app): State<App>) -> Response {
    let mut out = String::new();
    metrics::build_info(&mut out);
    let (streams, pinned) = app.streams.counts();
    metrics::put(&mut out, "streams", "", streams);
    metrics::put(&mut out, "streams_pinned", "", pinned);
    for db in app.open_dbs() {
        db.metrics.render(&db.name, &mut out);
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response()
}

/// sqld-style error body `{"error": "…"}`
fn error(status: StatusCode, message: impl Into<String>) -> Response {
    #[derive(serde::Serialize)]
    struct E {
        error: String,
    }
    (status, axum::Json(E { error: message.into() })).into_response()
}

fn text(status: StatusCode, message: impl Into<String>) -> Response {
    (status, message.into()).into_response()
}

fn missing(ns: &str) -> Response {
    error(StatusCode::NOT_FOUND, format!("Namespace `{ns}` doesn't exist"))
}

/// The database is the first label of the Host header (`tenant.db`, `platform.local`), like sqld
fn namespace(headers: &HeaderMap) -> Option<String> {
    let host = headers.get(header::HOST)?.to_str().ok()?;
    let label = host.split(['.', ':']).next()?;
    (!label.is_empty()).then(|| label.to_owned())
}

async fn database(app: &App, headers: &HeaderMap) -> Result<Arc<Db>, Box<Response>> {
    let ns = namespace(headers).unwrap_or_default();
    match app.get(&ns).await {
        Ok(Some(db)) => Ok(db),
        Ok(None) => Err(Box::new(missing(&ns))),
        Err(e) => {
            tracing::error!(ns, error = %e, "could not open database");
            Err(Box::new(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal error: {e}"),
            )))
        }
    }
}

async fn pipeline(
    State(app): State<App>,
    axum::Extension(Arrived(started)): axum::Extension<Arrived>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let db = match database(&app, &headers).await {
        Ok(db) => db,
        Err(r) => return *r,
    };
    let req: PipelineReq = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return text(
                StatusCode::BAD_REQUEST,
                format!("Cannot deserialize client message from JSON: {e}"),
            );
        }
    };
    drop(body);
    if let Err(m) = exec::validate(&req.requests, app.cfg.max_statements) {
        return text(StatusCode::BAD_REQUEST, m);
    }
    let lease = match app.streams.acquire(&db.name, req.baton.as_deref()) {
        Ok(l) => l,
        Err(e) => return text(StatusCode::BAD_REQUEST, e.message()),
    };
    let head = slow_log_head(&req.requests);
    let deadline = started + app.cfg.request_timeout();
    let outcome = run(&db, lease.id, lease.pinned, req.requests, deadline).await;
    let elapsed = started.elapsed();
    if elapsed.as_millis() >= 1000 {
        tracing::warn!(
            ns = &*db.name,
            ms = elapsed.as_millis() as u64,
            sql = head,
            "slow request"
        );
    }
    match outcome {
        Outcome::Done {
            results,
            closed,
            pinned,
        } => {
            let baton = app.streams.finish(lease, closed, pinned);
            axum::Json(PipelineResp {
                baton,
                base_url: None,
                results,
            })
            .into_response()
        }
        Outcome::StreamExpired => {
            app.streams.kill(lease.id);
            text(StatusCode::BAD_REQUEST, crate::stream::StreamError::Expired.message())
        }
        Outcome::QueueTimeout => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Write queue timeout: the request waited too long and was not run",
        ),
        Outcome::QueueFull => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Write queue full: the request was not run",
        ),
        Outcome::Deadline => error(StatusCode::GATEWAY_TIMEOUT, "Request timeout: the request was not run"),
        Outcome::Unavailable => error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Namespace `{}` is not available", db.name),
        ),
    }
}

/// First SQL of the request, for the slow-request log (text only, never the bound values)
fn slow_log_head(requests: &[StreamRequest]) -> String {
    exec::statements(requests).next().map(sql::head).unwrap_or_default()
}

async fn run(db: &Db, stream: u64, pinned: bool, mut requests: Vec<StreamRequest>, deadline: Instant) -> Outcome {
    if !pinned {
        if requests.iter().all(|r| matches!(r, StreamRequest::Close)) {
            let closed = !requests.is_empty();
            let results = requests
                .iter()
                .map(|_| StreamResult::Ok {
                    response: StreamResponse::Close,
                })
                .collect();
            return Outcome::Done {
                results,
                closed,
                pinned: false,
            };
        }
        if reader::looks_read_only(&requests) {
            db.metrics
                .read_requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match db.readers.run(requests, deadline).await {
                Ok(ReadOutcome::Done { results, closed }) => {
                    return Outcome::Done {
                        results,
                        closed,
                        pinned: false,
                    };
                }
                Ok(ReadOutcome::Deadline) => return Outcome::Deadline,
                Ok(ReadOutcome::NeedsWriter(r)) => {
                    db.metrics
                        .read_to_writer
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    requests = r;
                }
                Err(e) => {
                    tracing::error!(ns = &*db.name, error = %e, "read pool failed");
                    return Outcome::Unavailable;
                }
            }
        }
    }
    db.metrics
        .write_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (job, rx) = Job::new(requests, stream, pinned, deadline);
    db.writer.submit(job);
    rx.await.unwrap_or(Outcome::Unavailable)
}

#[derive(Deserialize)]
struct DumpQuery {
    preserve_row_ids: Option<bool>,
}

async fn dump(State(app): State<App>, headers: HeaderMap, Query(q): Query<DumpQuery>) -> Response {
    let db = match database(&app, &headers).await {
        Ok(db) => db,
        Err(r) => return *r,
    };
    // held until the export thread ends, not just until the response head is sent
    let Ok(permit) = app.dumps.clone().try_acquire_owned() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "Too many dumps in progress");
    };
    let path = app.cfg.db_file(&db.name);
    let cfg = app.cfg.clone();
    let ns = db.name.clone();
    let rt = tokio::runtime::Handle::current();
    let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(4);
    // set before the sender is dropped: the body then ends with an error, never with a clean end after a cut-off
    // dump (when the client stalled, the channel is full and the error itself cannot be queued)
    let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let failed_tail = failed.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let started = Instant::now();
        let mut out = ChannelWriter {
            tx: tx.clone(),
            buf: Vec::with_capacity(CHUNK),
            rt,
            stall: cfg.dump_stall_timeout(),
            until: started + cfg.dump_max(),
        };
        let r = dump::Snapshot::take(&path, &cfg)
            .and_then(|snap| dump::export(&snap.conn, &mut out, q.preserve_row_ids.unwrap_or(false)))
            .and_then(|_| out.flush().map_err(Into::into));
        match r {
            Ok(()) => tracing::info!(ns = &*ns, ms = started.elapsed().as_millis() as u64, "dump done"),
            Err(e) => {
                tracing::error!(ns = &*ns, error = %e, "dump failed");
                failed.store(true, std::sync::atomic::Ordering::Release);
            }
        }
    });
    let tail = futures_util::stream::once(async move {
        failed_tail
            .load(std::sync::atomic::Ordering::Acquire)
            .then(|| Err(io::Error::other("dump failed")))
    })
    .filter_map(std::future::ready);
    Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx).chain(tail)).into_response()
}

const CHUNK: usize = 64 * 1024;

/// Buffers dump output and hands it to the HTTP body in chunks. Fails once the client is gone, takes no chunk for
/// `--dump-stall-timeout-ms`, or the dump runs past `--dump-max-ms`.
struct ChannelWriter {
    tx: tokio::sync::mpsc::Sender<io::Result<Bytes>>,
    buf: Vec<u8>,
    rt: tokio::runtime::Handle,
    stall: Duration,
    until: Instant,
}

impl Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= CHUNK {
            self.flush()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if Instant::now() >= self.until {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "dump took longer than --dump-max-ms",
            ));
        }
        let chunk = Bytes::from(std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK)));
        let wait = self.stall.min(self.until.saturating_duration_since(Instant::now()));
        use tokio::sync::mpsc::error::SendTimeoutError;
        match self.rt.block_on(self.tx.send_timeout(Ok(chunk), wait)) {
            Ok(()) => Ok(()),
            Err(SendTimeoutError::Timeout(_)) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the client took no dump data in time",
            )),
            Err(SendTimeoutError::Closed(_)) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "client went away")),
        }
    }
}

#[derive(Deserialize)]
struct CreateReq {
    dump_url: Option<String>,
}

async fn create(State(app): State<App>, Path(ns): Path<String>, headers: HeaderMap, body: Bytes) -> Response {
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
        });
    if !json {
        return text(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Expected request with `Content-Type: application/json`",
        );
    }
    let req: CreateReq = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return text(
                StatusCode::BAD_REQUEST,
                format!("Failed to parse the request body as JSON: {e}"),
            );
        }
    };
    let mut pump = None;
    let dump = match req.dump_url {
        None => None,
        Some(url) => match fetch_dump(&url, app.cfg.allow_remote_dump_url, app.cfg.dump_url_timeout()).await {
            Ok((rx, task)) => {
                pump = Some(AbortOnDrop(task));
                Some(rx)
            }
            Err(e) => {
                tracing::warn!(ns, url = redact(&url), error = %e.1, "dump_url failed");
                return error(e.0, e.1);
            }
        },
    };
    let from_dump = dump.is_some();
    let r = app.create(&ns, dump).await;
    drop(pump);
    match r {
        Ok(()) => StatusCode::OK.into_response(),
        Err(AdminError::Exists(n)) => error(StatusCode::BAD_REQUEST, format!("Namespace `{n}` already exists")),
        Err(e) => {
            tracing::warn!(ns, from_dump, error = ?e, "create failed");
            admin_error(e)
        }
    }
}

async fn remove(State(app): State<App>, Path(ns): Path<String>) -> Response {
    match app.delete(&ns).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => admin_error(e),
    }
}

fn admin_error(e: AdminError) -> Response {
    match e {
        AdminError::Exists(n) => error(StatusCode::BAD_REQUEST, format!("Namespace `{n}` already exists")),
        AdminError::Missing(n) => missing(&n),
        AdminError::BadName(n) => error(StatusCode::BAD_REQUEST, format!("Invalid namespace name `{n}`")),
        AdminError::BadDump(m) => error(StatusCode::BAD_REQUEST, format!("Failed to load dump: {m}")),
        AdminError::Internal(e) => error(StatusCode::INTERNAL_SERVER_ERROR, format!("Internal error: {e}")),
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// `http://host[:port]/path` → (host, port, path-and-query). Only plain http: the dump comes from this machine.
fn parse_http_url(url: &str) -> Result<(String, u16, String), (StatusCode, String)> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err((StatusCode::BAD_REQUEST, "Invalid dump url".into()));
    };
    if !scheme.eq_ignore_ascii_case("http") {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Unsupported dump url scheme `{scheme}`, supported schemes are: `http`"),
        ));
    }
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    if authority.contains('@') || authority.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Invalid dump url".into()));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => (
            h,
            p.parse::<u16>()
                .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid dump url".to_owned()))?,
        ),
        _ => (authority, 80),
    };
    Ok((
        host.trim_start_matches('[').trim_end_matches(']').to_owned(),
        port,
        path.to_owned(),
    ))
}

/// Dump URLs carry a one-time token in the path or the query: logs and errors show only scheme, host and port
fn redact(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => format!(
            "{scheme}://{}/***",
            rest.split(['/', '?', '#']).next().unwrap_or_default()
        ),
        None => "***".into(),
    }
}

/// Host header for the dump URL's host (an IPv6 literal goes in brackets)
fn dump_host_header(host: &str, port: u16) -> String {
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    if port == 80 { host } else { format!("{host}:{port}") }
}

/// Downloads the dump: response head within [`DUMP_URL_HEAD_TIMEOUT`], no gap between chunks longer than
/// [`DUMP_URL_IDLE_TIMEOUT`], all of it within `total`
async fn fetch_dump(
    url: &str,
    allow_remote: bool,
    total: Duration,
) -> Result<(crate::db::DumpRx, tokio::task::JoinHandle<()>), (StatusCode, String)> {
    let until = tokio::time::Instant::now() + total;
    let (host, port, path) = parse_http_url(url)?;
    let addrs: Vec<std::net::SocketAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![(ip, port).into()],
        Err(_) => tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Cannot resolve dump url host: {e}")))?
            .collect(),
    };
    if addrs.is_empty() || (!allow_remote && !addrs.iter().all(|a| a.ip().is_loopback())) {
        return Err((
            StatusCode::BAD_REQUEST,
            "dump_url must point to a loopback address".into(),
        ));
    }
    let bad_gateway = |what: &str, e: &dyn std::fmt::Display| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Internal error: {what}: {e}"),
        )
    };
    let req = hyper::Request::get(path)
        .header(header::HOST, dump_host_header(&host, port))
        .body(http_body_util::Empty::<Bytes>::new())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid dump url: {e}")))?;
    let head = tokio::time::timeout_at(until.min(tokio::time::Instant::now() + DUMP_URL_HEAD_TIMEOUT), async {
        let stream = tokio::net::TcpStream::connect(&addrs[..])
            .await
            .map_err(|e| bad_gateway("cannot connect to dump url", &e))?;
        let (mut sender, conn) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .map_err(|e| bad_gateway("dump url handshake", &e))?;
        let conn_task = AbortOnDrop(tokio::spawn(async move {
            let _ = conn.await;
        }));
        let res = sender
            .send_request(req)
            .await
            .map_err(|e| bad_gateway("dump url request", &e))?;
        Ok((conn_task, res))
    });
    let (conn_task, res) = head
        .await
        .map_err(|_| bad_gateway("dump url", &"no response before timeout"))??;
    if !res.status().is_success() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("dump url answered HTTP {}", res.status().as_u16()),
        ));
    }
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let task = tokio::spawn(async move {
        let _conn = conn_task;
        let mut body = res.into_body();
        loop {
            let idle = tokio::time::Instant::now() + DUMP_URL_IDLE_TIMEOUT;
            let frame = match tokio::time::timeout_at(idle.min(until), body.frame()).await {
                Ok(Some(f)) => f,
                Ok(None) => {
                    // the whole body has arrived: only now may the loader take the end of the data for the end of
                    // the dump (this task is aborted when the create request is abandoned)
                    let _ = tx.send(Ok(None)).await;
                    break;
                }
                Err(_) => {
                    let why = if idle < until {
                        "dump url stalled"
                    } else {
                        "dump url download took longer than --dump-url-timeout-ms"
                    };
                    let _ = tx.send(Err(io::Error::new(io::ErrorKind::TimedOut, why))).await;
                    break;
                }
            };
            let item = match frame {
                Ok(f) => match f.into_data() {
                    Ok(d) => Ok(Some(d)),
                    Err(_) => continue,
                },
                Err(e) => Err(io::Error::other(e)),
            };
            let stop = item.is_err();
            if tx.send(item).await.is_err() || stop {
                break;
            }
        }
    });
    Ok((rx, task))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    fn app(tweak: impl FnOnce(&mut crate::config::Config)) -> (tempfile::TempDir, App) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::config::Config::for_tests(dir.path());
        tweak(&mut cfg);
        (dir, Registry::new(Arc::new(cfg)))
    }

    /// A database whose dump is several MB, far more than the body channel holds
    async fn big_tenant(app: &App) -> std::path::PathBuf {
        app.create("tenant", None).await.unwrap();
        let path = app.cfg.db_file("tenant");
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "CREATE TABLE big(v); WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400) \
             INSERT INTO big SELECT hex(randomblob(4000)) FROM n;",
        )
        .unwrap();
        path
    }

    async fn get(app: &App, uri: &str, host: &str) -> Response {
        let req = axum::http::Request::get(uri)
            .header(header::HOST, host)
            .body(Body::empty())
            .unwrap();
        user_router(app.clone()).oneshot(req).await.unwrap()
    }

    fn tmp_files(app: &App) -> usize {
        std::fs::read_dir(app.cfg.tmp_dir()).map(|d| d.count()).unwrap_or(0)
    }

    async fn served(
        tweak: impl FnOnce(&mut crate::config::Config),
    ) -> (
        tempfile::TempDir,
        std::net::SocketAddr,
        tokio::sync::watch::Sender<bool>,
    ) {
        let (dir, app) = app(tweak);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, rx) = tokio::sync::watch::channel(false);
        tokio::spawn(serve(listener, user_router(app.clone()), app.cfg.clone(), rx));
        (dir, addr, stop)
    }

    /// Raw client: sends `data`, then collects what the server writes until it closes or `wait` passes.
    /// Returns the text and whether the server closed the connection.
    async fn exchange(conn: &mut tokio::net::TcpStream, data: &[u8], wait: Duration) -> (String, bool) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        conn.write_all(data).await.unwrap();
        let until = tokio::time::Instant::now() + wait;
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            match tokio::time::timeout_at(until, conn.read(&mut buf)).await {
                Ok(Ok(0)) | Ok(Err(_)) => return (String::from_utf8_lossy(&out).into_owned(), true),
                Ok(Ok(n)) => out.extend_from_slice(&buf[..n]),
                Err(_) => return (String::from_utf8_lossy(&out).into_owned(), false),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dump_url_download_has_a_total_time_limit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // answers at once, then sends one byte every 50 ms: never stalls, never ends
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut c, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = c.read(&mut buf).await;
            let _ = c.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n").await;
            while c.write_all(b"-").await.is_ok() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        let started = Instant::now();
        let (mut rx, _task) = fetch_dump(&format!("http://{addr}/dump"), false, Duration::from_millis(400))
            .await
            .unwrap();
        let failure = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(item) = rx.recv().await {
                if let Err(e) = item {
                    return Some(e);
                }
            }
            None
        })
        .await
        .expect("the download went on past its time limit");
        let e = failure.expect("the download must end with an error");
        assert_eq!(e.kind(), io::ErrorKind::TimedOut, "{e}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    const HEALTH: &[u8] = b"GET /health HTTP/1.1\r\nHost: tenant.db\r\n\r\n";

    /// Review of 0.1.0, finding 6. What gives a web page away: the Host of a rebound domain, or the headers browsers
    /// add to cross-site requests. Node's `fetch` sends `sec-fetch-mode` and is a local client like any other.
    #[tokio::test(flavor = "multi_thread")]
    async fn requests_from_web_pages_are_refused() {
        let (_d, app) = app(|_| {});
        app.create("tenant", None).await.unwrap();
        let insert = r#"{"requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE t(a)"}}]}"#;
        let post = |host: &'static str, extra: Option<(&'static str, &'static str)>| {
            let mut req = axum::http::Request::post("/v2/pipeline")
                .header(header::HOST, host)
                .header(header::CONTENT_TYPE, "text/plain");
            if let Some((k, v)) = extra {
                req = req.header(k, v);
            }
            user_router(app.clone()).oneshot(req.body(Body::from(insert)).unwrap())
        };
        let refused = [
            // a page on a rebound name, as its own origin: with the headers of a POST, and with none at all
            (
                "tenant.attacker.example",
                Some(("origin", "http://tenant.attacker.example")),
            ),
            ("tenant.attacker.example", Some(("sec-fetch-site", "same-origin"))),
            ("tenant.attacker.example", None),
            ("tenant.db.attacker.example:8080", None),
            // a cross-site request to a local name: a form post, a `no-cors` fetch
            ("tenant.localhost:8080", Some(("origin", "https://attacker.example"))),
            ("tenant.localhost:8080", Some(("sec-fetch-site", "cross-site"))),
            ("tenant.db", Some(("origin", "null"))),
        ];
        for (host, extra) in refused {
            let r = post(host, extra).await.unwrap();
            assert_eq!(r.status(), StatusCode::FORBIDDEN, "{host} {extra:?}");
        }
        // the same-origin GET of a rebound page carries nothing but its Host
        for uri in ["/dump", "/metrics", "/health"] {
            let r = get(&app, uri, "tenant.attacker.example").await;
            assert_eq!(r.status(), StatusCode::FORBIDDEN, "{uri}");
        }
        for (host, extra) in [
            ("sqld-admin", Some(("sec-fetch-site", "cross-site"))),
            ("admin.attacker.example", None),
        ] {
            let mut delete = axum::http::Request::delete("/v1/namespaces/tenant").header(header::HOST, host);
            if let Some((k, v)) = extra {
                delete = delete.header(k, v);
            }
            let r = admin_router(app.clone())
                .oneshot(delete.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::FORBIDDEN, "{host}");
        }
        assert!(app.exists("tenant"));
        let c = rusqlite::Connection::open(app.cfg.db_file("tenant")).unwrap();
        let tables: i64 = c
            .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tables, 0, "nothing ran");
        // local clients: the hosts in use, with or without the header Node's fetch adds
        for (host, extra) in [
            ("tenant.db", Some(("sec-fetch-mode", "cors"))),
            ("tenant.local", None),
            ("tenant.localhost:8080", None),
        ] {
            let r = post(host, extra).await.unwrap();
            assert_eq!(r.status(), StatusCode::OK, "{host} {extra:?}");
        }
        assert_eq!(get(&app, "/health", "127.0.0.1:8080").await.status(), StatusCode::OK);
        assert_eq!(get(&app, "/health", "[::1]:8080").await.status(), StatusCode::OK);
        // a request without a Host header (HTTP/1.0) names no database
        let no_host = axum::http::Request::post("/v2/pipeline")
            .body(Body::from(insert))
            .unwrap();
        let r = user_router(app.clone()).oneshot(no_host).await.unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
        // other names are a matter of configuration
        let (_d, open) = self::app(|c| c.allowed_host_suffixes = vec!["sqld.example".into()]);
        assert_eq!(
            get(&open, "/health", "tenant.sqld.example").await.status(),
            StatusCode::OK
        );
        assert_eq!(get(&open, "/health", "tenant.db").await.status(), StatusCode::FORBIDDEN);
    }

    /// Review of 0.1.0, finding 5: the admin client gives up while the dump is still on its way
    #[tokio::test(flavor = "multi_thread")]
    async fn a_create_abandoned_during_the_download_creates_nothing() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut c, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let _ = c.read(&mut buf).await;
                    // the head at once, the body (a complete dump) later: like GET /dump while it copies the database
                    let _ = c
                        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                        .await;
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    let body = "PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\nCREATE TABLE t(a);\nCOMMIT;\n";
                    let _ = c
                        .write_all(format!("{:x}\r\n{}\r\n0\r\n\r\n", body.len(), body).as_bytes())
                        .await;
                });
            }
        });
        let (_d, app) = app(|_| {});
        let create = || {
            let req = axum::http::Request::post("/v1/namespaces/tenant/create")
                .header(header::HOST, "sqld-admin")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"dump_url":"http://{addr}/dump"}}"#)))
                .unwrap();
            admin_router(app.clone()).oneshot(req)
        };
        let r = tokio::time::timeout(Duration::from_millis(200), create()).await;
        assert!(r.is_err(), "the request should still be running");
        // the abandoned build ends without a database, and the name is free for the next try
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!app.exists("tenant"), "an abandoned create left a database in place");
        assert_eq!(create().await.unwrap().status(), StatusCode::OK);
        let c = rusqlite::Connection::open(app.cfg.db_file("tenant")).unwrap();
        let tables: i64 = c
            .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tables, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_heads_and_idle_connections_are_closed() {
        let (_d, addr, _stop) = served(|c| c.header_read_timeout_ms = 300).await;
        let started = Instant::now();
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, closed) = exchange(&mut c, b"GET /health HTTP/1.1\r\nHost: t", Duration::from_secs(3)).await;
        assert!(
            closed && started.elapsed() < Duration::from_secs(2),
            "incomplete head kept open"
        );
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (out, closed) = exchange(&mut c, HEALTH, Duration::from_secs(3)).await;
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(closed, "an idle keep-alive connection must be closed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_body_that_does_not_arrive_in_time_gets_408() {
        // review 2318 S1: a 1-byte body kept a request (and its time limit, counted after the body) open for minutes
        let (_d, addr, _stop) = served(|c| c.body_read_timeout_ms = 300).await;
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let started = Instant::now();
        let (out, _) = exchange(
            &mut c,
            b"POST /v2/pipeline HTTP/1.1\r\nHost: tenant.db\r\nContent-Length: 50\r\n\r\n{",
            Duration::from_secs(3),
        )
        .await;
        assert!(out.starts_with("HTTP/1.1 408"), "{out}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn requests_beyond_the_limit_get_503_but_health_is_answered() {
        let (_d, addr, _stop) = served(|c| {
            c.max_concurrent_requests = 1;
            c.body_read_timeout_ms = 3000;
        })
        .await;
        // holds the only slot while its body trickles in
        let mut slow = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = exchange(
            &mut slow,
            b"POST /v2/pipeline HTTP/1.1\r\nHost: tenant.db\r\nContent-Length: 50\r\n\r\n{",
            Duration::from_millis(200),
        )
        .await;
        let body = r#"{"requests":[]}"#;
        let req = format!(
            "POST /v2/pipeline HTTP/1.1\r\nHost: tenant.db\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (out, _) = exchange(&mut c, req.as_bytes(), Duration::from_millis(500)).await;
        assert!(out.starts_with("HTTP/1.1 503"), "{out}");
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (out, _) = exchange(&mut c, HEALTH, Duration::from_millis(500)).await;
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connections_beyond_the_limit_wait_for_a_free_one() {
        let (_d, addr, _stop) = served(|c| c.max_connections = 1).await;
        let first = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (out, _) = exchange(&mut second, HEALTH, Duration::from_millis(300)).await;
        assert!(out.is_empty(), "served past the connection limit: {out}");
        drop(first);
        let (out, _) = exchange(&mut second, b"", Duration::from_secs(2)).await;
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_slow_dump_client_does_not_hold_back_checkpoints() {
        // review 2318 M3: the dump read the database in one transaction while waiting for the client, so the WAL
        // could not be checkpointed and grew without bound
        let (_d, app) = app(|_| {});
        let path = big_tenant(&app).await;
        let res = get(&app, "/dump", "tenant.db").await;
        assert_eq!(res.status(), StatusCode::OK);
        let checkpointed = tokio::task::spawn_blocking(move || {
            let c = rusqlite::Connection::open(&path).unwrap();
            let until = Instant::now() + Duration::from_secs(5);
            while Instant::now() < until {
                c.execute("INSERT INTO big VALUES ('later')", []).unwrap();
                let r = c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))
                });
                if let Ok((0, log, done)) = r
                    && log == done
                {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            false
        })
        .await
        .unwrap();
        assert!(
            checkpointed,
            "the WAL could not be checkpointed while the dump client was not reading"
        );
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.ends_with("COMMIT;\n"));
        assert!(text.matches("INSERT INTO big VALUES").count() >= 400);
        assert_eq!(tmp_files(&app), 0, "the copy is removed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dumps_running_at_once_are_limited_until_their_export_ends() {
        let (_d, app) = app(|c| c.max_concurrent_dumps = 1);
        big_tenant(&app).await;
        // a client that does not read: the export thread keeps its permit after the response head was sent
        let first = get(&app, "/dump", "tenant.db").await;
        assert_eq!(first.status(), StatusCode::OK);
        let res = get(&app, "/dump", "tenant.db").await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(first);
        let until = Instant::now() + Duration::from_secs(5);
        while app.dumps.available_permits() == 0 && Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let res = get(&app, "/dump", "tenant.db").await;
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert!(body.ends_with(b"COMMIT;\n"));
    }

    #[tokio::test]
    async fn health_and_metrics_name_the_build() {
        let (_d, app) = app(|_| {});
        let res = get(&app, "/health", "tenant.db").await;
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(v["commit"], env!("XSQLD_GIT_COMMIT"));
        let res = get(&app, "/metrics", "tenant.db").await;
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let line = format!(
            "xsqld_build_info{{version=\"{}\",commit=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION"),
            env!("XSQLD_GIT_COMMIT")
        );
        assert!(std::str::from_utf8(&body).unwrap().starts_with(&line));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stalled_or_overlong_dump_is_abandoned_with_an_error() {
        let (_d, stalled) = app(|c| c.dump_stall_timeout_ms = 200);
        big_tenant(&stalled).await;
        let res = get(&stalled, "/dump", "tenant.db").await;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(tmp_files(&stalled), 0, "the copy is removed once the client stalls");
        assert!(
            res.into_body().collect().await.is_err(),
            "the body must not end cleanly"
        );

        let (_d2, overlong) = app(|c| c.dump_max_ms = 0);
        big_tenant(&overlong).await;
        let res = get(&overlong, "/dump", "tenant.db").await;
        assert!(res.into_body().collect().await.is_err());
        assert_eq!(tmp_files(&overlong), 0);
    }

    #[test]
    fn urls() {
        assert_eq!(
            parse_http_url("http://127.0.0.1:8787/dump/abc?x=1").unwrap(),
            ("127.0.0.1".into(), 8787, "/dump/abc?x=1".into())
        );
        assert_eq!(
            parse_http_url("http://localhost/x").unwrap(),
            ("localhost".into(), 80, "/x".into())
        );
        assert_eq!(
            parse_http_url("http://[::1]:9/x").unwrap(),
            ("::1".into(), 9, "/x".into())
        );
        assert_eq!(parse_http_url("ftp://x").unwrap_err().0, StatusCode::BAD_REQUEST);
        assert!(parse_http_url("file:///etc/passwd").is_err());
        assert!(parse_http_url("http://user@127.0.0.1/x").is_err());
        assert_eq!(redact("http://127.0.0.1:8787/dump/SECRET"), "http://127.0.0.1:8787/***");
        assert_eq!(
            redact("http://127.0.0.1:8787?token=SECRET"),
            "http://127.0.0.1:8787/***"
        );
        assert_eq!(redact("http://[::1]:9#SECRET"), "http://[::1]:9/***");
        assert_eq!(dump_host_header("::1", 9), "[::1]:9");
        assert_eq!(dump_host_header("::1", 80), "[::1]");
        assert_eq!(dump_host_header("127.0.0.1", 8787), "127.0.0.1:8787");
        assert_eq!(dump_host_header("localhost", 80), "localhost");
    }

    #[test]
    fn host_header() {
        let mut h = HeaderMap::new();
        for (host, ns) in [
            ("tenant.db", "tenant"),
            ("platform.local:8080", "platform"),
            ("restore-check.localhost", "restore-check"),
        ] {
            h.insert(header::HOST, host.parse().unwrap());
            assert_eq!(namespace(&h).as_deref(), Some(ns));
        }
    }
}
