use crate::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{FutureExt, future::BoxFuture};
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// How the miner proves itself to the node. The file is read once, and
/// again after the node answers 401 or 403, so a replaced token, or the new cookie
/// a restarted node writes, applies from the next request (the bitcoin-cli
/// way). A file that cannot be read fails the request: none goes out
/// without the credentials asked for.
#[derive(Debug, Clone)]
pub enum Auth {
    None,
    /// A loopback RPC cookie file (`__cookie__:<secret>`), sent as HTTP Basic.
    Cookie(PathBuf),
    /// A bearer token file for a node's remote operator listener; a miner's
    /// token needs `POST /mining/templates`, `POST /mining/work` and
    /// `GET /status` (and `GET /mining/events` to be pushed templates).
    Bearer(PathBuf),
}

impl Auth {
    async fn header(&self) -> Result<Option<String>, Error> {
        let (file, bearer) = match self {
            Auth::None => return Ok(None),
            Auth::Cookie(file) => (file, false),
            Auth::Bearer(file) => (file, true),
        };
        let content = tokio::fs::read_to_string(file)
            .await
            .map_err(|e| Error::Configuration(format!("{}: {e}", file.display())))?;
        let secret = content.trim();
        if secret.is_empty() {
            return Err(Error::Configuration(format!("{} is empty", file.display())));
        }
        Ok(Some(if bearer {
            format!("Bearer {secret}")
        } else {
            format!("Basic {}", STANDARD.encode(secret))
        }))
    }
}

/// The resubscribe wait after an event stream ends: it doubles from the
/// first to the last, and starts again from the first after a push.
pub const EVENTS_BACKOFF: (Duration, Duration) =
    (Duration::from_millis(250), Duration::from_secs(5));

/// A quiet stream still sends a keepalive comment every 15 seconds; this
/// long without a byte means the connection is gone.
pub const EVENTS_SILENCE: Duration = Duration::from_secs(45);

#[derive(Clone)]
pub struct HttpNodeClient {
    base: url::Url,
    request: Arc<RwLock<serde_json::Value>>,
    auth: Auth,
    /// The authorization header read from `auth`'s file, until a 401.
    header: Arc<RwLock<Option<String>>>,
    client: reqwest::Client,
    timeout: Duration,
    maximum_bytes: usize,
    unpaid: Arc<RwLock<Vec<String>>>,
}

impl HttpNodeClient {
    /// `timeout` bounds each request (a template request includes the node
    /// building it); the event stream is bounded by silence instead.
    pub fn new(
        base: &str,
        request: serde_json::Value,
        auth: Auth,
        timeout: Duration,
        maximum_bytes: usize,
    ) -> Result<Self, Error> {
        let mut base = url::Url::parse(base).map_err(|e| Error::Configuration(e.to_string()))?;
        if !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(Error::Configuration(
                "node URL credentials/query/fragment forbidden".into(),
            ));
        }
        // The node's listeners speak plain HTTP; a remote one belongs on a
        // private network or behind a TLS proxy (lattice-node rpc-api.md).
        if base.scheme() != "https" && base.scheme() != "http" {
            return Err(Error::Configuration(
                "node URL must be http or https".into(),
            ));
        }
        if timeout.is_zero() || maximum_bytes == 0 {
            return Err(Error::Configuration(
                "positive deadline/body bound required".into(),
            ));
        }
        let path = format!("{}/", base.path().trim_end_matches('/'));
        base.set_path(&path);
        Ok(Self {
            base,
            request: Arc::new(RwLock::new(request)),
            auth,
            header: Arc::default(),
            client: reqwest::Client::builder()
                .connect_timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            timeout,
            maximum_bytes,
            unpaid: Arc::default(),
        })
    }

    /// The chains the last fetched template pays no one (see
    /// [`TemplateResponse::unpaid_chains`]).
    pub fn unpaid_chains(&self) -> Vec<String> {
        self.unpaid.read().unwrap().clone()
    }

    /// The body of the next template requests: recipients and minimum work.
    pub fn set_request(&self, request: serde_json::Value) {
        *self.request.write().unwrap() = request;
    }

    async fn send(
        &self,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<reqwest::Response, Error> {
        let url = self
            .base
            .join(path)
            .map_err(|e| Error::Configuration(e.to_string()))?;
        let mut request = match body {
            Some(body) => self.client.post(url).json(&body),
            None => self.client.get(url),
        };
        let cached = self.header.read().unwrap().clone();
        let header = match cached {
            Some(header) => Some(header),
            None => {
                let read = self.auth.header().await?;
                self.header.write().unwrap().clone_from(&read);
                read
            }
        };
        if let Some(header) = header {
            request = request.header(reqwest::header::AUTHORIZATION, header);
        }
        let response = request.send().await?;
        if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            // A restarted node writes a new cookie: read it again.
            *self.header.write().unwrap() = None;
        }
        Ok(response)
    }

    async fn request(
        &self,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<(u16, Vec<u8>), Error> {
        tokio::time::timeout(self.timeout, async {
            let mut response = self.send(path, body).await?;
            let status = response.status().as_u16();
            if status == 401 || status == 403 {
                return Err(Error::Unauthorized(status));
            }
            if response
                .content_length()
                .is_some_and(|n| n > self.maximum_bytes as u64)
            {
                return Err(Error::Response(status));
            }
            let mut bytes = vec![];
            while let Some(chunk) = response.chunk().await? {
                if chunk.len() > self.maximum_bytes - bytes.len() {
                    return Err(Error::Response(status));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok((status, bytes))
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    /// Follows `GET /mining/events`, publishing each pushed template digest;
    /// `None` while there is no stream. A stream that ends (the node stopped,
    /// a refusal such as a token without the route, an older node without
    /// it, silence) says nothing about the chain: work goes on, and the
    /// stream is opened again after [`EVENTS_BACKOFF`]. Runs until `cancel`.
    pub fn digest_feed(&self, cancel: CancellationToken) -> watch::Receiver<Option<String>> {
        let (tx, rx) = watch::channel(None);
        let this = self.clone();
        tokio::spawn(async move {
            let (first, last) = EVENTS_BACKOFF;
            let mut wait = first;
            let mut announced_loss = false;
            loop {
                let mut pushed = false;
                let ended = tokio::select! {
                    _ = cancel.cancelled() => return,
                    ended = this.follow_events(&tx, &mut pushed) => ended,
                };
                tx.send_if_modified(|digest| digest.take().is_some());
                if pushed {
                    wait = first;
                    announced_loss = false;
                }
                if !announced_loss {
                    eprintln!(
                        "mining events: {ended}; watching GET /status until the stream is back"
                    );
                    announced_loss = true;
                }
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
                wait = (wait * 2).min(last);
            }
        });
        rx
    }

    /// One event stream, until it ends; returns why.
    async fn follow_events(&self, tx: &watch::Sender<Option<String>>, pushed: &mut bool) -> String {
        let opened = tokio::time::timeout(self.timeout, self.send("mining/events", None)).await;
        let mut response = match opened {
            Err(_) => return "no answer from GET /mining/events".into(),
            Ok(Err(e)) => return format!("GET /mining/events failed: {e}"),
            Ok(Ok(response)) if response.status() != reqwest::StatusCode::OK => {
                return format!(
                    "GET /mining/events answered HTTP {}",
                    response.status().as_u16()
                );
            }
            Ok(Ok(response)) => response,
        };
        let mut line = Vec::new();
        let mut data: Option<String> = None;
        loop {
            let chunk = match tokio::time::timeout(EVENTS_SILENCE, response.chunk()).await {
                Err(_) => {
                    return format!(
                        "the event stream was silent for {}s",
                        EVENTS_SILENCE.as_secs()
                    );
                }
                Ok(Err(e)) => return format!("the event stream failed: {e}"),
                Ok(Ok(None)) => return "the event stream ended".into(),
                Ok(Ok(Some(chunk))) => chunk,
            };
            for &byte in chunk.iter() {
                if byte != b'\n' {
                    if line.len() >= self.maximum_bytes {
                        return "an event stream line exceeded the byte limit".into();
                    }
                    line.push(byte);
                    continue;
                }
                let text = String::from_utf8_lossy(&line);
                let text = text.strip_suffix('\r').unwrap_or(&text);
                if text.is_empty() {
                    // An event ends at a blank line.
                    if let Some(digest) = data.take().filter(|d| !d.is_empty()) {
                        if !*pushed {
                            eprintln!("mining events: following GET /mining/events");
                        }
                        *pushed = true;
                        tx.send_replace(Some(digest));
                    }
                } else if let Some(value) = text.strip_prefix("data:") {
                    let value = value.strip_prefix(' ').unwrap_or(value);
                    match data.as_mut() {
                        Some(d) => {
                            d.push('\n');
                            d.push_str(value);
                        }
                        None => data = Some(value.to_owned()),
                    }
                }
                // Comments (`: keepalive`) and other fields carry nothing.
                line.clear();
            }
        }
    }
}

/// The reason in a node's `{"error":{"message":...}}` refusal body.
fn refusal_message(bytes: &[u8]) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Refusal {
        error: Reason,
    }
    #[derive(serde::Deserialize)]
    struct Reason {
        message: String,
    }
    serde_json::from_slice::<Refusal>(bytes)
        .ok()
        .map(|r| r.error.message)
}

pub fn decode_submission(status: u16, bytes: &[u8]) -> Result<Submission, Error> {
    if status == 401 || status == 403 {
        return Err(Error::Unauthorized(status));
    }
    if status >= 500 {
        return Err(Error::Response(status));
    }
    if let Ok(result) = serde_json::from_slice::<Submission>(bytes) {
        return Ok(result);
    }
    if status >= 400
        && let Some(message) = refusal_message(bytes)
    {
        return Ok(Submission {
            accepted: false,
            disposition: message,
            tip_cid: None,
        });
    }
    Err(Error::Response(status))
}

impl NodeClient for HttpNodeClient {
    fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
        async move {
            let request = self.request.read().unwrap().clone();
            let (status, bytes) = self.request("mining/templates", Some(request)).await?;
            if status == 200 {
                let template = serde_json::from_slice::<TemplateResponse>(&bytes)?;
                *self.unpaid.write().unwrap() = template.unpaid_chains();
                return Ok(Some(template.try_into()?));
            }
            // The node's own reason, when it gives one, says why there is
            // no template (not synced, a bad recipient, ...).
            match (status, refusal_message(&bytes)) {
                (_, Some(message)) => Err(Error::Refused(status, message)),
                (409 | 503, None) => Ok(None),
                _ => Err(Error::Response(status)),
            }
        }
        .boxed()
    }
    fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
        async move {
            let (status, bytes) = self.request("status", None).await?;
            if status != 200 {
                return Ok(None);
            }
            #[derive(serde::Deserialize)]
            struct Status {
                #[serde(rename = "tipCID")]
                tip: Option<String>,
                #[serde(rename = "templateDigest")]
                digest: Option<String>,
            }
            let s: Status = serde_json::from_slice(&bytes).map_err(|_| Error::Response(status))?;
            Ok(s.digest
                .filter(|s| !s.is_empty())
                .or_else(|| s.tip.filter(|s| !s.is_empty())))
        }
        .boxed()
    }
    fn submit(&self, work_id: String, nonce: u64) -> BoxFuture<'_, Result<Submission, Error>> {
        async move {
            let (status, bytes) = self
                .request(
                    "mining/work",
                    Some(serde_json::json!({"workID":work_id,"nonce":nonce})),
                )
                .await?;
            decode_submission(status, &bytes)
        }
        .boxed()
    }
}
