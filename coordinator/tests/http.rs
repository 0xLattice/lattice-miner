use base64::{Engine, engine::general_purpose::STANDARD};
use lattice_miner::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
async fn server(status: u16, body: String) -> (String, tokio::task::JoinHandle<String>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut bytes = vec![];
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            s.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
        }
        let head = String::from_utf8(bytes.clone()).unwrap();
        let len = head
            .lines()
            .find_map(|l| {
                l.to_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|n| n.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut bodyin = vec![0; len];
        s.read_exact(&mut bodyin).await.unwrap();
        bytes.extend(bodyin);
        let head = format!(
            "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        s.write_all(head.as_bytes()).await.unwrap();
        s.write_all(body.as_bytes()).await.unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (url, task)
}
fn client(url: &str) -> HttpNodeClient {
    HttpNodeClient::new(
        url,
        serde_json::json!({"recipients":[]}),
        Auth::None,
        Duration::from_secs(1),
        1024,
    )
    .unwrap()
}
#[tokio::test]
async fn status_digest_fallback_and_template_refusal() {
    for (body, expected) in [
        (r#"{"templateDigest":"new","tipCID":"old"}"#, Some("new")),
        (r#"{"templateDigest":"","tipCID":"old"}"#, Some("old")),
        ("{}", None),
    ] {
        let (url, t) = server(200, body.into()).await;
        assert_eq!(
            client(&url).stale_token().await.unwrap().as_deref(),
            expected
        );
        assert!(t.await.unwrap().starts_with("GET /status "));
    }
    for status in [409, 503] {
        let (url, t) = server(status, "{}".into()).await;
        assert!(client(&url).fetch_work().await.unwrap().is_none());
        let request = t.await.unwrap();
        assert!(request.starts_with("POST /mining/templates "));
        assert!(request.ends_with(r#"{"recipients":[]}"#));
    }
    // A refusal keeps the node's reason.
    let (url, t) = server(503, r#"{"error":{"message":"not synced"}}"#.into()).await;
    let error = client(&url).fetch_work().await.unwrap_err();
    assert_eq!(error.to_string(), "the node answered HTTP 503: not synced");
    t.await.unwrap();
}
#[tokio::test]
async fn submit_contract_cookie_and_refusals() {
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join("cookie");
    std::fs::write(&cookie, "__cookie__:new\n").unwrap();
    let (url, t) = server(409, r#"{"error":{"message":"expired"}}"#.into()).await;
    let c = HttpNodeClient::new(
        &url,
        serde_json::json!({}),
        Auth::Cookie(cookie),
        Duration::from_secs(1),
        1024,
    )
    .unwrap();
    let result = c.submit("work".into(), u64::MAX).await.unwrap();
    assert!(!result.accepted);
    assert_eq!(result.disposition, "expired");
    let request = t.await.unwrap();
    assert!(request.starts_with("POST /mining/work "));
    assert!(request.contains("authorization: Basic X19jb29raWVfXzpuZXc="));
    let body: serde_json::Value =
        serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(body["nonce"], u64::MAX);
    assert_eq!(body["workID"], "work");
}
#[test]
fn decode_http_and_url_policy() {
    for status in [401, 403] {
        assert!(matches!(
            decode_submission(status, b"{}"),
            Err(Error::Unauthorized(_))
        ));
    }
    for status in [500, 503] {
        assert!(
            decode_submission(status, br#"{"accepted":true,"disposition":"accepted"}"#).is_err()
        );
    }
    assert!(decode_submission(409, br#"{"accepted":false,"disposition":"stale"}"#).is_ok());
    assert!(decode_submission(200, b"not json").is_err());
    for url in [
        "ftp://node.example",
        "https://user:pass@example.com",
        "https://a.example/?token=x",
    ] {
        assert!(
            HttpNodeClient::new(
                url,
                serde_json::json!({}),
                Auth::None,
                Duration::from_secs(1),
                1024
            )
            .is_err()
        );
    }
}
#[tokio::test]
async fn bounded_reply_and_no_redirect() {
    let (url, t) = server(200, "x".repeat(2000)).await;
    assert!(client(&url).stale_token().await.is_err());
    t.await.unwrap();
    let (url, t) = server(302, "{}".into()).await;
    assert!(client(&url).stale_token().await.unwrap().is_none());
    t.await.unwrap();
}
/// Answers each connection with the next status, recording each request.
async fn replies(statuses: Vec<u16>) -> (String, Arc<Mutex<Vec<String>>>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(vec![]));
    let requests = seen.clone();
    tokio::spawn(async move {
        for status in statuses {
            let (mut s, _) = l.accept().await.unwrap();
            let mut head = vec![];
            let mut byte = [0];
            while !head.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            requests
                .lock()
                .unwrap()
                .push(String::from_utf8(head).unwrap());
            let body = r#"{"accepted":false,"disposition":"childOnly"}"#;
            let reply = format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            s.write_all(reply.as_bytes()).await.unwrap();
        }
    });
    (url, seen)
}
#[tokio::test]
async fn the_cookie_is_read_once_and_again_after_a_401() {
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join("cookie");
    let basic = |secret: &str| {
        format!(
            "authorization: Basic {}",
            STANDARD.encode(format!("__cookie__:{secret}"))
        )
    };
    std::fs::write(&cookie, "__cookie__:old\n").unwrap();
    let (url, seen) = replies(vec![200, 200, 401, 200]).await;
    let c = HttpNodeClient::new(
        &url,
        serde_json::json!({}),
        Auth::Cookie(cookie.clone()),
        Duration::from_secs(1),
        1024,
    )
    .unwrap();
    c.submit("w".into(), 1).await.unwrap();
    // Gone and replaced: the cookie read first is still sent, until a 401.
    std::fs::remove_file(&cookie).unwrap();
    c.submit("w".into(), 2).await.unwrap();
    std::fs::write(&cookie, "__cookie__:new\n").unwrap();
    assert!(matches!(
        c.submit("w".into(), 3).await,
        Err(Error::Unauthorized(401))
    ));
    c.submit("w".into(), 4).await.unwrap();
    let seen = seen.lock().unwrap().clone();
    let sent: Vec<bool> = seen.iter().map(|r| r.contains(&basic("old"))).collect();
    assert_eq!(sent, [true, true, true, false]);
    assert!(seen[3].contains(&basic("new")));
}
#[tokio::test]
async fn a_cookie_that_cannot_be_read_sends_no_request() {
    let dir = tempfile::tempdir().unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    for file in ["missing", "empty"] {
        let path = dir.path().join(file);
        if file == "empty" {
            std::fs::write(&path, "\n").unwrap();
        }
        let c = HttpNodeClient::new(
            &url,
            serde_json::json!({}),
            Auth::Cookie(path),
            Duration::from_secs(1),
            1024,
        )
        .unwrap();
        assert!(matches!(
            c.submit("w".into(), 1).await,
            Err(Error::Configuration(_))
        ));
        assert!(c.fetch_work().await.is_err());
    }
    // No request reached the node without the cookie.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), l.accept())
            .await
            .is_err()
    );
}
