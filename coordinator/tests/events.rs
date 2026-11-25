//! `GET /mining/events`: the pushed template digest.
use lattice_miner::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::watch,
};
use tokio_util::sync::CancellationToken;

/// Serves one scripted reply per connection, in order, recording each
/// request head. A reply is (status, chunks of body, hold open after).
async fn events_server(
    replies: Vec<(u16, Vec<&'static str>, bool)>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let heads = Arc::new(Mutex::new(vec![]));
    let seen = heads.clone();
    tokio::spawn(async move {
        for (status, chunks, hold) in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = vec![];
            let mut byte = [0];
            while !head.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            seen.lock().unwrap().push(String::from_utf8(head).unwrap());
            let reply = format!(
                "HTTP/1.1 {status} Fixture\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
            for chunk in chunks {
                let framed = format!("{:x}\r\n{chunk}\r\n", chunk.len());
                socket.write_all(framed.as_bytes()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if hold {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    drop(socket);
                });
            } else {
                socket.write_all(b"0\r\n\r\n").await.unwrap();
            }
        }
    });
    (url, heads)
}

async fn next_value(rx: &mut watch::Receiver<Option<String>>) -> Option<String> {
    tokio::time::timeout(Duration::from_secs(5), rx.changed())
        .await
        .expect("a digest change")
        .unwrap();
    rx.borrow_and_update().clone()
}

#[tokio::test]
async fn feed_publishes_digests_and_reconnects() {
    let (url, heads) = events_server(vec![
        // Split across chunks, with a keepalive comment and CRLF endings.
        (
            200,
            vec!["data: d1\n", "\n: keepalive\n\n", "data: d2\r\n\r\n"],
            false,
        ),
        // A refusal (a token without the route) is no stream.
        (403, vec![], false),
        (200, vec!["data: d3\n\n"], true),
    ])
    .await;
    let dir = tempfile::tempdir().unwrap();
    let token = dir.path().join("token");
    std::fs::write(&token, "secret\n").unwrap();
    let client = HttpNodeClient::new(
        &url,
        serde_json::json!({}),
        Auth::Bearer(token),
        Duration::from_secs(2),
        1024,
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let mut rx = client.digest_feed(cancel.clone());
    assert_eq!(next_value(&mut rx).await.as_deref(), Some("d1"));
    assert_eq!(next_value(&mut rx).await.as_deref(), Some("d2"));
    // The stream ended: nothing is pushed until it is back.
    assert_eq!(next_value(&mut rx).await, None);
    assert_eq!(next_value(&mut rx).await.as_deref(), Some("d3"));
    cancel.cancel();
    let heads = heads.lock().unwrap();
    assert_eq!(heads.len(), 3);
    for head in heads.iter() {
        assert!(head.starts_with("GET /mining/events "), "{head}");
        assert!(head.contains("authorization: Bearer secret\r\n"), "{head}");
    }
}
