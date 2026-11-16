//! `GET /mining/events`: the pushed template digest, and the coordinator
//! acting on it.
use futures_util::{FutureExt, future::BoxFuture};
use lattice_miner::*;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
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

fn work() -> Work {
    Work {
        work_id: "one".into(),
        block_hex: "00".into(),
        prefix: vec![],
        target: [255; 32],
        targets: vec![[255; 32]],
        stale_token: "d1".into(),
    }
}

/// A node whose status route always answers the work's digest, so only a
/// push can make work stale. It accepts every submission.
struct Node {
    status_reads: AtomicUsize,
    submits: Mutex<Vec<u64>>,
}
impl NodeClient for Node {
    fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
        async { Ok(Some(work())) }.boxed()
    }
    fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
        self.status_reads.fetch_add(1, Ordering::SeqCst);
        async { Ok(Some("d1".into())) }.boxed()
    }
    fn submit(&self, _: String, nonce: u64) -> BoxFuture<'_, Result<Submission, Error>> {
        self.submits.lock().unwrap().push(nonce);
        async {
            Ok(Submission {
                accepted: true,
                disposition: "acceptedSide".into(),
                tip_cid: None,
            })
        }
        .boxed()
    }
}

/// Searches until cancelled.
struct Endless;
impl Worker for Endless {
    fn id(&self) -> &str {
        "endless"
    }
    fn search(
        &self,
        _: Arc<Work>,
        _: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        async move {
            cancel.cancelled().await;
            Ok(None)
        }
        .boxed()
    }
}

#[tokio::test]
async fn a_pushed_digest_ends_the_batch_and_a_disconnect_does_not() {
    let (tx, rx) = watch::channel(Some("d1".to_string()));
    let node = Arc::new(Node {
        status_reads: AtomicUsize::new(0),
        submits: Mutex::new(vec![]),
    });
    let mut coordinator = Coordinator::new(
        node.clone(),
        vec![Arc::new(Endless)],
        CoordinatorOptions {
            // Long enough that polling cannot be what ends the batch.
            probe_interval: Duration::from_secs(3600),
            retry_delay: Duration::ZERO,
            digests: Some(rx),
            ..Default::default()
        },
    );
    let batch = tokio::spawn(async move {
        let cycle = coordinator.run_batch(&CancellationToken::new()).await;
        (coordinator, cycle)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    // A reconnect repeating the work's digest, and a dropped stream, leave
    // the work running.
    tx.send_replace(Some("d1".into()));
    tokio::time::sleep(Duration::from_millis(20)).await;
    tx.send_replace(None);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!batch.is_finished());
    // A reconnect whose first push names another digest ends it.
    tx.send_replace(Some("d2".into()));
    let (coordinator, cycle) = tokio::time::timeout(Duration::from_secs(2), batch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cycle,
        Cycle::Stale {
            work_id: "one".into()
        }
    );
    assert_eq!(coordinator.metrics().stale_abort_count, 1);
    // One status read: the batch's first freshness probe. The rest was pushed.
    assert_eq!(node.status_reads.load(Ordering::SeqCst), 1);
    assert!(node.submits.lock().unwrap().is_empty());
}

/// Holds the nonce it found until the search is stopped: the hit and the
/// tip move land together.
struct FoundAsStopped;
impl Worker for FoundAsStopped {
    fn id(&self) -> &str {
        "found"
    }
    fn search(
        &self,
        work: Arc<Work>,
        range: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        async move {
            cancel.cancelled().await;
            Ok(Some(WorkerResult {
                worker_id: "found".into(),
                work_id: work.work_id.clone(),
                nonce: range.start_nonce,
            }))
        }
        .boxed()
    }
}

#[tokio::test]
async fn a_pushed_digest_stops_the_search_but_a_found_nonce_is_submitted() {
    let (tx, rx) = watch::channel(Some("d1".to_string()));
    let node = Arc::new(Node {
        status_reads: AtomicUsize::new(0),
        submits: Mutex::new(vec![]),
    });
    let mut coordinator = Coordinator::new(
        node.clone(),
        vec![Arc::new(FoundAsStopped), Arc::new(Endless)],
        CoordinatorOptions {
            total_batch_size: 20,
            probe_interval: Duration::from_secs(3600),
            retry_delay: Duration::ZERO,
            digests: Some(rx),
            ..Default::default()
        },
    );
    let batch = tokio::spawn(async move {
        let cycle = coordinator.run_batch(&CancellationToken::new()).await;
        (coordinator, cycle)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.send_replace(Some("d2".into()));
    let (coordinator, cycle) = tokio::time::timeout(Duration::from_secs(2), batch)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&cycle, Cycle::Submitted { nonce: 0, submission, .. } if submission.accepted),
        "{cycle:?}"
    );
    assert_eq!(*node.submits.lock().unwrap(), vec![0]);
    assert_eq!(coordinator.metrics().stale_abort_count, 1);
}
