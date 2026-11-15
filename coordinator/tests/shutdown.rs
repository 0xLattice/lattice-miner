//! Shutdown does not drop a found block: a submission in flight when the
//! miner is cancelled is still answered, within SUBMIT_GRACE.
use futures_util::{FutureExt, future::BoxFuture};
use lattice_miner::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Node {
    submitting: Notify,
}
impl NodeClient for Node {
    fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>> {
        async {
            Ok(Some(Work {
                work_id: "one".into(),
                block_hex: "00".into(),
                prefix: vec![],
                target: [255; 32],
                targets: vec![[255; 32]],
                stale_token: "d1".into(),
            }))
        }
        .boxed()
    }
    fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>> {
        async { Ok(Some("d1".into())) }.boxed()
    }
    fn submit(&self, _: String, _: u64) -> BoxFuture<'_, Result<Submission, Error>> {
        async {
            self.submitting.notify_one();
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(Submission {
                accepted: true,
                disposition: "accepted".into(),
                tip_cid: Some("tip".into()),
            })
        }
        .boxed()
    }
}

/// Finds the range's first nonce at once.
struct Finds;
impl Worker for Finds {
    fn id(&self) -> &str {
        "finds"
    }
    fn search(
        &self,
        work: Arc<Work>,
        range: NonceRange,
        _: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        async move {
            Ok(Some(WorkerResult {
                worker_id: "finds".into(),
                work_id: work.work_id.clone(),
                nonce: range.start_nonce,
            }))
        }
        .boxed()
    }
}

#[tokio::test]
async fn a_submission_in_flight_is_answered_after_cancel() {
    let node = Arc::new(Node {
        submitting: Notify::new(),
    });
    let mut coordinator = Coordinator::new(
        node.clone(),
        vec![Arc::new(Finds)],
        CoordinatorOptions {
            stale_probe: false,
            ..Default::default()
        },
    );
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let batch = tokio::spawn(async move { coordinator.run_batch(&cancel).await });
    tokio::time::timeout(Duration::from_secs(5), node.submitting.notified())
        .await
        .expect("a submission");
    stop.cancel();
    let cycle = tokio::time::timeout(SUBMIT_GRACE, batch)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&cycle, Cycle::Submitted { submission, .. } if submission.accepted),
        "{cycle:?}"
    );
}
