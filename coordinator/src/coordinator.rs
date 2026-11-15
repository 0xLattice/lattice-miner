use crate::*;
use futures_util::{
    FutureExt,
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
pub trait NodeClient: Send + Sync + 'static {
    fn fetch_work(&self) -> BoxFuture<'_, Result<Option<Work>, Error>>;
    fn stale_token(&self) -> BoxFuture<'_, Result<Option<String>, Error>>;
    fn submit(&self, work_id: String, nonce: u64) -> BoxFuture<'_, Result<Submission, Error>>;
}
pub trait Worker: Send + Sync + 'static {
    fn id(&self) -> &str;
    fn search(
        &self,
        work: Arc<Work>,
        range: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>>;
}
#[derive(Debug, Default, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub current_work_id: Option<String>,
    pub active_worker_count: usize,
    pub assigned_range_count: u64,
    pub stale_abort_count: u64,
    pub worker_failure_count: u64,
    pub accepted_solution_count: u64,
    pub rejected_solution_count: u64,
    pub retry_backoff_count: u64,
}
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
#[serde(
    tag = "result",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Cycle {
    Backoff,
    NodeFailed {
        error: String,
    },
    NoSolution {
        work_id: String,
    },
    Stale {
        work_id: String,
    },
    WorkerFailed {
        work_id: String,
        worker_id: String,
        error: String,
    },
    Submitted {
        work_id: String,
        nonce: u64,
        #[serde(flatten)]
        submission: Submission,
    },
    Cancelled,
}
#[derive(Clone)]
pub struct CoordinatorOptions {
    pub total_batch_size: u64,
    pub nonce_offset: u64,
    pub stale_probe: bool,
    pub probe_interval: Duration,
    pub retry_delay: Duration,
    pub max_submit_retries: usize,
    /// The node's template digest as `GET /mining/events` pushes it
    /// ([`HttpNodeClient::digest_feed`]), `None` while not connected. While
    /// connected, a pushed digest other than the work's ends the batch as
    /// stale; while not, the status route is polled every `probe_interval`.
    pub digests: Option<tokio::sync::watch::Receiver<Option<String>>>,
}
impl Default for CoordinatorOptions {
    fn default() -> Self {
        Self {
            total_batch_size: 10000,
            nonce_offset: 0,
            stale_probe: true,
            probe_interval: Duration::from_secs(2),
            retry_delay: Duration::from_millis(250),
            max_submit_retries: 3,
            digests: None,
        }
    }
}
pub struct Coordinator {
    node: Arc<dyn NodeClient>,
    workers: Vec<Arc<dyn Worker>>,
    options: CoordinatorOptions,
    next_nonce: u64,
    nonce_work: Option<String>,
    metrics: Metrics,
}
enum BatchEvent {
    Search(usize, NonceRange, Result<Option<WorkerResult>, Error>),
    Fresh,
    Stale,
}
struct CancelGuard(CancellationToken);
impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
impl Coordinator {
    pub fn new(
        node: Arc<dyn NodeClient>,
        mut workers: Vec<Arc<dyn Worker>>,
        mut options: CoordinatorOptions,
    ) -> Self {
        if workers.is_empty() {
            workers.push(Arc::new(CpuWorker::new("local")));
        }
        options.total_batch_size = options.total_batch_size.max(1);
        options.probe_interval = options.probe_interval.max(Duration::from_millis(1));
        let next_nonce = options.nonce_offset;
        Self {
            node,
            workers,
            options,
            next_nonce,
            nonce_work: None,
            metrics: Metrics::default(),
        }
    }
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
    async fn stale(&self, work: &Work, cancel: &CancellationToken) -> bool {
        tokio::select! {biased;_=cancel.cancelled()=>true,result=self.node.stale_token()=>matches!(result,Ok(Some(token)) if token!=work.stale_token)}
    }
    async fn submit_retry(
        &mut self,
        work: &Work,
        result: WorkerResult,
        cancel: &CancellationToken,
    ) -> Cycle {
        let mut attempt = 0;
        loop {
            let answer = tokio::select! {biased;_=cancel.cancelled()=>return Cycle::Cancelled,r=self.node.submit(result.work_id.clone(),result.nonce)=>r};
            match answer {
                Ok(submission) => {
                    if submission.accepted {
                        self.metrics.accepted_solution_count += 1;
                    } else {
                        self.metrics.rejected_solution_count += 1;
                    }
                    return Cycle::Submitted {
                        work_id: work.work_id.clone(),
                        nonce: result.nonce,
                        submission,
                    };
                }
                Err(e @ Error::Unauthorized(_)) => {
                    self.metrics.worker_failure_count += 1;
                    return Cycle::NodeFailed {
                        error: e.to_string(),
                    };
                }
                Err(_) => {
                    attempt += 1;
                    if attempt > self.options.max_submit_retries {
                        self.metrics.rejected_solution_count += 1;
                        return Cycle::Submitted {
                            work_id: work.work_id.clone(),
                            nonce: result.nonce,
                            submission: Submission {
                                accepted: false,
                                disposition: "submitFailed".into(),
                                tip_cid: None,
                            },
                        };
                    }
                    self.metrics.retry_backoff_count += 1;
                    tokio::select! {biased;_=cancel.cancelled()=>return Cycle::Cancelled,_=tokio::time::sleep(self.options.retry_delay)=>{}}
                    if self.stale(work, cancel).await {
                        self.metrics.stale_abort_count += 1;
                        return Cycle::Stale {
                            work_id: work.work_id.clone(),
                        };
                    }
                }
            }
        }
    }
    pub async fn run_batch(&mut self, cancel: &CancellationToken) -> Cycle {
        let fetched = tokio::select! {biased;_=cancel.cancelled()=>return Cycle::Cancelled,r=self.node.fetch_work()=>r};
        let work = match fetched {
            Ok(Some(w)) => Arc::new(w),
            Ok(None) => {
                self.metrics.retry_backoff_count += 1;
                return Cycle::Backoff;
            }
            Err(e) => {
                self.metrics.retry_backoff_count += 1;
                return if matches!(e, Error::Unauthorized(_)) {
                    Cycle::NodeFailed {
                        error: e.to_string(),
                    }
                } else {
                    Cycle::Backoff
                };
            }
        };
        self.metrics.current_work_id = Some(work.work_id.clone());
        if self
            .nonce_work
            .as_ref()
            .is_some_and(|old| old != &work.work_id)
        {
            self.next_nonce = 0;
        }
        self.nonce_work = Some(work.work_id.clone());
        self.metrics.active_worker_count = self.workers.len();
        let ranges = allocate_ranges(
            self.options.total_batch_size,
            self.workers.len(),
            self.next_nonce,
        );
        self.metrics.assigned_range_count = ranges.len() as u64;
        self.next_nonce = ranges
            .iter()
            .fold(self.next_nonce, |n, r| n.wrapping_add(r.count));
        let child = cancel.child_token();
        let _guard = CancelGuard(child.clone());
        let mut tasks: FuturesUnordered<BoxFuture<'static, BatchEvent>> = FuturesUnordered::new();
        let workers = self.workers.clone();
        let search = |tasks: &mut FuturesUnordered<BoxFuture<'static, BatchEvent>>,
                      i: usize,
                      assignment: Arc<Work>,
                      range: NonceRange| {
            let worker = workers[i].clone();
            let cancel = child.clone();
            tasks.push(
                async move {
                    BatchEvent::Search(i, range, worker.search(assignment, range, cancel).await)
                }
                .boxed(),
            );
        };
        for (i, range) in ranges.iter().enumerate() {
            search(&mut tasks, i, work.clone(), *range);
        }
        let mut searching = ranges.len();
        let mut probing = self.options.stale_probe;
        if probing {
            let node = self.node.clone();
            let token = work.stale_token.clone();
            tasks.push(
                async move {
                    if matches!(node.stale_token().await,Ok(Some(t))if t!=token) {
                        BatchEvent::Stale
                    } else {
                        BatchEvent::Fresh
                    }
                }
                .boxed(),
            );
            // The tip can move at any point in a batch, and against a hard
            // target no submission comes along to notice it, so freshness is
            // watched until the batch ends: pushed when the node streams its
            // digest, polled when it does not. Taken before the first probe,
            // so a change that probe misses is a change here.
            let mut digests = self.options.digests.clone();
            if let Some(feed) = digests.as_mut() {
                feed.borrow_and_update();
            }
            tasks.push(
                watch_freshness(
                    self.node.clone(),
                    digests,
                    work.stale_token.clone(),
                    self.options.probe_interval,
                )
                .boxed(),
            );
        }
        let mut open = work.targets.first().copied();
        let mut last_child = None;
        while searching > 0 || probing {
            let event = tokio::select! {biased;_=cancel.cancelled()=>return Cycle::Cancelled,e=tasks.next()=>match e{Some(e)=>e,None=>break}};
            match event {
                BatchEvent::Fresh => probing = false,
                BatchEvent::Stale => {
                    self.metrics.stale_abort_count += 1;
                    return Cycle::Stale {
                        work_id: work.work_id.clone(),
                    };
                }
                BatchEvent::Search(i, range, result) => {
                    searching -= 1;
                    let result = match result {
                        Ok(Some(r)) if r.work_id == work.work_id => r,
                        Ok(_) => continue,
                        Err(e) => {
                            self.metrics.worker_failure_count += 1;
                            return Cycle::WorkerFailed {
                                work_id: work.work_id.clone(),
                                worker_id: self.workers[i].id().into(),
                                error: e.to_string(),
                            };
                        }
                    };
                    let hash = (!work.prefix.is_empty())
                        .then(|| lattice_pow::hash(&work.prefix, result.nonce));
                    let cleared = matches!((hash,open,work.targets.first()),(Some(h),Some(o),Some(t)) if h>o&&h<=*t);
                    if !cleared {
                        if self.options.stale_probe && self.stale(&work, cancel).await {
                            self.metrics.stale_abort_count += 1;
                            return Cycle::Stale {
                                work_id: work.work_id.clone(),
                            };
                        }
                        let outcome = self.submit_retry(&work, result.clone(), cancel).await;
                        match (&outcome, hash) {
                            (Cycle::Submitted { submission, .. }, Some(hash))
                                if !submission.accepted
                                    && submission.disposition == "childOnly" =>
                            {
                                let Some(next) = work.targets.iter().find(|t| **t < hash) else {
                                    return outcome;
                                };
                                open = Some(*next);
                                last_child = Some(outcome);
                            }
                            _ => return outcome,
                        }
                    }
                    let searched = result.nonce.wrapping_sub(range.start_nonce);
                    if let Some(target) = open
                        && searched < range.count
                        && range.count - searched > 1
                    {
                        let mut assignment = (*work).clone();
                        assignment.target = target;
                        let next = NonceRange {
                            start_nonce: result.nonce.wrapping_add(1),
                            count: range.count - searched - 1,
                        };
                        search(&mut tasks, i, Arc::new(assignment), next);
                        searching += 1;
                    }
                }
            }
        }
        last_child.unwrap_or_else(|| Cycle::NoSolution {
            work_id: work.work_id.clone(),
        })
    }
    /// Owned lifecycle: one mutable coordinator means batches cannot overlap.
    /// Cancellation stops the current batch and backoff without another submission.
    pub async fn run(
        mut self,
        cancel: CancellationToken,
        events: tokio::sync::mpsc::Sender<Cycle>,
    ) -> Metrics {
        loop {
            let result = self.run_batch(&cancel).await;
            let backoff = matches!(
                result,
                Cycle::Backoff
                    | Cycle::Submitted {
                        submission: Submission {
                            accepted: false,
                            ..
                        },
                        ..
                    }
            );
            let stop = matches!(
                result,
                Cycle::Cancelled | Cycle::NodeFailed { .. } | Cycle::WorkerFailed { .. }
            );
            tokio::select! {_=cancel.cancelled()=>break,r=events.send(result)=>if r.is_err(){break;}}
            if stop {
                break;
            }
            if backoff {
                tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(self.options.retry_delay)=>{}}
            }
        }
        self.metrics
    }
}

/// Resolves only when the work is stale. While the digest feed is connected,
/// each digest it pushes is compared with the work's; a pushed digest says
/// nothing about the chain until it differs. While it is not connected the
/// status route is polled, and a reconnect's first push is compared at once,
/// since changes made while disconnected were never pushed.
async fn watch_freshness(
    node: Arc<dyn NodeClient>,
    mut digests: Option<tokio::sync::watch::Receiver<Option<String>>>,
    token: String,
    interval: Duration,
) -> BatchEvent {
    let mut compare = false;
    loop {
        if let Some(feed) = digests.as_mut() {
            let pushed = feed.borrow_and_update().clone();
            if let Some(digest) = pushed {
                if compare && digest != token {
                    return BatchEvent::Stale;
                }
                compare = true;
                if feed.changed().await.is_err() {
                    digests = None;
                }
                continue;
            }
            tokio::select! {
                changed = feed.changed() => {
                    if changed.is_err() {
                        digests = None;
                    }
                    compare = true;
                    continue;
                }
                _ = tokio::time::sleep(interval) => {}
            }
        } else {
            tokio::time::sleep(interval).await;
        }
        if matches!(node.stale_token().await, Ok(Some(t)) if t != token) {
            return BatchEvent::Stale;
        }
    }
}
