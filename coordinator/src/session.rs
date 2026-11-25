//! The search `lattice-miner run` keeps going: stream workers that live for
//! the whole run (`--stream`, one process each), handed the node's current
//! template as a job, the Stratum way. A pushed template digest leads to one
//! template fetch, at most one in flight, while the workers keep hashing the
//! job they have; the new job replaces it when the fetch returns. Every hit
//! is submitted, one at a time and in order, old work included; hits found
//! meanwhile wait in the session, so a worker never waits to hand one over
//! and always takes its next job. Nothing here
//! expires on a clock: only transport (a failed fetch, a crashed worker)
//! waits before trying again.
use crate::*;
use futures_util::{FutureExt, future::BoxFuture};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::Instant,
};

/// What a stream worker is told to search: a new `work` starts at
/// `start_nonce`; the same work again changes only the target.
#[derive(Debug, Clone)]
pub struct Job {
    pub work: Arc<Work>,
    pub target: Target,
    pub start_nonce: u64,
}

/// A worker that runs for a whole session.
pub trait StreamWorker: Send + Sync + 'static {
    /// Searches each job `jobs` brings, sending every hit to `hits`, until
    /// `jobs` closes (`Ok`) or the worker fails. A zero target idles it.
    fn stream(
        &self,
        jobs: mpsc::UnboundedReceiver<Job>,
        hits: mpsc::UnboundedSender<StreamHit>,
    ) -> BoxFuture<'static, Result<(), Error>>;
}

#[derive(Clone)]
pub struct SessionOptions {
    /// How often `GET /status` is asked while no event stream is connected.
    pub probe_interval: Duration,
    /// The wait before a failed template fetch or a failed worker is tried
    /// again.
    pub retry_wait: Duration,
    pub retry_delay: Duration,
    pub max_submit_retries: usize,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(2),
            retry_wait: Duration::from_secs(5),
            retry_delay: Duration::from_millis(250),
            max_submit_retries: 3,
        }
    }
}

/// What happened, for the caller to log and act on.
#[derive(Debug)]
pub enum Event {
    /// A template is wanted: the caller refreshes its template request (and
    /// settings), then calls [`Session::fetch`].
    FetchDue,
    /// A template fetch ended: `None` when the node has none (HTTP 409/503).
    Fetched(Result<Option<Arc<Work>>, Error>),
    /// A hit's submission was answered (or failed after its retries).
    Submitted {
        work_id: String,
        nonce: u64,
        result: Result<Submission, Error>,
    },
    /// A worker ended while it had jobs; it is started again after the
    /// retry wait.
    WorkerFailed { worker: usize, error: Error },
}

/// No hash is at or below it: a worker given it idles where it is.
const IDLE: Target = [0; 32];

type Pending<T> = Option<BoxFuture<'static, T>>;

pub struct Session {
    node: Arc<dyn NodeClient>,
    workers: Vec<Arc<dyn StreamWorker>>,
    options: SessionOptions,
    digests: Option<watch::Receiver<Option<String>>>,
    /// Per worker, the generation running and its job queue.
    running: BTreeMap<usize, (u64, mpsc::UnboundedSender<Job>)>,
    generation: u64,
    tasks: JoinSet<(usize, u64, Result<(), Error>)>,
    hits: mpsc::UnboundedReceiver<StreamHit>,
    hit_sender: mpsc::UnboundedSender<StreamHit>,
    /// The work searched, and the target the workers search it at.
    current: Option<(Arc<Work>, Target)>,
    pushed: Option<String>,
    fetched: Option<String>,
    due: bool,
    fetching: Pending<Result<Option<Work>, Error>>,
    submitting: Pending<(StreamHit, Result<Submission, Error>)>,
    probing: Pending<Option<String>>,
    next_probe: Instant,
    retry_fetch_at: Option<Instant>,
    respawn_at: Option<Instant>,
    paused_until: Option<Instant>,
}

impl Session {
    /// Starts the workers; the first [`Event`] is [`Event::FetchDue`].
    pub fn new(
        node: Arc<dyn NodeClient>,
        workers: Vec<Arc<dyn StreamWorker>>,
        digests: Option<watch::Receiver<Option<String>>>,
        options: SessionOptions,
    ) -> Self {
        let (hit_sender, hits) = mpsc::unbounded_channel();
        let mut session = Self {
            node,
            workers: vec![],
            options,
            digests,
            running: BTreeMap::new(),
            generation: 0,
            tasks: JoinSet::new(),
            hits,
            hit_sender,
            current: None,
            pushed: None,
            fetched: None,
            due: true,
            fetching: None,
            submitting: None,
            probing: None,
            next_probe: Instant::now(),
            retry_fetch_at: None,
            respawn_at: None,
            paused_until: None,
        };
        session.replace(session.node.clone(), workers);
        session
    }

    /// Fetches a template; the new job goes to the workers when it returns.
    pub fn fetch(&mut self) {
        if self.fetching.is_none() {
            let node = self.node.clone();
            self.fetching = Some(async move { node.fetch_work().await }.boxed());
        }
    }

    /// Withholds jobs for `hold`: the workers idle where they are (their
    /// processes kept), then a template is fetched.
    pub fn pause(&mut self, hold: Duration) {
        self.paused_until = Some(Instant::now() + hold);
        if let Some((work, _)) = self.current.clone() {
            self.search(work, IDLE);
        }
    }

    /// Mines through `node` with `workers` from now on. The workers replaced
    /// finish the chunk they are on, and their hits are still submitted.
    pub fn replace(&mut self, node: Arc<dyn NodeClient>, workers: Vec<Arc<dyn StreamWorker>>) {
        self.node = node;
        self.running.clear();
        self.workers = workers;
        for worker in 0..self.workers.len() {
            self.start(worker);
        }
    }

    fn start(&mut self, worker: usize) {
        self.generation += 1;
        let (jobs, receiver) = mpsc::unbounded_channel();
        let search = self.workers[worker].stream(receiver, self.hit_sender.clone());
        let generation = self.generation;
        self.tasks
            .spawn(async move { (worker, generation, search.await) });
        if let Some((work, target)) = self.current.clone() {
            let _ = jobs.send(job(work, target, worker, self.workers.len()));
        }
        self.running.insert(worker, (generation, jobs));
    }

    /// Every running worker searches `work` at `target`; while paused, it
    /// idles.
    fn search(&mut self, work: Arc<Work>, target: Target) {
        let target = if self.paused_until.is_some() {
            IDLE
        } else {
            target
        };
        self.current = Some((work.clone(), target));
        let count = self.workers.len();
        for (worker, (_, jobs)) in &self.running {
            let _ = jobs.send(job(work.clone(), target, *worker, count));
        }
    }

    fn pushed(&mut self, digest: String) {
        if self.fetched.as_ref() != Some(&digest) {
            self.due = true;
        }
        self.pushed = Some(digest);
    }

    fn connected(&self) -> bool {
        self.digests.as_ref().is_some_and(|d| d.borrow().is_some())
    }

    /// The next thing that happened. Cancel-safe.
    pub async fn next(&mut self) -> Event {
        loop {
            if self.due && self.fetching.is_none() && self.paused_until.is_none() {
                self.due = false;
                self.retry_fetch_at = None;
                return Event::FetchDue;
            }
            let polling = !self.connected() && self.probing.is_none();
            let submitting = self.submitting.is_some();
            tokio::select! {
                changed = changed(&mut self.digests) => {
                    match changed {
                        Some(Some(digest)) => self.pushed(digest),
                        // Disconnected: a reconnect's first push is compared at once.
                        Some(None) => {}
                        None => self.digests = None,
                    }
                }
                _ = tokio::time::sleep_until(self.next_probe), if polling => {
                    let node = self.node.clone();
                    self.probing = Some(async move { node.stale_token().await.ok().flatten() }.boxed());
                }
                token = poll(&mut self.probing) => {
                    self.next_probe = Instant::now() + self.options.probe_interval;
                    if let Some(token) = token && !self.connected() {
                        self.pushed(token);
                    }
                }
                fetched = poll(&mut self.fetching) => {
                    let fetched = fetched.map(|w| w.map(Arc::new));
                    match &fetched {
                        Ok(Some(work)) => {
                            self.fetched = Some(work.stale_token.clone());
                            // Fetched again only if the node pushed another since.
                            self.due = self.pushed.as_ref().is_some_and(|p| *p != work.stale_token);
                            // The same work goes on where it is, unless it
                            // was idled: then it is searched again.
                            let going = self.current.as_ref().is_some_and(|(w, t)| w.work_id == work.work_id && *t != IDLE);
                            if !going && self.paused_until.is_none() {
                                self.search(work.clone(), work.target);
                            }
                        }
                        _ => self.retry_fetch_at = Some(Instant::now() + self.options.retry_wait),
                    }
                    return Event::Fetched(fetched);
                }
                _ = until(self.retry_fetch_at) => {
                    self.retry_fetch_at = None;
                    self.due = true;
                }
                _ = until(self.paused_until) => {
                    self.paused_until = None;
                    self.due = true;
                }
                _ = until(self.respawn_at) => {
                    self.respawn_at = None;
                    for worker in 0..self.workers.len() {
                        if !self.running.contains_key(&worker) {
                            self.start(worker);
                        }
                    }
                }
                Some(Ok((worker, generation, result))) = self.tasks.join_next() => {
                    // A worker replaced is expected to end.
                    if self.running.get(&worker).is_some_and(|(g, _)| *g == generation) {
                        self.running.remove(&worker);
                        self.respawn_at.get_or_insert(Instant::now() + self.options.retry_wait);
                        let error = result.err().unwrap_or_else(|| Error::Worker("exited while given jobs".into()));
                        return Event::WorkerFailed { worker, error };
                    }
                }
                Some(hit) = self.hits.recv(), if !submitting => {
                    let (node, options) = (self.node.clone(), self.options.clone());
                    self.submitting = Some(async move {
                        let result = submit(&*node, &hit, &options).await;
                        (hit, result)
                    }.boxed());
                }
                (hit, result) = poll(&mut self.submitting) => {
                    self.submitted(&hit, &result);
                    return Event::Submitted { work_id: hit.work_id, nonce: hit.nonce, result };
                }
            }
        }
    }

    /// A child-only hit on the work searched moves its search on to the next
    /// harder target; an accepted block idles it until the next template;
    /// another refusal fetches one at once; a duplicate changes nothing.
    fn submitted(&mut self, hit: &StreamHit, result: &Result<Submission, Error>) {
        let Ok(submission) = result else { return };
        let current = self
            .current
            .clone()
            .filter(|(work, _)| work.work_id == hit.work_id);
        if submission.accepted {
            // The tip moved.
            self.due = true;
            if let Some((work, _)) = current {
                self.search(work, IDLE);
            }
            return;
        }
        let Some((work, target)) = current else {
            return;
        };
        match submission.disposition.as_str() {
            "duplicate" => {}
            "childOnly" => {
                let hash = lattice_pow::hash(&work.prefix, hit.nonce);
                let harder = work.targets.iter().copied().find(|t| *t < hash);
                let next = harder.unwrap_or(IDLE).min(target);
                if next < target {
                    self.search(work, next);
                }
            }
            // The node will not take this work's hits: a template now.
            _ => self.due = true,
        }
    }

    /// Stops the workers and submits every hit they have found, those still
    /// to come as they stop included. Returns what each submission answered.
    pub async fn finish(mut self) -> Vec<Event> {
        // Their jobs closed, the workers stop after the chunk they are on.
        self.running.clear();
        while self.tasks.join_next().await.is_some() {}
        let mut answered = vec![];
        if let Some(submitting) = self.submitting.take() {
            let (hit, result) = submitting.await;
            answered.push((hit, result));
        }
        while let Ok(hit) = self.hits.try_recv() {
            let result = submit(&*self.node, &hit, &self.options).await;
            answered.push((hit, result));
        }
        answered
            .into_iter()
            .map(|(hit, result)| Event::Submitted {
                work_id: hit.work_id,
                nonce: hit.nonce,
                result,
            })
            .collect()
    }
}

/// Worker `worker` of `count` starts its search of a new work at its own
/// share of the nonce space.
fn job(work: Arc<Work>, target: Target, worker: usize, count: usize) -> Job {
    let start_nonce = allocate_ranges(u64::MAX, count, 0)[worker].start_nonce;
    Job {
        work,
        target,
        start_nonce,
    }
}

/// A submission, retried while it fails in transit.
async fn submit(
    node: &dyn NodeClient,
    hit: &StreamHit,
    options: &SessionOptions,
) -> Result<Submission, Error> {
    let mut attempt = 0;
    loop {
        match node.submit(hit.work_id.clone(), hit.nonce).await {
            Err(e)
                if !matches!(e, Error::Unauthorized(_)) && attempt < options.max_submit_retries =>
            {
                attempt += 1;
                tokio::time::sleep(options.retry_delay).await;
            }
            answer => return answer,
        }
    }
}

/// The next value of the digest feed: `None` when the feed is gone.
async fn changed(digests: &mut Option<watch::Receiver<Option<String>>>) -> Option<Option<String>> {
    match digests {
        Some(feed) => match feed.changed().await {
            Ok(()) => Some(feed.borrow_and_update().clone()),
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}

/// A pending future's output, taking it; never, while there is none.
async fn poll<T>(pending: &mut Pending<T>) -> T {
    match pending {
        Some(future) => {
            let output = future.await;
            *pending = None;
            output
        }
        None => std::future::pending().await,
    }
}

/// `at`; never, without one.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
