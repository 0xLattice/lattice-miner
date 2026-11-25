use crate::*;
use futures_util::{FutureExt, future::BoxFuture};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};
use tokio_util::sync::CancellationToken;
pub struct CpuWorker {
    id: String,
}
impl CpuWorker {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}
impl Worker for CpuWorker {
    fn id(&self) -> &str {
        &self.id
    }
    fn search(
        &self,
        work: Arc<Work>,
        range: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        let id = self.id.clone();
        async move {
            let bytes = hex::decode(&work.block_hex).map_err(|e| Error::Worker(e.to_string()))?;
            let block: lattice_primitives::Block = lattice_cashew::cbor::decode(&bytes)?;
            let prefix = block.pow_prefix();
            tokio::task::spawn_blocking(move || {
                let mut base = Sha256::new();
                base.update(prefix);
                for offset in 0..range.count {
                    let nonce = range.start_nonce.wrapping_add(offset);
                    if offset & 1023 == 0 && cancel.is_cancelled() {
                        return None;
                    }
                    let mut h = base.clone();
                    h.update(nonce.to_be_bytes());
                    let hash: [u8; 32] = h.finalize().into();
                    if hash <= work.target {
                        return Some(WorkerResult {
                            worker_id: id,
                            work_id: work.work_id.clone(),
                            nonce,
                        });
                    }
                }
                None
            })
            .await
            .map_err(|e| Error::Worker(e.to_string()))
        }
        .boxed()
    }
}
#[derive(Clone)]
pub struct ProcessWorker {
    id: String,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub maximum_stdout: usize,
    pub drain_timeout: Duration,
}
impl ProcessWorker {
    pub fn new(id: impl Into<String>, executable: PathBuf) -> Self {
        Self {
            id: id.into(),
            executable,
            arguments: vec![],
            maximum_stdout: 64 * 1024,
            drain_timeout: Duration::from_secs(1),
        }
    }
}
#[cfg(unix)]
struct GroupGuard(i32);
#[cfg(unix)]
impl Drop for GroupGuard {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}
struct ReaderGuard<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for ReaderGuard<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn output(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    limit: usize,
    tail: bool,
) -> Result<Vec<u8>, Error> {
    let mut out = vec![];
    let mut chunk = [0; 8192];
    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            return Ok(out);
        }
        if tail {
            out.extend_from_slice(&chunk[..n]);
            if out.len() > limit {
                out.drain(..out.len() - limit);
            }
        } else {
            if n > limit.saturating_sub(out.len()) {
                return Err(Error::Worker("stdout exceeds byte limit".into()));
            }
            out.extend_from_slice(&chunk[..n]);
        }
    }
}
impl Worker for ProcessWorker {
    fn id(&self) -> &str {
        &self.id
    }
    fn search(
        &self,
        work: Arc<Work>,
        range: NonceRange,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Option<WorkerResult>, Error>> {
        async move {
            let mut command = Command::new(&self.executable);
            command.args(&self.arguments).args([
                "--work-id",
                &work.work_id,
                "--block-hex",
                &work.block_hex,
                "--target",
                &hex::encode(work.target),
                "--start-nonce",
                &range.start_nonce.to_string(),
                "--count",
                &range.count.to_string(),
            ]);
            if !work.prefix.is_empty() {
                command.args(["--prefix-hex", &hex::encode(&work.prefix)]);
            }
            command
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                command.as_std_mut().process_group(0);
            }
            let mut child = command.spawn()?;
            #[cfg(unix)]
            let group = GroupGuard(child.id().expect("spawned PID") as i32);
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();
            let mut out = ReaderGuard(tokio::spawn(output(stdout, self.maximum_stdout, false)));
            let mut err = ReaderGuard(tokio::spawn(output(stderr, 4096, true)));
            // Work never expires on a clock: only cancellation (the work went
            // stale, or the batch ended) stops a search early.
            // Once its output is complete, a cancelled worker's result is
            // still read: a nonce found as the search stops is not dropped.
            // `None` is a worker stopped after its output was complete.
            let mut early = None;
            let status = tokio::select! {
                biased;
                result = &mut out.0 => {
                    early = Some(result.map_err(|e| Error::Worker(e.to_string()))??);
                    tokio::select! {
                        biased;
                        s = child.wait() => Some(s?),
                        _ = cancel.cancelled() => {
                            let _ = child.kill().await;
                            None
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    let _ = child.kill().await;
                    return Ok(None);
                }
                s = child.wait() => Some(s?),
            };
            // The direct child may have left descendants holding pipe ends. Always tear
            // down its isolated process group before the bounded final drain.
            #[cfg(unix)]
            drop(group);
            let bytes = match early {
                Some(bytes) => bytes,
                None => tokio::time::timeout(self.drain_timeout, &mut out.0)
                    .await
                    .map_err(|_| Error::Worker("stdout drain deadline exceeded".into()))?
                    .map_err(|e| Error::Worker(e.to_string()))??,
            };
            let stderr = tokio::time::timeout(self.drain_timeout, &mut err.0)
                .await
                .map_err(|_| Error::Worker("stderr drain deadline exceeded".into()))?
                .map_err(|e| Error::Worker(e.to_string()))??;
            if let Some(status) = status
                && !status.success()
            {
                return Err(Error::Worker(format!(
                    "exit {status}: {}",
                    String::from_utf8_lossy(&stderr)
                )));
            }
            let result: WorkerOutput = match serde_json::from_slice(&bytes) {
                Ok(result) => result,
                // Stopped with no result written: nothing was found.
                Err(_) if status.is_none() => return Ok(None),
                Err(_) => {
                    return Err(Error::Worker(format!(
                        "invalid worker JSON: {}",
                        String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(4096)..])
                    )));
                }
            };
            if result.work_id != work.work_id
                || result.range_start != range.start_nonce
                || result.range_count != range.count
            {
                return Err(Error::Worker(
                    "worker assignment identity/range mismatch".into(),
                ));
            }
            match result.status.as_str() {
                "found" => {
                    let nonce = result
                        .nonce
                        .ok_or_else(|| Error::Worker("found without nonce".into()))?;
                    if !range.contains(nonce) {
                        return Err(Error::Worker("nonce outside assignment".into()));
                    }
                    Ok(Some(WorkerResult {
                        worker_id: self.id.clone(),
                        work_id: result.work_id,
                        nonce,
                    }))
                }
                "exhausted" => {
                    if result.nonce.is_some() {
                        return Err(Error::Worker("exhausted with nonce".into()));
                    }
                    Ok(None)
                }
                _ => Err(Error::Worker("unknown worker status".into())),
            }
        }
        .boxed()
    }
}

/// Nonces the built-in search covers between looks at its jobs.
const CPU_STREAM_CHUNK: u64 = 1 << 16;

impl StreamWorker for CpuWorker {
    fn stream(
        &self,
        mut jobs: tokio::sync::mpsc::UnboundedReceiver<Job>,
        hits: tokio::sync::mpsc::Sender<StreamHit>,
    ) -> BoxFuture<'static, Result<(), Error>> {
        async move {
            tokio::task::spawn_blocking(move || {
                use tokio::sync::mpsc::error::TryRecvError;
                // The job searched, its prefix midstate and where it is.
                let mut current: Option<(Job, Sha256, u64)> = None;
                loop {
                    loop {
                        let next = match current {
                            None => jobs.blocking_recv(),
                            Some(_) => match jobs.try_recv() {
                                Ok(job) => Some(job),
                                Err(TryRecvError::Empty) => break,
                                Err(TryRecvError::Disconnected) => None,
                            },
                        };
                        let Some(next) = next else { return };
                        match current.as_mut() {
                            Some((job, _, _)) if job.work.work_id == next.work.work_id => {
                                job.target = next.target
                            }
                            _ => {
                                let mut base = Sha256::new();
                                base.update(&next.work.prefix);
                                let start = next.start_nonce;
                                current = Some((next, base, start));
                            }
                        }
                    }
                    // Up to the end of a chunk or the next hit, then a look
                    // at the jobs, as the worker executable does.
                    let (job, base, nonce) = current.as_mut().expect("a job to search");
                    for _ in 0..CPU_STREAM_CHUNK {
                        let mut h = base.clone();
                        h.update(nonce.to_be_bytes());
                        let hash: [u8; 32] = h.finalize().into();
                        let found = *nonce;
                        *nonce = nonce.wrapping_add(1);
                        if hash <= job.target {
                            let hit = StreamHit {
                                work_id: job.work.work_id.clone(),
                                nonce: found,
                                hash: hex::encode(hash),
                            };
                            if hits.blocking_send(hit).is_err() {
                                return;
                            }
                            break;
                        }
                    }
                }
            })
            .await
            .map_err(|e| Error::Worker(e.to_string()))
        }
        .boxed()
    }
}

impl StreamWorker for ProcessWorker {
    /// The executable with `--stream`: one JSON job per line in, one JSON
    /// hit per line out. Closing its stdin ends it.
    fn stream(
        &self,
        mut jobs: tokio::sync::mpsc::UnboundedReceiver<Job>,
        hits: tokio::sync::mpsc::Sender<StreamHit>,
    ) -> BoxFuture<'static, Result<(), Error>> {
        let this = self.clone();
        async move {
            use futures_util::StreamExt;
            use tokio::io::AsyncWriteExt;
            let mut command = Command::new(&this.executable);
            command
                .args(&this.arguments)
                .arg("--stream")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                command.as_std_mut().process_group(0);
            }
            let mut child = command.spawn()?;
            #[cfg(unix)]
            let _group = GroupGuard(child.id().expect("spawned PID") as i32);
            let mut stdin = child.stdin.take();
            let mut lines = tokio_util::codec::FramedRead::new(
                child.stdout.take().unwrap(),
                tokio_util::codec::LinesCodec::new_with_max_length(this.maximum_stdout),
            );
            let mut err = ReaderGuard(tokio::spawn(output(
                child.stderr.take().unwrap(),
                4096,
                true,
            )));
            loop {
                tokio::select! {
                    job = jobs.recv(), if stdin.is_some() => match job {
                        Some(job) => {
                            let line = serde_json::to_string(&StreamJob {
                                work_id: job.work.work_id.clone(),
                                prefix_hex: hex::encode(&job.work.prefix),
                                target: hex::encode(job.target),
                                start_nonce: job.start_nonce,
                            })? + "\n";
                            let stdin = stdin.as_mut().expect("open while given jobs");
                            // A worker that has exited is reported below.
                            let _ = stdin.write_all(line.as_bytes()).await;
                        }
                        None => stdin = None,
                    },
                    line = lines.next() => match line {
                        Some(Ok(line)) => {
                            let hit: StreamHit = serde_json::from_str(&line).map_err(|_| {
                                Error::Worker(format!("invalid worker line: {line}"))
                            })?;
                            if hits.send(hit).await.is_err() {
                                return Ok(());
                            }
                        }
                        Some(Err(e)) => return Err(Error::Worker(e.to_string())),
                        None => break,
                    },
                }
            }
            let status = child.wait().await?;
            let stderr = tokio::time::timeout(this.drain_timeout, &mut err.0)
                .await
                .map_err(|_| Error::Worker("stderr drain deadline exceeded".into()))?
                .map_err(|e| Error::Worker(e.to_string()))??;
            if !status.success() || stdin.is_some() {
                return Err(Error::Worker(format!(
                    "exit {status}: {}",
                    String::from_utf8_lossy(&stderr)
                )));
            }
            Ok(())
        }
        .boxed()
    }
}
