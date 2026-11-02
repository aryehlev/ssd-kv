//! The replication transport of a data node (DESIGN §23.7, §23.13, §6.7).
//!
//! A small tokio runtime inside the `kvd` process moves replication bytes: it accepts members
//! and migration destinations on `cluster-repl-port`, dials primaries and migration sources for
//! this node's partitions, reads frames off each socket and writes the bytes the workers
//! produce. It never interprets a stream: every inbound `Hello` is handed, with the socket's
//! stream handle, to the worker owning the Hello's partition, and every later frame of that
//! stream goes to the same worker, which serves the stream alone (LC-37). Each stream is a
//! single per-partition connection started by its own peer `Hello`.
//!
//! Admission (§6.7): a stream stops reading its socket while the frames it handed to its worker
//! and not yet handled reach a single fixed limit, and resumes at the same threshold. Large
//! logical frames sent as `PUT_PART` fragments are reassembled here, so workers only see whole
//! frames.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use kv_repl::frame::{Frame, FrameDecoder, Hello, Reassembly};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, watch};

use crate::runtime::xmsg::XMsg;
use crate::shared::NodeShared;

/// Bytes of handed-over, not yet handled frames at which a stream stops reading (one limit to
/// stop and to resume).
const QUEUED_BYTES_LIMIT: usize = 32 << 20;
/// Time a peer has to send its `Hello` after connecting.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
/// Time a dial has to connect.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// The worker's handle on one replication stream. Dropping it ends the stream after the bytes
/// already handed over are written; [`StreamIo::abort`] ends it at once.
pub struct StreamIo {
    pub id: u64,
    pub peer: SocketAddr,
    out: mpsc::UnboundedSender<Vec<u8>>,
    unsent: Arc<AtomicUsize>,
    queued: Arc<AtomicUsize>,
    resume: Arc<Notify>,
    abort: watch::Sender<bool>,
}

impl StreamIo {
    /// Hands bytes to the socket writer (dropped when the stream is gone).
    pub fn send(&self, b: Vec<u8>) {
        if b.is_empty() {
            return;
        }
        let n = b.len();
        self.unsent.fetch_add(n, Ordering::AcqRel);
        if self.out.send(b).is_err() {
            self.unsent.fetch_sub(n, Ordering::AcqRel);
        }
    }

    /// Bytes handed over and not yet written to the socket (the send window of the walks).
    pub fn unsent(&self) -> usize {
        self.unsent.load(Ordering::Acquire)
    }

    /// The worker handled a frame of `bytes` wire bytes (lifts the read stop when it drops
    /// below the limit).
    pub fn consumed(&self, bytes: usize) {
        // `bytes` were added when the frame was handed over, so this never wraps
        let before = self.queued.fetch_sub(bytes, Ordering::AcqRel);
        if before >= QUEUED_BYTES_LIMIT {
            self.resume.notify_one();
        }
    }

    /// Ends the stream now (pending output is discarded).
    pub fn abort(&self) {
        let _ = self.abort.send(true);
    }
}

/// What the transport tells a worker.
pub enum ReplEvent {
    /// A member or a migration destination dialed in and said `Hello` (`term`: the frame's).
    Inbound {
        stream: StreamIo,
        hello: Box<Hello>,
        term: u32,
    },
    /// A dial of this node connected and sent its `Hello` (`stream`), or failed (`None`).
    Dialed {
        tag: u64,
        partition: u16,
        stream: Option<StreamIo>,
    },
    /// A whole frame of `stream` (`bytes` on the wire, to be reported with `consumed`).
    Frame {
        stream: u64,
        term: u32,
        frame: Frame,
        bytes: usize,
    },
    /// The stream ended (peer closed, error, or abort).
    Closed { stream: u64 },
}

/// The transport.
pub struct ReplNet {
    rt: tokio::runtime::Runtime,
    node: Weak<NodeShared>,
    next_id: Arc<AtomicU64>,
    pub listen_addr: SocketAddr,
}

/// Parts of a stream shared between its tasks and its handle.
#[derive(Clone)]
struct Pipes {
    id: u64,
    worker: u16,
    unsent: Arc<AtomicUsize>,
    queued: Arc<AtomicUsize>,
    resume: Arc<Notify>,
}

fn deliver(node: &Weak<NodeShared>, worker: u16, ev: ReplEvent) -> bool {
    match node.upgrade() {
        Some(n) => match n.workers.get(usize::from(worker)) {
            Some(w) => {
                w.send_side(XMsg::Repl(ev));
                true
            }
            None => false,
        },
        None => false,
    }
}

impl ReplNet {
    /// Starts the transport and its listener on `listen` (bound before returning, so a bad
    /// address fails startup).
    pub fn start(node: &Arc<NodeShared>, listen: SocketAddr) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("kvd-repl")
            .enable_all()
            .build()?;
        let std_l = std::net::TcpListener::bind(listen)?;
        std_l.set_nonblocking(true)?;
        let listen_addr = std_l.local_addr()?;
        let net = ReplNet {
            rt,
            node: Arc::downgrade(node),
            next_id: Arc::new(AtomicU64::new(1)),
            listen_addr,
        };
        let weak = net.node.clone();
        let ids = net.next_id.clone();
        net.rt.spawn(async move {
            let l = match TcpListener::from_std(std_l) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("replication listener: {e}");
                    return;
                }
            };
            loop {
                let (sock, peer) = match l.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::debug!("replication accept: {e}");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                };
                let _ = sock.set_nodelay(true);
                tokio::spawn(inbound(sock, peer, weak.clone(), ids.clone()));
            }
        });
        Ok(net)
    }

    /// Dials `addr` for `partition` of worker `worker` and sends `hello` (an encoded frame);
    /// the outcome arrives as [`ReplEvent::Dialed`] with `tag`.
    pub fn dial(&self, worker: u16, partition: u16, tag: u64, addr: SocketAddr, hello: Vec<u8>) {
        let node = self.node.clone();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.rt.spawn(async move {
            let sock = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(s)) => s,
                _ => {
                    deliver(
                        &node,
                        worker,
                        ReplEvent::Dialed {
                            tag,
                            partition,
                            stream: None,
                        },
                    );
                    return;
                }
            };
            let _ = sock.set_nodelay(true);
            let (io, rx, abort_rx, pipes) = handle(id, worker, addr);
            io.send(hello);
            if !deliver(
                &node,
                worker,
                ReplEvent::Dialed {
                    tag,
                    partition,
                    stream: Some(io),
                },
            ) {
                return;
            }
            pump(sock, Vec::new(), pipes, rx, abort_rx, node).await;
        });
    }
}

/// A stream handle and the ends its tasks keep.
fn handle(
    id: u64,
    worker: u16,
    peer: SocketAddr,
) -> (
    StreamIo,
    mpsc::UnboundedReceiver<Vec<u8>>,
    watch::Receiver<bool>,
    Pipes,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (abort, abort_rx) = watch::channel(false);
    let pipes = Pipes {
        id,
        worker,
        unsent: Arc::new(AtomicUsize::new(0)),
        queued: Arc::new(AtomicUsize::new(0)),
        resume: Arc::new(Notify::new()),
    };
    let io = StreamIo {
        id,
        peer,
        out: tx,
        unsent: pipes.unsent.clone(),
        queued: pipes.queued.clone(),
        resume: pipes.resume.clone(),
        abort,
    };
    (io, rx, abort_rx, pipes)
}

/// An accepted connection: its `Hello`, then the worker owning the Hello's partition serves it.
async fn inbound(
    mut sock: TcpStream,
    peer: SocketAddr,
    node: Weak<NodeShared>,
    ids: Arc<AtomicU64>,
) {
    let mut buf = Vec::with_capacity(4096);
    let mut dec = FrameDecoder::new();
    let first = tokio::time::timeout(HELLO_TIMEOUT, async {
        loop {
            match dec.feed(&buf) {
                Ok(Some(x)) => return Some(x),
                Ok(None) => {}
                Err(_) => return None,
            }
            let mut chunk = [0u8; 4096];
            match sock.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
    })
    .await;
    let Ok(Some((_, term, Frame::Hello(hello), used))) = first else {
        tracing::debug!(%peer, "replication peer sent no hello");
        return;
    };
    let Some(n) = node.upgrade() else { return };
    let partitions = n.partitions;
    let workers = n.workers.len() as u16;
    drop(n);
    if hello.partition >= partitions || workers == 0 {
        return;
    }
    let worker = hello.partition % workers;
    let id = ids.fetch_add(1, Ordering::Relaxed);
    let (io, rx, abort_rx, pipes) = handle(id, worker, peer);
    buf.drain(..used);
    if !deliver(
        &node,
        worker,
        ReplEvent::Inbound {
            stream: io,
            hello: Box::new(hello),
            term,
        },
    ) {
        return;
    }
    pump(sock, buf, pipes, rx, abort_rx, node).await;
}

/// Moves bytes of one stream until it ends: frames to the worker, worker bytes to the socket.
async fn pump(
    sock: TcpStream,
    mut buf: Vec<u8>,
    pipes: Pipes,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
    abort_rx: watch::Receiver<bool>,
    node: Weak<NodeShared>,
) {
    let (mut rd, mut wr) = sock.into_split();
    let unsent = pipes.unsent.clone();
    let mut wabort = abort_rx.clone();
    let writer = tokio::spawn(async move {
        // a dropped handle (no abort) still flushes what it handed over
        let mut watching = true;
        loop {
            tokio::select! {
                b = rx.recv() => match b {
                    Some(b) => {
                        let n = b.len();
                        let r = wr.write_all(&b).await;
                        unsent.fetch_sub(n, Ordering::AcqRel);
                        if r.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
                r = wabort.changed(), if watching => match r {
                    Err(_) => watching = false,
                    Ok(()) => if *wabort.borrow() { break },
                },
            }
        }
        let _ = wr.shutdown().await;
    });
    let mut rabort = abort_rx;
    let mut dec = FrameDecoder::new();
    let mut parts = Reassembly::new();
    let mut chunk = vec![0u8; 64 << 10];
    'outer: loop {
        // hand every complete frame to the worker
        loop {
            match dec.feed(&buf) {
                Ok(Some((_, term, frame, used))) => {
                    buf.drain(..used);
                    let frame = match frame {
                        Frame::PutPart {
                            offset,
                            total,
                            data,
                            ..
                        } => match parts.push(offset, total, &data) {
                            Ok(Some((_, _, whole))) => whole,
                            Ok(None) => continue,
                            Err(_) => break 'outer,
                        },
                        f => f,
                    };
                    pipes.queued.fetch_add(used, Ordering::AcqRel);
                    if !deliver(
                        &node,
                        pipes.worker,
                        ReplEvent::Frame {
                            stream: pipes.id,
                            term,
                            frame,
                            bytes: used,
                        },
                    ) {
                        break 'outer;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!(stream = pipes.id, "replication frame error: {e}");
                    break 'outer;
                }
            }
        }
        // admission: stop reading at the limit, resume below it
        while pipes.queued.load(Ordering::Acquire) >= QUEUED_BYTES_LIMIT {
            let wake = pipes.resume.notified();
            if pipes.queued.load(Ordering::Acquire) < QUEUED_BYTES_LIMIT {
                break;
            }
            tokio::select! {
                _ = wake => {}
                r = rabort.changed() => if r.is_err() || *rabort.borrow() { break 'outer },
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
        tokio::select! {
            r = rd.read(&mut chunk) => match r {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            },
            r = rabort.changed() => if r.is_err() || *rabort.borrow() { break },
        }
    }
    deliver(&node, pipes.worker, ReplEvent::Closed { stream: pipes.id });
    // the writer flushes what the worker handed over, unless the stream was aborted
    let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
}
