//! Streams (batons). A stream does not own a connection: it is only a name the client sends back. Only when a request
//! leaves a transaction open does the write thread stay with that stream (pinned), for at most `--pinned-tx-timeout-ms`
//! (see `writer.rs`). Batons change with every response; an old, unknown or expired baton is answered with 400, and
//! the client opens a new stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use parking_lot::Mutex;

use crate::proto::BASE64;

const BATON_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamError {
    Invalid,
    Reused,
    Expired,
    Busy,
}

impl StreamError {
    /// Texts as sqld 0.24.32 sends them (the client only looks at the 4xx status)
    pub fn message(self) -> &'static str {
        match self {
            StreamError::Invalid => "Received an invalid baton",
            StreamError::Reused => "Received a baton that has already been used",
            StreamError::Expired => "The stream has expired due to inactivity",
            StreamError::Busy => "The stream is already handling another request",
        }
    }
}

struct Entry {
    ns: Arc<str>,
    secret: [u8; 16],
    seq: u64,
    last_used: Instant,
    busy: bool,
    pinned: bool,
}

pub struct Streams {
    inner: Mutex<Inner>,
    idle: Duration,
}

struct Inner {
    next_id: u64,
    map: HashMap<u64, Entry>,
}

/// A stream taken by one HTTP request; dropping it (request cancelled) makes the stream usable again
pub struct Lease {
    streams: Arc<Streams>,
    pub id: u64,
    pub pinned: bool,
    finished: bool,
}

impl Streams {
    pub fn new(idle: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                next_id: 1,
                map: HashMap::new(),
            }),
            idle,
        })
    }

    pub fn acquire(self: &Arc<Self>, ns: &Arc<str>, baton: Option<&str>) -> Result<Lease, StreamError> {
        let mut g = self.inner.lock();
        let id = match baton {
            None => {
                let id = g.next_id;
                g.next_id += 1;
                g.map.insert(
                    id,
                    Entry {
                        ns: ns.clone(),
                        secret: random(),
                        seq: 0,
                        last_used: Instant::now(),
                        busy: true,
                        pinned: false,
                    },
                );
                return Ok(Lease {
                    streams: self.clone(),
                    id,
                    pinned: false,
                    finished: false,
                });
            }
            Some(b) => {
                let (id, seq, secret) = decode(b).ok_or(StreamError::Invalid)?;
                let next_id = g.next_id;
                let Some(e) = g.map.get_mut(&id) else {
                    return Err(if id < next_id {
                        StreamError::Expired
                    } else {
                        StreamError::Invalid
                    });
                };
                if e.secret != secret || e.ns != *ns {
                    return Err(StreamError::Invalid);
                }
                if e.seq != seq {
                    return Err(StreamError::Reused);
                }
                if e.busy {
                    return Err(StreamError::Busy);
                }
                e.busy = true;
                e.last_used = Instant::now();
                id
            }
        };
        let pinned = g.map[&id].pinned;
        Ok(Lease {
            streams: self.clone(),
            id,
            pinned,
            finished: false,
        })
    }

    /// End of a request: the baton for the next request, or None when the stream was closed or has been removed
    /// meanwhile (the write thread gave up a pinned transaction)
    pub fn finish(&self, mut lease: Lease, closed: bool, pinned: bool) -> Option<String> {
        lease.finished = true;
        let mut g = self.inner.lock();
        if closed {
            g.map.remove(&lease.id);
            return None;
        }
        let e = g.map.get_mut(&lease.id)?;
        e.seq += 1;
        e.busy = false;
        e.pinned = pinned;
        e.last_used = Instant::now();
        Some(encode(lease.id, e.seq, &e.secret))
    }

    pub fn kill(&self, id: u64) {
        self.inner.lock().map.remove(&id);
    }

    pub fn kill_namespace(&self, ns: &str) {
        self.inner.lock().map.retain(|_, e| &*e.ns != ns);
    }

    /// Drops streams unused for the idle time. Pinned ones are ended by the write thread; they are only dropped here
    /// if that somehow did not happen.
    pub fn sweep(&self, pinned_limit: Duration) {
        let now = Instant::now();
        let idle = self.idle;
        self.inner.lock().map.retain(|_, e| {
            let age = now.duration_since(e.last_used);
            e.busy || age < if e.pinned { idle + pinned_limit * 2 } else { idle }
        });
    }

    pub fn counts(&self) -> (usize, usize) {
        let g = self.inner.lock();
        (g.map.len(), g.map.values().filter(|e| e.pinned).count())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(e) = self.streams.inner.lock().map.get_mut(&self.id) {
            e.busy = false;
            e.last_used = Instant::now();
        }
    }
}

fn random() -> [u8; 16] {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("OS random source");
    b
}

fn encode(id: u64, seq: u64, secret: &[u8; 16]) -> String {
    let mut b = [0u8; BATON_LEN];
    b[..8].copy_from_slice(&id.to_le_bytes());
    b[8..16].copy_from_slice(&seq.to_le_bytes());
    b[16..].copy_from_slice(secret);
    BASE64.encode(b)
}

fn decode(baton: &str) -> Option<(u64, u64, [u8; 16])> {
    let b = BASE64.decode(baton.as_bytes()).ok()?;
    if b.len() != BATON_LEN {
        return None;
    }
    let id = u64::from_le_bytes(b[..8].try_into().ok()?);
    let seq = u64::from_le_bytes(b[8..16].try_into().ok()?);
    Some((id, seq, b[16..].try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baton_lifecycle() {
        let s = Streams::new(Duration::from_millis(50));
        let ns: Arc<str> = "tenant".into();
        let lease = s.acquire(&ns, None).unwrap();
        let b1 = s.finish(lease, false, false).unwrap();
        let lease = s.acquire(&ns, Some(&b1)).unwrap();
        assert_eq!(s.acquire(&ns, Some(&b1)).err(), Some(StreamError::Busy));
        let b2 = s.finish(lease, false, false).unwrap();
        assert_ne!(b1, b2);
        assert_eq!(s.acquire(&ns, Some(&b1)).err(), Some(StreamError::Reused));
        assert_eq!(
            s.acquire(&"platform".into(), Some(&b2)).err(),
            Some(StreamError::Invalid)
        );
        assert_eq!(s.acquire(&ns, Some("AAAA")).err(), Some(StreamError::Invalid));
        let mut forged = BASE64.decode(&b2).unwrap();
        forged[20] ^= 1;
        assert_eq!(
            s.acquire(&ns, Some(&BASE64.encode(&forged))).err(),
            Some(StreamError::Invalid)
        );

        let lease = s.acquire(&ns, Some(&b2)).unwrap();
        assert_eq!(s.finish(lease, true, false), None);
        assert_eq!(s.acquire(&ns, Some(&b2)).err(), Some(StreamError::Expired));
    }

    #[test]
    fn cancelled_request_frees_stream_and_idle_ones_expire() {
        let s = Streams::new(Duration::from_millis(30));
        let ns: Arc<str> = "tenant".into();
        let b = s.finish(s.acquire(&ns, None).unwrap(), false, false).unwrap();
        drop(s.acquire(&ns, Some(&b)).unwrap());
        let lease = s.acquire(&ns, Some(&b)).unwrap();
        let b = s.finish(lease, false, false).unwrap();
        std::thread::sleep(Duration::from_millis(40));
        s.sweep(Duration::from_secs(5));
        assert_eq!(s.acquire(&ns, Some(&b)).err(), Some(StreamError::Expired));
        assert_eq!(s.counts(), (0, 0));
    }

    #[test]
    fn killed_stream_gets_no_new_baton() {
        let s = Streams::new(Duration::from_secs(10));
        let ns: Arc<str> = "tenant".into();
        let lease = s.acquire(&ns, None).unwrap();
        s.kill(lease.id);
        assert_eq!(s.finish(lease, false, true), None);
    }
}
