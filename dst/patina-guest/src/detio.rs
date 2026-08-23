//! `DetIo`: a thin wrapper around turso's real `UnixIO` that can DEFER and
//! REORDER completions.
//!
//! `UnixIO` completes every `pread`/`pwrite`/`pwritev`/`sync`/`truncate`
//! synchronously inside the submit call, so on the default backend the
//! re-entrancy paths of turso's `IOResult::IO` state machines ("the completion
//! is still pending, yield and come back") are only ever exercised by io_uring.
//! This wrapper keeps the bytes on patina's deterministic filesystem (so fs
//! faults and crashes still apply) but, seeded per run, holds a submitted
//! operation in a queue and performs it later, from `step()`, in a seeded
//! order — which is also how completions arrive from a real async backend.
//!
//! Everything here is a pure function of the seed handed to `DetIo::new`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use turso_core::io::{FileId, FileSyncType};
use turso_core::{
    Buffer, Clock, Completion, CompletionError, File, LimboError, MonotonicInstant, OpenFlags,
    PlatformIO, Result, WallClockInstant, IO,
};

/// Small local PRNG (splitmix64).
pub struct Prng(pub u64);

impl Prng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    /// Uniform draw in `[lo, hi]`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
    pub fn chance(&mut self, permille: u64) -> bool {
        self.next() % 1000 < permille
    }
}

type Deferred = Box<dyn FnOnce() + Send>;

/// Which file operation a guest-armed one-shot failure targets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    Pread,
    Pwrite,
    Pwritev,
    Sync,
    Truncate,
}

fn injected(ctx: &'static str) -> LimboError {
    turso_core::io_error(std::io::Error::from_raw_os_error(5), ctx)
}

pub struct DetIoState {
    pending: Mutex<VecDeque<Deferred>>,
    rng: Mutex<Prng>,
    /// Per-op probability (permille) that an op is deferred instead of done inline.
    pub defer_permille: AtomicU64,
    /// Whether a `step()` drains in a seeded order (reordering completions).
    pub reorder: bool,
    /// Probability (permille) that a step leaves one op behind for the next step.
    pub hold_back_permille: u64,
    pub deferred_ops: AtomicU64,
    pub reordered_steps: AtomicU64,
    pub steps: AtomicU64,
    /// Guest-armed failures: `(kind, remaining ops of that kind to skip)`.
    /// When the counter reaches zero the next op of that kind fails with EIO
    /// at submit time (exactly how `UnixIO` surfaces a syscall error).
    armed: Mutex<Vec<(OpKind, u32)>>,
    /// Guest-armed power cuts: the op of that kind is performed, then the
    /// filesystem crashes (so the op's bytes are the last unsynced write).
    armed_crash: Mutex<Vec<(OpKind, u32)>>,
    /// Guest-armed SHORT reads: `(skip, bytes)` — the skip-th next pread
    /// returns only `bytes` bytes (a legal POSIX outcome).
    armed_short: Mutex<Vec<(u32, usize)>>,
    pub injected_failures: AtomicU64,
}

pub struct DetIo {
    inner: Arc<PlatformIO>,
    pub state: Arc<DetIoState>,
}

impl DetIo {
    pub fn new(seed: u64, defer_permille: u64, reorder: bool, hold_back_permille: u64) -> Self {
        Self {
            inner: Arc::new(PlatformIO::new().expect("platform io")),
            state: Arc::new(DetIoState {
                pending: Mutex::new(VecDeque::new()),
                rng: Mutex::new(Prng(seed)),
                defer_permille: AtomicU64::new(defer_permille),
                reorder,
                hold_back_permille,
                deferred_ops: AtomicU64::new(0),
                reordered_steps: AtomicU64::new(0),
                steps: AtomicU64::new(0),
                armed: Mutex::new(Vec::new()),
                armed_crash: Mutex::new(Vec::new()),
                armed_short: Mutex::new(Vec::new()),
                injected_failures: AtomicU64::new(0),
            }),
        }
    }

    /// Fail the `skip`-th next operation of `kind` (0 = the very next one).
    pub fn arm_fail(&self, kind: OpKind, skip: u32) {
        self.state.armed.lock().unwrap().push((kind, skip));
    }

    /// Power-cut right after the `skip`-th next operation of `kind`.
    pub fn arm_crash(&self, kind: OpKind, skip: u32) {
        self.state.armed_crash.lock().unwrap().push((kind, skip));
    }

    /// The `skip`-th next pread returns only `bytes` bytes.
    pub fn arm_short_read(&self, skip: u32, bytes: usize) {
        self.state.armed_short.lock().unwrap().push((skip, bytes));
    }

    pub fn pending_len(&self) -> usize {
        self.state.pending.lock().unwrap().len()
    }
}

impl DetIoState {
    pub fn clear_armed(&self) {
        self.armed.lock().unwrap().clear();
        self.armed_crash.lock().unwrap().clear();
        self.armed_short.lock().unwrap().clear();
    }

    fn take_short(&self) -> Option<usize> {
        let mut armed = self.armed_short.lock().unwrap();
        let mut i = 0;
        while i < armed.len() {
            if armed[i].0 == 0 {
                let n = armed[i].1;
                armed.remove(i);
                self.injected_failures.fetch_add(1, Ordering::Relaxed);
                return Some(n);
            }
            armed[i].0 -= 1;
            i += 1;
        }
        None
    }

    /// Consume an armed power cut for `kind`, if its turn has come.
    fn take_crash(&self, kind: OpKind) -> bool {
        let mut armed = self.armed_crash.lock().unwrap();
        let mut i = 0;
        while i < armed.len() {
            if armed[i].0 == kind {
                if armed[i].1 == 0 {
                    armed.remove(i);
                    return true;
                }
                armed[i].1 -= 1;
            }
            i += 1;
        }
        false
    }

    pub fn armed_crash_len(&self) -> usize {
        self.armed_crash.lock().unwrap().len()
    }

    pub fn set_defer_permille(&self, permille: u64) {
        self.defer_permille.store(permille, Ordering::Relaxed);
    }

    /// Consume an armed failure for `kind`, if its turn has come.
    fn take_failure(&self, kind: OpKind) -> bool {
        let mut armed = self.armed.lock().unwrap();
        let mut fire = false;
        let mut i = 0;
        while i < armed.len() {
            if armed[i].0 == kind {
                if armed[i].1 == 0 {
                    armed.remove(i);
                    fire = true;
                    break;
                }
                armed[i].1 -= 1;
            }
            i += 1;
        }
        if fire {
            self.injected_failures.fetch_add(1, Ordering::Relaxed);
        }
        fire
    }

    fn should_defer(&self) -> bool {
        let permille = self.defer_permille.load(Ordering::Relaxed);
        if permille == 0 {
            return false;
        }
        self.rng.lock().unwrap().chance(permille)
    }

    fn defer(&self, op: Deferred) {
        self.deferred_ops.fetch_add(1, Ordering::Relaxed);
        self.pending.lock().unwrap().push_back(op);
    }

    /// Run queued operations: all of them in submission order, or — when
    /// reordering is on — in a seeded order, optionally leaving one behind.
    fn drain(&self) {
        self.steps.fetch_add(1, Ordering::Relaxed);
        loop {
            let op = {
                let mut pending = self.pending.lock().unwrap();
                if pending.is_empty() {
                    return;
                }
                let mut rng = self.rng.lock().unwrap();
                // Leave one op behind (forcing another IO round-trip) sometimes,
                // but never starve: only when at least one op already ran this
                // step is guaranteed by the caller structure below.
                if pending.len() == 1
                    && self.hold_back_permille > 0
                    && self.held_this_step()
                    && rng.chance(self.hold_back_permille)
                {
                    return;
                }
                if self.reorder && pending.len() > 1 {
                    let idx = rng.range(0, pending.len() as u64 - 1) as usize;
                    if idx != 0 {
                        self.reordered_steps.fetch_add(1, Ordering::Relaxed);
                    }
                    pending.remove(idx).unwrap()
                } else {
                    pending.pop_front().unwrap()
                }
            };
            op();
            self.mark_ran_this_step();
        }
    }

    // A tiny per-step flag so the hold-back rule can never starve a waiter:
    // a step always performs at least one queued op.
    fn held_this_step(&self) -> bool {
        RAN_THIS_STEP.with(|f| f.get())
    }
    fn mark_ran_this_step(&self) {
        RAN_THIS_STEP.with(|f| f.set(true));
    }
}

thread_local! {
    static RAN_THIS_STEP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Clock for DetIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for DetIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(DetFile {
            inner,
            state: self.state.clone(),
            path: path.to_string(),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }
    fn step(&self) -> Result<()> {
        RAN_THIS_STEP.with(|f| f.set(false));
        self.state.drain();
        self.inner.step()
    }
    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest)
    }
}

pub struct DetFile {
    inner: Arc<dyn File>,
    state: Arc<DetIoState>,
    path: String,
}

fn fail_completion(c: &Completion, err: LimboError) {
    let err = match err {
        LimboError::CompletionError(e) => e,
        _ => CompletionError::IOError(std::io::ErrorKind::Other, "deferred-op"),
    };
    c.error(err);
}

impl File for DetFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        if self.state.take_failure(OpKind::Pread) {
            return Err(injected("pread"));
        }
        if self.state.take_crash(OpKind::Pread) {
            let r = self.inner.pread(pos, c);
            crate::power_cut();
            return r;
        }
        if let Some(n) = self.state.take_short() {
            // A short read: fill the first `n` bytes from the file and report
            // `n`, exactly as a kernel pread that returned early would.
            use std::os::unix::fs::FileExt;
            let r = c.as_read();
            let buf = r.buf();
            // SAFETY: same contract UnixFile::pread relies on — the read buffer
            // is exclusively ours until the completion fires.
            let slice = unsafe { buf.as_mut_slice() };
            let n = n.min(slice.len());
            let file = std::fs::File::open(&self.path)
                .map_err(|e| turso_core::io_error(e, "pread"))?;
            let got = file
                .read_at(&mut slice[..n], pos)
                .map_err(|e| turso_core::io_error(e, "pread"))?;
            c.complete(got as i32);
            return Ok(c);
        }
        if self.state.should_defer() {
            let inner = self.inner.clone();
            let cc = c.clone();
            self.state.defer(Box::new(move || {
                if let Err(e) = inner.pread(pos, cc.clone()) {
                    fail_completion(&cc, e);
                }
            }));
            return Ok(c);
        }
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        if self.state.take_failure(OpKind::Pwrite) {
            return Err(injected("pwrite"));
        }
        if self.state.take_crash(OpKind::Pwrite) {
            let r = self.inner.pwrite(pos, buffer, c);
            crate::power_cut();
            return r;
        }
        if self.state.should_defer() {
            let inner = self.inner.clone();
            let cc = c.clone();
            self.state.defer(Box::new(move || {
                if let Err(e) = inner.pwrite(pos, buffer, cc.clone()) {
                    fail_completion(&cc, e);
                }
            }));
            return Ok(c);
        }
        self.inner.pwrite(pos, buffer, c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        if self.state.take_failure(OpKind::Pwritev) {
            return Err(injected("pwritev"));
        }
        if self.state.take_crash(OpKind::Pwritev) {
            let r = self.inner.pwritev(pos, buffers, c);
            crate::power_cut();
            return r;
        }
        if self.state.should_defer() {
            let inner = self.inner.clone();
            let cc = c.clone();
            self.state.defer(Box::new(move || {
                if let Err(e) = inner.pwritev(pos, buffers, cc.clone()) {
                    fail_completion(&cc, e);
                }
            }));
            return Ok(c);
        }
        self.inner.pwritev(pos, buffers, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        if self.state.take_failure(OpKind::Sync) {
            return Err(injected("sync"));
        }
        if self.state.take_crash(OpKind::Sync) {
            let r = self.inner.sync(c, sync_type);
            crate::power_cut();
            return r;
        }
        if self.state.should_defer() {
            let inner = self.inner.clone();
            let cc = c.clone();
            self.state.defer(Box::new(move || {
                if let Err(e) = inner.sync(cc.clone(), sync_type) {
                    fail_completion(&cc, e);
                }
            }));
            return Ok(c);
        }
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        if self.state.take_failure(OpKind::Truncate) {
            return Err(injected("truncate"));
        }
        if self.state.take_crash(OpKind::Truncate) {
            let r = self.inner.truncate(len, c);
            crate::power_cut();
            return r;
        }
        if self.state.should_defer() {
            let inner = self.inner.clone();
            let cc = c.clone();
            self.state.defer(Box::new(move || {
                if let Err(e) = inner.truncate(len, cc.clone()) {
                    fail_completion(&cc, e);
                }
            }));
            return Ok(c);
        }
        self.inner.truncate(len, c)
    }
    fn has_hole(&self, pos: usize, len: usize) -> Result<bool> {
        self.inner.has_hole(pos, len)
    }
    fn punch_hole(&self, pos: usize, len: usize) -> Result<()> {
        self.inner.punch_hole(pos, len)
    }
}
