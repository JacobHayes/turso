//! Deterministic-simulation driver for turso_core under Patina.
//!
//! The system-under-test is the real `turso_core` engine (pager, WAL, B-tree,
//! VDBE) opened on patina's deterministic in-memory filesystem through turso's
//! own `UnixIO` (wrapped by `DetIo`, which can defer/reorder completions). The
//! workload — transactions of INSERT/UPDATE/DELETE/SAVEPOINT over one table, a
//! second reader connection, explicit checkpoints of every mode, integrity
//! checks, guest-triggered power cuts and reopen/recovery — is a pure function
//! of the run seed (`patina_dst::rng()`).
//!
//! Faults come from patina's knobs (fs errors / short writes / crash points /
//! latency, preemption), from `buggify!` sites inside turso_core, and from the
//! driver's own seeded crashes (`patina_crash`).
//!
//! Outcomes are reported through the verdict ABI: every invariant breach is a
//! `Violation` under a stable label; a clean run reports one `Pass` with a
//! digest. `turso_assert!` sites inside turso are mirrored into
//! `patina_dst::always!`, so an engine-internal invariant breach is a verdict
//! too.

mod detio;
mod model;
mod scenario;

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use detio::{DetIo, Prng};
use model::{digest_model, digest_rows, History, Model, SyncMode};
use patina_dst::VerdictKind;
use turso_core::{
    CheckpointMode, Connection, Database, DatabaseOpts, LimboError, OpenFlags, SqliteDialect,
    StepResult, IO,
};

const ROOT: &str = "/turso-dst";
const DB_PATH: &str = "/turso-dst/main.db";
const SENTINEL_PATH: &str = "/turso-dst/sentinel";
/// Boundary-op budget per statement before it is declared stuck.
const STEP_BUDGET: u64 = 400_000;

static VIOLATIONS: AtomicU64 = AtomicU64::new(0);

#[cfg(patina_shim)]
extern "C" {
    fn patina_crash() -> i32;
}

/// Power-cut: every open descriptor is invalidated and unsynced writes may be
/// torn/lost (patina's crash model). Outside patina this is a no-op and the
/// driver falls back to "drop the handles without closing".
fn power_cut() -> bool {
    #[cfg(patina_shim)]
    {
        let rc = unsafe { patina_crash() };
        return rc == 0;
    }
    #[allow(unreachable_code)]
    false
}

/// Detects a power cut that happened INSIDE the engine (a `dst_crash_point!`
/// in turso, or patina's own `--fs-crash-at`): the crash model invalidates
/// every open descriptor, so `fstat` on a handle opened before the cut fails
/// with EBADF (an injected EIO is not a crash and is told apart by errno).
struct Sentinel {
    file: Option<std::fs::File>,
}

impl Sentinel {
    fn arm(&mut self, io: &DetIo) {
        self.file = None;
        for _ in 0..20 {
            match std::fs::File::create(SENTINEL_PATH) {
                Ok(f) => {
                    self.file = Some(f);
                    return;
                }
                Err(_) => backoff(io, 5),
            }
        }
    }
    fn crashed(&self) -> bool {
        match &self.file {
            Some(f) => match f.metadata() {
                Ok(_) => false,
                Err(e) => e.raw_os_error() == Some(9),
            },
            None => false,
        }
    }
}

fn violation(label: &str, detail: &str) {
    VIOLATIONS.fetch_add(1, Ordering::Relaxed);
    patina_dst::verdict(VerdictKind::Violation, label, detail);
    println!("TURSO_DST_VIOLATION label={label} detail={detail}");
}

#[derive(Debug)]
enum Fail {
    /// The engine returned Busy / BusySnapshot.
    Busy,
    /// A constraint violation (expected by the workload for duplicate keys).
    Constraint(String),
    /// Any other engine error (I/O, corrupt, internal, ...).
    Error(String),
    /// The statement exceeded the boundary-op budget.
    Stuck,
    /// The statement panicked (caught).
    Panic(String),
}

fn classify(e: LimboError) -> Fail {
    match e {
        LimboError::Busy | LimboError::BusySnapshot => Fail::Busy,
        LimboError::Constraint(s) => Fail::Constraint(s),
        other => Fail::Error(format!("{other}")),
    }
}

fn panic_msg(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic".into()
    }
}

struct Db {
    io: Arc<DetIo>,
    db: Arc<Database>,
    conn: Arc<Connection>,
    reader: Arc<Connection>,
}

fn verbose() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var_os("TURSO_DST_VERBOSE").is_some())
}

/// Run one SQL statement to completion, collecting rows as strings.
fn exec(io: &DetIo, conn: &Arc<Connection>, sql: &str) -> Result<Vec<Vec<String>>, Fail> {
    let r = exec_inner(io, conn, sql);
    if verbose() {
        let short: String = sql.chars().take(90).collect();
        let line = match &r {
            Ok(rows) => format!("sql: {short} -> ok rows={}{}", rows.len(),
                if rows.len() <= 2 { format!(" {rows:?}") } else { String::new() }),
            Err(e) => format!("sql: {short} -> ERR {e:?}"),
        };
        println!("{line}");
        // Same line into the tracing stream so it interleaves with engine trace.
        tracing::info!(target: "turso_dst", "{line}");
    }
    r
}

fn exec_inner(io: &DetIo, conn: &Arc<Connection>, sql: &str) -> Result<Vec<Vec<String>>, Fail> {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<Vec<Vec<String>>, Fail> {
        let mut stmt = conn.prepare(sql).map_err(classify)?;
        let mut rows = Vec::new();
        let mut steps = 0u64;
        loop {
            match stmt.step().map_err(classify)? {
                StepResult::Done => break,
                StepResult::Row => {
                    let row = stmt.row().expect("row present");
                    rows.push(row.get_values().map(|v| format!("{v}")).collect());
                }
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    steps += 1;
                    if steps > STEP_BUDGET {
                        return Err(Fail::Stuck);
                    }
                    io.step().map_err(classify)?;
                }
                StepResult::Interrupt => return Err(Fail::Error("interrupt".into())),
                StepResult::Busy => return Err(Fail::Busy),
            }
        }
        Ok(rows)
    }));
    match result {
        Ok(r) => r,
        Err(p) => Err(Fail::Panic(panic_msg(p))),
    }
}

/// `SELECT id, n, v FROM kv ORDER BY id` into a model.
fn read_model(io: &DetIo, conn: &Arc<Connection>) -> Result<Model, Fail> {
    let rows = exec(io, conn, "SELECT id, n, v FROM kv ORDER BY id")?;
    let mut m = Model::new();
    for r in rows {
        let id: i64 = r[0].parse().map_err(|_| Fail::Error(format!("bad id {:?}", r[0])))?;
        let n: i64 = r[1].parse().map_err(|_| Fail::Error(format!("bad n {:?}", r[1])))?;
        m.insert(id, (n, r[2].clone()));
    }
    Ok(m)
}

fn read_digest(io: &DetIo, conn: &Arc<Connection>) -> Result<(u64, usize), Fail> {
    let rows = exec(io, conn, "SELECT id, n, v FROM kv ORDER BY id")?;
    let mut parsed = Vec::with_capacity(rows.len());
    for r in &rows {
        let id: i64 = r[0].parse().map_err(|_| Fail::Error(format!("bad id {:?}", r[0])))?;
        let n: i64 = r[1].parse().map_err(|_| Fail::Error(format!("bad n {:?}", r[1])))?;
        parsed.push((id, n, r[2].as_str()));
    }
    Ok((digest_rows(parsed.iter().copied()), rows.len()))
}

/// Sleep a little virtual time so a retry lands in a different fault draw.
fn backoff(io: &DetIo, ms: u64) {
    io.sleep(Duration::from_millis(ms));
}

struct Config {
    sync_mode: SyncMode,
    cache_pages: i64,
    autockpt: i64,
    with_index: bool,
    epochs: u64,
    txs_per_epoch: u64,
    big_value_permille: u64,
    reader_window_permille: u64,
    checkpoint_permille: u64,
    integrity_permille: u64,
    crash_permille: u64,
    mid_tx_crash_permille: u64,
    io_defer_permille: u64,
    io_reorder: bool,
    io_hold_back_permille: u64,
    /// Probability (per epoch) of running a background reader/checkpointer
    /// thread concurrently with the writer.
    bg_thread_permille: u64,
}

impl Config {
    fn derive(rng: &mut Prng) -> Self {
        let sync_mode = if rng.chance(600) {
            SyncMode::Full
        } else {
            SyncMode::Normal
        };
        let cache_pages = [2, 5, 10, 50, 2000][rng.range(0, 4) as usize];
        let autockpt = [0, 5, 20, 100, 1000][rng.range(0, 4) as usize];
        let io_defer_permille = [0, 0, 200, 500, 900][rng.range(0, 4) as usize];
        Self {
            sync_mode,
            cache_pages,
            autockpt,
            with_index: rng.chance(500),
            epochs: rng.range(2, 5),
            txs_per_epoch: rng.range(6, 24),
            big_value_permille: rng.range(20, 150),
            reader_window_permille: rng.range(50, 250),
            checkpoint_permille: rng.range(50, 300),
            integrity_permille: rng.range(30, 120),
            crash_permille: rng.range(300, 900),
            mid_tx_crash_permille: rng.range(100, 400),
            io_defer_permille,
            io_reorder: io_defer_permille > 0 && rng.chance(700),
            io_hold_back_permille: if io_defer_permille > 0 {
                rng.range(0, 300)
            } else {
                0
            },
            bg_thread_permille: rng.range(300, 800),
        }
    }
}

/// A background thread sharing the `Database`: its own connection runs read
/// transactions (snapshot-isolation checked), count queries, and checkpoints
/// of every mode, interleaved with the writer by patina's deterministic
/// scheduler. Errors are expected under faults and ignored; a panic is a
/// finding.
struct BgThread {
    stop: Arc<std::sync::atomic::AtomicBool>,
    handle: std::thread::JoinHandle<BTreeMap<&'static str, u64>>,
}

fn spawn_bg(db: Arc<Database>, io: Arc<DetIo>, seed: u64) -> BgThread {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = stop.clone();
    let handle = std::thread::Builder::new()
        .name("turso-dst-bg".into())
        .spawn(move || {
            let mut stats: BTreeMap<&'static str, u64> = BTreeMap::new();
            let r = catch_unwind(AssertUnwindSafe(|| {
                let mut rng = Prng(seed);
                let Ok(conn) = db.connect() else {
                    stats.insert("bg-connect-failed", 1);
                    return;
                };
                let _ = exec(&io, &conn, "PRAGMA data_sync_retry=ON");
                let mut iters = 0u64;
                while !stop2.load(Ordering::Relaxed) && iters < 400 {
                    iters += 1;
                    match rng.range(0, 9) {
                        0..=3 => {
                            // Read transaction: two reads must agree.
                            if exec(&io, &conn, "BEGIN").is_ok() {
                                let a = read_digest(&io, &conn);
                                io.sleep(Duration::from_millis(rng.range(0, 3)));
                                let b = read_digest(&io, &conn);
                                if let (Ok((da, na)), Ok((dbg, nb))) = (&a, &b) {
                                    if da != dbg {
                                        violation(
                                            "snapshot-isolation-violated-bg",
                                            &format!("bg reader saw rows={na} then rows={nb} in one read tx"),
                                        );
                                    }
                                    *stats.entry("bg-read-tx").or_insert(0) += 1;
                                }
                                let _ = exec(&io, &conn, "COMMIT");
                            }
                        }
                        4..=5 => {
                            if exec(&io, &conn, "SELECT count(*) FROM kv").is_ok() {
                                *stats.entry("bg-count").or_insert(0) += 1;
                            }
                        }
                        _ => {
                            let mode = match rng.range(0, 3) {
                                0 => CheckpointMode::Passive { upper_bound_inclusive: None },
                                1 => CheckpointMode::Full,
                                2 => CheckpointMode::Restart,
                                _ => CheckpointMode::Truncate { upper_bound_inclusive: None },
                            };
                            match conn.checkpoint(mode) {
                                Ok(_) => *stats.entry("bg-checkpoint-ok").or_insert(0) += 1,
                                Err(LimboError::Busy) => {
                                    *stats.entry("bg-checkpoint-busy").or_insert(0) += 1
                                }
                                Err(_) => *stats.entry("bg-checkpoint-error").or_insert(0) += 1,
                            }
                        }
                    }
                    io.sleep(Duration::from_millis(rng.range(0, 5)));
                }
                let _ = conn.close();
            }));
            if let Err(p) = r {
                violation("panic-in-bg-thread", &panic_msg(p));
            }
            stats
        })
        .expect("spawn bg thread");
    BgThread { stop, handle }
}

fn stop_bg(bg: Option<BgThread>, wl: &mut Workload) {
    let Some(bg) = bg else { return };
    bg.stop.store(true, Ordering::Relaxed);
    match bg.handle.join() {
        Ok(stats) => {
            for (k, v) in stats {
                *wl.stats.entry(k).or_insert(0) += v;
            }
            patina_dst::sometimes!(true, "bg-thread-ran");
        }
        Err(_) => violation("bg-thread-join-failed", ""),
    }
}

fn open(io: &Arc<DetIo>, cfg: &Config) -> Result<Db, Fail> {
    let io_dyn: Arc<dyn IO> = io.clone();
    let db = Database::open_file_with_flags(
        io_dyn,
        DB_PATH,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .map_err(classify)?;
    let conn = db.connect().map_err(classify)?;
    let reader = db.connect().map_err(classify)?;
    let sync = match cfg.sync_mode {
        SyncMode::Full => "FULL",
        SyncMode::Normal => "NORMAL",
    };
    for c in [&conn, &reader] {
        exec(io, c, &format!("PRAGMA synchronous={sync}"))?;
        // Default-off: an injected fsync failure would otherwise be a
        // by-design panic ("crash > corrupt") instead of an error we can
        // observe and recover from.
        exec(io, c, "PRAGMA data_sync_retry=ON")?;
        exec(io, c, &format!("PRAGMA cache_size={}", cfg.cache_pages))?;
        exec(io, c, &format!("PRAGMA wal_autocheckpoint={}", cfg.autockpt))?;
    }
    Ok(Db {
        io: io.clone(),
        db,
        conn,
        reader,
    })
}

fn open_with_retry(io: &Arc<DetIo>, cfg: &Config) -> Option<Db> {
    let mut last = None;
    for attempt in 0..40 {
        match open(io, cfg) {
            Ok(db) => {
                patina_dst::sometimes!(attempt > 0, "open-retried");
                return Some(db);
            }
            Err(e) => {
                last = Some(format!("{e:?}"));
                backoff(io, 20);
            }
        }
    }
    violation("reopen-failed", &last.unwrap_or_default());
    None
}

/// Drop every handle. Under a crash the descriptors are already dead and the
/// engine's own cleanup may fail; a panic there is attributed separately.
fn drop_db(db: Db, after_crash: bool) {
    let Db {
        io: _,
        db,
        conn,
        reader,
    } = db;
    let r = catch_unwind(AssertUnwindSafe(move || {
        drop(reader);
        drop(conn);
        drop(db);
    }));
    if let Err(p) = r {
        let msg = panic_msg(p);
        if after_crash {
            violation("panic-in-drop-after-crash", &msg);
        } else {
            violation("panic-in-drop", &msg);
        }
    }
}

/// Close the writer cleanly (runs the shutdown checkpoint), then drop.
fn close_db(db: Db) {
    let r = catch_unwind(AssertUnwindSafe(|| {
        if let Err(e) = db.conn.close() {
            println!("note: close error {e}");
        }
        if let Err(e) = db.reader.close() {
            println!("note: reader close error {e}");
        }
    }));
    if let Err(p) = r {
        violation("panic-in-close", &panic_msg(p));
    }
    drop_db(db, false);
}

struct Workload {
    rng: Prng,
    next_id: i64,
    stats: BTreeMap<&'static str, u64>,
}

impl Workload {
    fn bump(&mut self, k: &'static str) {
        *self.stats.entry(k).or_insert(0) += 1;
    }

    fn value(&mut self, big_permille: u64) -> String {
        if self.rng.chance(big_permille) {
            let len = self.rng.range(1500, 24000) as usize;
            let seed = self.rng.next();
            let mut s = String::with_capacity(len);
            for i in 0..len {
                s.push((b'a' + ((seed.wrapping_add(i as u64).wrapping_mul(31) % 26) as u8)) as char);
            }
            s
        } else {
            format!("v{}", self.rng.next() % 100_000)
        }
    }

    /// Generate one statement and its effect on `m` (applied only if the
    /// statement is expected to succeed).
    fn statement(&mut self, m: &Model, big_permille: u64) -> (String, Model, Expect) {
        let existing: Vec<i64> = m.keys().copied().collect();
        let pick = |rng: &mut Prng| -> Option<i64> {
            if existing.is_empty() {
                None
            } else {
                Some(existing[rng.range(0, existing.len() as u64 - 1) as usize])
            }
        };
        let mut next = m.clone();
        let roll = self.rng.range(0, 99);
        if roll < 40 || existing.is_empty() {
            // insert
            let dup = !existing.is_empty() && self.rng.chance(80);
            let id = if dup {
                pick(&mut self.rng).unwrap()
            } else {
                self.next_id += 1;
                self.next_id
            };
            let n = self.rng.range(0, 1000) as i64;
            let v = self.value(big_permille);
            let sql = format!("INSERT INTO kv VALUES({id}, {n}, '{v}')");
            if dup {
                self.bump("insert-dup");
                return (sql, next, Expect::Constraint);
            }
            next.insert(id, (n, v));
            self.bump("insert");
            (sql, next, Expect::Ok)
        } else if roll < 65 {
            // update one or a range
            if self.rng.chance(700) {
                let id = pick(&mut self.rng).unwrap();
                let n = self.rng.range(0, 1000) as i64;
                let v = self.value(big_permille);
                let sql = format!("UPDATE kv SET n = {n}, v = '{v}' WHERE id = {id}");
                next.insert(id, (n, v));
                self.bump("update");
                (sql, next, Expect::Ok)
            } else {
                let a = pick(&mut self.rng).unwrap();
                let b = a + self.rng.range(1, 40) as i64;
                let d = self.rng.range(1, 9) as i64;
                let sql = format!("UPDATE kv SET n = n + {d} WHERE id BETWEEN {a} AND {b}");
                for (id, (n, _)) in next.iter_mut() {
                    if *id >= a && *id <= b {
                        *n += d;
                    }
                }
                self.bump("update-range");
                (sql, next, Expect::Ok)
            }
        } else if roll < 85 {
            // delete one or a range
            if self.rng.chance(700) {
                let id = pick(&mut self.rng).unwrap();
                let sql = format!("DELETE FROM kv WHERE id = {id}");
                next.remove(&id);
                self.bump("delete");
                (sql, next, Expect::Ok)
            } else {
                let a = pick(&mut self.rng).unwrap();
                let b = a + self.rng.range(1, 30) as i64;
                let sql = format!("DELETE FROM kv WHERE id BETWEEN {a} AND {b}");
                next.retain(|id, _| !(*id >= a && *id <= b));
                self.bump("delete-range");
                (sql, next, Expect::Ok)
            }
        } else {
            // a read inside the transaction: must see the tx-local state
            self.bump("select-count");
            (
                "SELECT count(*), coalesce(sum(n), 0) FROM kv".into(),
                next,
                Expect::Count,
            )
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum Expect {
    Ok,
    Constraint,
    Count,
}

/// Outcome of one transaction attempt.
enum TxOutcome {
    Committed(Model),
    RolledBack,
    /// COMMIT (or the last autocommit statement) reported an error: applied or not.
    Uncertain(Model),
    /// An I/O-ish error made us abandon the transaction (rollback attempted).
    Aborted(String),
    /// The engine said Busy.
    Busy,
}

fn run_tx(
    db: &Db,
    wl: &mut Workload,
    hist: &mut History,
    cfg: &Config,
    crash_mid_tx: bool,
) -> (TxOutcome, bool /*crashed*/) {
    let io = &db.io;
    let conn = &db.conn;
    let base: Model = (*hist.model).clone();
    let autocommit = wl.rng.chance(300);
    if autocommit {
        let (sql, next, expect) = wl.statement(&base, cfg.big_value_permille);
        return match (exec(io, conn, &sql), expect) {
            (Ok(rows), Expect::Count) => {
                check_count(&rows, &base, "autocommit");
                (TxOutcome::Committed(base), false)
            }
            (Ok(_), Expect::Ok) => (TxOutcome::Committed(next), false),
            (Ok(_), Expect::Constraint) => {
                violation("constraint-not-enforced", &sql);
                (TxOutcome::Committed(base), false)
            }
            (Err(Fail::Constraint(_)), Expect::Constraint) => {
                (TxOutcome::Committed(base), false)
            }
            (Err(Fail::Busy), _) => (TxOutcome::Busy, false),
            (Err(e), _) => {
                wl.bump("autocommit-error");
                // An autocommit write that errors is an uncertain commit.
                if expect == Expect::Ok && !matches!(e, Fail::Constraint(_)) {
                    println!("note: autocommit error: {e:?}");
                    (TxOutcome::Uncertain(next), false)
                } else {
                    (TxOutcome::Aborted(format!("{e:?}")), false)
                }
            }
        };
    }

    if let Err(e) = exec(io, conn, "BEGIN") {
        return match e {
            Fail::Busy => (TxOutcome::Busy, false),
            other => (TxOutcome::Aborted(format!("begin: {other:?}")), false),
        };
    }
    let nstmts = wl.rng.range(1, 15);
    let crash_after = if crash_mid_tx {
        Some(wl.rng.range(0, nstmts - 1))
    } else {
        None
    };
    let mut cur = base.clone();
    // Savepoint stack of tx-local models.
    let mut savepoints: Vec<(String, Model)> = Vec::new();
    for i in 0..nstmts {
        if crash_after == Some(i) {
            wl.bump("crash-mid-tx");
            let ok = power_cut();
            patina_dst::sometimes!(ok, "guest-power-cut-mid-tx");
            // Whatever happens from here on, the tx must not be visible.
            let _ = exec(io, conn, "ROLLBACK");
            return (TxOutcome::RolledBack, true);
        }
        // Savepoint dance, sometimes.
        if wl.rng.chance(120) {
            let name = format!("sp{}", savepoints.len());
            match exec(io, conn, &format!("SAVEPOINT {name}")) {
                Ok(_) => {
                    wl.bump("savepoint");
                    savepoints.push((name, cur.clone()));
                }
                Err(Fail::Busy) => return (TxOutcome::Busy, false),
                Err(e) => {
                    let _ = exec(io, conn, "ROLLBACK");
                    return (TxOutcome::Aborted(format!("savepoint: {e:?}")), false);
                }
            }
        } else if !savepoints.is_empty() && wl.rng.chance(250) {
            let (name, saved) = savepoints.pop().unwrap();
            if wl.rng.chance(500) {
                match exec(io, conn, &format!("ROLLBACK TO {name}")) {
                    Ok(_) => {
                        wl.bump("rollback-to");
                        cur = saved;
                        // ROLLBACK TO keeps the savepoint; release it too so the
                        // stack stays simple.
                        if let Err(e) = exec(io, conn, &format!("RELEASE {name}")) {
                            if let Fail::Busy = e {
                                return (TxOutcome::Busy, false);
                            }
                            let _ = exec(io, conn, "ROLLBACK");
                            return (TxOutcome::Aborted(format!("release: {e:?}")), false);
                        }
                    }
                    Err(Fail::Busy) => return (TxOutcome::Busy, false),
                    Err(e) => {
                        let _ = exec(io, conn, "ROLLBACK");
                        return (TxOutcome::Aborted(format!("rollback-to: {e:?}")), false);
                    }
                }
            } else {
                match exec(io, conn, &format!("RELEASE {name}")) {
                    Ok(_) => wl.bump("release"),
                    Err(Fail::Busy) => return (TxOutcome::Busy, false),
                    Err(e) => {
                        let _ = exec(io, conn, "ROLLBACK");
                        return (TxOutcome::Aborted(format!("release: {e:?}")), false);
                    }
                }
            }
        }

        let (sql, next, expect) = wl.statement(&cur, cfg.big_value_permille);
        match (exec(io, conn, &sql), expect) {
            (Ok(rows), Expect::Count) => check_count(&rows, &cur, "in-tx"),
            (Ok(_), Expect::Ok) => cur = next,
            (Ok(_), Expect::Constraint) => violation("constraint-not-enforced", &sql),
            (Err(Fail::Constraint(_)), Expect::Constraint) => {}
            (Err(Fail::Constraint(msg)), _) => {
                violation("unexpected-constraint-error", &format!("{sql:.80}: {msg}"));
            }
            (Err(Fail::Busy), _) => {
                let _ = exec(io, conn, "ROLLBACK");
                return (TxOutcome::Busy, false);
            }
            (Err(e), _) => {
                let _ = exec(io, conn, "ROLLBACK");
                return (TxOutcome::Aborted(format!("{sql:.60}: {e:?}")), false);
            }
        }
    }
    // Read-your-writes at the end of the transaction.
    if wl.rng.chance(400) {
        match read_digest(io, conn) {
            Ok((d, n)) => {
                if d != digest_model(&cur) {
                    violation(
                        "read-your-writes-violated",
                        &format!("in-tx digest mismatch rows_db={n} rows_model={}", cur.len()),
                    );
                }
            }
            Err(Fail::Busy) => {}
            Err(e) => {
                let _ = exec(io, conn, "ROLLBACK");
                return (TxOutcome::Aborted(format!("in-tx read: {e:?}")), false);
            }
        }
    }
    if wl.rng.chance(100) {
        return match exec(io, conn, "ROLLBACK") {
            Ok(_) => (TxOutcome::RolledBack, false),
            Err(Fail::Busy) => (TxOutcome::Busy, false),
            Err(e) => (TxOutcome::Aborted(format!("rollback: {e:?}")), false),
        };
    }
    match exec(io, conn, "COMMIT") {
        Ok(_) => (TxOutcome::Committed(cur), false),
        Err(Fail::Busy) => {
            let _ = exec(io, conn, "ROLLBACK");
            (TxOutcome::Busy, false)
        }
        Err(e) => {
            wl.bump("commit-error");
            println!("note: COMMIT error: {e:?}");
            let _ = exec(io, conn, "ROLLBACK");
            (TxOutcome::Uncertain(cur), false)
        }
    }
}

fn check_count(rows: &[Vec<String>], m: &Model, ctx: &str) {
    let got_count: i64 = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);
    let got_sum: i64 = rows
        .first()
        .and_then(|r| r.get(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(i64::MIN);
    let want_count = m.len() as i64;
    let want_sum: i64 = m.values().map(|(n, _)| *n).sum();
    if got_count != want_count || got_sum != want_sum {
        violation(
            "read-your-writes-violated",
            &format!("{ctx} count/sum: got ({got_count},{got_sum}) want ({want_count},{want_sum})"),
        );
    }
}

/// Verify a (re)opened database against the acceptable states, then re-sync
/// the model from what is actually there.
fn verify_reopen(db: &Db, hist: &mut History, crashed: bool, dirsynced: bool) -> bool {
    let io = &db.io;
    let mut last_err = String::new();
    let mut observed: Option<Model> = None;
    for _ in 0..30 {
        match read_model(io, &db.conn) {
            Ok(m) => {
                observed = Some(m);
                break;
            }
            Err(e) => {
                last_err = format!("{e:?}");
                if last_err.contains("no such table") {
                    break;
                }
                backoff(io, 20);
            }
        }
    }
    let Some(observed) = observed else {
        let db_exists = std::fs::metadata(DB_PATH).map(|m| m.len()).ok();
        let wal_exists = std::fs::metadata(format!("{DB_PATH}-wal")).map(|m| m.len()).ok();
        let detail = format!(
            "{last_err} (crashed={crashed} dirsynced={dirsynced} db_len={db_exists:?} wal_len={wal_exists:?})"
        );
        if last_err.contains("no such table") && crashed && !dirsynced {
            violation("db-lost-after-crash-without-dirsync", &detail);
        } else if last_err.contains("no such table") {
            violation("schema-lost-after-reopen", &detail);
        } else {
            violation("post-open-read-failed", &detail);
        }
        return false;
    };
    let d = digest_model(&observed);
    let acceptable = hist.acceptable(crashed);
    if !acceptable.iter().any(|s| s.digest == d) {
        let label = if crashed {
            match hist.sync_mode {
                SyncMode::Full => "durability-violated-after-crash",
                SyncMode::Normal => "prefix-consistency-violated-after-crash",
            }
        } else {
            "committed-state-lost-on-reopen"
        };
        let detail = format!(
            "observed rows={} digest={d:x}; acceptable: {}",
            observed.len(),
            acceptable
                .iter()
                .map(|s| format!(
                    "[{} rows={} certain={} {:x}]",
                    s.what, s.rows, s.certain, s.digest
                ))
                .collect::<Vec<_>>()
                .join(" ")
        );
        violation(label, &detail);
        describe_diff(&observed, hist);
    } else {
        let idx = acceptable.iter().position(|s| s.digest == d).unwrap();
        // Coverage: did a crash actually roll us back to an earlier state?
        patina_dst::sometimes!(
            crashed && idx + 1 < acceptable.len(),
            "crash-lost-unsynced-commits"
        );
    }
    // Integrity.
    let mut ok = false;
    for _ in 0..20 {
        match exec(io, &db.conn, "PRAGMA integrity_check") {
            Ok(rows) => {
                let joined = rows
                    .iter()
                    .map(|r| r.join("|"))
                    .collect::<Vec<_>>()
                    .join("; ");
                if joined != "ok" {
                    violation("integrity-check-failed-after-reopen", &joined);
                }
                ok = true;
                break;
            }
            Err(e) => {
                last_err = format!("{e:?}");
                backoff(io, 20);
            }
        }
    }
    if !ok {
        violation("post-open-integrity-check-failed", &last_err);
    }
    if crashed {
        hist.reset_from(observed);
    } else {
        hist.adopt(observed);
    }
    true
}

fn describe_diff(observed: &Model, hist: &History) {
    let model = &hist.model;
    let mut missing = 0;
    let mut extra = 0;
    let mut changed = 0;
    for (id, v) in model.iter() {
        match observed.get(id) {
            None => missing += 1,
            Some(o) if o != v => changed += 1,
            _ => {}
        }
    }
    for id in observed.keys() {
        if !model.contains_key(id) {
            extra += 1;
        }
    }
    println!(
        "TURSO_DST_DIFF vs last observed model: missing={missing} extra={extra} changed={changed}"
    );
    let fmt = |m: &Model| {
        m.iter()
            .map(|(id, (n, v))| format!("{id}:{n}:{}", &v[..v.len().min(6)]))
            .collect::<Vec<_>>()
            .join(",")
    };
    println!("TURSO_DST_DIFF observed=[{}]", fmt(observed));
    println!("TURSO_DST_DIFF model=[{}]", fmt(model));
    println!(
        "TURSO_DST_DIFF history: floor={} snaps=[{}]",
        hist.floor,
        hist.snaps
            .iter()
            .map(|s| format!("{}({}r,{:x})", s.what, s.rows, s.digest))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let db_len = std::fs::metadata(DB_PATH).map(|m| m.len()).ok();
    let wal_len = std::fs::metadata(format!("{DB_PATH}-wal")).map(|m| m.len()).ok();
    println!("TURSO_DST_DIFF files: db_len={db_len:?} wal_len={wal_len:?}");
}

fn checkpoint(db: &Db, wl: &mut Workload, hist: &mut History) {
    let mode = match wl.rng.range(0, 3) {
        0 => CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
        1 => CheckpointMode::Full,
        2 => CheckpointMode::Restart,
        _ => CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    };
    let label = match mode {
        CheckpointMode::Passive { .. } => "passive",
        CheckpointMode::Full => "full",
        CheckpointMode::Restart => "restart",
        CheckpointMode::Truncate { .. } => "truncate",
    };
    println!("note: checkpoint {label} start");
    let r = catch_unwind(AssertUnwindSafe(|| db.conn.checkpoint(mode)));
    match r {
        Ok(Ok(res)) => {
            wl.bump("checkpoint-ok");
            patina_dst::sometimes!(
                res.wal_checkpoint_backfilled > 0,
                "checkpoint-backfilled-frames"
            );
            patina_dst::sometimes!(
                matches!(mode, CheckpointMode::Passive { .. })
                    && res.wal_total_backfilled < res.wal_max_frame,
                "passive-checkpoint-partial"
            );
            println!(
                "note: checkpoint {label}: max_frame={} total_backfilled={} backfilled={}",
                res.wal_max_frame, res.wal_total_backfilled, res.wal_checkpoint_backfilled
            );
            hist.checkpoint_ok();
        }
        Ok(Err(LimboError::Busy)) => {
            wl.bump("checkpoint-busy");
            patina_dst::sometimes!(true, "checkpoint-busy");
        }
        Ok(Err(e)) => {
            wl.bump("checkpoint-error");
            println!("note: checkpoint {label} error: {e}");
        }
        Err(p) => violation("panic-in-checkpoint", &panic_msg(p)),
    }
}

/// Reader snapshot-isolation window: open a read transaction on the reader
/// connection, remember what it sees, let the writer commit, and check the
/// reader still sees its snapshot until it ends the transaction.
struct ReaderWindow {
    digest: u64,
    rows: usize,
}

fn reader_begin(db: &Db) -> Option<ReaderWindow> {
    if exec(&db.io, &db.reader, "BEGIN").is_err() {
        return None;
    }
    match read_digest(&db.io, &db.reader) {
        Ok((digest, rows)) => Some(ReaderWindow { digest, rows }),
        Err(_) => {
            let _ = exec(&db.io, &db.reader, "ROLLBACK");
            None
        }
    }
}

fn reader_check_and_end(db: &Db, w: &ReaderWindow, writer_committed: bool) {
    match read_digest(&db.io, &db.reader) {
        Ok((d, n)) => {
            if d != w.digest {
                violation(
                    "snapshot-isolation-violated",
                    &format!(
                        "reader saw rows={} then rows={n} within one read tx",
                        w.rows
                    ),
                );
            }
            patina_dst::sometimes!(writer_committed, "reader-held-snapshot-across-commit");
        }
        Err(e) => println!("note: reader re-read failed: {e:?}"),
    }
    let _ = exec(&db.io, &db.reader, "COMMIT");
}

fn main() {
    let seed = patina_dst::rng();
    let mut rng = Prng(seed);
    let cfg = Config::derive(&mut rng);
    println!(
        "TURSO_DST_CONFIG sync={:?} cache_pages={} autockpt={} index={} epochs={} txs={} defer={} reorder={} hold={} crash={}",
        cfg.sync_mode,
        cfg.cache_pages,
        cfg.autockpt,
        cfg.with_index,
        cfg.epochs,
        cfg.txs_per_epoch,
        cfg.io_defer_permille,
        cfg.io_reorder,
        cfg.io_hold_back_permille,
        cfg.crash_permille
    );

    if let Some(filter) = std::env::var_os("TURSO_DST_TRACE") {
        // Engine-internal tracing on demand (e.g. `turso_core::storage=debug`).
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
        let _ = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .without_time()
                    .with_writer(std::io::stderr),
            )
            .with(tracing_subscriber::EnvFilter::new(filter.to_string_lossy()))
            .try_init();
    }
    if let Some(name) = std::env::var_os("TURSO_DST_SCENARIO") {
        // Focused deterministic scenarios (no random faults): exact fault
        // placement through DetIo, printed step by step.
        let io = Arc::new(DetIo::new(rng.next(), 0, false, 0));
        scenario::run(&name.to_string_lossy(), io, &cfg);
        return;
    }
    let io = Arc::new(DetIo::new(
        rng.next(),
        cfg.io_defer_permille,
        cfg.io_reorder,
        cfg.io_hold_back_permille,
    ));

    // Setup (retry through injected faults: not part of what is being tested).
    let mut made = false;
    for _ in 0..50 {
        if std::fs::create_dir_all(ROOT).is_ok() {
            made = true;
            break;
        }
        backoff(&io, 10);
    }
    if !made {
        println!("TURSO_DST_SETUP_FAILED mkdir");
        std::process::exit(3);
    }
    // The crash sentinel is armed BEFORE the first open so a power cut placed
    // anywhere in setup (e.g. `--fs-crash-at open:2`) is seen and setup is
    // redone on fresh handles.
    let mut sentinel = Sentinel { file: None };
    sentinel.arm(&io);
    let Some(mut db) = open_with_retry(&io, &cfg) else {
        std::process::exit(1);
    };
    // Setup runs under synchronous=FULL and ends with a checkpoint so the
    // schema is durable before the configured mode takes over: a NORMAL-mode
    // schema commit could legitimately be lost by the first power cut.
    let mut created = false;
    let mut dirsynced = false;
    let want_dirsync = std::env::var_os("TURSO_DST_NO_DIRSYNC").is_none();
    for _ in 0..40 {
        if sentinel.crashed() {
            // An engine-internal crash point fired during setup: the handles
            // are dead; start over on fresh ones.
            let old = db;
            drop_db(old, true);
            let Some(new_db) = open_with_retry(&io, &cfg) else {
                std::process::exit(1);
            };
            db = new_db;
            sentinel.arm(&io);
        }
        let _ = exec(&io, &db.conn, "PRAGMA synchronous=FULL");
        let r = exec(
            &io,
            &db.conn,
            "CREATE TABLE IF NOT EXISTS kv(id INTEGER PRIMARY KEY, n INTEGER, v TEXT)",
        );
        if r.is_ok() {
            if cfg.with_index {
                let _ = exec(&io, &db.conn, "CREATE INDEX IF NOT EXISTS kv_n ON kv(n)");
            }
            // The schema commit ran under FULL, so it is durable already; the
            // checkpoint is best effort (a buggify-forced partial checkpoint
            // legitimately reports Busy).
            let ck = db.conn.checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            });
            if let Err(e) = ck {
                println!("note: setup checkpoint skipped: {e}");
            }
            // Make the freshly created DB/WAL directory entries durable. turso
            // itself never fsyncs the parent directory (SQLite does, for
            // journal/WAL files); under patina's crash model an un-synced
            // creation is LOST on power cut. Default on (an established
            // finding); `--env TURSO_DST_NO_DIRSYNC=1` re-exposes it.
            if want_dirsync {
                dirsynced = false;
                for _ in 0..20 {
                    if std::fs::File::open(ROOT).and_then(|d| d.sync_all()).is_ok() {
                        dirsynced = true;
                        break;
                    }
                    backoff(&io, 10);
                }
            }
            if sentinel.crashed() {
                // A power cut landed inside setup (checkpoint/dirsync): loop —
                // the top of the loop reopens on fresh handles.
                println!("note: setup hit a power cut; redoing setup");
                continue;
            }
            created = true;
            break;
        } else {
            println!("note: setup create failed: {:?}", r.err());
        }
        backoff(&io, 20);
    }
    if !created {
        println!("TURSO_DST_SETUP_FAILED create-table");
        std::process::exit(3);
    }
    if cfg.sync_mode == SyncMode::Normal {
        let _ = exec(&io, &db.conn, "PRAGMA synchronous=NORMAL");
    }
    sentinel.arm(&io);
    // Engine-internal crash points (dst_crash_point! in turso) are armed only
    // now: during setup they would starve the run of its schema.
    turso_core::dst::enable_crash_points(true);
    println!("TURSO_DST_SETUP dirsynced={dirsynced}");
    patina_dst::lifecycle::setup_complete();

    let mut hist = History::new(Model::new(), cfg.sync_mode);
    let mut wl = Workload {
        rng: Prng(rng.next()),
        next_id: 0,
        stats: BTreeMap::new(),
    };
    let mut epochs_done = 0u64;
    let mut crashes = 0u64;
    let mut consecutive_failures = 0u32;

    'epochs: for epoch in 0..cfg.epochs {
        let mut crashed = false;
        let mut reader_window: Option<ReaderWindow> = None;
        let mut reader_saw_commit = false;
        let bg = if wl.rng.chance(cfg.bg_thread_permille) {
            wl.bump("bg-thread-spawned");
            Some(spawn_bg(db.db.clone(), io.clone(), wl.rng.next()))
        } else {
            None
        };
        let crash_at_tx = if wl.rng.chance(cfg.crash_permille) {
            Some(wl.rng.range(0, cfg.txs_per_epoch))
        } else {
            None
        };
        for t in 0..cfg.txs_per_epoch {
            // Occasionally end a reader window before a new one.
            if let Some(w) = reader_window.take() {
                reader_check_and_end(&db, &w, reader_saw_commit);
                reader_saw_commit = false;
            }
            if wl.rng.chance(cfg.reader_window_permille) {
                reader_window = reader_begin(&db);
            }
            if crash_at_tx == Some(t) {
                wl.bump("crash-between-tx");
                let ok = power_cut();
                patina_dst::sometimes!(ok, "guest-power-cut-between-tx");
                crashed = ok;
                crashes += 1;
                break;
            }
            // A pending uncertain commit must be settled before the next
            // transaction is built on top of it: retry the live read through
            // injected faults, and if it stays unreadable, restart the process
            // (reopen verifies against both candidates).
            if hist.shadow.is_some() {
                let mut resolved = false;
                for _ in 0..12 {
                    match read_model(&io, &db.conn) {
                        Ok(live) => {
                            if !hist.resolve_live(live.clone()) {
                                violation(
                                    "uncertain-commit-left-neither-state",
                                    &format!("live rows={} last rows={}", live.len(), hist.last().rows),
                                );
                                describe_diff(&live, &hist);
                                hist.reset_from(live);
                            }
                            resolved = true;
                            break;
                        }
                        Err(_) => backoff(&io, 10),
                    }
                }
                if !resolved {
                    wl.bump("forced-reopen-unresolved-commit");
                    break;
                }
            }
            let mid = wl.rng.chance(cfg.mid_tx_crash_permille)
                && crash_at_tx.is_none()
                && t + 1 == cfg.txs_per_epoch;
            println!("note: e{epoch} t{t} tx start (rows={})", hist.model.len());
            let (outcome, did_crash) = run_tx(&db, &mut wl, &mut hist, &cfg, mid);
            if did_crash {
                crashed = true;
                crashes += 1;
                break;
            }
            if sentinel.crashed() {
                // An engine-internal crash point (or --fs-crash-at) fired
                // inside that transaction. If COMMIT had already returned Ok
                // the commit is on the observed path; if the cut landed inside
                // COMMIT (reported as an error) the transaction may or may not
                // be durable — a shadow candidate for recovery.
                wl.bump("crash-detected-in-engine");
                patina_dst::sometimes!(true, "engine-internal-power-cut-detected");
                match outcome {
                    TxOutcome::Committed(m) => {
                        hist.commit_attempt(m, true, &format!("e{epoch}t{t}"));
                    }
                    TxOutcome::Uncertain(applied) => {
                        hist.commit_attempt_uncertain(applied, &format!("e{epoch}t{t}!"));
                    }
                    _ => {}
                }
                crashed = true;
                crashes += 1;
                break;
            }
            match outcome {
                TxOutcome::Committed(m) => {
                    consecutive_failures = 0;
                    println!("note: e{epoch} t{t} committed rows={}", m.len());
                    hist.commit_attempt(m, true, &format!("e{epoch}t{t}"));
                    reader_saw_commit = true;
                    wl.bump("tx-committed");
                }
                TxOutcome::RolledBack => {
                    consecutive_failures = 0;
                    wl.bump("tx-rolled-back");
                }
                TxOutcome::Uncertain(applied) => {
                    wl.bump("tx-uncertain");
                    patina_dst::sometimes!(true, "commit-outcome-uncertain");
                    let pre_rows = hist.model.len();
                    hist.commit_attempt_uncertain(applied, &format!("e{epoch}t{t}?"));
                    // Resolve against the live view when we can.
                    match read_model(&io, &db.conn) {
                        Ok(live) => {
                            let applied_live = digest_model(&live) == hist.last().digest;
                            if !hist.resolve_live(live.clone()) {
                                violation(
                                    "uncertain-commit-left-neither-state",
                                    &format!(
                                        "live rows={} pre rows={pre_rows} post rows={}",
                                        live.len(),
                                        hist.last().rows
                                    ),
                                );
                                hist.reset_from(live);
                            } else {
                                patina_dst::sometimes!(applied_live, "uncertain-commit-was-applied");
                                patina_dst::sometimes!(!applied_live, "uncertain-commit-was-not-applied");
                            }
                        }
                        Err(e) => {
                            println!("note: live read after uncertain commit failed: {e:?}");
                            consecutive_failures += 1;
                        }
                    }
                }
                TxOutcome::Aborted(why) => {
                    wl.bump("tx-aborted");
                    consecutive_failures += 1;
                    println!("note: tx aborted: {why}");
                    if why.contains("Stuck") {
                        violation("statement-stuck", &why);
                        break 'epochs;
                    }
                    if why.contains("Panic") {
                        violation("panic-in-statement", &why);
                    }
                }
                TxOutcome::Busy => {
                    wl.bump("tx-busy");
                    patina_dst::sometimes!(true, "writer-saw-busy");
                    backoff(&io, 5);
                }
            }
            if consecutive_failures >= 6 {
                // The engine is not making progress (dead descriptors after a
                // crash, or persistent injected faults): reopen.
                wl.bump("forced-reopen");
                consecutive_failures = 0;
                break;
            }
            // Periodic model check outside any transaction.
            if wl.rng.chance(250) {
                match read_model(&io, &db.conn) {
                    Ok(live) => {
                        let (d, n) = (digest_model(&live), live.len());
                        if !hist.resolve_live(live.clone()) {
                            violation(
                                "committed-state-mismatch",
                                &format!(
                                    "live rows={n} digest={d:x} expected {:x} rows={}",
                                    hist.last().digest,
                                    hist.last().rows
                                ),
                            );
                            describe_diff(&live, &hist);
                        }
                    }
                    Err(e) => println!("note: periodic read failed: {e:?}"),
                }
            }
            if wl.rng.chance(cfg.checkpoint_permille) {
                checkpoint(&db, &mut wl, &mut hist);
                if sentinel.crashed() {
                    wl.bump("crash-detected-in-checkpoint");
                    patina_dst::sometimes!(true, "engine-internal-power-cut-in-checkpoint");
                    crashed = true;
                    crashes += 1;
                    break;
                }
            }
            if wl.rng.chance(cfg.integrity_permille) {
                match exec(&io, &db.conn, "PRAGMA integrity_check") {
                    Ok(rows) => {
                        let joined = rows
                            .iter()
                            .map(|r| r.join("|"))
                            .collect::<Vec<_>>()
                            .join("; ");
                        if joined != "ok" {
                            violation("integrity-check-failed", &joined);
                        }
                    }
                    Err(e) => println!("note: integrity_check failed to run: {e:?}"),
                }
            }
        }
        stop_bg(bg, &mut wl);
        // A power cut fired by the background thread (engine crash point in
        // its checkpoint) after the writer's last check: account for it before
        // deciding which states are acceptable.
        if !crashed && sentinel.crashed() {
            wl.bump("crash-detected-at-epoch-end");
            crashed = true;
            crashes += 1;
        }
        if let Some(w) = reader_window.take() {
            if !crashed {
                reader_check_and_end(&db, &w, reader_saw_commit);
            }
        }
        epochs_done += 1;

        // End of epoch: restart the "process".
        let old = db;
        if crashed {
            drop_db(old, true);
        } else {
            match wl.rng.range(0, 2) {
                0 => {
                    wl.bump("clean-close");
                    close_db(old);
                }
                1 => {
                    wl.bump("drop-without-close");
                    drop_db(old, false);
                }
                _ => {
                    wl.bump("crash-at-quiet-point");
                    let ok = power_cut();
                    patina_dst::sometimes!(ok, "guest-power-cut-quiet");
                    if ok {
                        crashed = true;
                        crashes += 1;
                    }
                    drop_db(old, ok);
                }
            }
        }
        let Some(new_db) = open_with_retry(&io, &cfg) else {
            break 'epochs;
        };
        db = new_db;
        if !verify_reopen(&db, &mut hist, crashed, dirsynced) {
            break 'epochs;
        }
        sentinel.arm(&io);
        patina_dst::sometimes!(crashed, "recovered-after-power-cut");
    }

    let deferred = io.state.deferred_ops.load(Ordering::Relaxed);
    let reordered = io.state.reordered_steps.load(Ordering::Relaxed);
    patina_dst::sometimes!(deferred > 0, "io-completions-deferred");
    patina_dst::sometimes!(reordered > 0, "io-completions-reordered");
    println!(
        "TURSO_DST_STATS epochs={epochs_done} crashes={crashes} deferred_ops={deferred} reordered_steps={reordered} {:?}",
        wl.stats
    );
    let violations = VIOLATIONS.load(Ordering::Relaxed);
    if violations > 0 {
        println!("TURSO_DST_VIOLATIONS count={violations}");
        std::process::exit(1);
    }
    let last = hist.last();
    let outcome = format!(
        "digest={:x} rows={} epochs={epochs_done} crashes={crashes}",
        last.digest, last.rows
    );
    patina_dst::verdict(VerdictKind::Pass, "turso-dst-outcome", &outcome);
    println!("TURSO_DST_OK {outcome}");
}
