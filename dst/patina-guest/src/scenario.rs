//! Focused, deterministic fault scenarios (`TURSO_DST_SCENARIO=<name>`): one
//! exactly-placed I/O failure through `DetIo::arm_fail`, then the sequence of
//! observations that pins the engine's behavior. Every expectation breach is a
//! verdict; the printed `scenario:` lines are the human-readable protocol.

use std::sync::Arc;

use crate::detio::{DetIo, OpKind};
use crate::model::SyncMode;
use crate::{drop_db, exec, open, violation, Config, Db};
use turso_core::{CheckpointMode, IO};

fn count(db: &Db) -> String {
    match exec(&db.io, &db.conn, "SELECT count(*), coalesce(sum(n),0) FROM kv") {
        Ok(rows) => format!("ok {:?}", rows[0]),
        Err(e) => format!("ERR {e:?}"),
    }
}

fn sql(db: &Db, s: &str) -> String {
    match exec(&db.io, &db.conn, s) {
        Ok(rows) => format!("ok rows={}", rows.len()),
        Err(e) => format!("ERR {e:?}"),
    }
}

fn step(name: &str, what: &str, got: &str, expect: &str) {
    let ok = got.starts_with(expect);
    println!("scenario[{name}]: {what} -> {got}   (expect {expect}){}", if ok { "" } else { "  <-- MISMATCH" });
    if !ok {
        violation(&format!("scenario-{name}"), &format!("{what}: got {got}, expected {expect}"));
    }
}

fn setup(io: &Arc<DetIo>, cfg: &Config, rows: i64, cache_pages: i64) -> Db {
    std::fs::create_dir_all(crate::ROOT).expect("mkdir");
    let db = open(io, cfg).expect("open");
    exec(io, &db.conn, "PRAGMA synchronous=FULL").unwrap();
    exec(io, &db.conn, &format!("PRAGMA cache_size={cache_pages}")).unwrap();
    exec(io, &db.conn, "CREATE TABLE kv(id INTEGER PRIMARY KEY, n INTEGER, v TEXT)").unwrap();
    for i in 1..=rows {
        exec(io, &db.conn, &format!("INSERT INTO kv VALUES({i}, {i}, 'row{i}')")).unwrap();
    }
    db.conn
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .expect("setup checkpoint");
    let _ = std::fs::File::open(crate::ROOT).and_then(|d| d.sync_all());
    db
}

fn reopen(io: &Arc<DetIo>, cfg: &Config, db: Db) -> Db {
    drop_db(db, false);
    open(io, cfg).expect("reopen")
}

pub fn run(name: &str, io: Arc<DetIo>, cfg: &Config) {
    let cfg = Config {
        sync_mode: SyncMode::Full,
        cache_pages: 2000,
        autockpt: 0,
        ..*cfg
    };
    match name {
        // COMMIT whose WAL frame write fails: the transaction must not be
        // visible afterwards (on the same connection, after ROLLBACK, in a new
        // transaction, and after reopen).
        "commit-write-fail" | "commit-write-fail-warm" | "commit-sync-fail" | "commit-sync-fail-2"
        | "commit-header-write-fail" | "autocommit-header-write-fail" => {
            let mut db = setup(&io, &cfg, 4, 2000);
            let autocommit = name == "autocommit-header-write-fail";
            if !autocommit {
                step(name, "BEGIN", &sql(&db, "BEGIN"), "ok");
                step(name, "INSERT 5", &sql(&db, "INSERT INTO kv VALUES(5, 5, 'row5')"), "ok");
            }
            if name == "commit-write-fail-warm" {
                // Warm page 1 so COMMIT's first step reaches pwritev without an
                // intervening IO yield.
                step(name, "count (warm)", &count(&db), "ok [\"5\"");
            }
            match name {
                "commit-write-fail" | "commit-write-fail-warm" => io.arm_fail(OpKind::Pwritev, 0),
                // After a TRUNCATE checkpoint the first commit fsyncs the new
                // WAL header; the SECOND sync is the commit fsync proper.
                "commit-sync-fail" => io.arm_fail(OpKind::Sync, 0),
                "commit-sync-fail-2" => io.arm_fail(OpKind::Sync, 1),
                // After a TRUNCATE checkpoint the next commit first rewrites
                // the WAL header with a plain pwrite.
                _ => io.arm_fail(OpKind::Pwrite, 0),
            }
            let r = if autocommit {
                sql(&db, "INSERT INTO kv VALUES(5, 5, 'row5')")
            } else {
                sql(&db, "COMMIT")
            };
            println!("scenario[{name}]: COMMIT -> {r}");
            println!("scenario[{name}]: auto_commit after COMMIT = {}", db.conn.get_auto_commit());
            let injected = io.state.injected_failures.load(std::sync::atomic::Ordering::Relaxed);
            println!("scenario[{name}]: injected failures so far = {injected}");
            if r.starts_with("ok") {
                // Commit succeeded (the armed failure did not hit the commit):
                // then 5 rows is the truth from here on.
                step(name, "count after ok COMMIT", &count(&db), "ok [\"5\"");
                return finish(name);
            }
            step(name, "count right after failed COMMIT (same conn)", &count(&db), "ok [\"4\"");
            println!("scenario[{name}]: ROLLBACK -> {}", sql(&db, "ROLLBACK"));
            step(name, "count after ROLLBACK", &count(&db), "ok [\"4\"");
            step(name, "BEGIN (new tx)", &sql(&db, "BEGIN"), "ok");
            step(name, "count in new tx", &count(&db), "ok [\"4\"");
            step(name, "COMMIT (new tx)", &sql(&db, "COMMIT"), "ok");
            step(name, "INSERT 6 autocommit", &sql(&db, "INSERT INTO kv VALUES(6, 6, 'row6')"), "ok");
            step(name, "count after INSERT 6", &count(&db), "ok [\"5\"");
            step(name, "integrity_check", &sql(&db, "PRAGMA integrity_check"), "ok rows=1");
            db = reopen(&io, &cfg, db);
            step(name, "count after reopen", &count(&db), "ok [\"5\"");
            drop_db(db, false);
        }
        // A single failed page read outside any transaction, then a read in a
        // fresh transaction: must see the committed 4 rows.
        "read-fail-then-count" | "rollback-then-read-fail" => {
            let mut db = setup(&io, &cfg, 4, if name == "rollback-then-read-fail" { 2 } else { 2000 });
            if name == "rollback-then-read-fail" {
                step(name, "BEGIN", &sql(&db, "BEGIN"), "ok");
                for i in 10..30 {
                    let big = "x".repeat(3000);
                    let r = sql(&db, &format!("INSERT INTO kv VALUES({i}, {i}, '{big}')"));
                    if !r.starts_with("ok") {
                        println!("scenario[{name}]: insert {i} -> {r}");
                    }
                }
                step(name, "count in tx", &count(&db), "ok [\"24\"");
                step(name, "ROLLBACK", &sql(&db, "ROLLBACK"), "ok");
            }
            io.arm_fail(OpKind::Pread, 0);
            let r = sql(&db, "SELECT id, n, v FROM kv ORDER BY id");
            println!("scenario[{name}]: SELECT with failing pread -> {r}");
            step(name, "count after failed read", &count(&db), "ok [\"4\"");
            step(name, "BEGIN", &sql(&db, "BEGIN"), "ok");
            step(name, "count in new tx", &count(&db), "ok [\"4\"");
            step(name, "COMMIT", &sql(&db, "COMMIT"), "ok");
            step(name, "integrity_check", &sql(&db, "PRAGMA integrity_check"), "ok rows=1");
            db = reopen(&io, &cfg, db);
            step(name, "count after reopen", &count(&db), "ok [\"4\"");
            drop_db(db, false);
        }
        // WAL recovery under a failing read: commit rows 5..8 (FULL, so they
        // are durable WAL frames), reopen while the k-th pread fails, and see
        // what recovery yields; then reopen again cleanly. Committed rows must
        // never be lost permanently.
        "recovery-read-fail" => {
            let mut db = setup(&io, &cfg, 4, 2000);
            for i in 5..=8 {
                step(name, &format!("INSERT {i}"), &sql(&db, &format!("INSERT INTO kv VALUES({i}, {i}, 'row{i}')")), "ok");
            }
            step(name, "count before", &count(&db), "ok [\"8\"");
            for k in 0..6u32 {
                drop_db(db, false);
                io.arm_fail(OpKind::Pread, k);
                let opened = open(&io, &cfg);
                let Ok(reopened) = opened else {
                    println!("scenario[{name}]: reopen with pread#{k} failing -> open ERR {:?}", opened.err());
                    // Disarm any leftover and reopen cleanly for the next round.
                    io.state.clear_armed();
                    db = open(&io, &cfg).expect("clean reopen");
                    step(name, &format!("count after failed open (k={k}) + clean reopen"), &count(&db), "ok [\"8\"");
                    continue;
                };
                db = reopened;
                let c = count(&db);
                println!("scenario[{name}]: reopen with pread#{k} failing -> open ok, count {c}");
                io.state.clear_armed();
                // Write something so a truncated recovery would get persisted.
                let w = sql(&db, &format!("INSERT INTO kv VALUES({}, 1, 'after')", 100 + k));
                println!("scenario[{name}]: insert after reopen -> {w}");
                db = reopen(&io, &cfg, db);
                let c2 = count(&db);
                step(name, &format!("count after clean reopen (k={k})"), &c2, &format!("ok [\"{}\"", 8 + (k + 1)));
            }
            drop_db(db, false);
        }
        // ROLLBACK while spilled-page WAL writes are still in flight (all I/O
        // deferred; cache_size=2 forces spills). `-drained` is the control:
        // every pending write is completed before ROLLBACK.
        "rollback-inflight-spill" | "rollback-inflight-spill-drained" => {
            // Setup with synchronous I/O, then switch the wrapper to
            // defer-everything for the transaction under test.
            let mut db = setup(&io, &cfg, 8, 2);
            step(name, "count before", &count(&db), "ok [\"8\"");
            io.state.set_defer_permille(1000);
            step(name, "BEGIN", &sql(&db, "BEGIN"), "ok");
            for i in 20..44 {
                let big = "y".repeat(2500);
                let r = sql(&db, &format!("INSERT INTO kv VALUES({i}, {i}, '{big}')"));
                if !r.starts_with("ok") {
                    println!("scenario[{name}]: insert {i} -> {r}");
                }
            }
            step(name, "count in tx", &count(&db), "ok [\"32\"");
            println!("scenario[{name}]: pending deferred I/O before ROLLBACK = {}", io.pending_len());
            if name.ends_with("-drained") {
                while io.pending_len() > 0 {
                    let _ = io.step();
                }
            }
            step(name, "ROLLBACK", &sql(&db, "ROLLBACK"), "ok");
            println!("scenario[{name}]: pending deferred I/O after ROLLBACK = {}", io.pending_len());
            io.state.set_defer_permille(0);
            while io.pending_len() > 0 {
                let _ = io.step();
            }
            step(name, "count after ROLLBACK", &count(&db), "ok [\"8\"");
            step(name, "BEGIN (new tx)", &sql(&db, "BEGIN"), "ok");
            step(name, "count in new tx", &count(&db), "ok [\"8\"");
            step(name, "INSERT 50 in new tx", &sql(&db, "INSERT INTO kv VALUES(50, 50, 'row50')"), "ok");
            step(name, "count in new tx after insert", &count(&db), "ok [\"9\"");
            step(name, "COMMIT (new tx)", &sql(&db, "COMMIT"), "ok");
            step(name, "count after COMMIT", &count(&db), "ok [\"9\"");
            step(name, "integrity_check", &sql(&db, "PRAGMA integrity_check"), "ok rows=1");
            db = reopen(&io, &cfg, db);
            step(name, "count after reopen", &count(&db), "ok [\"9\"");
            drop_db(db, false);
        }
        // A TRUNCATE/RESTART checkpoint that fails part-way (the k-th pread or
        // sync inside it), then one more commit, then a power cut. Under
        // synchronous=FULL every committed row must survive.
        "checkpoint-fail-then-crash" => {
            let kinds = [(OpKind::Pread, "pread"), (OpKind::Sync, "sync"), (OpKind::Pwrite, "pwrite")];
            // `TURSO_DST_SCENARIO_ONLY=sync:1` narrows the sweep to one placement.
            let only = std::env::var("TURSO_DST_SCENARIO_ONLY").ok();
            for (kind, kname) in kinds {
                for k in 0..6u32 {
                    if let Some(o) = &only {
                        if *o != format!("{kname}:{k}") {
                            continue;
                        }
                    }
                    // Fresh database per round (own directory so files never clash).
                    let root = format!("{}/ck-{kname}-{k}", crate::ROOT);
                    std::fs::create_dir_all(&root).expect("mkdir");
                    let path = format!("{root}/main.db");
                    let open_at = |io: &Arc<DetIo>| -> Db {
                        let io_dyn: Arc<dyn IO> = io.clone();
                        let dbh = turso_core::Database::open_file_with_flags(
                            io_dyn, &path, turso_core::OpenFlags::default(), turso_core::DatabaseOpts::new(), None,
                            Arc::new(turso_core::SqliteDialect)).expect("open");
                        let conn = dbh.connect().expect("connect");
                        let reader = dbh.connect().expect("connect");
                        exec(io, &conn, "PRAGMA synchronous=FULL").unwrap();
                        exec(io, &conn, "PRAGMA data_sync_retry=ON").unwrap();
                        Db { io: io.clone(), db: dbh, conn, reader }
                    };
                    let db = open_at(&io);
                    exec(&io, &db.conn, "CREATE TABLE kv(id INTEGER PRIMARY KEY, n INTEGER, v TEXT)").unwrap();
                    for i in 1..=8 {
                        exec(&io, &db.conn, &format!("INSERT INTO kv VALUES({i}, {i}, 'row{i}')")).unwrap();
                    }
                    let _ = std::fs::File::open(&root).and_then(|d| d.sync_all());
                    io.arm_fail(kind, k);
                    let ck = db.conn.checkpoint(CheckpointMode::Truncate { upper_bound_inclusive: None });
                    let fired = io.state.injected_failures.load(std::sync::atomic::Ordering::Relaxed);
                    io.state.clear_armed();
                    let ck_s = match &ck { Ok(r) => format!("ok backfilled={}", r.wal_checkpoint_backfilled), Err(e) => format!("ERR {e}") };
                    let ins = sql(&db, "INSERT INTO kv VALUES(9, 9, 'row9')");
                    let c_live = count(&db);
                    let cut = crate::power_cut();
                    drop_db(db, true);
                    let db = open_at(&io);
                    let c_after = count(&db);
                    let ok_after = c_after.starts_with("ok [\"8\"") || c_after.starts_with("ok [\"9\"");
                    println!(
                        "scenario[{name}]: {kname}#{k}: checkpoint -> {ck_s}; INSERT 9 -> {ins}; live {c_live}; power_cut={cut}; after reopen {c_after}{}",
                        if ok_after { "" } else { "   <-- DATA LOSS" }
                    );
                    if !ok_after {
                        violation(&format!("scenario-{name}"), &format!("{kname}#{k}: checkpoint {ck_s}; after crash+reopen {c_after}"));
                    }
                    let _ = fired;
                    drop_db(db, false);
                }
            }
        }
        // Power cut right after the k-th op of a kind INSIDE a TRUNCATE
        // checkpoint (every commit before it was synchronous=FULL): after
        // recovery all 16 rows must be there.
        "ckpt-crash-sweep" => {
            let kinds = [(OpKind::Pread, "pread"), (OpKind::Pwrite, "pwrite"), (OpKind::Pwritev, "pwritev"), (OpKind::Sync, "sync"), (OpKind::Truncate, "truncate")];
            let only = std::env::var("TURSO_DST_SCENARIO_ONLY").ok();
            for (kind, kname) in kinds {
                for k in 0..6u32 {
                    if let Some(o) = &only {
                        if *o != format!("{kname}:{k}") {
                            continue;
                        }
                    }
                    let root = format!("{}/cc-{kname}-{k}", crate::ROOT);
                    std::fs::create_dir_all(&root).expect("mkdir");
                    let path = format!("{root}/main.db");
                    let open_at = |io: &Arc<DetIo>| -> Db {
                        let io_dyn: Arc<dyn IO> = io.clone();
                        let dbh = turso_core::Database::open_file_with_flags(
                            io_dyn, &path, turso_core::OpenFlags::default(), turso_core::DatabaseOpts::new(), None,
                            Arc::new(turso_core::SqliteDialect)).expect("open");
                        let conn = dbh.connect().expect("connect");
                        let reader = dbh.connect().expect("connect");
                        exec(io, &conn, "PRAGMA synchronous=FULL").unwrap();
                        exec(io, &conn, "PRAGMA data_sync_retry=ON").unwrap();
                        Db { io: io.clone(), db: dbh, conn, reader }
                    };
                    let db = open_at(&io);
                    exec(&io, &db.conn, "CREATE TABLE kv(id INTEGER PRIMARY KEY, n INTEGER, v TEXT)").unwrap();
                    for i in 1..=16 {
                        exec(&io, &db.conn, &format!("INSERT INTO kv VALUES({i}, {i}, 'row{i}')")).unwrap();
                    }
                    let _ = std::fs::File::open(&root).and_then(|d| d.sync_all());
                    io.arm_crash(kind, k);
                    let ck = db.conn.checkpoint(CheckpointMode::Truncate { upper_bound_inclusive: None });
                    let cut = io.state.armed_crash_len() == 0;
                    io.state.clear_armed();
                    let ck_s = match &ck { Ok(r) => format!("ok backfilled={}", r.wal_checkpoint_backfilled), Err(e) => format!("ERR {e}") };
                    drop_db(db, cut);
                    let db = open_at(&io);
                    let c_after = count(&db);
                    let ok_after = c_after.starts_with("ok [\"16\"");
                    println!(
                        "scenario[{name}]: crash after {kname}#{k}: cut={cut} checkpoint -> {ck_s}; after reopen {c_after}{}",
                        if ok_after { "" } else { "   <-- DATA LOSS" }
                    );
                    if cut && !ok_after {
                        violation(&format!("scenario-{name}"), &format!("crash after {kname}#{k} in TRUNCATE checkpoint: after reopen {c_after}"));
                    }
                    drop_db(db, false);
                }
            }
        }
        // Inside BEGIN + SAVEPOINT, a write whose page read fails: must be an
        // error, never an assertion abort; ROLLBACK restores 4 rows.
        "savepoint-read-fail" => {
            for k in 0..6u32 {
                let root = format!("{}/sp-{k}", crate::ROOT);
                std::fs::create_dir_all(&root).expect("mkdir");
                let path = format!("{root}/main.db");
                let io_dyn: Arc<dyn IO> = io.clone();
                let dbh = turso_core::Database::open_file_with_flags(
                    io_dyn, &path, turso_core::OpenFlags::default(), turso_core::DatabaseOpts::new(), None,
                    Arc::new(turso_core::SqliteDialect)).expect("open");
                let conn = dbh.connect().expect("connect");
                let reader = dbh.connect().expect("connect");
                let db = Db { io: io.clone(), db: dbh, conn, reader };
                exec(&io, &db.conn, "PRAGMA synchronous=FULL").unwrap();
                exec(&io, &db.conn, "PRAGMA cache_size=2").unwrap();
                exec(&io, &db.conn, "CREATE TABLE kv(id INTEGER PRIMARY KEY, n INTEGER, v TEXT)").unwrap();
                for i in 1..=4 {
                    let big = "z".repeat(3000);
                    exec(&io, &db.conn, &format!("INSERT INTO kv VALUES({i}, {i}, '{big}')")).unwrap();
                }
                let _ = db.conn.checkpoint(CheckpointMode::Truncate { upper_bound_inclusive: None });
                step(name, &format!("k={k} BEGIN"), &sql(&db, "BEGIN"), "ok");
                step(name, &format!("k={k} SAVEPOINT"), &sql(&db, "SAVEPOINT sp0"), "ok");
                io.arm_fail(OpKind::Pread, k);
                let r = sql(&db, "UPDATE kv SET n = n + 1 WHERE id BETWEEN 1 AND 4");
                let r2 = sql(&db, "INSERT INTO kv VALUES(5, 5, 'row5')");
                io.state.clear_armed();
                println!("scenario[{name}]: k={k} UPDATE -> {r}; INSERT -> {r2}");
                let ac_after_update = db.conn.get_auto_commit();
                let rb = sql(&db, "ROLLBACK");
                println!("scenario[{name}]: k={k} UPDATE_err={} autocommit_after_update={ac_after_update} ROLLBACK -> {rb}", r.starts_with("ERR"));
                // SQLite-COMPATIBLE behavior (verified against SQLite's docs and
                // sqlite3VdbeHalt): an I/O error (SQLITE_IOERR) during a write
                // statement with no statement journal rolls back the WHOLE
                // transaction and returns the connection to autocommit. So when
                // the UPDATE hits the injected read error the transaction is
                // gone: the UPDATE is undone, the following INSERT commits
                // standalone (5 rows), and ROLLBACK reports "no transaction is
                // active". This is NOT a bug. If the fault did not hit the
                // UPDATE, the whole transaction (incl. the INSERT) rolls back
                // normally (4 rows).
                let expected = if r.starts_with("ERR") { "ok [\"5\"" } else { "ok [\"4\"" };
                step(name, &format!("k={k} count after ROLLBACK"), &count(&db), expected);
                drop_db(db, false);
            }
        }
        // A SHORT read (16 of the requested bytes) of the k-th pread during
        // open/recovery. Committed rows 1..8 (FULL) must survive: either the
        // open fails, or it recovers everything — never "opens empty".
        "open-short-read" => {
            for k in 0..5u32 {
                let root = format!("{}/osr-{k}", crate::ROOT);
                std::fs::create_dir_all(&root).expect("mkdir");
                let path = format!("{root}/main.db");
                let open_at = |io: &Arc<DetIo>| -> Result<Db, String> {
                    let io_dyn: Arc<dyn IO> = io.clone();
                    let dbh = turso_core::Database::open_file_with_flags(
                        io_dyn, &path, turso_core::OpenFlags::default(), turso_core::DatabaseOpts::new(), None,
                        Arc::new(turso_core::SqliteDialect)).map_err(|e| format!("{e}"))?;
                    let conn = dbh.connect().map_err(|e| format!("{e}"))?;
                    let reader = dbh.connect().map_err(|e| format!("{e}"))?;
                    exec(io, &conn, "PRAGMA synchronous=FULL").map_err(|e| format!("{e:?}"))?;
                    Ok(Db { io: io.clone(), db: dbh, conn, reader })
                };
                let db = open_at(&io).expect("open");
                exec(&io, &db.conn, "CREATE TABLE kv(id INTEGER PRIMARY KEY, n INTEGER, v TEXT)").unwrap();
                for i in 1..=8 {
                    exec(&io, &db.conn, &format!("INSERT INTO kv VALUES({i}, {i}, 'row{i}')")).unwrap();
                }
                let _ = std::fs::File::open(&root).and_then(|d| d.sync_all());
                drop_db(db, false);
                io.arm_short_read(k, 16);
                let opened = open_at(&io);
                io.state.clear_armed();
                match opened {
                    Err(e) => {
                        println!("scenario[{name}]: short pread#{k} at open -> open ERR {e}");
                        let db = open_at(&io).expect("clean reopen");
                        step(name, &format!("k={k} count after clean reopen"), &count(&db), "ok [\"8\"");
                        drop_db(db, false);
                    }
                    Ok(db) => {
                        let c = count(&db);
                        println!("scenario[{name}]: short pread#{k} at open -> open ok, count {c}");
                        let w = sql(&db, &format!("INSERT INTO kv VALUES({}, 1, 'after')", 100 + k));
                        println!("scenario[{name}]: insert after open -> {w}");
                        // Reopen the SCENARIO-LOCAL path, not the global DB_PATH.
                        drop_db(db, false);
                        let db = open_at(&io).expect("clean reopen");
                        let c2 = count(&db);
                        let okc = c2.starts_with("ok [\"9\"") || (c2.starts_with("ok [\"8\"") && !w.starts_with("ok"));
                        println!("scenario[{name}]: k={k} count after clean reopen -> {c2}{}", if okc { "" } else { "   <-- DATA LOSS" });
                        if !okc {
                            violation(&format!("scenario-{name}"), &format!("short pread#{k} at open: opened with {c}; after insert+reopen {c2}"));
                        }
                        drop_db(db, false);
                    }
                }
            }
        }
        other => {
            println!("scenario: unknown scenario {other}");
            std::process::exit(2);
        }
    }
    finish(name)
}

fn finish(name: &str) {
    let v = crate::VIOLATIONS.load(std::sync::atomic::Ordering::Relaxed);
    if v > 0 {
        println!("TURSO_DST_SCENARIO {name} FAILED violations={v}");
        std::process::exit(1);
    }
    patina_dst::verdict(patina_dst::VerdictKind::Pass, "scenario", name);
    println!("TURSO_DST_SCENARIO {name} OK");
}
