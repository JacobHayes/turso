# Turso DST findings (Patina)

> **Status (2026-08-23, after native re-validation — read this first).**
> Only finding **#1 is a real turso bug**; it is fixed on branch
> `fix/commit-wal-io-error-rollback` (`core/vdbe/mod.rs` + an upstream-style
> red/green test in `tests/integration/wal/commit_wal_io_error_rollback.rs`).
> **#2 is not a bug**: a whole-transaction rollback on an I/O error inside
> `BEGIN … UPDATE` is exactly SQLite's documented `SQLITE_IOERR` behaviour
> (`sqlite3VdbeHalt` full-rolls-back and sets `autoCommit=1`); the oracle's
> expectation was wrong. **#3 was a harness bug**: the `open-short-read`
> scenario's "clean reopen" opened the global `DB_PATH` instead of its
> scenario-local file, so it compared against an unrelated empty database —
> turso's WAL was intact and a real reopen recovers every row. Both scenarios
> are fixed in `src/scenario.rs` and now pass. The write-ups below are kept
> verbatim as the investigation record; treat #2 and #3 as **retracted**.
> The fuzz campaign's `prefix-consistency-violated-after-crash` class (e.g.
> `./run.sh --seed 1`, `synchronous=NORMAL`, checkpoint-ok then crash) is
> still **unvalidated** outside the harness and should be treated as a
> hypothesis until reproduced natively.

Deterministic-simulation testing of `turso_core` under
[Patina](https://github.com/JacobHayes/patina). The harness (`dst/patina-guest/`)
drives the **real** engine — pager, WAL, B-tree, VDBE — on Patina's deterministic
filesystem through turso's own `UnixIO`, wrapped by `DetIo` (`src/detio.rs`) which
can defer/reorder completions and inject **exactly placed** one-shot I/O failures,
short reads, and power cuts. Every run is a pure function of its seed; a failure is
a seed and replays byte-for-byte.

The three findings below come from **focused scenarios** (`src/scenario.rs`) that
use only public `turso_core` APIs plus `DetIo` fault injection. They do **not**
depend on the `turso_assert!`→`patina_dst` macro routing or the `dst_crash_point!`
instrumentation added elsewhere in this branch (those drive the fuzz campaign, not
these repros), so they reflect unmodified engine behavior.

Reproduce any scenario (from `dst/patina-guest/`):

```sh
./run.sh --seed 1 --env TURSO_DST_SCENARIO=<name>
# add --env TURSO_DST_VERBOSE=1                       for a per-statement log
# add --env TURSO_DST_TRACE=turso_core::storage=debug for engine tracing
```

Recorded traces + HTML timelines for each finding are in `/tmp/turso-dst-evidence/`.

---

## 1. A COMMIT that fails on WAL I/O leaves the transaction committed-but-reported-failed  (confidence: high)

**Scenario:** `commit-sync-fail`, `commit-header-write-fail` (control:
`autocommit-header-write-fail`).

Under `PRAGMA synchronous=FULL; PRAGMA data_sync_retry=ON`, inside an explicit
`BEGIN … COMMIT`, if the WAL fsync (or the fresh-generation WAL-header `pwrite`)
fails, `COMMIT` returns an `I/O error` to the caller — but the row stays visible on
the same connection, a following `ROLLBACK` reports *"cannot rollback - no
transaction is active"*, and the next statement **silently commits** the write.

```
BEGIN                              -> ok
INSERT INTO kv VALUES(5,5,'row5')  -> ok            (page 2 dirtied, tx_state=Write)
COMMIT                             -> ERR I/O error (sync)      <-- app sees failure
SELECT count(*) FROM kv            -> 5              <-- but the row is there
ROLLBACK                           -> ERR "no transaction is active"
-- and every later reader/reopen sees 5
```

**Mechanism** (from `TURSO_DST_TRACE` of `commit-sync-fail`):

1. `op_auto_commit` (`core/vdbe/execute.rs`) sets `auto_commit = true` **before**
   `commit_txn` runs.
2. `commit_tx` → `commit_wal` → the WAL header fsync fails; `commit_wal_inner`
   returns `Err`, which propagates up out of `commit_tx`.
3. The error path does **not** roll back: no `rollback_tx`, no `clear_dirty`,
   `tx_state` stays `Write`, page 2 stays dirty in the cache.
4. The connection is now torn: `auto_commit == true` **and** an open `Write`
   transaction with dirty pages. The next statement's `Halt` (`commit_txn_wal`,
   `core/vdbe/mod.rs`) sees `auto_commit && tx_state==Write` and re-drives the
   commit — this time the fsync isn't failing — so the "failed" transaction is
   now durably committed.

The pure-autocommit path rolls back correctly (`autocommit-header-write-fail` →
4 rows), because there the failing INSERT's own `abort()` runs `rollback_current_txn`;
it is the *separate* `COMMIT` statement of an explicit transaction, which pre-flips
`auto_commit`, that leaves the torn state.

**Why it matters:** an application that gets an error from `COMMIT` will (correctly)
believe the transaction did not commit — retrying, surfacing the error, or aborting a
larger operation — while turso commits the data anyway on the next unrelated
statement. It is an atomicity + durability-reporting violation, only on the
`data_sync_retry=ON` path (OFF panics by design). `pwritev`-failures (the
`WaitWrites` state) roll back correctly; the gap is the WAL-header/`prepare_wal`
sync path.

Evidence: `/tmp/turso-dst-evidence/commit-sync-fail.patina`,
`commit-header-write-fail.patina` (+ `.html`). Deterministic: 5 verdict violations
every run; `cargo patina replay` reproduces.

---

## 2. [RETRACTED — SQLite-compatible behaviour, see Status] An I/O error mid-statement tears the connection's transaction state  (original confidence: high)

**Scenario:** `savepoint-read-fail`.

Inside `BEGIN; SAVEPOINT sp0;`, a write statement whose page **read** fails leaves
the transaction in an inconsistent state: the *next* statement commits standalone
and a later `ROLLBACK` finds no transaction.

```
BEGIN                                      -> ok
SAVEPOINT sp0                              -> ok
UPDATE kv SET n=n+1 WHERE id BETWEEN 1 AND 4  -> ERR I/O error (pread)
INSERT INTO kv VALUES(5,5,'row5')          -> ok    <-- committed outside the intended tx
ROLLBACK                                   -> ERR "no transaction is active"
SELECT count(*)                            -> 5     (expected 4 after ROLLBACK)
```

A transient read error while executing a statement in an explicit transaction should
either keep the transaction open (so `ROLLBACK` discards everything) or roll it back —
not silently end it and let subsequent statements auto-commit. This is the read-path
sibling of #1: `abort()`'s handling of a generic `CompletionError` in an explicit
transaction leaves `auto_commit`/`tx_state` inconsistent.

Evidence: `/tmp/turso-dst-evidence/savepoint-read-fail.patina` (+ `.html`).

---

## 3. [RETRACTED — harness reopen-path bug, see Status] A transient short/failed read during open discards the WAL  (original confidence: medium-high)

**Scenario:** `open-short-read`.

Commit 8 rows under `synchronous=FULL` (durable WAL frames; no checkpoint, so the
data lives only in the `-wal`), close cleanly, then reopen while the k-th `pread`
returns short (16 bytes — a legal POSIX outcome real storage can produce). No power
loss is involved.

- `k=0,1` (short read of the DB header / page 1): open **errors** — safe.
- `k=2,3` (short read during the WAL scan / schema load): open **"succeeds" empty**
  (`no such table: kv`), and the loss is **permanent** — a subsequent clean reopen
  with faults disarmed still shows no table.

**Mechanism:** WAL recovery treats a failed/short read as end-of-log. In
`core/storage/sqlite3_ondisk.rs`, both `handle_header_read` and `handle_chunk_read`
do `let Ok(..) = res else { self.finalize_loading(); return; }`, and
`AwaitHeader`/`AwaitChunk` only re-yield on `!succeeded()` — so an I/O error mid-scan
finalizes recovery at however many frames were read so far (zero, on a header short
read) instead of failing loudly. With the committed data resident only in the WAL,
truncating recovery drops it; the empty view is then persisted, so a clean retry
cannot get it back.

A transient read hiccup is not the same as a corrupt WAL. Real SQLite retries/erros
rather than silently discarding committed transactions. This is the highest-severity
class found (silent data loss under FULL, **no crash required**), and it matches the
recovery error-handling the harness flags with the `wal-recovery-cut-short-by-read-error`
`sometimes!` oracle. The *exact* reason the truncated state becomes permanent for the
`k=2,3` sub-case (WAL discard vs. partial checkpoint of the empty view) is worth a
turso-side trace; the harness confirms the loss but not yet the precise persisting write.

Evidence: `/tmp/turso-dst-evidence/open-short-read.patina` (+ `.html`).

---

## Also observed (lower priority)

- **Parent directory is never fsynced** after creating the `.db`/`-wal`. Under
  Patina's crash model an un-synced namespace op is lost on power cut (as on a real
  filesystem). SQLite fsyncs the parent for journal/WAL files; turso does not. The
  harness works around it by fsyncing the dir itself
  (`--env TURSO_DST_NO_DIRSYNC=1` re-exposes the gap).
- **Short-read `turso_assert!` abort:** under `--fs-short-permille`, a short WAL read
  during checkpoint trips `turso_assert!(read == buf_len, "read bytes does not match
  expected buffer length")` at `core/storage/wal.rs` (`issue_wal_read_into_buffer`),
  aborting the process. A short read is a legal I/O outcome and should surface as a
  `ShortRead` error (as `begin_read_btree_page` already does), not an assertion.

## Fuzz campaign

`cargo patina campaign <frozen-guest> --gens 150 --buggify --faults --sched-pct --swarm`
over the full randomized workload (multi-connection, all checkpoint modes, seeded
power cuts + reopen/recovery, background reader/checkpointer thread) reproduces the
same classes at scale — `committed-state-lost-on-reopen`, `durability-violated-after-crash`,
`schema-lost-after-reopen`, `read-your-writes-violated`, `uncertain-commit-left-neither-state`
— i.e. the campaign's random exploration keeps landing on the torn-transaction and
recovery-discard behaviors that the focused scenarios above isolate.
