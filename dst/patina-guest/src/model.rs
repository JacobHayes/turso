//! The reference model and the durability/atomicity oracle.
//!
//! The model is the table `kv(id INTEGER PRIMARY KEY, n INTEGER, v TEXT)` as a
//! `BTreeMap`. The oracle keeps the OBSERVED PATH of committed states as a list
//! of digests plus the "durable floor", and decides which states a reopened
//! database may legally show:
//!
//! - without a crash: only the last observed state (the filesystem kept every
//!   write, crashed or not, so nothing may be lost) — plus the "shadow" state
//!   of the last commit attempt if that attempt's outcome was uncertain;
//! - after a crash (power loss: unsynced writes may be torn/lost): any state
//!   on the observed path at or after the durable floor, plus the shadow.
//!
//! The floor advances on every certain commit under `synchronous=FULL`, and on
//! every successful checkpoint under `NORMAL` (the checkpoint fsyncs the WAL
//! before backfilling, so everything committed before it is on stable storage).

use std::collections::BTreeMap;
use std::sync::Arc;

pub type Model = BTreeMap<i64, (i64, String)>;

pub fn digest_rows<'a>(rows: impl Iterator<Item = (i64, i64, &'a str)>) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut n = 0u64;
    for (id, k, v) in rows {
        for b in id
            .to_le_bytes()
            .iter()
            .chain(k.to_le_bytes().iter())
            .chain(v.as_bytes())
            .chain([0x1eu8].iter())
        {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        n += 1;
    }
    h ^ n.rotate_left(17)
}

pub fn digest_model(m: &Model) -> u64 {
    digest_rows(m.iter().map(|(id, (k, v))| (*id, *k, v.as_str())))
}

#[derive(Clone, Debug)]
pub struct Snap {
    pub digest: u64,
    pub certain: bool,
    pub what: String,
    pub rows: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    Full,
    Normal,
}

pub struct History {
    /// Observed path of states; `snaps[0]` is the state at (re)open.
    pub snaps: Vec<Snap>,
    /// Index into `snaps` at/after which a post-crash state must fall.
    pub floor: usize,
    /// The "other" candidate state of the most recent commit attempt when its
    /// outcome was uncertain (COMMIT reported an error, or an error landed after
    /// publish); cleared by the next commit attempt.
    pub shadow: Option<Snap>,
    pub sync_mode: SyncMode,
    /// Live model (what a SELECT on the writer connection must show when no
    /// transaction is open).
    pub model: Arc<Model>,
}

impl History {
    pub fn new(model: Model, sync_mode: SyncMode) -> Self {
        let digest = digest_model(&model);
        let rows = model.len();
        Self {
            snaps: vec![Snap {
                digest,
                certain: true,
                what: "open".into(),
                rows,
            }],
            floor: 0,
            shadow: None,
            sync_mode,
            model: Arc::new(model),
        }
    }

    pub fn last(&self) -> &Snap {
        self.snaps.last().expect("history never empty")
    }

    /// A commit attempt finished. `applied` is the model if the commit took
    /// effect; `observed_applied` says what the live connection shows now.
    pub fn commit_attempt(&mut self, applied: Model, certain: bool, what: &str) {
        let digest = digest_model(&applied);
        let rows = applied.len();
        self.snaps.push(Snap {
            digest,
            certain,
            what: what.to_string(),
            rows,
        });
        self.model = Arc::new(applied);
        self.shadow = None;
        if certain && self.sync_mode == SyncMode::Full {
            self.floor = self.snaps.len() - 1;
        }
    }

    /// A commit attempt whose outcome is unknown (COMMIT reported an error, or
    /// the power went out inside COMMIT): the observed path tentatively takes
    /// the applied state, and the pre-state stays as the shadow. Either may be
    /// what the engine shows next; `resolve_live` settles it.
    pub fn commit_attempt_uncertain(&mut self, applied: Model, what: &str) {
        let pre = Snap {
            digest: self.last().digest,
            certain: false,
            what: format!("{what}:pre"),
            rows: self.last().rows,
        };
        self.commit_attempt(applied, false, &format!("{what}:applied"));
        self.shadow = Some(pre);
    }

    /// The live connection (no transaction open) shows `observed`. Returns
    /// `true` when that is a legal state (the last state or the shadow); when
    /// it is the shadow the two candidates swap so the model tracks what the
    /// engine shows while the other stays reachable by a crash.
    pub fn resolve_live(&mut self, observed: Model) -> bool {
        let d = digest_model(&observed);
        if d == self.last().digest {
            return true;
        }
        let Some(shadow) = self.shadow.clone() else {
            return false;
        };
        if d != shadow.digest {
            return false;
        }
        let previous = self.last().clone();
        self.snaps.push(Snap {
            digest: d,
            certain: false,
            what: format!("{}:live", shadow.what),
            rows: observed.len(),
        });
        self.model = Arc::new(observed);
        self.shadow = Some(previous);
        true
    }

    /// A successful checkpoint under NORMAL makes everything committed so far
    /// durable.
    pub fn checkpoint_ok(&mut self) {
        if self.sync_mode == SyncMode::Normal {
            self.floor = self.snaps.len() - 1;
        }
    }

    /// Which states may a reopened database show?
    pub fn acceptable(&self, crashed: bool) -> Vec<Snap> {
        let mut out = Vec::new();
        if crashed {
            out.extend(self.snaps[self.floor..].iter().cloned());
        } else {
            out.push(self.last().clone());
        }
        if let Some(s) = &self.shadow {
            out.push(s.clone());
        }
        out
    }

    /// A reopen WITHOUT a crash showed `model` (verified to be the last state
    /// or the shadow): continue the observed path from it. The durable floor
    /// is kept — a plain restart (handles dropped, OS alive) makes nothing
    /// durable that was not already, so a later crash may still roll back to
    /// the floor.
    pub fn adopt(&mut self, model: Model) {
        let digest = digest_model(&model);
        let rows = model.len();
        if digest != self.last().digest {
            self.snaps.push(Snap {
                digest,
                certain: false,
                what: "reopen".into(),
                rows,
            });
        }
        self.shadow = None;
        self.model = Arc::new(model);
    }

    /// Restart the observed path from a verified post-CRASH state: the
    /// post-crash image is the new durable baseline.
    pub fn reset_from(&mut self, model: Model) {
        let digest = digest_model(&model);
        let rows = model.len();
        self.snaps = vec![Snap {
            digest,
            certain: true,
            what: "reopen".into(),
            rows,
        }];
        self.floor = 0;
        self.shadow = None;
        self.model = Arc::new(model);
    }
}
