//! Deterministic-simulation hooks (Patina DST).
//!
//! Every macro/function here is a no-op outside a `cargo patina` build
//! (`--cfg patina` / `--cfg patina_shim`), so production builds carry nothing.
//!
//! - [`dst_gap!`]: a seeded scheduling gap (`patina_dst::buggify_delay!`)
//!   placed between two effects whose ordering matters (write -> fsync,
//!   fsync -> publish), so other threads/tasks get to interleave there.
//! - [`dst_crash_point!`]: a seeded power cut (patina's filesystem crash model)
//!   at an engine-internal point a `--fs-crash-at` op counter cannot name
//!   semantically. Gated by `enable_crash_points` so the guest can keep them
//!   inert during its own setup.
//! - [`dst_sometimes!`]: a coverage oracle for an interesting branch.
//! - [`dst_buggify!`]: seeded rare-path activation (`patina_dst::buggify!`).
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(patina_shim)]
extern "C" {
    fn patina_crash() -> i32;
}

/// Engine-internal crash points stay inert until the driver enables them
/// (after its own setup), so a high buggify rate cannot starve a run of the
/// schema it needs before any transaction runs.
static CRASH_POINTS_ENABLED: AtomicBool = AtomicBool::new(false);
/// How many engine-internal power cuts fired (driver-visible).
pub static CRASH_POINTS_FIRED: AtomicU64 = AtomicU64::new(0);

pub fn enable_crash_points(enabled: bool) {
    CRASH_POINTS_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn crash_points_enabled() -> bool {
    CRASH_POINTS_ENABLED.load(Ordering::Relaxed)
}

/// Perform a modeled power cut now. No-op outside Patina.
pub fn power_cut() {
    #[cfg(patina_shim)]
    {
        CRASH_POINTS_FIRED.fetch_add(1, Ordering::Relaxed);
        // SAFETY: plain FFI into the shim; no pointers.
        let _ = unsafe { patina_crash() };
    }
}

#[macro_export]
macro_rules! dst_gap {
    ($label:literal) => {{
        #[cfg(patina)]
        {
            let _ = ::patina_dst::buggify_delay!($label);
        }
    }};
}

#[macro_export]
macro_rules! dst_crash_point {
    ($label:literal) => {{
        #[cfg(patina)]
        {
            if $crate::dst::crash_points_enabled() && ::patina_dst::buggify!($label) {
                $crate::dst::power_cut();
            }
        }
    }};
}

#[macro_export]
macro_rules! dst_sometimes {
    ($cond:expr, $label:literal) => {{
        #[cfg(patina)]
        {
            ::patina_dst::sometimes!($cond, $label);
        }
        #[cfg(not(patina))]
        {
            let _ = &$cond;
        }
    }};
}

/// Seeded rare-path activation (`patina_dst::buggify!`); `false` outside Patina.
#[macro_export]
macro_rules! dst_buggify {
    ($label:literal) => {{
        #[cfg(patina)]
        {
            ::patina_dst::buggify!($label)
        }
        #[cfg(not(patina))]
        {
            false
        }
    }};
}
