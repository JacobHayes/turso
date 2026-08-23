#!/usr/bin/env bash
# Usage: ./run.sh [cargo patina run args...]   (e.g. ./run.sh --seed 3 --buggify)
set -uo pipefail
export PATH="$HOME/.cargo/bin:$HOME/src/github.com/JacobHayes/patina/target/release:$PATH"
export RUSTC_WRAPPER=""
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${TURSO_DST_BIN:-$here/target/x86_64-unknown-linux-gnu/debug/turso-patina-guest}"
# libloading (extension loading), set_permissions (backup), simsimd AVX-512 kernel: none reached.
ALLOW='dlopen,dlclose,dlerror,chmod,simsimd_fma_f64_skylake'
exec cargo patina run "$BIN" --allow-unsupported-symbols "$ALLOW" "$@"
