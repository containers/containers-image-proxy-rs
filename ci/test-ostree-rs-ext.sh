#!/bin/bash
set -xeuo pipefail
cd ostree-rs-ext
./ci/installdeps.sh
#cat >> Cargo.toml <<'EOF'
#[patch.crates-io]
#containers-image-proxy = { path = ".." }
#EOF
cargo test
