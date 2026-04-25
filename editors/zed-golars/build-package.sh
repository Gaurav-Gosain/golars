#!/usr/bin/env bash
# Build a self-contained, prebuilt golars Zed extension package.
#
# Output layout (mirrors what Zed expects under
# `<extensions>/installed/golars/`):
#
#   dist/golars-zed-extension/
#     extension.toml
#     extension.wasm
#     grammars/golars.wasm
#     languages/golars/{config.toml,*.scm}
#
# The release workflow tars this directory and uploads it so the
# curl-installable `install-zed-extension.sh` can drop it in place
# without needing a Rust + wasm toolchain on the user's machine.
#
# Required tools: cargo (with wasm32-wasip1 target), wasm-tools,
# tree-sitter CLI, curl, tar.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
GRAMMAR_DIR="$REPO_ROOT/editors/tree-sitter-golars"
DIST_DIR="$HERE/dist"
PKG_DIR="$DIST_DIR/golars-zed-extension"

# WASI snapshot-preview1 reactor adapter. Pinned to a known-good
# wasmtime release; bump alongside zed_extension_api upgrades.
ADAPTER_URL="https://github.com/bytecodealliance/wasmtime/releases/download/v23.0.2/wasi_snapshot_preview1.reactor.wasm"
ADAPTER="$DIST_DIR/wasi_snapshot_preview1.reactor.wasm"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[!!] missing required tool: $1" >&2
    exit 1
  }
}

need cargo
need wasm-tools
need tree-sitter
need curl
need tar

echo "[*] cleaning $DIST_DIR"
rm -rf "$DIST_DIR"
mkdir -p "$PKG_DIR/grammars" "$PKG_DIR/languages"

echo "[*] fetching WASI reactor adapter"
curl -fsSL "$ADAPTER_URL" -o "$ADAPTER"

echo "[*] adding wasm32-wasip1 target (idempotent)"
rustup target add wasm32-wasip1 >/dev/null

echo "[*] building Rust extension (wasm32-wasip1)"
(cd "$HERE" && cargo build --release --target wasm32-wasip1 --quiet)

echo "[*] composing component with WASI adapter"
wasm-tools component new \
  "$HERE/target/wasm32-wasip1/release/zed_golars.wasm" \
  --adapt "$ADAPTER" \
  -o "$PKG_DIR/extension.wasm"

echo "[*] generating + building tree-sitter grammar wasm"
(cd "$GRAMMAR_DIR" && tree-sitter generate >/dev/null && tree-sitter build --wasm -o "$PKG_DIR/grammars/golars.wasm")

echo "[*] copying language assets"
cp "$HERE/extension.toml" "$PKG_DIR/extension.toml"
cp -R "$HERE/languages/golars" "$PKG_DIR/languages/golars"

VERSION="${1:-$(awk -F'"' '/^version/ {print $2; exit}' "$HERE/extension.toml")}"
TARBALL="$DIST_DIR/golars-zed-extension_${VERSION}.tar.gz"

echo "[*] tarballing $TARBALL"
tar -czf "$TARBALL" -C "$DIST_DIR" "golars-zed-extension"

echo "[ok] built: $TARBALL"
echo "    contents:"
tar -tzf "$TARBALL" | sed 's/^/      /'
