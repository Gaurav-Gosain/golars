#!/usr/bin/env bash
# golars Zed extension installer.
#
# Downloads the prebuilt Zed extension package (extension.wasm +
# tree-sitter grammar wasm + language assets) from the latest GitHub
# release and drops it into Zed's installed-extensions directory.
# If `golars-lsp` isn't on PATH, also fetches the matching binary
# from the same release so the LSP just works.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install-zed-extension.sh | bash
#   curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install-zed-extension.sh | bash -s -- v0.1.3
#
# Honours $GOLARS_INSTALL_DIR for the LSP binary location.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

REPO="Gaurav-Gosain/golars"
EXT_ID="golars"
LSP_BIN="golars-lsp"

info()    { echo -e "${BLUE}[*]${NC} $1"; }
success() { echo -e "${GREEN}[ok]${NC} $1"; }
err()     { echo -e "${RED}[!!]${NC} $1" >&2; }
warn()    { echo -e "${YELLOW}[..]${NC} $1"; }

need_dl() {
  if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
    err "need curl or wget"
    exit 1
  fi
}

download_file() {
  # $1 url, $2 dest
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  else
    wget -q -O "$2" "$1"
  fi
}

detect_os() {
  case "$(uname -s)" in
    Linux*)         echo "Linux" ;;
    Darwin*)        echo "Darwin" ;;
    CYGWIN*|MINGW*|MSYS*) echo "Windows" ;;
    *) err "unsupported OS: $(uname -s)"; exit 1 ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)   echo "x86_64" ;;
    arm64|aarch64)  echo "arm64" ;;
    *) err "unsupported arch: $(uname -m)"; exit 1 ;;
  esac
}

# Where Zed loads installed extensions from. Layout:
#   <extensions>/installed/<id>/{extension.toml,extension.wasm,grammars/,languages/}
#   <extensions>/index.json
zed_extensions_dir() {
  case "$(uname -s)" in
    Darwin)
      echo "$HOME/Library/Application Support/Zed/extensions"
      ;;
    Linux)
      if [ -n "${XDG_DATA_HOME:-}" ]; then
        echo "$XDG_DATA_HOME/zed/extensions"
      else
        echo "$HOME/.local/share/zed/extensions"
      fi
      ;;
    CYGWIN*|MINGW*|MSYS*)
      if [ -n "${LOCALAPPDATA:-}" ]; then
        echo "$LOCALAPPDATA/Zed/extensions"
      else
        echo "$HOME/AppData/Local/Zed/extensions"
      fi
      ;;
    *) err "unsupported OS: $(uname -s)"; exit 1 ;;
  esac
}

latest_tag() {
  if [ -n "${1:-}" ]; then
    echo "$1"; return
  fi
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep -E '"tag_name":' \
      | head -1 \
      | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/'
  else
    wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep -E '"tag_name":' \
      | head -1 \
      | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/'
  fi
}

# Pick the install dir for the LSP binary (mirrors install.sh).
lsp_install_dir() {
  if [ -n "${GOLARS_INSTALL_DIR:-}" ]; then
    echo "$GOLARS_INSTALL_DIR"; return
  fi
  if [ -w "/usr/local/bin" ] 2>/dev/null; then
    echo "/usr/local/bin"
  elif [ -d "$HOME/.local/bin" ] || mkdir -p "$HOME/.local/bin" 2>/dev/null; then
    echo "$HOME/.local/bin"
  else
    mkdir -p "$HOME/bin"
    echo "$HOME/bin"
  fi
}

install_extension_assets() {
  local version="$1"
  local extensions_dir="$2"
  local installed_dir="$extensions_dir/installed/$EXT_ID"
  local version_no_v="${version#v}"
  local tarball="golars-zed-extension_${version_no_v}.tar.gz"
  local url="https://github.com/${REPO}/releases/download/${version}/${tarball}"

  local tmp
  tmp="$(mktemp -d)"
  trap "rm -rf -- '$tmp'" RETURN

  info "downloading ${tarball}"
  if ! download_file "$url" "$tmp/$tarball"; then
    err "could not download $url"
    err "make sure release ${version} ships the Zed extension package"
    exit 1
  fi

  info "extracting"
  tar -xzf "$tmp/$tarball" -C "$tmp"

  local wrap="$tmp/golars-zed-extension"
  if [ ! -d "$wrap" ]; then
    err "archive layout unexpected (no golars-zed-extension/ root)"
    exit 1
  fi

  mkdir -p "$extensions_dir/installed"
  if [ -d "$installed_dir" ]; then
    info "removing existing $installed_dir"
    rm -rf "$installed_dir"
  fi

  mv "$wrap" "$installed_dir"
  success "installed extension to $installed_dir"

  # Force Zed to rebuild its extension index on next start; otherwise
  # the cached entry can mask our drop-in.
  if [ -f "$extensions_dir/index.json" ]; then
    info "removing stale $extensions_dir/index.json (Zed rebuilds on launch)"
    rm -f "$extensions_dir/index.json"
  fi
}

install_lsp_if_missing() {
  local version="$1"

  if command -v "$LSP_BIN" >/dev/null 2>&1; then
    success "$LSP_BIN already on PATH ($(command -v $LSP_BIN))"
    return
  fi

  warn "$LSP_BIN not on PATH; fetching from release ${version}"

  local os arch version_no_v archive url tmp wrap target sudo
  os="$(detect_os)"
  arch="$(detect_arch)"
  if [ "$os" = "Windows" ]; then
    err "Windows binary auto-install is not supported by this script"
    err "download manually from https://github.com/${REPO}/releases/${version}"
    return
  fi
  version_no_v="${version#v}"
  archive="golars_${version_no_v}_${os}_${arch}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${version}/${archive}"

  tmp="$(mktemp -d)"
  trap "rm -rf -- '$tmp'" RETURN

  info "downloading ${archive}"
  download_file "$url" "$tmp/$archive"
  tar -xzf "$tmp/$archive" -C "$tmp"
  wrap="$(find "$tmp" -maxdepth 1 -type d -name "golars_${version_no_v}_${os}_${arch}" | head -1)"
  if [ -z "$wrap" ] || [ ! -f "$wrap/$LSP_BIN" ]; then
    err "could not find $LSP_BIN in $archive"
    return
  fi

  target="$(lsp_install_dir)"
  sudo=""
  if ! [ -w "$target" ]; then
    warn "need sudo for ${target}"
    sudo="sudo"
  fi

  $sudo install -m 755 "$wrap/$LSP_BIN" "$target/$LSP_BIN"
  success "installed $LSP_BIN to $target/$LSP_BIN"

  if ! command -v "$LSP_BIN" >/dev/null 2>&1; then
    warn "$target is not on PATH; add it to your shell rc, e.g."
    warn "  export PATH=\"$target:\$PATH\""
    warn "or set the binary path in Zed settings:"
    warn '  "lsp": { "golars-lsp": { "binary": { "path": "'$target'/'$LSP_BIN'" } } }'
  fi
}

main() {
  need_dl

  local version extensions_dir
  version="$(latest_tag "${1:-}")"
  if [ -z "$version" ]; then
    err "could not resolve latest release tag"
    exit 1
  fi
  extensions_dir="$(zed_extensions_dir)"

  echo
  info "golars Zed extension installer"
  info "  version:        $version"
  info "  extensions dir: $extensions_dir"
  echo

  install_extension_assets "$version" "$extensions_dir"
  install_lsp_if_missing "$version"

  echo
  success "done. restart Zed (or run 'zed: reload extensions' from the command palette)"
  success "then open any .glr file"
}

main "$@"
