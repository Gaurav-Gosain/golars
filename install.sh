#!/usr/bin/env bash

# golars installation script.
# Pulls the latest release archive for the host OS + arch and drops
# `golars`, `golars-lsp`, and `golars-mcp` onto the user's PATH.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install.sh | bash
#   curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install.sh | bash -s -- v0.1.1
#
# Honours $GOLARS_INSTALL_DIR if the user wants a non-default bin dir.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

REPO="Gaurav-Gosain/golars"
BINARIES=("golars" "golars-lsp" "golars-mcp")

info()    { echo -e "${BLUE}[*]${NC} $1"; }
success() { echo -e "${GREEN}[ok]${NC} $1"; }
err()     { echo -e "${RED}[!!]${NC} $1" >&2; }
warn()    { echo -e "${YELLOW}[..]${NC} $1"; }

detect_os() {
  case "$(uname -s)" in
    Linux*)  echo "Linux" ;;
    Darwin*) echo "Darwin" ;;
    *)
      err "unsupported OS: $(uname -s)"
      exit 1
      ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "x86_64" ;;
    arm64|aarch64) echo "arm64" ;;
    *)
      err "unsupported arch: $(uname -m)"
      exit 1
      ;;
  esac
}

latest_tag() {
  # Accept an explicit tag via $1; otherwise ask GitHub for the latest.
  if [ -n "${1:-}" ]; then
    echo "$1"
    return
  fi
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep -E '"tag_name":' \
      | head -1 \
      | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/'
  else
    err "curl is required"
    exit 1
  fi
}

download_file() {
  # $1 url, $2 dest
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$2" "$1"
  else
    err "need curl or wget"
    exit 1
  fi
}

install_dir() {
  if [ -n "${GOLARS_INSTALL_DIR:-}" ]; then
    echo "$GOLARS_INSTALL_DIR"
    return
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

main() {
  local os arch version version_no_v archive_name url tmp dest target
  os="$(detect_os)"
  arch="$(detect_arch)"
  version="$(latest_tag "${1:-}")"
  if [ -z "$version" ]; then
    err "could not resolve release tag"
    exit 1
  fi
  version_no_v="${version#v}"
  archive_name="golars_${version_no_v}_${os}_${arch}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${version}/${archive_name}"

  info "installing golars ${version} (${os}/${arch})"
  tmp="$(mktemp -d)"
  trap 'rm -rf -- "$tmp"' EXIT

  info "downloading ${archive_name}"
  download_file "$url" "$tmp/$archive_name"

  info "extracting"
  tar -xzf "$tmp/$archive_name" -C "$tmp"

  # goreleaser wraps the archive in a top-level directory; find it.
  local wrap
  wrap="$(find "$tmp" -maxdepth 1 -type d -name "golars_${version_no_v}_${os}_${arch}" | head -1)"
  if [ -z "$wrap" ]; then
    err "archive layout unexpected"
    exit 1
  fi

  target="$(install_dir)"
  info "installing binaries to ${target}"

  local sudo=""
  if ! [ -w "$target" ]; then
    warn "need sudo for ${target}"
    sudo="sudo"
  fi

  for bin in "${BINARIES[@]}"; do
    if [ ! -f "$wrap/$bin" ]; then
      err "missing $bin in archive"
      exit 1
    fi
    $sudo install -m 755 "$wrap/$bin" "$target/$bin"
    success "installed $bin"
  done

  if ! command -v golars >/dev/null 2>&1; then
    warn "$target is not on PATH; add it to your shell rc"
  fi

  echo
  success "done. try: golars version"
  success "then:    golars browse <file.csv>"
}

main "$@"
