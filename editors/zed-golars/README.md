# Zed Extension for golars

Syntax highlighting, LSP client, and editor features for the golars `.glr` scripting language.

## Features

- **Syntax Highlighting**: Full tree-sitter grammar support for the `.glr` scripting language
- **LSP Client**: Connects to `golars-lsp` for completions, diagnostics, hover, and more
- **Language Server**: Uses `stdio` transport to communicate with the `golars-lsp` binary
- **Editor Features**: Comment toggling, bracket matching, code outline

## Installation

### Quick Install (One-liner)

```bash
curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install-zed-extension.sh | bash
```

The installer downloads the prebuilt extension package (extension.wasm,
tree-sitter grammar wasm, language assets) from the latest GitHub
release and drops it into Zed's installed-extensions directory. If
`golars-lsp` isn't on PATH it also fetches the matching binary from
the same release, so a fresh machine ends up with a working setup
in one step. Restart Zed (or run `zed: reload extensions`) and open
any `.glr` file.

Pin a specific version: `... | bash -s -- v0.1.3`.

### Dev Extension (for local development)

1. Build artifacts: `./editors/zed-golars/build-package.sh`
2. Open Zed → Extensions → Install Dev Extension
3. Select `editors/zed-golars/` from this repository

### From Zed Extension Registry

(Coming soon - pending PR to zed-industries/extensions)

## Configuration

You can configure a custom path to the `golars-lsp` binary in your Zed settings:

```json
{
  "lsp": {
    "golars-lsp": {
      "binary": {
        "path": "/path/to/golars-lsp"
      }
    }
  }
}
```

## File Structure

```
zed-golars/
├── extension.toml          # Extension manifest
├── Cargo.toml              # Rust crate for LSP WASM extension
├── src/
│   └── lib.rs              # LSP client implementation
├── languages/
│   └── golars/
│       ├── config.toml     # Language configuration
│       ├── highlights.scm  # Syntax highlighting queries
│       ├── brackets.scm    # Bracket matching
│       ├── indents.scm     # Indentation rules
│       ├── outline.scm     # Code outline
│       └── injections.scm  # Language injection rules
└── LICENSE                 # MIT License
```

## Development

The end-to-end build (Rust extension component + tree-sitter grammar
wasm + language assets, packaged into the same tarball CI uploads to
each release) is in `build-package.sh`:

```bash
./editors/zed-golars/build-package.sh
# -> editors/zed-golars/dist/golars-zed-extension_<version>.tar.gz
```

It expects `cargo` (with the `wasm32-wasip1` target), `wasm-tools`,
`tree-sitter`, and `curl` on PATH. The script downloads the WASI
reactor adapter from a pinned wasmtime release; bump the URL when
upgrading `zed_extension_api`.

For just the Rust step:

```bash
cd editors/zed-golars
cargo build --target wasm32-wasip1 --release
```

## Grammar Source

The tree-sitter grammar lives at [nkapila6/tree-sitter-golars](https://github.com/nkapila6/tree-sitter-golars).

## License

MIT - see LICENSE file
