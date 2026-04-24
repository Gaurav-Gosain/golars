# Zed Extension for golars

Syntax highlighting, LSP client, and editor features for the golars `.glr` scripting language.

## Features

- **Syntax Highlighting**: Full tree-sitter grammar support for the `.glr` scripting language
- **LSP Client**: Connects to `golars-lsp` for completions, diagnostics, hover, and more
- **Language Server**: Uses `stdio` transport to communicate with the `golars-lsp` binary
- **Editor Features**: Comment toggling, bracket matching, code outline

## Installation

### Prerequisites

Install the language server binary:

```bash
go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest
```

### Quick Install (One-liner)

```bash
curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install-zed-extension.sh | bash
```

This clones the extension directly to Zed's extensions directory. Restart Zed to activate.

### Dev Extension (for local development)

1. Open Zed
2. Go to Extensions → Install Dev Extension
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

To build the extension:

```bash
cd editors/zed-golars
cargo build --target wasm32-wasip1 --release
```

## Grammar Source

The tree-sitter grammar lives at [nkapila6/tree-sitter-golars](https://github.com/nkapila6/tree-sitter-golars).

## License

MIT - see LICENSE file
