# vscode-golars

VS Code extension for the [golars](https://github.com/Gaurav-Gosain/golars)
`.glr` scripting language.

Features:

- Syntax highlighting for all built-in commands, keywords, strings, numbers, and comments.
- LSP client that connects to the `golars-lsp` binary for completion, hover, and go-to-definition.
- LSP features in the cells of a golars-kernel notebook. Each cell knows the frames of the cells above it.
- Two commands in the command palette:
  - `golars: Preview focused frame` - runs the script in preview mode and renders the final head.
  - `golars: Explain plan` - prints the optimiser trace for the script.

## Install

From source (until the Marketplace listing is ready):

```sh
cd editors/vscode-golars
npm install
npm run compile
code --install-extension .
```

## Configure

```jsonc
// settings.json
{
  "golars.serverPath": "golars-lsp",
  "golars.cliPath": "golars",
  "golars.logFile": ""
}
```

`serverPath` and `cliPath` default to the binary name on `$PATH`.
Override them if you installed to a different location.

Set `logFile` to a path to copy the golars-lsp log to that file. The
server reads the path from `GOLARS_LSP_LOG`.

## Develop

- `npm run watch` - TypeScript compile in watch mode.
- `F5` in this folder launches a sandbox VS Code window with the
  extension loaded.
