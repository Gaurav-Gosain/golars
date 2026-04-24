# nvim-golars

Neovim language support for the golars `.glr` scripting language.
Registers the filetype, default buffer options, and the
`golars-lsp` language server.

What you get:

* Inline **diagnostics** for unknown commands and missing required args
* **Completions** for commands, keywords (`as`, `on`, `asc`, `desc`, `inner`, `left`, `cross`), staged frame names (from earlier `load ... as NAME` in the same file), and filesystem paths after `load` / `save` / `join` / `source`
* **Hover** docs with signature + full description on any command token, plus `# ^?` probe previews rendered as virtual text
* Proper `commentstring = "# %s"` so `gcc` / `gc{motion}` comment the right way

## Install (lazy.nvim, remote / monorepo)

The plugin lives at `editors/nvim-golars/` inside the main `golars`
repo. lazy clones the whole repo; we prepend the subdir to the
runtimepath so Neovim picks up `lua/golars`, `ftdetect/`, `syntax/`.

`~/.config/nvim/lua/plugins/golars.lua`:

```lua
return {
  {
    url = "https://github.com/Gaurav-Gosain/golars",
    name = "nvim-golars",
    ft = "glr",
    init = function()
      vim.filetype.add({ extension = { glr = "glr" } })
    end,
    config = function()
      local root = vim.fn.stdpath("data") .. "/lazy/nvim-golars/editors/nvim-golars"
      vim.opt.rtp:prepend(root)
      vim.cmd("runtime! ftdetect/*.lua ftdetect/*.vim syntax/*.vim")
      require("golars").setup({})
    end,
  },
}
```

If `golars-lsp` isn't on `$PATH`, pass an absolute path:

```lua
require("golars").setup({
  cmd = { vim.fn.expand("~/go/bin/golars-lsp") },
  preview_cmd = { vim.fn.expand("~/go/bin/golars") },
})
```

## Install (local checkout, for plugin development)

```lua
return {
  {
    dir = vim.fn.expand("~/dev/golars/editors/nvim-golars"),
    name = "nvim-golars",
    ft = "glr",
    init = function()
      vim.filetype.add({ extension = { glr = "glr" } })
    end,
    config = function()
      require("golars").setup({})
    end,
  },
}
```

## Install the golars binaries

Either grab the prebuilt tarball with the curl installer:

```sh
curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/install.sh | sh
```

...or build from source with `go`:

```sh
go install github.com/Gaurav-Gosain/golars/cmd/golars@latest
go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest
```

For local development (repo checkout), run from the repo root:

```sh
go install ./cmd/golars ./cmd/golars-lsp
```

## Tree-sitter highlighting

The tree-sitter grammar lives in the same monorepo at
[`../tree-sitter-golars/`](../tree-sitter-golars/). Plug it into
nvim-treesitter with:

```lua
require("nvim-treesitter.parsers").get_parser_configs().golars = {
  install_info = {
    url = "https://github.com/Gaurav-Gosain/golars",
    files = { "src/parser.c" },
    location = "editors/tree-sitter-golars",
    branch = "main",
    generate_requires_npm = false,
    requires_generate_from_grammar = false,
  },
  filetype = "glr",
}
```

Then `:TSInstall golars` once and restart. The LSP plugin works
without tree-sitter, but syntax highlighting + incremental selection
are much nicer with it.

## Verify

Open any `.glr` file and check `:LspInfo`: you should see
`golars-lsp` attached. Type a bad command (e.g. `fooz bar`) and the
sign column should show a diagnostic. Hover over a known command
with `K` and the signature + doc pop up.

If nothing happens, check `:checkhealth vim.lsp` and the server's
stderr via:

```sh
tail -f ~/.local/state/nvim/lsp.log
```
