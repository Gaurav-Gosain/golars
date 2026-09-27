# tree-sitter-golars

Tree-sitter grammar for the golars `.glr` scripting language. See
[`docs/scripting.md`](../../docs/scripting.md) for the language itself.

Ships: `grammar.js` + `queries/highlights.scm` + a minimal
`package.json`. Generate the parser with the `tree-sitter` CLI.

## Install (Neovim via nvim-treesitter)

Add a local parser config to your nvim setup:

```lua
-- in ~/.config/nvim/lua/plugins/golars.lua (lazy.nvim) or similar
require('nvim-treesitter.parsers').get_parser_configs().golars = {
  install_info = {
    url = "/path/to/golars/editors/tree-sitter-golars",  -- local path OK
    files = { "src/parser.c" },
    branch = "main",
    generate_requires_npm = true,
    requires_generate_from_grammar = true,
  },
  filetype = "glr",
}

vim.filetype.add({
  extension = { glr = "glr" },
})
```

Then `:TSInstall golars`. Copy `queries/highlights.scm` to
`~/.config/nvim/queries/golars/highlights.scm` so Neovim finds them.

## Install (Helix)

Add to `~/.config/helix/languages.toml`:

```toml
[[language]]
name = "glr"
scope = "source.golars"
file-types = ["glr"]
comment-token = "#"
roots = []
indent = { tab-width = 2, unit = "  " }

[[grammar]]
name = "glr"
source = { path = "/path/to/golars/editors/tree-sitter-golars" }
```

Then `hx --grammar fetch && hx --grammar build`. Copy the queries to
`~/.config/helix/runtime/queries/glr/`.

## Install (VS Code)

VS Code's built-in highlighting doesn't use tree-sitter yet. For a
lightweight TextMate-based alternative, see this grammar's token
names: they map cleanly to TextMate scopes (`@keyword` →
`keyword.control.golars`, `@string` → `string.quoted.double.golars`,
etc.). A full VS Code extension isn't in this repo; contributions
welcome.

## Build + test locally

```sh
cd editors/tree-sitter-golars
tree-sitter generate --abi 14        # keep the committed ABI
tree-sitter test                     # corpus in test/corpus/
tree-sitter parse ../../examples/script/expressions.glr
```

## Tree shape

A `statement` is an optional `.`, a `command` and its `args`. An
argument is an expression, an `assignment` (`name = expr`), an
`agg_spec` (`col:op[:alias]`), a `path`, a `duration` (`30m`) or a
comma. Expressions are `binary_expression` (with `left`, `operator`,
`right` fields and the precedence of `script/exprparse`),
`unary_expression`, `membership_expression` (`x [not] in [...]`),
`null_check_expression`, `when_expression` (`when_clause`s and an
optional `otherwise_clause`), `call_expression` (`function`,
`arguments`), `member_expression` (`object`, `property`),
`argument_list`, `keyword_argument`, `list`, `parenthesized_expression`
and the literals `string`, `number`, `boolean`, `null`.

Statement keywords (`as`, `on`, `by`, `desc`, ...) and namespaces
(`str`, `dt`, ...) stay plain identifiers so columns with those names
still parse; the highlight queries pick them out by text.

Division needs spaces around `/`: unspaced `a/b` reads as a path.

## Status

Every script in `examples/script/` and every glr block in
`docs/scripting.md` parses without errors. A new command is a one-line
change to the `command` rule in `grammar.js`; a Go drift test in
`script/drift_test.go` fails until it is added.
