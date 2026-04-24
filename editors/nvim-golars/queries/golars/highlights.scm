; Tree-sitter highlight queries for golars .glr
;
; Capture names follow the nvim-treesitter / Helix / Zed convention.
; Third-party themes that recognize @keyword / @string etc. render
; these without extra configuration.

; Comments
(comment) @comment

; Commands: one distinct highlight so the pipeline-opening verb stands out.
(command) @function.builtin

; Structural keywords: `as`, `on`, `asc`, `desc`, `and`, `or`, join types.
(keyword) @keyword

; Comparison operators in filter predicates.
(operator) @operator

; Literals.
(string) @string
(number) @number
(boolean) @constant.builtin

; col:op[:alias] aggregation spec. Highlight as a single unit.
(agg_spec) @attribute

; Bare identifiers for column names / paths / user-defined commands.
(identifier) @variable

; The leading '.' on REPL-style commands.
(statement "." @punctuation.special)
