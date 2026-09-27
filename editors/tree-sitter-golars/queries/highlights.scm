; Tree-sitter highlight queries for golars .glr
;
; Capture names follow the nvim-treesitter convention. Every node is
; captured by at most one pattern, so the result does not depend on
; whether an editor lets the first or the last matching pattern win.

(comment) @comment

; Commands: the pipeline verb that opens each statement.
(command) @function.builtin

; The leading '.' on REPL-style commands.
(statement "." @punctuation.special)

; Statement keywords are plain identifiers in the tree so columns
; with the same names still parse.
(statement
  args: (identifier) @keyword
  (#any-of? @keyword
    "as" "on" "asc" "desc" "inner" "left" "cross" "by" "every" "period"
    "offset" "closed" "label" "start_by" "backward" "forward" "nearest"
    "tolerance" "drop_first"))

(statement
  args: (identifier) @variable
  (#not-any-of? @variable
    "as" "on" "asc" "desc" "inner" "left" "cross" "by" "every" "period"
    "offset" "closed" "label" "start_by" "backward" "forward" "nearest"
    "tolerance" "drop_first"))

; `name = expr` defines a column.
(assignment name: (identifier) @property)

; `cut(x, labels=[...])`: keyword argument names.
(keyword_argument name: (identifier) @variable.parameter)

; Function calls: `round(x, 2)`, `concat_str(a, b)`.
(call_expression function: (identifier) @function.call)

; Namespaces: `dt.year(ts)`, `ts.dt.year()`, `name.str.upper()`.
(member_expression
  object: (identifier) @module
  (#any-of? @module "str" "dt" "list" "arr" "struct" "name" "bin" "cat")
  property: (identifier) @_method
  (#not-any-of? @_method "str" "dt" "list" "arr" "struct" "name" "bin" "cat"))

(member_expression
  property: (identifier) @module
  (#any-of? @module "str" "dt" "list" "arr" "struct" "name" "bin" "cat"))

; Methods: `x.round(2)`, `price.sum`, `ts.dt.year()`.
(member_expression
  property: (identifier) @function.method.call
  (#not-any-of? @function.method.call "str" "dt" "list" "arr" "struct" "name" "bin" "cat"))

; A receiver column, or a column whose name is also a namespace
; (`name.str.upper()` reads the column `name`).
(member_expression
  object: (identifier) @variable
  (#not-any-of? @variable "str" "dt" "list" "arr" "struct" "name" "bin" "cat"))

(member_expression
  object: (identifier) @variable
  (#any-of? @variable "str" "dt" "list" "arr" "struct" "name" "bin" "cat")
  property: (identifier) @_ns
  (#any-of? @_ns "str" "dt" "list" "arr" "struct" "name" "bin" "cat"))

; Column references inside expressions.
(assignment value: (identifier) @variable)
(keyword_argument value: (identifier) @variable)
(binary_expression (identifier) @variable)
(unary_expression operand: (identifier) @variable)
(membership_expression (identifier) @variable)
(null_check_expression operand: (identifier) @variable)
(when_clause (identifier) @variable)
(otherwise_clause (identifier) @variable)
(argument_list (identifier) @variable)
(list (identifier) @variable)
(parenthesized_expression (identifier) @variable)

; Operators.
[
  "==" "!=" "<" "<=" ">" ">="
  "+" "-" "*" "/" "//" "%" "**"
  "="
] @operator

[
  "and" "or" "not" "in"
  "is_null" "is_not_null"
  "contains" "starts_with" "ends_with" "like" "not_like"
] @keyword.operator

[
  "when" "then" "otherwise"
] @keyword.conditional

; Literals.
(string) @string
(number) @number
(duration) @string.special
(path) @string.special.path
(boolean) @boolean
(null) @constant.builtin

; col:op[:alias] aggregation spec. Highlight as a single unit.
(agg_spec) @attribute

(member_expression "." @punctuation.delimiter)
["(" ")" "[" "]"] @punctuation.bracket
"," @punctuation.delimiter

(line_continuation) @punctuation.special
