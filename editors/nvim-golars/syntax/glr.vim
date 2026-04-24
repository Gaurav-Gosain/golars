" Vim syntax file for golars .glr scripts.
" Kept simple on purpose: the language is line-oriented with a
" fixed command set. Tree-sitter users can swap this out for the
" grammar at editors/tree-sitter-golars/.

if exists("b:current_syntax")
  finish
endif

" Comments from # to EOL.
syn match   glrComment      "#.*$"      contains=@Spell

" Strings: double-quoted, backslash escapes, no multi-line.
syn region  glrString       start=+"+ skip=+\\"+ end=+"+ oneline

" Numbers: optional minus, digits, optional decimal fraction.
syn match   glrNumber       "\v<-?\d+(\.\d+)?>"

" Booleans.
syn keyword glrBoolean      true false

" Core commands: one distinct highlight group keeps the
" pipeline-opening verb visually prominent.
syn keyword glrCommand      load save use stash frames drop_frame
syn keyword glrCommand      show ishow browse schema describe head tail
syn keyword glrCommand      select drop filter sort limit groupby join rename
syn keyword glrCommand      with unnest explode upsample reverse sample shuffle unique
syn keyword glrCommand      explain explain_tree tree graph show_graph mermaid
syn keyword glrCommand      collect reset source null_count null_count_all
syn keyword glrCommand      sum_all mean_all min_all max_all std_all var_all median_all
syn keyword glrCommand      timing info clear help exit quit

" Structural keywords (join types, ordering, logical operators,
" null predicates).
syn keyword glrKeyword      as on asc desc and or
syn keyword glrKeyword      is_null is_not_null
syn keyword glrKeyword      inner left cross

" Comparison operators.
syn match   glrOperator     "\v(\=\=|!\=|\<\=|\>\=|\<|\>)"

" Aggregation spec `col:op[:alias]` as one token, highlighted as
" a cohesive unit.
syn match   glrAggSpec      "\v<[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*(:[A-Za-z_][A-Za-z0-9_]*)?>"

" Leading '.' on REPL-style commands is punctuation.
syn match   glrDot          "\v^\s*\."

highlight def link glrComment   Comment
highlight def link glrString    String
highlight def link glrNumber    Number
highlight def link glrBoolean   Boolean
highlight def link glrCommand   Function
highlight def link glrKeyword   Keyword
highlight def link glrOperator  Operator
highlight def link glrAggSpec   Identifier
highlight def link glrDot       Delimiter

let b:current_syntax = "glr"
