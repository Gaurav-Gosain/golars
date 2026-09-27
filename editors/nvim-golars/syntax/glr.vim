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
syn keyword glrCommand      load use stash frames drop_frame save write show ishow
syn keyword glrCommand      browse schema describe head tail select drop filter
syn keyword glrCommand      sort limit groupby join explain explain_tree tree graph
syn keyword glrCommand      show_graph mermaid collect reset source timing info
syn keyword glrCommand      clear help h exit quit q reverse sample shuffle unique
syn keyword glrCommand      null_count glimpse size cast fill_null fillnull drop_null
syn keyword glrCommand      dropnull rename sum mean avg min max median std with_row_index
syn keyword glrCommand      pwd ls cd sum_horizontal mean_horizontal min_horizontal
syn keyword glrCommand      max_horizontal all_horizontal any_horizontal sum_all
syn keyword glrCommand      mean_all min_all max_all std_all var_all median_all
syn keyword glrCommand      count_all null_count_all with unnest explode upsample
syn keyword glrCommand      to_dummies join_asof group_by_dynamic groupby_dynamic
syn keyword glrCommand      scan_csv scan_parquet scan_ipc scan_arrow scan_ndjson
syn keyword glrCommand      scan_jsonl scan_json scan_auto fill_nan forward_fill
syn keyword glrCommand      ff backward_fill bf top_k bottom_k transpose unpivot
syn keyword glrCommand      melt partition_by skew kurtosis approx_n_unique approx_nunique
syn keyword glrCommand      corr cov pivot

" Structural keywords (join types, ordering, logical operators,
" null predicates).
syn keyword glrKeyword      as on asc desc and or
syn keyword glrKeyword      is_null is_not_null
syn keyword glrKeyword      inner left cross
syn keyword glrKeyword      not in when then otherwise
syn keyword glrKeyword      contains starts_with ends_with like not_like
syn keyword glrKeyword      every period offset by backward forward nearest tolerance

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
