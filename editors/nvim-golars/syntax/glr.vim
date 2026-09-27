" Vim syntax file for golars .glr scripts.
" Tree-sitter users can swap this out for the grammar at
" editors/tree-sitter-golars/.

if exists("b:current_syntax")
  finish
endif

" Comments from # to EOL. Strings are matched first where they start
" earlier, so a quoted # is not a comment.
syn match   glrComment      "#.*$"      contains=@Spell

" Strings: double or single quoted, backslash escapes, no multi-line.
syn region  glrString       start=+"+ skip=+\\\\\|\\"+ end=+"\|$+ oneline contains=glrEscape
syn region  glrString       start=+'+ skip=+\\\\\|\\'+ end=+'\|$+ oneline contains=glrEscape
syn match   glrEscape       "\\." contained

" Numbers: digits, optional fraction and exponent. Durations such as
" 30m, 1h, 1mo and 1d12h.
syn match   glrNumber       "\v<\d+(\.\d*)?([eE][+-]?\d+)?>"
syn match   glrNumber       "\v<\.\d+([eE][+-]?\d+)?>"
syn match   glrDuration     "\v<\d+[a-z]+(\d+[a-z]+)*>"

syn keyword glrBoolean      true false
syn match   glrNull         "\v<null>"

" Commands only count at the start of a statement, after an optional
" leading dot.
syn match   glrCommandPos   "\v^\s*\zs\.?(null-count|\?|\h\w*)" contains=glrDot,glrCommand,glrSymbolCommand
syn match   glrSymbolCommand "\v(null-count|\?)" contained
syn keyword glrCommand      load use stash frames drop_frame save write show ishow contained
syn keyword glrCommand      browse schema describe head tail select drop filter contained
syn keyword glrCommand      sort limit groupby join explain explain_tree tree graph contained
syn keyword glrCommand      show_graph mermaid collect reset source timing info contained
syn keyword glrCommand      clear help h exit quit q reverse sample shuffle unique contained
syn keyword glrCommand      null_count glimpse size cast fill_null fillnull drop_null contained
syn keyword glrCommand      dropnull rename sum mean avg min max median std with_row_index contained
syn keyword glrCommand      pwd ls cd sum_horizontal mean_horizontal min_horizontal contained
syn keyword glrCommand      max_horizontal all_horizontal any_horizontal sum_all contained
syn keyword glrCommand      mean_all min_all max_all std_all var_all median_all contained
syn keyword glrCommand      count_all null_count_all with unnest explode upsample contained
syn keyword glrCommand      to_dummies join_asof group_by_dynamic groupby_dynamic contained
syn keyword glrCommand      scan_csv scan_parquet scan_ipc scan_arrow scan_ndjson contained
syn keyword glrCommand      scan_jsonl scan_json scan_auto fill_nan forward_fill contained
syn keyword glrCommand      ff backward_fill bf top_k bottom_k transpose unpivot contained
syn keyword glrCommand      melt partition_by skew kurtosis approx_n_unique approx_nunique contained
syn keyword glrCommand      corr cov pivot contained

" Statement keywords (join types, ordering, window options).
syn keyword glrKeyword      as on asc desc inner left cross right full outer semi anti suffix
syn keyword glrKeyword      every period offset by closed label start_by
syn keyword glrKeyword      backward forward nearest tolerance drop_first

" Expression operators spelled as words.
syn keyword glrOperatorWord and or not in is_null is_not_null
syn keyword glrOperatorWord starts_with ends_with like not_like
" `contains` is a :syntax option name, so it cannot be a keyword.
syn match   glrOperatorWord "\v<contains>(\s*\()@!"
syn keyword glrConditional  when then otherwise

" Function calls, namespaces and methods: `round(x, 2)`,
" `dt.year(ts)`, `ts.dt.year()`, `price.sum`.
syn match   glrFunction     "\v<\h\w*\ze\("
syn match   glrMethod       "\v\.\zs\h\w*"
syn match   glrNamespace    "\v(^|[^.[:alnum:]_])\zs(str|dt|list|arr|struct|name|bin|cat)\ze\.\h"
syn match   glrNamespace    "\v\.\zs(str|dt|list|arr|struct|name|bin|cat)\ze\.\h"

" Keyword arguments inside calls: `labels=[...]`.
syn match   glrKwarg        "\v\h\w*\ze\=[^=]" contained
syn region  glrArgs         matchgroup=glrParen start="(" end=")" transparent contains=ALLBUT,glrCommandPos,glrCommand,glrSymbolCommand,glrEscape,glrDot

" Operators.
syn match   glrOperator     "\v(\=\=|!\=|\<\=|\>\=|\<|\>|\*\*|//|[-+*%=])"
syn match   glrOperator     "\v\s\zs/\ze\s"

" Paths: a slash, ~, .., or a data or script file name.
syn match   glrPath         "\v(\s)@<=([[:alnum:]_.~-]+/[[:alnum:]_./~-]*|/[[:alpha:]_.~][[:alnum:]_./~-]*|\~|\.\.|[[:alnum:]_-]+(\.[[:alnum:]_-]+)*\.(csv|tsv|parquet|pq|arrow|ipc|json|ndjson|jsonl|glr))\ze(\s|$)"

" Aggregation spec `col:op[:alias]` as one token.
syn match   glrAggSpec      "\v<[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*(:[A-Za-z_][A-Za-z0-9_]*)?>"

" Leading '.' on REPL-style commands is punctuation.
syn match   glrDot          "\." contained

" A trailing backslash continues the statement on the next line.
syn match   glrContinuation "\\$"

highlight def link glrComment       Comment
highlight def link glrString        String
highlight def link glrEscape        SpecialChar
highlight def link glrNumber        Number
highlight def link glrDuration      Number
highlight def link glrBoolean       Boolean
highlight def link glrNull          Constant
highlight def link glrCommand       Statement
highlight def link glrSymbolCommand Statement
highlight def link glrKeyword       Keyword
highlight def link glrOperatorWord  Operator
highlight def link glrConditional   Conditional
highlight def link glrFunction      Function
highlight def link glrMethod        Function
highlight def link glrNamespace     Type
highlight def link glrKwarg         Identifier
highlight def link glrOperator      Operator
highlight def link glrPath          Directory
highlight def link glrAggSpec       Identifier
highlight def link glrDot           Delimiter
highlight def link glrContinuation  SpecialChar

let b:current_syntax = "glr"
