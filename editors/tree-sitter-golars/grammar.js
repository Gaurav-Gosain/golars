// Tree-sitter grammar for the golars .glr scripting language.
//
// Generate with: npx tree-sitter generate
// Parse test:    npx tree-sitter parse path/to/file.glr
//
// The language is intentionally tiny: line-oriented, one command
// per line, # for comments. This grammar handles line-continuation
// with a trailing backslash, double-quoted string literals with \
// escapes, and the closed set of known command names so editors can
// highlight them distinctly.

module.exports = grammar({
  name: 'golars',

  extras: $ => [/[ \t]/, $.line_continuation],

  externals: $ => [],

  conflicts: $ => [],

  rules: {
    source_file: $ => repeat(choice(
      $.comment,
      $.statement,
      $._newline,
    )),

    // One command and its args, terminated by a newline.
    statement: $ => seq(
      optional('.'),
      field('command', $.command),
      field('args', repeat($._arg)),
      $._newline,
    ),

    // Closed set of known commands; everything else falls back to
    // identifier so user-defined Executor hosts still parse cleanly.
    command: $ => choice(
      'load', 'save', 'use', 'stash', 'frames', 'drop_frame',
      'with', 'unnest', 'explode', 'upsample',
      'tree', 'graph', 'show_graph', 'mermaid', 'explain_tree',
      'select', 'drop', 'filter', 'sort', 'limit',
      'head', 'tail', 'show', 'schema', 'describe',
      'groupby', 'join',
      'explain', 'collect', 'reset', 'source',
      'timing', 'info', 'clear', 'exit', 'quit',
      'reverse', 'sample', 'shuffle', 'unique',
      'null_count', 'glimpse', 'size',
      'cast', 'fill_null', 'drop_null', 'rename',
      'sum', 'mean', 'avg', 'min', 'max', 'median', 'std',
      'write', 'with_row_index',
      'pwd', 'ls', 'cd',
      'sum_horizontal', 'mean_horizontal', 'min_horizontal',
      'max_horizontal', 'all_horizontal', 'any_horizontal',
      'sum_all', 'mean_all', 'min_all', 'max_all',
      'std_all', 'var_all', 'median_all',
      'count_all', 'null_count_all',
      'scan_csv', 'scan_parquet', 'scan_ipc', 'scan_arrow',
      'scan_json', 'scan_ndjson', 'scan_jsonl', 'scan_auto',
      'fill_nan', 'forward_fill', 'backward_fill',
      'top_k', 'bottom_k', 'transpose', 'unpivot', 'melt',
      'partition_by',
      'skew', 'kurtosis', 'approx_n_unique', 'corr', 'cov',
      'pivot',
      'help',
      $.identifier, // host-defined command
    ),

    _arg: $ => choice(
      $.keyword,
      $.operator,
      $.string,
      $.number,
      $.boolean,
      $.agg_spec,
      $.identifier,
    ),

    // Structural keywords.
    keyword: $ => choice(
      'as', 'on', 'asc', 'desc', 'and', 'or',
      'is_null', 'is_not_null',
      'inner', 'left', 'cross',
    ),

    operator: $ => choice(
      '==', '!=', '<=', '>=', '<', '>',
    ),

    // `col:op[:alias]` aggregation spec.
    agg_spec: $ => token(
      seq(/[A-Za-z_][A-Za-z0-9_]*/, ':', /[A-Za-z_][A-Za-z0-9_]*/, optional(seq(':', /[A-Za-z_][A-Za-z0-9_]*/))),
    ),

    string: $ => seq(
      '"',
      repeat(choice(
        /[^"\\\n]/,
        seq('\\', /./),
      )),
      '"',
    ),

    number: $ => /-?\d+(\.\d+)?/,
    boolean: $ => choice('true', 'false'),

    // Identifiers include '.' and '/' and '-' so paths and column
    // names parse without ugly string quoting.
    identifier: $ => /[A-Za-z_][A-Za-z0-9_./-]*/,

    comment: $ => token(seq('#', /[^\n]*/)),

    line_continuation: $ => /\\\r?\n/,
    _newline: $ => /\r?\n/,
  },
});
