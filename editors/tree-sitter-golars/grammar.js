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

  conflicts: $ => [
    [$._arg, $._expr_operand],
    [$._arg, $._expr_operand, $.method_call],
    [$._expr_operand, $.method_call],
  ],

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

    // Closed set of known commands and their aliases, mirroring
    // script.Commands in script/spec.go (a Go test fails when the two
    // drift). Everything else falls back to identifier so
    // user-defined Executor hosts still parse cleanly.
    command: $ => choice(
      // io
      'load', 'save', 'write',
      'scan_csv', 'scan_parquet', 'scan_ipc', 'scan_arrow',
      'scan_json', 'scan_ndjson', 'scan_jsonl', 'scan_auto',
      // frames
      'use', 'stash', 'frames', 'drop_frame',
      // pipeline
      'select', 'drop', 'filter', 'sort', 'limit', 'groupby', 'join',
      'group_by_dynamic', 'groupby_dynamic', 'join_asof',
      'with', 'collect', 'reset', 'reverse', 'unique',
      'cast', 'fill_null', 'fillnull', 'drop_null', 'dropnull',
      'fill_nan', 'forward_fill', 'ff', 'backward_fill', 'bf',
      'rename', 'with_row_index',
      'sum_horizontal', 'mean_horizontal', 'min_horizontal',
      'max_horizontal', 'all_horizontal', 'any_horizontal',
      // reshape
      'sample', 'shuffle', 'top_k', 'bottom_k', 'transpose',
      'unpivot', 'melt', 'pivot', 'unnest', 'explode', 'upsample',
      'to_dummies',
      // inspect
      'show', 'head', 'tail', 'schema', 'describe', 'glimpse', 'size',
      'null_count', 'null-count', 'ishow', 'browse', 'partition_by',
      // aggregate
      'sum', 'mean', 'avg', 'min', 'max', 'median', 'std',
      'skew', 'kurtosis', 'approx_n_unique', 'approx_nunique',
      'corr', 'cov',
      'sum_all', 'mean_all', 'min_all', 'max_all',
      'std_all', 'var_all', 'median_all',
      'count_all', 'null_count_all',
      // plan
      'explain', 'explain_tree', 'tree', 'graph', 'show_graph', 'mermaid',
      // session
      'source', 'help', 'h', '?', 'exit', 'quit', 'q',
      'timing', 'info', 'clear', 'pwd', 'ls', 'cd',
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
      $.expr,
      '=',
      '.',
      '(', ')', ',', '/', '//', '%', '+', '*', '**', '!', '&', '|',
    ),

    expr: $ => choice(
      prec.left(1, seq($._expr_operand, repeat1(seq(
        choice('+', '-', '*', '/', '//', '%', '**'),
        $._expr_operand,
      )))),
      $._expr_operand,
    ),
    _expr_operand: $ => choice(
      prec(1, $.method_call),
      $.number,
      $.string,
      $.boolean,
      $.identifier,
      seq('(', $.expr, ')'),
      seq('-', $._expr_operand),
      $.list,
    ),
    // `[a, b]` list literal, as in `cut(x, [0, 10])`.
    list: $ => seq('[', optional(seq($.expr, repeat(seq(',', $.expr)))), ']'),
    // `name=value` keyword argument, as in `cut(x, [0], labels=["a", "b"])`.
    kwarg: $ => seq(field('name', $.identifier), '=', $.expr),
    _call_arg: $ => choice($.expr, $.kwarg),
    method_call: $ => prec.left(seq(
      field('receiver', $.identifier),
      repeat(seq('.', field('method', $.identifier))),
      '(', optional(seq($._call_arg, repeat(seq(',', $._call_arg)))), ')',
    )),

    // Structural keywords.
    keyword: $ => choice(
      'as', 'on', 'asc', 'desc', 'and', 'or', 'not', 'in',
      'is_null', 'is_not_null',
      'when', 'then', 'otherwise',
      'inner', 'left', 'cross',
      'every', 'period', 'offset', 'by',
      'backward', 'forward', 'nearest', 'tolerance',
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
