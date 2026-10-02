// Tree-sitter grammar for the golars .glr scripting language.
//
// Generate with: tree-sitter generate --abi 14
// Parse test:    tree-sitter parse path/to/file.glr
//
// A script is line-oriented: one statement per line, `#` comments,
// and a trailing backslash to continue a statement on the next line.
// A statement is a command followed by arguments. Arguments are
// expressions from the glr expression language (see
// script/exprparse), `name = expr` assignments, `col:op[:alias]`
// aggregation specs, file paths, durations such as `30m`, and commas.
//
// Statement keywords (`as`, `on`, `by`, `desc`, `every`, ...) are
// plain identifiers in the tree so that columns with those names
// still parse; the highlight queries pick them out by text.
// Namespaces (`str`, `dt`, `list`, ...) are identifiers too, for the
// same reason: `name.str.upper()` reads the column `name`.

const PREC = {
  or: 1,
  and: 2,
  not: 3,
  compare: 4,
  add: 5,
  mul: 6,
  unary: 7,
  power: 8,
  postfix: 9,
};

const commaSep1 = rule => seq(rule, repeat(seq(',', rule)), optional(','));
const commaSep = rule => optional(commaSep1(rule));

module.exports = grammar({
  name: 'golars',

  extras: $ => [/[ \t]/, $.line_continuation, $.comment],

  word: $ => $.identifier,

  rules: {
    // Every statement but the last needs a newline; the last one may
    // end at the end of the file.
    source_file: $ => seq(
      repeat(seq(optional($.statement), $._newline)),
      optional($.statement),
    ),

    // One command and its args.
    statement: $ => seq(
      optional('.'),
      field('command', $.command),
      repeat(field('args', $._arg)),
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
      $._expression,
      $.assignment,
      $.agg_spec,
      $.path,
      $.duration,
      ',',
    ),

    // `name = expr` in with, select and groupby. The parenthesized
    // form `(name = expr)` lets a groupby item contain spaces.
    assignment: $ => choice(
      seq(field('name', $.identifier), '=', field('value', $._expression)),
      seq('(', field('name', $.identifier), '=', field('value', $._expression), ')'),
    ),

    // ---------------------------------------------------------------
    // Expression language
    // ---------------------------------------------------------------

    _expression: $ => choice(
      $.binary_expression,
      $.unary_expression,
      $.membership_expression,
      $.null_check_expression,
      $.when_expression,
      $._primary,
    ),

    _primary: $ => choice(
      $.identifier,
      $.number,
      $.string,
      $.boolean,
      $.null,
      $.list,
      $.parenthesized_expression,
      $.call_expression,
      $.member_expression,
    ),

    binary_expression: $ => {
      const table = [
        [PREC.or, 'or'],
        [PREC.and, 'and'],
        [PREC.compare, choice('==', '!=', '<', '<=', '>', '>=')],
        [PREC.compare, choice('contains', 'starts_with', 'ends_with', 'like', 'not_like')],
        [PREC.add, choice('+', '-')],
        [PREC.mul, choice('*', '/', '//', '%')],
      ];
      return choice(
        ...table.map(([p, op]) => prec.left(p, seq(
          field('left', $._expression),
          field('operator', op),
          field('right', $._expression),
        ))),
        prec.right(PREC.power, seq(
          field('left', $._expression),
          field('operator', '**'),
          field('right', $._expression),
        )),
      );
    },

    unary_expression: $ => choice(
      prec(PREC.not, seq(field('operator', 'not'), field('operand', $._expression))),
      prec(PREC.unary, seq(field('operator', '-'), field('operand', $._expression))),
    ),

    // `x in [1, 2]` and `x not in [...]`.
    membership_expression: $ => prec.left(PREC.compare, seq(
      field('left', $._expression),
      optional(field('negated', 'not')),
      'in',
      field('right', $._expression),
    )),

    // `x is_null` and `x is_not_null`.
    null_check_expression: $ => prec.left(PREC.compare, seq(
      field('operand', $._expression),
      field('operator', choice('is_null', 'is_not_null')),
    )),

    // when c then a [when c2 then b]... [otherwise d]
    when_expression: $ => prec.right(seq(
      repeat1($.when_clause),
      optional($.otherwise_clause),
    )),
    when_clause: $ => prec.right(seq(
      'when', field('condition', $._expression),
      'then', field('value', $._expression),
    )),
    otherwise_clause: $ => prec.right(seq('otherwise', field('value', $._expression))),

    // `f(x)`, `dt.year(ts)`, `x.round(2)`.
    call_expression: $ => prec(PREC.postfix, seq(
      field('function', choice($.identifier, $.member_expression)),
      field('arguments', $.argument_list),
    )),

    // `x.sum`, `ts.dt`, `str.len_chars`: a method (or namespace)
    // reached with a dot. Without an argument list it is a call with
    // no arguments.
    member_expression: $ => prec(PREC.postfix, seq(
      field('object', $._primary),
      token.immediate('.'),
      field('property', $.identifier),
    )),

    argument_list: $ => seq('(', commaSep(choice($._expression, $.keyword_argument)), ')'),

    // `name=value`, as in `cut(x, [0, 10], labels=["a", "b", "c"])`.
    keyword_argument: $ => seq(field('name', $.identifier), '=', field('value', $._expression)),

    list: $ => seq('[', commaSep($._expression), ']'),

    parenthesized_expression: $ => seq('(', $._expression, ')'),

    // ---------------------------------------------------------------
    // Tokens
    // ---------------------------------------------------------------

    // `col:op[:alias]` aggregation spec.
    agg_spec: $ => token(prec(1,
      seq(/[A-Za-z_][A-Za-z0-9_]*/, ':', /[A-Za-z_][A-Za-z0-9_]*/, optional(seq(':', /[A-Za-z_][A-Za-z0-9_]*/))),
    )),

    // File paths: anything with a slash, `~`, `.` / `..` prefixes, or
    // a bare file name with a data or script extension.
    // Division needs spaces around `/` (`a / b`): unspaced `a/b` and
    // `a /b` read as paths.
    path: $ => token(prec(1, choice(
      /[A-Za-z0-9_.~\-]+\/[A-Za-z0-9_.\/~\-]*/,
      /\/[A-Za-z_.~][A-Za-z0-9_.\/~\-]*/,
      /~/,
      /\.\./,
      /[A-Za-z0-9_\-]+(\.[A-Za-z0-9_\-]+)*\.(csv|tsv|parquet|pq|arrow|ipc|json|ndjson|jsonl|glr)/,
    ))),

    // Durations: `30m`, `1h`, `1mo`, `1d12h`, `3i`.
    duration: $ => token(prec(1, /\d+[a-z]+(\d+[a-z]+)*/)),

    string: $ => token(choice(
      seq('"', repeat(choice(/[^"\\\n]/, /\\./)), '"'),
      seq("'", repeat(choice(/[^'\\\n]/, /\\./)), "'"),
    )),

    number: $ => token(choice(
      /\d+(\.\d*)?([eE][+-]?\d+)?/,
      /\.\d+([eE][+-]?\d+)?/,
    )),

    boolean: $ => choice('true', 'false'),
    null: $ => 'null',

    identifier: $ => /[A-Za-z_][A-Za-z0-9_]*/,

    comment: $ => token(seq('#', /[^\n]*/)),

    line_continuation: $ => /\\\r?\n/,
    _newline: $ => /\r?\n/,
  },
});
