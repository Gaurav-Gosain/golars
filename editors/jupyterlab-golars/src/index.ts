// jupyterlab-golars: registers a CodeMirror 6 StreamLanguage for the
// golars `.glr` scripting language so JupyterLab cells get syntax
// highlighting + jupyter-lsp routes textDocument/* requests through to
// golars-lsp.
//
// The language is line-oriented: one statement per line, optional
// leading `.` (REPL form), `#` comments, and a trailing `\` continues a
// statement. The command table below mirrors script/spec.go; the rest
// of a statement uses the expression language (calls, namespaces,
// keyword arguments, lists, operators, when/then/otherwise).

import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { IEditorLanguageRegistry } from '@jupyterlab/codemirror';
import {
  LanguageSupport,
  StreamLanguage,
  StringStream
} from '@codemirror/language';

const COMMANDS = new Set([
  // Mirrors script.Commands in script/spec.go (names and aliases); a Go
  // test in script/drift_test.go fails when this list drifts.
  // io
  'load', 'save', 'write',
  'scan_csv', 'scan_parquet', 'scan_ipc', 'scan_arrow',
  'scan_json', 'scan_ndjson', 'scan_jsonl', 'scan_auto',
  // frames
  'use', 'stash', 'frames', 'drop_frame',
  // pipeline
  'select', 'drop', 'filter', 'sort', 'limit', 'groupby', 'join',
  'with', 'collect', 'reset', 'reverse', 'unique',
  'cast', 'fill_null', 'fillnull', 'drop_null', 'dropnull',
  'fill_nan', 'forward_fill', 'ff', 'backward_fill', 'bf',
  'rename', 'with_row_index',
  'sum_horizontal', 'mean_horizontal', 'min_horizontal',
  'max_horizontal', 'all_horizontal', 'any_horizontal',
  // reshape
  'sample', 'shuffle', 'top_k', 'bottom_k', 'transpose',
  'unpivot', 'melt', 'pivot', 'unnest', 'explode', 'upsample',
  'to_dummies', 'join_asof', 'group_by_dynamic', 'groupby_dynamic',
  // inspect
  'show', 'head', 'tail', 'schema', 'describe', 'glimpse', 'size',
  'null_count', 'ishow', 'browse', 'partition_by',
  // aggregate
  'sum', 'mean', 'avg', 'min', 'max', 'median', 'std',
  'skew', 'kurtosis', 'approx_n_unique', 'approx_nunique',
  'corr', 'cov',
  'sum_all', 'mean_all', 'min_all', 'max_all',
  'std_all', 'var_all', 'median_all', 'count_all', 'null_count_all',
  // plan
  'explain', 'explain_tree', 'tree', 'graph', 'show_graph', 'mermaid',
  // session
  'source', 'help', 'h', 'exit', 'quit', 'q',
  'timing', 'info', 'clear', 'pwd', 'ls', 'cd'
]);

// Commands a word pattern cannot match.
const SYMBOL_COMMANDS = new Set(['null-count', '?']);

// Statement keywords (join types, ordering, window options).
const KEYWORDS = new Set([
  'as', 'on', 'asc', 'desc',
  'inner', 'left', 'cross', 'right', 'full', 'outer', 'semi', 'anti', 'suffix',
  'every', 'period', 'offset', 'by', 'closed', 'label', 'start_by',
  'backward', 'forward', 'nearest', 'tolerance', 'drop_first'
]);

// Expression operators spelled as words.
const WORD_OPERATORS = new Set([
  'and', 'or', 'not', 'in', 'is_null', 'is_not_null',
  'contains', 'starts_with', 'ends_with', 'like', 'not_like'
]);

const CONTROL = new Set(['when', 'then', 'otherwise']);

// Expression namespaces: `dt.year(ts)`, `ts.dt.year()`.
const NAMESPACES = new Set(['str', 'dt', 'list', 'arr', 'struct', 'name', 'bin', 'cat']);

// Paths: a slash, `~`, `..`, or a data or script file name.
const PATH = /^(?:[\w.~-]+\/[\w./~-]*|\/[A-Za-z_.~][\w./~-]*|~|\.\.|[\w-]+(?:\.[\w-]+)*\.(?:csv|tsv|parquet|pq|arrow|ipc|json|ndjson|jsonl|glr))(?=\s|$)/;

interface GlrState {
  // atStart is true until the command of the current statement has
  // been read.
  atStart: boolean;
  // continued is true when the previous line ended with a backslash.
  continued: boolean;
  // afterDot is true right after a `.` member access.
  afterDot: boolean;
  // parens counts open `(` so `name=` inside a call reads as a keyword
  // argument rather than a column definition.
  parens: number;
}

// glrLanguage is a per-line tokeniser; CodeMirror calls token() in a
// loop until the stream is exhausted, advancing the cursor each time.
const glrLanguage = StreamLanguage.define<GlrState>({
  name: 'golars',
  startState: () => ({ atStart: true, continued: false, afterDot: false, parens: 0 }),
  copyState: s => ({ ...s }),
  token(stream: StringStream, state: GlrState): string | null {
    if (stream.sol()) {
      if (!state.continued) {
        state.atStart = true;
        state.parens = 0;
      }
      state.continued = false;
      state.afterDot = false;
    }
    if (stream.eatSpace()) {
      return null;
    }
    if (stream.match(/^#.*/)) {
      return 'comment';
    }
    if (stream.match(/^\\$/)) {
      state.continued = true;
      return 'escape';
    }
    if (state.atStart) {
      if (stream.eat('.')) {
        // Lead-dot REPL form: `.help`, `.show` etc.
        return 'meta';
      }
      state.atStart = false;
      const cmd = stream.match(/^(?:null-count|\?|[A-Za-z_][A-Za-z0-9_]*)/) as string[] | null;
      if (cmd) {
        return COMMANDS.has(cmd[0]) || SYMBOL_COMMANDS.has(cmd[0]) ? 'keyword' : 'variableName';
      }
    }
    const afterDot = state.afterDot;
    state.afterDot = false;
    if (stream.match(/^"(?:[^"\\]|\\.)*"?/) || stream.match(/^'(?:[^'\\]|\\.)*'?/)) {
      return 'string';
    }
    if (stream.match(PATH)) {
      return 'string.special';
    }
    // Durations: `30m`, `1h`, `1mo`.
    if (stream.match(/^\d+[a-z]+(?:\d+[a-z]+)*(?![\w.])/)) {
      return 'unit';
    }
    if (stream.match(/^(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/)) {
      return 'number';
    }
    // Aggregation spec col:op[:alias] as one cohesive token.
    if (stream.match(/^[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*(:[A-Za-z_][A-Za-z0-9_]*)?/)) {
      return 'attributeName';
    }
    if (stream.match(/^(?:\*\*|\/\/|[<>!=]=|[<>=+\-*/%])/)) {
      return 'operator';
    }
    if (stream.eat('.')) {
      state.afterDot = true;
      return 'punctuation';
    }
    if (stream.eat('(')) {
      state.parens++;
      return 'paren';
    }
    if (stream.eat(')')) {
      state.parens = Math.max(0, state.parens - 1);
      return 'paren';
    }
    if (stream.match(/^[[\],]/)) {
      return 'punctuation';
    }
    const word = stream.match(/^[A-Za-z_][A-Za-z0-9_]*/) as string[] | null;
    if (word) {
      const w = word[0];
      if (afterDot) {
        if (NAMESPACES.has(w) && stream.peek() === '.') return 'namespace';
        return 'propertyName.function';
      }
      if (NAMESPACES.has(w) && stream.match(/^\.(?=[A-Za-z_])/, false)) return 'namespace';
      if (stream.match(/^\s*=(?!=)/, false)) {
        return state.parens > 0 ? 'attributeName' : 'variableName.definition';
      }
      if (stream.match(/^\(/, false)) return 'variableName.function';
      if (CONTROL.has(w)) return 'controlKeyword';
      if (WORD_OPERATORS.has(w)) return 'operatorKeyword';
      if (w === 'true' || w === 'false') return 'bool';
      if (w === 'null') return 'null';
      if (KEYWORDS.has(w)) return 'modifier';
      return 'variableName';
    }
    stream.next();
    return null;
  }
});

const glrSupport = new LanguageSupport(glrLanguage);

const plugin: JupyterFrontEndPlugin<void> = {
  id: 'jupyterlab-golars:plugin',
  description: 'Syntax highlighting for golars .glr cells.',
  autoStart: true,
  requires: [IEditorLanguageRegistry],
  activate: (app: JupyterFrontEnd, langs: IEditorLanguageRegistry) => {
    langs.addLanguage({
      name: 'golars',
      mime: ['text/x-glr', 'text/x-golars'],
      extensions: ['glr'],
      support: glrSupport
    });
    console.log('jupyterlab-golars: registered glr language');
  }
};

export default plugin;
