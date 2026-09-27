// jupyterlab-golars: registers a CodeMirror 6 StreamLanguage for the
// golars `.glr` scripting language so JupyterLab cells get syntax
// highlighting + jupyter-lsp routes textDocument/* requests through to
// golars-lsp.
//
// The language is line-oriented: one statement per line, optional
// leading `.` (REPL form), `#` comments. The token table below mirrors
// script/spec.go - bump it when the dispatcher gains commands.

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

const KEYWORDS = new Set([
  'as', 'on', 'asc', 'desc', 'and', 'or',
  'is_null', 'is_not_null',
  'inner', 'left', 'cross',
  'contains', 'starts_with', 'ends_with', 'like', 'not_like',
  'not', 'in', 'when', 'then', 'otherwise',
  'every', 'period', 'offset', 'by', 'backward', 'forward', 'nearest', 'tolerance'
]);

const ATOMS = new Set(['true', 'false', 'null']);

// glrLanguage is a per-line tokeniser; CodeMirror calls token() in a
// loop until the stream is exhausted, advancing the cursor each time.
const glrLanguage = StreamLanguage.define<{}>({
  name: 'golars',
  startState: () => ({}),
  token(stream: StringStream): string | null {
    if (stream.sol() && stream.eat('.')) {
      // Lead-dot REPL form: `.help`, `.show` etc.
      return 'meta';
    }
    if (stream.eatSpace()) {
      return null;
    }
    if (stream.match(/#.*/)) {
      return 'comment';
    }
    if (stream.match(/"(?:[^"\\]|\\.)*"/)) {
      return 'string';
    }
    if (stream.match(/'(?:[^'\\]|\\.)*'/)) {
      return 'string';
    }
    if (stream.match(/-?\d+(\.\d+)?/)) {
      return 'number';
    }
    // Aggregation spec col:op[:alias] as one cohesive token.
    if (stream.match(/[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*(:[A-Za-z_][A-Za-z0-9_]*)?/)) {
      return 'attribute';
    }
    if (stream.match(/[<>!]=|==|>|<|=|\+|-|\*|\//)) {
      return 'operator';
    }
    const word = stream.match(/[A-Za-z_][A-Za-z0-9_]*/) as string[] | null;
    if (word) {
      const w = word[0];
      if (COMMANDS.has(w)) return 'keyword';
      if (KEYWORDS.has(w)) return 'modifier';
      if (ATOMS.has(w)) return 'atom';
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
