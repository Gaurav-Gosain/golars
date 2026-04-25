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
  // I/O + frame registry
  'load', 'save', 'write', 'use', 'stash', 'frames', 'drop_frame', 'reset',
  // inspection
  'show', 'ishow', 'browse', 'schema', 'describe', 'head', 'tail',
  'glimpse', 'size', 'null_count', 'null_count_all', 'info',
  // pipeline
  'select', 'drop', 'filter', 'sort', 'limit', 'with', 'groupby', 'rename',
  'join', 'collect', 'cast', 'fill_null', 'drop_null', 'reverse',
  'sample', 'shuffle', 'unique', 'unnest', 'explode', 'upsample',
  // plan introspection
  'explain', 'explain_tree', 'tree', 'graph', 'show_graph', 'mermaid',
  // horizontal aggs
  'sum', 'mean', 'min', 'max', 'sum_all', 'mean_all', 'min_all', 'max_all',
  'std_all', 'var_all', 'median_all',
  // meta
  'source', 'timing', 'clear', 'help', 'exit', 'quit'
]);

const KEYWORDS = new Set([
  'as', 'on', 'asc', 'desc', 'and', 'or',
  'is_null', 'is_not_null',
  'inner', 'left', 'cross',
  'contains', 'starts_with', 'ends_with', 'like', 'not_like'
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
