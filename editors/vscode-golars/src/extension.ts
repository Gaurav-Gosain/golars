import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";

// The VS Code extension connects to the golars-lsp binary over stdio
// and registers two convenience commands: "Preview focused frame"
// and "Explain plan". Both shell out to the golars CLI against the
// currently-active .glr file.

let client: LanguageClient | undefined;

export async function activate(context: vscode.ExtensionContext) {
  const config = vscode.workspace.getConfiguration("golars");
  const serverPath = config.get<string>("serverPath", "golars-lsp");
  const cliPath = config.get<string>("cliPath", "golars");

  // Start the LSP client. GOLARS_LSP_LOG is honoured by the server
  // (writes a duplicate of stderr there); set golars.logFile in
  // settings.json to enable.
  const logFile = config.get<string>("logFile", "");
  const env = logFile ? { ...process.env, GOLARS_LSP_LOG: logFile } : process.env;
  const serverOptions: ServerOptions = {
    run: { command: serverPath, transport: TransportKind.stdio, options: { env } },
    debug: { command: serverPath, transport: TransportKind.stdio, options: { env } },
  };
  const clientOptions: LanguageClientOptions = {
    documentSelector: [
      { scheme: "file", language: "golars" },
      { scheme: "vscode-notebook-cell", language: "golars" },
    ],
  };
  client = new LanguageClient(
    "golars",
    "golars language server",
    serverOptions,
    clientOptions,
  );
  await client.start();

  context.subscriptions.push(
    vscode.commands.registerCommand("golars.previewFrame", async () => {
      const editor = vscode.window.activeTextEditor;
      if (!editor || editor.document.languageId !== "golars") {
        vscode.window.showWarningMessage("golars: open a .glr file first");
        return;
      }
      const doc = vscode.window.createOutputChannel("golars preview");
      doc.show(true);
      doc.clear();
      await runCli(cliPath, ["--preview", editor.document.uri.fsPath], (line) =>
        doc.appendLine(line),
      );
    }),

    vscode.commands.registerCommand("golars.explainPlan", async () => {
      const editor = vscode.window.activeTextEditor;
      if (!editor || editor.document.languageId !== "golars") {
        vscode.window.showWarningMessage("golars: open a .glr file first");
        return;
      }
      const doc = vscode.window.createOutputChannel("golars explain");
      doc.show(true);
      doc.clear();
      await runCli(cliPath, ["explain", editor.document.uri.fsPath], (line) =>
        doc.appendLine(line),
      );
    }),
  );
}

export async function deactivate(): Promise<void> {
  if (client) {
    await client.stop();
  }
}

// runCli spawns the golars CLI and streams stdout line-by-line into
// the given sink. Kept intentionally small: we don't need full
// streaming to the output channel.
async function runCli(
  bin: string,
  args: string[],
  sink: (line: string) => void,
): Promise<void> {
  const { spawn } = await import("node:child_process");
  return new Promise<void>((resolve) => {
    const child = spawn(bin, args);
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    let buf = "";
    const flush = (s: string) => {
      buf += s;
      let nl = buf.indexOf("\n");
      while (nl !== -1) {
        sink(buf.slice(0, nl));
        buf = buf.slice(nl + 1);
        nl = buf.indexOf("\n");
      }
    };
    child.stdout.on("data", flush);
    child.stderr.on("data", flush);
    child.on("close", () => {
      if (buf.length > 0) {
        sink(buf);
      }
      resolve();
    });
  });
}
