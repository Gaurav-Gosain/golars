# jupyterlab-golars

Syntax highlighting + LSP wiring for golars `.glr` cells in JupyterLab.

## Install

```sh
pip install jupyterlab-golars
# (also installs the prebuilt labextension)
```

Or from a local checkout:

```sh
cd editors/jupyterlab-golars
pip install -e .
jlpm
jlpm build
```

After install, restart JupyterLab. golars-kernel cells get keyword
highlighting (load / filter / groupby / ...) and any registered LSP
server (`jupyterlab-lsp` + `golars-lsp`) routes diagnostics, hover,
and completion to those cells via the `text/x-glr` mime.
