import { readFileSync } from "node:fs";
import { join } from "node:path";
import { rehypeCodeDefaultOptions, remarkMdxMermaid } from "fumadocs-core/mdx-plugins";
import {
  defineConfig,
  defineDocs,
  frontmatterSchema,
  metaSchema,
} from 'fumadocs-mdx/config';

// You can customise Zod schemas for frontmatter and `meta.json` here
// see https://fumadocs.dev/docs/mdx/collections
export const docs = defineDocs({
  dir: 'content/docs',
  docs: {
    schema: frontmatterSchema,
    postprocess: {
      includeProcessedMarkdown: true,
    },
  },
  meta: {
    schema: metaSchema,
  },
});

// Highlight ```glr blocks with the grammar the VS Code extension ships.
// Shiki does not bundle glr, so the build fails without it.
const glrGrammar = JSON.parse(
  readFileSync(
    join(process.cwd(), "../editors/vscode-golars/syntaxes/glr.tmLanguage.json"),
    "utf8",
  ),
);

export default defineConfig({
  mdxOptions: {
    remarkPlugins: [remarkMdxMermaid],
    rehypeCodeOptions: {
      ...rehypeCodeDefaultOptions,
      langs: ["ts", "tsx", { ...glrGrammar, name: "glr", aliases: ["golars"] }],
    },
  },
});
