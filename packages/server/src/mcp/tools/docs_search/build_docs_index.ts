/**
 * Build a bundled lunr index over the Malloy documentation for the malloy_searchDocs MCP tool.
 *
 * The output JSON is committed as a build-time asset so the server has no runtime network
 * dependency. Regenerate it from a Malloy docs checkout when the docs are updated.
 *
 * Usage:
 *   bun run packages/server/src/mcp/tools/docs_search/build_docs_index.ts <docs-dir> [out.json]
 *
 * <docs-dir> points at the documentation root of a Malloy docs checkout, e.g.
 *   .../malloydata.github.io/src/documentation
 */
import * as fs from "fs";
import * as path from "path";
import lunr from "lunr";

const DOCS_BASE_URL = "https://docs.malloydata.dev";
const DOC_EXTENSIONS = new Set([".md", ".malloynb"]);

interface DocRecord {
   id: string;
   title: string;
   path: string;
   url: string;
   body: string;
}

function walk(dir: string, acc: string[] = []): string[] {
   for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
         walk(full, acc);
      } else if (DOC_EXTENSIONS.has(path.extname(entry.name))) {
         acc.push(full);
      }
   }
   return acc;
}

/** First markdown heading, else a title-cased file name. */
function deriveTitle(body: string, relPath: string): string {
   const heading = body.match(/^#\s+(.+)$/m);
   if (heading) return heading[1].trim();
   const base = path.basename(relPath).replace(/\.[^.]+$/, "");
   return base.replace(/[-_]/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/** docs/foo/bar.md -> https://docs.malloydata.dev/documentation/foo/bar */
function deriveUrl(relPath: string): string {
   const noExt = relPath.replace(/\.[^.]+$/, "");
   return `${DOCS_BASE_URL}/documentation/${noExt}`;
}

/** A short, single-line excerpt: first prose line that is not a heading or fence. */
function deriveExcerpt(body: string): string {
   for (const raw of body.split("\n")) {
      const line = raw.trim();
      if (
         !line ||
         line.startsWith("#") ||
         line.startsWith("```") ||
         line.startsWith(">>>") ||
         line === "---"
      )
         continue;
      return line.replace(/\s+/g, " ").slice(0, 280);
   }
   return "";
}

function main(): void {
   const docsDir = process.argv[2];
   const outFile =
      process.argv[3] || path.join(import.meta.dir, "malloy_docs_index.json");
   if (!docsDir || !fs.existsSync(docsDir)) {
      console.error(
         `docs dir not found: ${docsDir}\nUsage: bun run build_docs_index.ts <docs-dir> [out.json]`,
      );
      process.exit(1);
   }

   const files = walk(docsDir);
   const docs: DocRecord[] = files.map((file, i) => {
      const body = fs.readFileSync(file, "utf8");
      const relPath = path.relative(docsDir, file);
      return {
         id: String(i),
         title: deriveTitle(body, relPath),
         path: relPath,
         url: deriveUrl(relPath),
         body,
      };
   });

   const index = lunr(function () {
      this.ref("id");
      this.field("title", { boost: 4 });
      this.field("body");
      for (const doc of docs) {
         this.add({ id: doc.id, title: doc.title, body: doc.body });
      }
   });

   // Store metadata + excerpt per doc for the tool's response; the raw body is not
   // shipped (only the serialized lunr postings are needed for matching).
   const store: Record<string, Omit<DocRecord, "body">> = {};
   for (const doc of docs) {
      store[doc.id] = {
         id: doc.id,
         title: doc.title,
         path: doc.path,
         url: doc.url,
         excerpt: deriveExcerpt(doc.body),
      } as Omit<DocRecord, "body">;
   }

   fs.writeFileSync(
      outFile,
      JSON.stringify({
         builtFrom: docsDir,
         docCount: docs.length,
         index,
         store,
      }),
   );
   console.log(`Wrote ${docs.length} docs to ${outFile}`);
}

// Only run when invoked directly (bun run ...), not when imported by a test.
if (import.meta.main) {
   main();
}
