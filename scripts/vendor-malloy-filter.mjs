// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Regenerate the copy of Malloy's filter grammar that the storefront example's
// page loads.
//
//   node scripts/vendor-malloy-filter.mjs
//   bun run vendor:malloy-filter
//
// Why this exists. The storefront data app writes `filter<string>` and
// `filter<number>` given values, which are Malloy filter syntax rather than
// plain values. Getting that escaping wrong does not fail loudly: an unescaped
// value starting with `-` becomes a negation, a value that is `%` becomes a
// match-anything pattern, and `null` becomes the null operator. Each of those
// returns the wrong rows and presents them as the rows you asked for.
//
// So the page does not hand-roll the escaping. It calls the same
// `@malloydata/malloy-filter` printer and parser the server and the React SDK
// call, and the two agree by construction rather than by our reading of the
// grammar. A no-build page cannot resolve a bare npm specifier, so the library
// is bundled to a browser ES module here and committed next to the page, the
// same way the chart library is.
//
// Not minified, deliberately. This is a generated artifact committed to an
// example directory that people and agents read, and an unreadable 53KB blob
// buys nothing over a readable 220KB one on a page served from localhost.
// Regenerating it after a Malloy version bump is the whole maintenance story,
// and an unminified diff is one a reviewer can actually spot-check.
import { build } from "esbuild";
import { createRequire } from "node:module";
import { readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = join(
   repoRoot,
   "examples/storefront/public/vendor/malloy-filter.js",
);

// The public entry point, not a path into dist/. A deep import would bundle
// less (the temporal parsers are unused here) at the cost of depending on the
// package's internal file layout, which is not something it promises to keep.
const PACKAGE = "@malloydata/malloy-filter";
const ENTRY = `export { StringFilterExpression, NumberFilterExpression } from "${PACKAGE}";\n`;

const manifest = JSON.parse(
   await readFile(require.resolve(`${PACKAGE}/package.json`), "utf8"),
);

const banner = `// Generated file. Do not edit.
//
// ${PACKAGE}@${manifest.version} (${manifest.license}), bundled as a browser ES
// module by scripts/vendor-malloy-filter.mjs. Regenerate with:
//
//   bun run vendor:malloy-filter
//
// Exports StringFilterExpression and NumberFilterExpression. The page uses
// their unparse/parse pair so a picked or typed value is escaped by the
// grammar's own printer rather than by a rule this repository maintains.
`;

const result = await build({
   stdin: { contents: ENTRY, resolveDir: repoRoot, sourcefile: "vendor-entry.mjs" },
   bundle: true,
   format: "esm",
   platform: "browser",
   // The example is loaded straight from disk by the browser, so nothing here
   // may reference a bare specifier or a Node built-in at runtime.
   target: ["es2022"],
   legalComments: "inline",
   banner: { js: banner },
   write: false,
});

const [output] = result.outputFiles;
await writeFile(OUT, output.text);

console.log(
   `Wrote ${OUT} (${PACKAGE}@${manifest.version}, ${output.text.length} bytes)`,
);
