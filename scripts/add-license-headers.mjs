#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Prepend the project's license header to source files that lack one.
//
// Idempotent: a file already carrying an SPDX line is left alone. Runs over
// every git-tracked file by default, or over the paths given as arguments
// (the build's `generate-api-types` passes the regenerated api.ts so the
// header survives regeneration).
//
//   node scripts/add-license-headers.mjs            # all tracked files
//   node scripts/add-license-headers.mjs --check    # exit 1 if any lack one
//   node scripts/add-license-headers.mjs a.ts b.md  # just these

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync, lstatSync } from "node:fs";
import { basename, extname, resolve } from "node:path";

const LINES = ["Copyright (c) Credible Data Inc.", "SPDX-License-Identifier: MIT"];
const SPDX = "SPDX-License-Identifier:";

// Paths never touched: third-party code, generated output that is not
// post-processed, test fixture data whose bytes are part of the test, and
// binaries. Matched against the repo-relative path.
const EXCLUDE = [
   /(^|\/)vendor\//,
   /(^|\/)node_modules\//,
   /(^|\/)dist\//,
   /(^|\/)build\//,
   /(^|\/)tests?\/fixtures\//,
   /(^|\/)__fixtures__\//,
   /^packages\/python-client\/openapi-client\.json$/,
   /^packages\/server\/src\/api\.ts$/, // header added by generate-api-types
];

// Comment syntax by extension, or by basename for extensionless files.
// Absent from both maps means "no header" (JSON, data, binaries, dotfiles).
const BY_EXT = {
   ".ts": "slash",
   ".tsx": "slash",
   ".js": "slash",
   ".mjs": "slash",
   ".cjs": "slash",
   ".malloy": "slash",
   ".css": "block",
   ".py": "hash",
   ".sh": "hash",
   ".yml": "hash",
   ".yaml": "hash",
   ".toml": "hash",
   ".ini": "hash",
   ".in": "hash",
   ".docker": "hash",
   ".md": "html",
   ".html": "html",
};
const BY_NAME = { Dockerfile: "hash", Makefile: "hash" };

function styleFor(path) {
   const name = basename(path);
   if (name.startsWith(".")) return undefined;
   return BY_EXT[extname(name)] ?? BY_NAME[name];
}

function render(style) {
   switch (style) {
      case "slash":
         return LINES.map((l) => `// ${l}`).join("\n") + "\n";
      case "hash":
         return LINES.map((l) => `# ${l}`).join("\n") + "\n";
      case "block":
         return `/*\n${LINES.map((l) => ` * ${l}`).join("\n")}\n */\n`;
      case "html":
         return `<!--\n${LINES.join("\n")}\n-->\n`;
   }
}

// Lines that must stay first in the file: the header goes after them.
function prefixLength(lines, style, path) {
   let i = 0;
   if (lines[i]?.startsWith("#!")) i++;
   if (style === "hash") {
      // Docker parser directives and Python encoding declarations.
      while (/^#\s*(syntax|escape)=/.test(lines[i] ?? "")) i++;
      if (/^#.*coding[:=]/.test(lines[i] ?? "")) i++;
   }
   if (style === "html") {
      if (/^<!doctype/i.test(lines[i] ?? "")) i++;
      else if (extname(path) === ".md" && lines[0] === "---") {
         // YAML front matter must open the file. Keep it intact.
         const close = lines.indexOf("---", 1);
         if (close > 0) i = close + 1;
      }
   }
   return i;
}

function apply(path, check) {
   const style = styleFor(path);
   if (!style) return "skip";
   const text = readFileSync(path, "utf8");
   if (text.length === 0) return "skip";
   const head = text.slice(0, 2048);
   if (head.includes(SPDX)) return "ok";
   if (check) return "missing";

   const lines = text.split("\n");
   const n = prefixLength(lines, style, path);
   const before = lines.slice(0, n);
   const after = lines.slice(n);
   // Front matter already ends in a line break; keep one blank line between
   // the header and whatever follows, and never introduce a leading blank.
   while (after.length && after[0] === "") after.shift();
   const out = [...before, render(style).trimEnd(), "", ...after].join("\n");
   writeFileSync(path, out);
   return "added";
}

const argv = process.argv.slice(2);
const check = argv.includes("--check");
const explicit = argv.filter((a) => a !== "--check");
const root = execFileSync("git", ["rev-parse", "--show-toplevel"]).toString().trim();
const files = explicit.length
   ? explicit
   : execFileSync("git", ["ls-files", "-z"], { cwd: root })
        .toString()
        .split("\0")
        .filter(Boolean);

const counts = { added: 0, ok: 0, skip: 0, missing: 0 };
const missing = [];
for (const rel of files) {
   // Explicit paths are taken as given (relative to the cwd) and bypass the
   // exclusion list: that is how generated output gets its header on regen.
   if (!explicit.length && EXCLUDE.some((re) => re.test(rel))) continue;
   const abs = explicit.length ? resolve(rel) : resolve(root, rel);
   let st;
   try {
      st = lstatSync(abs);
   } catch {
      continue;
   }
   if (!st.isFile()) continue; // symlinks are covered through their targets
   const r = apply(abs, check);
   counts[r]++;
   if (r === "missing") missing.push(rel);
}
for (const m of missing) console.log(m);
console.error(
   `license headers: ${counts.added} added, ${counts.ok} present, ${counts.missing} missing, ${counts.skip} skipped`,
);
process.exit(check && counts.missing > 0 ? 1 : 0);
