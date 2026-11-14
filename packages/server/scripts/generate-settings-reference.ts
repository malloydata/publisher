// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rewrites the generated settings table in docs/configuration.md, and
 * publisher.config.json.example at the repo root, from the registry
 * (src/settings/registry). src/settings/reference.spec.ts fails until this has
 * been run after a registry change.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
   exampleConfigText,
   REFERENCE_END,
   REFERENCE_START,
   withReferenceTable,
} from "../src/settings/reference";

const repo = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../..",
);
// The reference, or another file a test names.
const doc = process.argv[2] ?? path.join(repo, "docs/configuration.md");
// The example config, or another file a test names. A run that names only a
// doc, as a test does, writes no example.
const example =
   process.argv[3] ??
   (process.argv[2] === undefined
      ? path.join(repo, "publisher.config.json.example")
      : undefined);

const before = fs.readFileSync(doc, "utf8");
const after = withReferenceTable(before);
if (!before.includes(REFERENCE_START) || !before.includes(REFERENCE_END)) {
   throw new Error(`${doc} has no generated settings table to rewrite`);
}
if (after !== before) fs.writeFileSync(doc, after);
console.log(after === before ? `${doc} is up to date` : `Updated ${doc}`);

if (example !== undefined) {
   const was = fs.existsSync(example)
      ? fs.readFileSync(example, "utf8")
      : undefined;
   const now = exampleConfigText();
   if (was !== now) fs.writeFileSync(example, now);
   console.log(was === now ? `${example} is up to date` : `Updated ${example}`);
}
