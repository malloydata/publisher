// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rewrites the generated settings table in docs/configuration.md from the
 * registry (src/settings/registry). src/settings/reference.spec.ts fails
 * until this has been run after a registry change.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
   REFERENCE_END,
   REFERENCE_START,
   withReferenceTable,
} from "../src/settings/reference";

const doc = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../../../docs/configuration.md",
);
const before = fs.readFileSync(doc, "utf8");
const after = withReferenceTable(before);
if (!before.includes(REFERENCE_START) || !before.includes(REFERENCE_END)) {
   throw new Error(`${doc} has no generated settings table to rewrite`);
}
if (after !== before) fs.writeFileSync(doc, after);
console.log(after === before ? `${doc} is up to date` : `Updated ${doc}`);
