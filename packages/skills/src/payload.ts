// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where the shipped skill files are.
 *
 * Its own module, rather than a constant in index.ts, because both the reader
 * (`listSkills`) and the writer (`installSkills`) need it and index.ts re-exports
 * the writer: importing it from there would make the two modules a cycle.
 */
import * as path from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Absolute path to the directory holding the skill directories.
 *
 * Resolved from this module's own URL, which works both from dist/ once
 * installed and from src/ in the repo, since both sit one level under the
 * package root.
 */
export const skillsDir: string = path.join(
   path.dirname(fileURLToPath(import.meta.url)),
   "..",
   "skills",
);
