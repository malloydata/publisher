// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Golden files for the materialization characterization specs: each actual
 * output is compared with a file under `tests/goldens/materialization/`, and
 * `UPDATE_GOLDENS=1` writes the files instead of comparing them. A mismatch
 * fails with the differing lines and the command that regenerates the file.
 */

import * as fs from "fs";
import * as path from "path";

/** Where the goldens live, under `packages/server`. */
export const GOLDENS_ROOT = path.resolve(
   __dirname,
   "../../goldens/materialization",
);

/** `UPDATE_GOLDENS=1` rewrites every golden a spec produces. */
export const UPDATING = process.env.UPDATE_GOLDENS === "1";

const REGENERATE =
   "cd packages/server && UPDATE_GOLDENS=1 bun test --timeout 200000 tests/component/materialization_goldens";

/** Lines of context and of each side shown in a mismatch. */
const CONTEXT = 3;
const SHOWN = 40;

/** Text as stored: LF line endings and one trailing newline. */
function canonical(text: string): string {
   return `${text.replace(/\r\n/g, "\n").replace(/\n+$/, "")}\n`;
}

/**
 * The differing region of two texts, as a short unified-style hunk: the
 * common leading and trailing lines are trimmed, and what is left is shown
 * with a few lines of context, each side capped.
 */
export function lineDiff(expected: string, actual: string): string {
   const a = expected.split("\n");
   const b = actual.split("\n");
   let start = 0;
   while (start < a.length && start < b.length && a[start] === b[start]) {
      start++;
   }
   let endA = a.length;
   let endB = b.length;
   while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) {
      endA--;
      endB--;
   }
   const from = Math.max(0, start - CONTEXT);
   const out: string[] = [
      `@@ golden lines ${start + 1}-${endA}, actual lines ${start + 1}-${endB} @@`,
   ];
   for (let i = from; i < start; i++) out.push(`  ${a[i]}`);
   const removed = a.slice(start, endA);
   const added = b.slice(start, endB);
   for (const line of removed.slice(0, SHOWN)) out.push(`- ${line}`);
   if (removed.length > SHOWN) {
      out.push(`- … ${removed.length - SHOWN} more`);
   }
   for (const line of added.slice(0, SHOWN)) out.push(`+ ${line}`);
   if (added.length > SHOWN) out.push(`+ … ${added.length - SHOWN} more`);
   for (let i = endA; i < Math.min(a.length, endA + CONTEXT); i++) {
      out.push(`  ${a[i]}`);
   }
   return out.join("\n");
}

/**
 * Compare `actual` with the golden at `relative` (under {@link GOLDENS_ROOT}),
 * or write it when {@link UPDATING}. `undefined` means the output does not
 * exist for this case: the golden must not exist either (an update deletes
 * it).
 */
export function expectGolden(relative: string, actual: string | undefined) {
   const file = path.join(GOLDENS_ROOT, relative);
   if (UPDATING) {
      if (actual === undefined) {
         fs.rmSync(file, { force: true });
         return;
      }
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, canonical(actual));
      return;
   }
   const exists = fs.existsSync(file);
   if (actual === undefined) {
      if (exists) {
         throw new Error(
            `golden ${relative} exists, but this case no longer produces it.\n` +
               `If that is intended, regenerate: ${REGENERATE}`,
         );
      }
      return;
   }
   if (!exists) {
      throw new Error(
         `golden ${relative} is missing. Generate it: ${REGENERATE}`,
      );
   }
   const expected = canonical(fs.readFileSync(file, "utf8"));
   const got = canonical(actual);
   if (expected !== got) {
      throw new Error(
         `golden ${relative} differs from the current output.\n` +
            `${lineDiff(expected, got)}\n` +
            `If the change is intended, regenerate and commit the goldens with ` +
            `a message that says why: ${REGENERATE}`,
      );
   }
}

/**
 * The goldens under `directory` that no case wrote this run, when every case
 * in it ran. Each spec calls it once at the end, so a renamed or deleted case
 * does not leave a stale golden behind.
 */
export function unclaimedGoldens(
   directory: string,
   claimed: ReadonlySet<string>,
): string[] {
   const root = path.join(GOLDENS_ROOT, directory);
   if (!fs.existsSync(root)) return [];
   const out: string[] = [];
   const walk = (dir: string) => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
         const full = path.join(dir, entry.name);
         if (entry.isDirectory()) walk(full);
         else {
            const rel = path
               .relative(GOLDENS_ROOT, full)
               .split(path.sep)
               .join("/");
            if (!claimed.has(rel)) out.push(rel);
         }
      }
   };
   walk(root);
   return out.sort();
}

/** Remove the goldens {@link unclaimedGoldens} found (an update run only). */
export function removeGoldens(relatives: readonly string[]): void {
   for (const rel of relatives) {
      fs.rmSync(path.join(GOLDENS_ROOT, rel), { force: true });
   }
}

/** A file-name-safe slug for a case label. */
export function slug(label: string): string {
   return label
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
}
