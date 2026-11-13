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

/** Text as stored: LF line endings and one trailing newline. */
function canonical(text: string): string {
   return `${text.replace(/\r\n/g, "\n").replace(/\n+$/, "")}\n`;
}

/** Lines of context around each change, and changed lines shown in all. */
const CONTEXT = 3;
const SHOWN = 80;
/** Past this many lines between the common ends, the diff is one block. */
const LCS_LIMIT = 4000;

/** One line of a diff: kept (` `), removed (`-`) or added (`+`). */
type DiffLine = { op: " " | "-" | "+"; text: string; a: number; b: number };

/**
 * The line edit script from `a` to `b`: a longest common subsequence over
 * what lies between the common leading and trailing lines, so two separate
 * changes read as two changes. A middle too long for the table is shown as
 * one removed and one added block.
 */
function editScript(a: string[], b: string[]): DiffLine[] {
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
   const out: DiffLine[] = [];
   for (let i = 0; i < start; i++) {
      out.push({ op: " ", text: a[i], a: i, b: i });
   }
   const n = endA - start;
   const m = endB - start;
   if (n > LCS_LIMIT || m > LCS_LIMIT) {
      for (let i = start; i < endA; i++) {
         out.push({ op: "-", text: a[i], a: i, b: start });
      }
      for (let j = start; j < endB; j++) {
         out.push({ op: "+", text: b[j], a: endA, b: j });
      }
   } else {
      // lcs[i][j]: the common subsequence length of a[start+i..] and b[start+j..].
      const lcs = Array.from({ length: n + 1 }, () =>
         new Uint32Array(m + 1),
      );
      for (let i = n - 1; i >= 0; i--) {
         for (let j = m - 1; j >= 0; j--) {
            lcs[i][j] =
               a[start + i] === b[start + j]
                  ? lcs[i + 1][j + 1] + 1
                  : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
         }
      }
      let i = 0;
      let j = 0;
      while (i < n || j < m) {
         const ai = start + i;
         const bj = start + j;
         if (i < n && j < m && a[ai] === b[bj]) {
            out.push({ op: " ", text: a[ai], a: ai, b: bj });
            i++;
            j++;
         } else if (i < n && (j === m || lcs[i + 1][j] >= lcs[i][j + 1])) {
            out.push({ op: "-", text: a[ai], a: ai, b: bj });
            i++;
         } else {
            out.push({ op: "+", text: b[bj], a: ai, b: bj });
            j++;
         }
      }
   }
   for (let i = endA, j = endB; i < a.length; i++, j++) {
      out.push({ op: " ", text: a[i], a: i, b: j });
   }
   return out;
}

/**
 * The differing lines of two texts as unified-style hunks, each with a few
 * lines of context and its golden and actual line numbers; the changed lines
 * shown are capped across all hunks.
 */
export function lineDiff(expected: string, actual: string): string {
   const script = editScript(expected.split("\n"), actual.split("\n"));
   const changed = script
      .map((line, k) => (line.op === " " ? -1 : k))
      .filter((k) => k >= 0);
   if (changed.length === 0) return "(no line differs)";
   // Group the changes into hunks whose context windows touch.
   const hunks: [number, number][] = [];
   for (const k of changed) {
      const last = hunks[hunks.length - 1];
      if (last && k - last[1] <= 2 * CONTEXT + 1) last[1] = k;
      else hunks.push([k, k]);
   }
   const out: string[] = [
      `${changed.length} line(s) differ in ${hunks.length} place(s):`,
   ];
   let shown = 0;
   for (const [first, last] of hunks) {
      if (shown >= SHOWN) {
         const rest = changed.filter((k) => k >= first).length;
         out.push(`… ${rest} more changed line(s) not shown`);
         break;
      }
      const from = Math.max(0, first - CONTEXT);
      const to = Math.min(script.length - 1, last + CONTEXT);
      out.push(
         `@@ golden line ${script[from].a + 1}, actual line ${script[from].b + 1} @@`,
      );
      for (let k = from; k <= to; k++) {
         const line = script[k];
         if (line.op !== " ") {
            if (shown >= SHOWN) {
               out.push("  …");
               break;
            }
            shown++;
         }
         out.push(`${line.op} ${line.text}`);
      }
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

/**
 * The entries directly under {@link GOLDENS_ROOT} that are not one of the
 * `areas` a spec owns: a golden there, or under a renamed area, would be
 * claimed by no spec and checked by no housekeeping test.
 */
export function unownedEntries(areas: readonly string[]): string[] {
   if (!fs.existsSync(GOLDENS_ROOT)) return [];
   return fs
      .readdirSync(GOLDENS_ROOT)
      .filter((entry) => !areas.includes(entry))
      .sort();
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
