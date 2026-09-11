// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";

/**
 * The scaffolder command must carry `@latest` everywhere it is written down.
 *
 * Without it, `npm create` and `npx` resolve an unversioned name through npm's
 * npx cache and any copy already there satisfies it, so a machine that has run
 * the command before never asks the registry: the user silently scaffolds from
 * whatever version they first installed. registry_check.ts has the measurement
 * and the reasoning.
 *
 * Every one of those sites also carries a sentence saying why the version is
 * there, because a bare `@latest` reads as noise. But the failure mode is not
 * someone disagreeing with the sentence, it is someone tidying a README and
 * dropping the suffix without ever reading it, and prose does not survive that.
 * This test does. It reaches the two READMEs, the templates and the skill, which
 * no unit test over this package's output can.
 */
const PACKAGE_ROOT = path.resolve(import.meta.dir, "..");
const REPO_ROOT = path.resolve(PACKAGE_ROOT, "..", "..");

/**
 * Repo-relative path with forward slashes on every platform.
 *
 * `path.relative` gives backslashes on Windows, so comparing its output against
 * a written-out path matches on macOS and Linux and fails on Windows. Every
 * comparison and every reported path goes through here.
 */
const repoRelative = (file: string): string =>
   path.relative(REPO_ROOT, file).split(path.sep).join("/");

/**
 * How the command can appear, and what has to follow it.
 *
 * Built fresh per use rather than shared: a global regex carries `lastIndex`
 * between calls, so one reused across files silently skips matches in some of
 * them, which here would read as "everything is pinned".
 */
const INVOCATION_SOURCE =
   "(npm create @malloy-publisher/malloy-package|npx @malloy-publisher/create-malloy-package)(@[^\\s`]+)?";
const invocations = (s: string): RegExpMatchArray[] => [
   ...s.matchAll(new RegExp(INVOCATION_SOURCE, "g")),
];

/**
 * Lines that name the bare command on purpose, because they are describing the
 * broken form rather than telling anyone to run it.
 *
 * Held as an allowlist rather than skipped by pattern: each of these is a
 * deliberate exception and should have to be re-justified if it moves. A naive
 * "no bare form anywhere" rule fails on the very sentences doing the warning,
 * which is the same trap the xlsx template's `::number.sum()` assertion hit.
 */
const NAMES_THE_BROKEN_FORM_ON_PURPOSE = [
   // The docstring explaining what goes wrong without the pin.
   "resolves an unversioned name",
   // Why a `workspace:*` dep in the manifest breaks the published scaffolder.
   "with EUNSUPPORTEDPROTOCOL",
];

function scannedFiles(): string[] {
   const files: string[] = [];
   const dir = (root: string, keep: (f: string) => boolean): void => {
      if (!fs.existsSync(root)) return;
      for (const f of fs.readdirSync(root).sort()) {
         const full = path.join(root, f);
         if (fs.statSync(full).isFile() && keep(f)) files.push(full);
      }
   };
   // Whole directories, not a hand-listed set, so a new template or module is
   // covered the day it lands rather than the day someone remembers this file.
   dir(path.join(PACKAGE_ROOT, "src"), (f) => f.endsWith(".ts"));
   dir(path.join(PACKAGE_ROOT, "templates"), () => true);
   dir(path.join(PACKAGE_ROOT, "scripts"), (f) => f.endsWith(".ts"));
   for (const rel of [
      path.join(PACKAGE_ROOT, "README.md"),
      // The command is documented outside this package too, and those copies are
      // just as easy to tidy. The npm-published packages/skills copy and the MCP
      // skills_bundle.json are both generated from skills/, and the server's
      // skills_bundle.spec.ts asserts that, so the source tree is enough.
      path.join(REPO_ROOT, "README.md"),
      path.join(REPO_ROOT, "skills", "malloy-getting-started", "SKILL.md"),
   ]) {
      if (fs.existsSync(rel)) files.push(rel);
   }
   return files;
}

describe("the scaffolder command is pinned wherever it is written down", () => {
   test("every invocation carries a version, or is an allowlisted exception", () => {
      const unpinned: string[] = [];
      for (const file of scannedFiles()) {
         // This file names the bare form throughout, in the regex and in the
         // prose above it, and asserting against itself proves nothing.
         if (file === import.meta.path) continue;
         const rel = repoRelative(file);
         const lines = fs.readFileSync(file, "utf8").split("\n");
         for (const [i, line] of lines.entries()) {
            if (
               NAMES_THE_BROKEN_FORM_ON_PURPOSE.some((s) => line.includes(s))
            ) {
               continue;
            }
            for (const m of invocations(line)) {
               if (m[2] === undefined) {
                  unpinned.push(`${rel}:${i + 1}: ${line.trim()}`);
               }
            }
         }
      }
      // Named rather than counted, so a failure says which line to fix.
      expect(unpinned).toEqual([]);
   });

   test("the scan actually reaches the files it claims to", () => {
      // Without this the test above passes just as well on an empty file list,
      // which is how a path that quietly stops resolving goes unnoticed.
      const scanned = scannedFiles().map(repoRelative);
      for (const expected of [
         "README.md",
         "packages/create-malloy-package/README.md",
         "packages/create-malloy-package/src/index.ts",
         "packages/create-malloy-package/src/scaffold.ts",
         "packages/create-malloy-package/templates/AGENTS.md",
         "skills/malloy-getting-started/SKILL.md",
      ]) {
         expect(scanned).toContain(expected);
      }
      // Guard the match count too: a regex that stopped matching anything would
      // make the test above vacuously green over a full file list.
      const withCommand = scannedFiles().filter(
         (f) =>
            f !== import.meta.path &&
            invocations(fs.readFileSync(f, "utf8")).length > 0,
      );
      expect(withCommand.length).toBeGreaterThanOrEqual(4);
   });

   test("the allowlist names only lines that still exist", () => {
      // A stale entry here is a hole: it goes on excusing a line that has since
      // been reworded into a real, unpinned instruction.
      const corpus = scannedFiles()
         .filter((f) => f !== import.meta.path)
         .map((f) => fs.readFileSync(f, "utf8"))
         .join("\n");
      for (const entry of NAMES_THE_BROKEN_FORM_ON_PURPOSE) {
         expect(corpus).toContain(entry);
      }
   });
});
