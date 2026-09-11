// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The contract between `.github/workflows/skills-npm.yml`'s bump check and
 * `scripts/exclusions.ts`.
 *
 * exclusions.ts's own docstring states the rule this enforces: "The copy, the
 * pack audit, and the tests all have to agree exactly. When they drift, one of
 * them silently permits what another forbids." The workflow's version-bump check
 * is now a FOURTH reader of that list — it subtracts the unpublishable files
 * from its scoping diff, so that editing one does not demand a version bump for
 * a byte that never reaches the tarball — and it was the only reader with no
 * test behind it.
 *
 * The drift that matters is not the obvious direction. If someone decides
 * `skills/README.md` SHOULD ship (drops `isSourceReadme`, or renames the file)
 * and the workflow keeps excluding it, a README-only PR then really does change
 * the tarball while the check reports "no published skills content changed". It
 * merges without a bump, and `publish-packages` finds the version already on
 * npm, skips the package, and the release stays green — the exact failure the
 * bump check exists to close, re-entering through the exclusion that check adds.
 *
 * Reading a sibling workflow from a unit test is ugly. It is also the only thing
 * that makes this contract fail loudly, and it costs one file read.
 */
import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import { isExcluded } from "../scripts/exclusions";

const WORKFLOW = path.join(
   import.meta.dir,
   "..",
   "..",
   "..",
   ".github",
   "workflows",
   "skills-npm.yml",
);

/**
 * The `:!<path>` entries of the check's `EXCLUDE=(...)` array.
 *
 * Deliberately strict about finding it. A rename of the variable, or the array
 * moving to a form this cannot read, must fail rather than quietly return an
 * empty list — an empty list would pass every assertion below and switch this
 * test off for good, which is the same failure mode the workflow's own
 * watched-path assertion exists to prevent.
 */
function excludePathspecs(yaml: string): string[] {
   const line = /^\s*EXCLUDE=\(([^)]*)\)\s*$/m.exec(yaml);
   expect(
      line,
      "skills-npm.yml no longer has a single-line EXCLUDE=(...) array; this test cannot verify the contract it exists for",
   ).not.toBeNull();

   const specs = Array.from(line![1].matchAll(/'([^']*)'|"([^"]*)"/g)).map(
      (m) => m[1] ?? m[2],
   );
   expect(specs.length, "EXCLUDE=(...) parsed as empty").toBeGreaterThan(0);
   return specs;
}

describe("skills-npm.yml's EXCLUDE agrees with exclusions.ts", () => {
   const yaml = fs.readFileSync(WORKFLOW, "utf8");

   it("excludes only paths the packer actually refuses to ship", () => {
      for (const spec of excludePathspecs(yaml)) {
         // git's "exclude this pathspec" form, which is what the diff consumes.
         expect(spec.startsWith(":!"), `${spec} is not a :! pathspec`).toBe(
            true,
         );
         const repoPath = spec.slice(2);

         // TWO governance domains, and an entry in neither is the real failure:
         // it would be an exclusion nothing checks, which is the state this
         // whole file exists to end.
         if (repoPath.startsWith("skills/")) {
            // exclusions.ts works in paths relative to skills/.
            const relative = repoPath.slice("skills/".length);
            expect(
               isExcluded(relative),
               `skills-npm.yml excludes ${repoPath} from its bump check, but exclusions.ts would PACK it. ` +
                  `A change to that file reaches the published tarball while the check reports "no published skills content changed", ` +
                  `so it merges without a version bump and the release silently skips the package.`,
            ).toBe(true);
            continue;
         }

         if (repoPath.startsWith("packages/skills/")) {
            // Nothing under packages/skills/ is packed directly: `files` ships
            // `dist`, which tsc emits from src/ minus its own exclude list. So
            // the authority here is tsconfig.build.json, and the same drift
            // applies — start emitting specs into dist/ and this exclusion
            // silently stops the check noticing a change that ships.
            const buildTsconfig = JSON.parse(
               fs
                  .readFileSync(
                     path.join(import.meta.dir, "..", "tsconfig.build.json"),
                     "utf8",
                  )
                  // tsconfig files allow comments; this one has none today, and
                  // a stray one should fail loudly here rather than silently.
                  .trim(),
            ) as { exclude?: string[] };
            const excludes = buildTsconfig.exclude ?? [];
            // The EXACT glob, not "some glob ending in *.spec.ts". A suffix
            // test passes on a NARROWING — `src/legacy/**/*.spec.ts` still ends
            // that way while `src/*.spec.ts` is emitted into dist/ and ships,
            // with the workflow still excluding all of it from the bump check.
            // Measured: under that edit this file passed 2/0 while dist/ gained
            // workflow-exclusions.spec.js. Removing the glob is the mutation
            // that is easy to imagine; narrowing it is the one someone actually
            // makes. Brittle in the fail-CLOSED direction on purpose, the same
            // trade `excludePathspecs` makes about the array's exact shape.
            expect(
               excludes,
               `skills-npm.yml excludes ${repoPath} from its bump check because specs are not built into dist/, ` +
                  `but tsconfig.build.json's exclude no longer contains exactly "src/**/*.spec.ts" — so a spec under src/ may now be ` +
                  `emitted into dist/ and published while the check still reports "no published skills content changed".`,
            ).toContain("src/**/*.spec.ts");
            continue;
         }

         throw new Error(
            `${repoPath} is under neither skills/ nor packages/skills/, so nothing in this test governs it. ` +
               `An exclusion no test checks is exactly the drift this file exists to prevent.`,
         );
      }
   });

   it("excludes every unpublishable file that is actually in the tree", () => {
      // The other direction, and the cheaper failure: a file the packer drops
      // but the workflow still watches only demands a version bump nobody can
      // justify. Scoped to the top level of skills/, because that is where a
      // whole-file exclusion like README.md lives; `credible-*` is asserted
      // absent from this repo elsewhere, and dotfiles are not content.
      const excluded = new Set(
         excludePathspecs(yaml).map((spec) => spec.slice(2)),
      );
      const skillsDir = path.join(import.meta.dir, "..", "..", "..", "skills");

      for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
         if (!entry.isFile() || entry.name.startsWith(".")) continue;
         if (!isExcluded(entry.name)) continue;
         expect(
            excluded.has(`skills/${entry.name}`),
            `exclusions.ts keeps skills/${entry.name} out of the tarball, but skills-npm.yml's bump check still watches it, ` +
               `so editing it demands a version bump for a byte that is never published.`,
         ).toBe(true);
      }
   });
});
