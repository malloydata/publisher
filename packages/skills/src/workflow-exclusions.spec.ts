// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The contract between `scripts/publish-packages.sh`'s `skills_diff_status`
 * and `scripts/exclusions.ts`.
 *
 * exclusions.ts's own docstring states the rule this enforces: "The copy, the
 * pack audit, and the tests all have to agree exactly. When they drift, one of
 * them silently permits what another forbids." `skills_diff_status` is now a
 * FOURTH reader of that list: at release time it diffs this checkout against
 * npm latest's recorded gitHead, and its own `EXCLUDE=(...)` subtracts the
 * unpublishable files from that diff, so that editing one does not make the
 * release think published content changed when it did not — and it was the
 * only reader with no test behind it.
 *
 * The drift that matters is not the obvious direction. If someone decides
 * `skills/README.md` SHOULD ship (drops `isSourceReadme`, or renames the file)
 * and `skills_diff_status` keeps excluding it, a README-only change then really
 * does change the tarball while the release's diff reports "unchanged". The
 * release skips publishing, and the version already on npm silently keeps
 * shipping stale content under the name a reader thinks is current — the exact
 * failure this exclusion list exists to close, re-entering through the
 * exclusion itself.
 *
 * Reading a sibling shell script from a unit test is ugly. It is also the only
 * thing that makes this contract fail loudly, and it costs one file read.
 */
import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import { isExcluded } from "../scripts/exclusions";

const PUBLISH_SCRIPT = path.join(
   import.meta.dir,
   "..",
   "..",
   "..",
   "scripts",
   "publish-packages.sh",
);

/**
 * The `:!<path>` entries of publish-packages.sh's top-level `EXCLUDE=(...)`
 * array, shared by `skills_diff_status` (the release-time content diff) and
 * `publish_resolved` (documented there as deliberately NOT re-applied to its
 * own "main moved" file matching).
 *
 * Deliberately strict about finding it. A rename of the variable, or the
 * array moving to a form this cannot read, must fail rather than quietly
 * return an empty list — an empty list would pass every assertion below and
 * switch this test off for good, which is the same failure mode the script's
 * own watched-path assertions exist to prevent.
 */
function excludePathspecs(script: string): string[] {
   const line = /^EXCLUDE=\(([^)]*)\)\s*$/m.exec(script);
   expect(
      line,
      "scripts/publish-packages.sh no longer has a single-line top-level " +
         "`EXCLUDE=(...)` array; this test cannot verify the contract it " +
         "exists for",
   ).not.toBeNull();

   const specs = Array.from(line![1].matchAll(/'([^']*)'|"([^"]*)"/g)).map(
      (m) => m[1] ?? m[2],
   );
   expect(specs.length, "EXCLUDE=(...) parsed as empty").toBeGreaterThan(0);
   return specs;
}

describe("publish-packages.sh's skills EXCLUDE agrees with exclusions.ts", () => {
   const script = fs.readFileSync(PUBLISH_SCRIPT, "utf8");

   it("skills_diff_status's git diff actually passes EXCLUDE", () => {
      // The array existing is not enough on its own: skills_diff_status has
      // to actually hand it to `git diff`, or excluding a path here does
      // nothing to the release-time content check it exists to fix.
      const fn = /skills_diff_status\(\)\s*\{[\s\S]*?\n\}/.exec(script);
      expect(
         fn,
         "could not find the skills_diff_status() function body",
      ).not.toBeNull();
      expect(
         fn![0],
         'skills_diff_status\'s git diff does not pass "${EXCLUDE[@]}"',
      ).toContain('"${EXCLUDE[@]}"');
   });

   it("excludes only paths the packer actually refuses to ship", () => {
      for (const spec of excludePathspecs(script)) {
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
               `publish-packages.sh excludes ${repoPath} from its release-time content diff, but exclusions.ts would PACK it. ` +
                  `A change to that file reaches the published tarball while the diff reports "unchanged since npm latest's gitHead", ` +
                  `so the release decides there is nothing to publish and skills quietly ships stale content.`,
            ).toBe(true);
            continue;
         }

         if (repoPath.startsWith("packages/skills/")) {
            // Nothing under packages/skills/ is packed directly: `files` ships
            // `dist`, which tsc emits from src/ minus its own exclude list. So
            // the authority here is tsconfig.build.json, and the same drift
            // applies — start emitting specs into dist/ and this exclusion
            // silently stops the diff noticing a change that ships.
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
            // with the diff still excluding all of it. Measured: under that
            // edit this file passed 2/0 while dist/ gained
            // workflow-exclusions.spec.js. Removing the glob is the mutation
            // that is easy to imagine; narrowing it is the one someone actually
            // makes. Brittle in the fail-CLOSED direction on purpose, the same
            // trade `excludePathspecs` makes about the array's exact shape.
            expect(
               excludes,
               `publish-packages.sh excludes ${repoPath} from its release-time content diff because specs are not built into dist/, ` +
                  `but tsconfig.build.json's exclude no longer contains exactly "src/**/*.spec.ts" — so a spec under src/ may now be ` +
                  `emitted into dist/ and published while the diff still reports "unchanged".`,
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
      // but the diff still watches only makes the release re-publish content
      // that never changed. Scoped to the top level of skills/, because that
      // is where a whole-file exclusion like README.md lives; `credible-*` is
      // asserted absent from this repo elsewhere, and dotfiles are not
      // content.
      const excluded = new Set(
         excludePathspecs(script).map((spec) => spec.slice(2)),
      );
      const skillsDir = path.join(import.meta.dir, "..", "..", "..", "skills");

      for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
         if (!entry.isFile() || entry.name.startsWith(".")) continue;
         if (!isExcluded(entry.name)) continue;
         expect(
            excluded.has(`skills/${entry.name}`),
            `exclusions.ts keeps skills/${entry.name} out of the tarball, but publish-packages.sh's release-time diff still watches it, ` +
               `so editing it looks like published content changed when it never reaches the tarball.`,
         ).toBe(true);
      }
   });
});
