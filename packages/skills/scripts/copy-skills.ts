// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Copy the repo's top-level skills/ into this package so npm can pack it.
 *
 * npm cannot pack above the package directory, and a symlink named in "files"
 * packs as nothing at all (you get a tarball holding package.json, exit 0, no
 * warning), so the files have to physically exist here at pack time. Runs from
 * prepack; the copy is gitignored.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { isExcluded } from "./exclusions";
import { locateFrontmatterClose } from "./frontmatter";
import { manifestPath, manifestSkillNames } from "./manifest";

const packageDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const source = path.join(packageDir, "..", "..", "skills");
const destination = path.join(packageDir, "skills");

// What ships is the manifest, not whatever happens to be in skills/.
//
// The manifest is exhaustive by construction, not a way to hold a skill back:
// manifest.spec.ts fails on any skill in the tree the manifest omits, and
// check-pack.ts fails the publish on the same state. So this filter never
// subtracts today. It earns its keep by making the set stated rather than
// incidental -- registering a skill is one line, forgetting is a red build --
// and by giving `groups` something to name.
const shipping = new Set(manifestSkillNames());
const missing = [...shipping].filter(
   (name) => !fs.existsSync(path.join(source, name, "SKILL.md")),
);
if (missing.length > 0) {
   console.error(
      `${manifestPath} names ${missing.length} skill(s) with no SKILL.md in ` +
         `${source}: ${missing.join(", ")}`,
   );
   process.exit(1);
}

fs.rmSync(destination, { recursive: true, force: true });
fs.cpSync(source, destination, {
   recursive: true,
   filter: (from: string) => {
      const relative = path.relative(source, from);
      if (relative === "") return true;
      if (isExcluded(relative)) return false;
      // Keep the manifest's skills and everything under them; drop the rest.
      // path.sep rather than "/" so this holds on Windows, which the
      // cross-platform job actually runs.
      const [top] = relative.split(path.sep);
      return shipping.has(top);
   },
});

const copied = fs
   .readdirSync(destination, { withFileTypes: true })
   .filter((entry) =>
      fs.existsSync(path.join(destination, entry.name, "SKILL.md")),
   );

if (copied.length === 0) {
   console.error(`No skills found in ${source}`);
   process.exit(1);
}

// Stamp each SKILL.md's frontmatter with the package version at pack time, so
// a copy installed months ago is identifiable on disk. Without this, a stale
// install and a current one are byte-identical in every way that matters to a
// reader, and a QA session lost real time to skills that named tools the
// server no longer serves with nothing saying how old they were (field notes
// F3). Pack-time only: the repo's source skills stay unstamped so the
// upstream-sync copies remain byte-identical in both directions.
const version = (
   JSON.parse(
      fs.readFileSync(path.join(packageDir, "package.json"), "utf8"),
   ) as { version: string }
).version;
const unstampable: string[] = [];
for (const entry of copied) {
   const skillFile = path.join(destination, entry.name, "SKILL.md");
   const text = fs.readFileSync(skillFile, "utf8");
   // Insert into the existing frontmatter block rather than appending a second
   // one. Both failures below are the same shape: we cannot say where the
   // frontmatter is, or a `version:` is already there and a second one would
   // make the block a duplicate-key YAML error that every strict parser
   // rejects — which would take the skill out of a host entirely, a far worse
   // outcome than the staleness this stamp exists to expose. Upstream owns
   // these files (skills/README.md), so a synced skill can grow its own
   // `version:` without anyone here noticing; fail the pack instead.
   const location = locateFrontmatterClose(text);
   if (!location) {
      unstampable.push(`${entry.name} (no frontmatter block)`);
      continue;
   }
   const { index: close, newline } = location;
   if (/^version:/m.test(text.slice(0, close))) {
      unstampable.push(`${entry.name} (frontmatter already declares version)`);
      continue;
   }
   fs.writeFileSync(
      skillFile,
      `${text.slice(0, close)}${newline}version: ${version}${text.slice(close)}`,
   );
}

if (unstampable.length > 0) {
   console.error(
      `Cannot stamp ${unstampable.length} of ${copied.length} skills with ` +
         `version ${version}: ${unstampable.join(", ")}`,
   );
   process.exit(1);
}

console.log(
   `Copied and stamped ${copied.length} skills with version ${version} to ${destination}`,
);
