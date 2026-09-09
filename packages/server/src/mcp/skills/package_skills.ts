// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import * as path from "path";
import {
   parseReference,
   parseSkill,
   referenceFiles,
   referencePointer,
   type SkillEntry,
} from "./build_skills_bundle";

/** The directory a package keeps its own agent skills in. */
export const PACKAGE_SKILLS_DIR = "skills";

/** Where a skill came from, once the bundled and package sets are resolved. */
export type SkillOrigin = "bundled" | "package";

export interface ResolvedSkill extends SkillEntry {
   origin: SkillOrigin;
}

export interface PackageSkills {
   skills: SkillEntry[];
   /**
    * Package-relative paths of every file read, so the caller can fold them
    * into the package's content hash. Produced by the same walk that produced
    * `skills`, so the two cannot drift.
    */
   paths: string[];
   /**
    * Non-fatal findings. A package that fails to load is skipped entirely, and
    * losing a working model over a malformed markdown file is the wrong trade,
    * so nothing here throws.
    */
   warnings: string[];
}

/**
 * Read the skills a package ships, from `<package>/skills/<name>/SKILL.md`
 * plus that skill's optional `reference/*.md`.
 *
 * Parsing is shared with the repo's own skills (`parseSkill`, `parseReference`)
 * so one definition decides what a SKILL.md means. Selection deliberately is
 * NOT shared: `buildSkills` gates on `manifests/publisher-local.json`, which
 * answers "what does this server ship" and has no counterpart in a package. A
 * package ships what it contains, and the absence of a manifest is why this
 * cannot simply call `buildSkills`.
 *
 * The repo's `credible-*` exclusion is likewise not applied. That rule exists
 * because those directories are a gitignored local install target inside this
 * repo; a customer's package has no such convention, and silently dropping a
 * skill because of its name would be a trap.
 */
export function readPackageSkills(packagePath: string): PackageSkills {
   const root = path.join(packagePath, PACKAGE_SKILLS_DIR);
   const skills: SkillEntry[] = [];
   const paths: string[] = [];
   const warnings: string[] = [];

   let entries: fs.Dirent[];
   try {
      if (!fs.statSync(root).isDirectory()) return { skills, paths, warnings };
      entries = fs.readdirSync(root, { withFileTypes: true });
   } catch {
      // No skills/ directory is the common case, not a problem to report.
      return { skills, paths, warnings };
   }

   const seen = new Map<string, string>();
   for (const entry of [...entries].sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   )) {
      if (!entry.isDirectory() || entry.name.startsWith(".")) continue;
      const skillDir = path.join(root, entry.name);
      const skillFile = path.join(skillDir, "SKILL.md");
      let raw: string;
      try {
         raw = fs.readFileSync(skillFile, "utf8");
      } catch {
         warnings.push(
            `Package skill directory '${PACKAGE_SKILLS_DIR}/${entry.name}' has no readable SKILL.md, so it is not served. Fix: add ${PACKAGE_SKILLS_DIR}/${entry.name}/SKILL.md, or remove the directory.`,
         );
         continue;
      }

      const skill = parseSkill(raw, entry.name);
      if (!skill.description) {
         // The description is what a caller reads before deciding to fetch the
         // body, so a skill without one is invisible in every listing.
         warnings.push(
            `Package skill '${skill.name}' has no description in its frontmatter, so nothing tells a caller when to read it. Fix: add a one-line 'description:' to ${PACKAGE_SKILLS_DIR}/${entry.name}/SKILL.md.`,
         );
      }
      const previous = seen.get(skill.name);
      if (previous) {
         warnings.push(
            `Package skills '${previous}' and '${entry.name}' both declare the name '${skill.name}'; only '${previous}' is served. Fix: give each skill a distinct 'name:'.`,
         );
         continue;
      }
      seen.set(skill.name, entry.name);

      const refFiles = referenceFiles(skillDir);
      if (refFiles.length > 0) {
         skill.body = `${skill.body}\n\n${referencePointer(skill.name, refFiles)}`;
      }
      skills.push(skill);
      paths.push(`${PACKAGE_SKILLS_DIR}/${entry.name}/SKILL.md`);

      for (const file of refFiles) {
         const relative = `${PACKAGE_SKILLS_DIR}/${entry.name}/reference/${file}`;
         try {
            skills.push(
               parseReference(
                  fs.readFileSync(
                     path.join(skillDir, "reference", file),
                     "utf8",
                  ),
                  skill.name,
                  file,
               ),
            );
            paths.push(relative);
         } catch {
            warnings.push(
               `Package skill reference '${relative}' could not be read, so it is not served.`,
            );
         }
      }
   }

   return { skills, paths, warnings };
}

/**
 * The skills in force for one package: the bundled set, with the package's own
 * skills shadowing any bundled skill of the same name and adding the rest.
 *
 * Shadowing by name is the whole mechanism. Publisher does not know which
 * skills the calling agent already holds, so it cannot patch or merge one; it
 * can only decide what to return when asked for a name. A package that wants
 * to change how an agent uses IT ships a file under that agent-facing name, and
 * gets its version back.
 *
 * `origin` rides on every entry because a replaced base skill is otherwise
 * indistinguishable from the original, which is exactly the thing a reader
 * debugging an agent's behaviour needs to see.
 */
export function resolveSkills(
   bundled: SkillEntry[],
   packageSkills: SkillEntry[],
): ResolvedSkill[] {
   const shadows = new Map(packageSkills.map((s) => [s.name, s]));
   const bundledNames = new Set(bundled.map((s) => s.name));
   // Bundled order first, each entry replaced in place by the package's copy
   // when there is one, then the package's new names in their own order. Two
   // packages' listings therefore line up row for row wherever they agree.
   return [
      ...bundled.map((s): ResolvedSkill => {
         const shadow = shadows.get(s.name);
         return shadow
            ? { ...shadow, origin: "package" }
            : { ...s, origin: "bundled" };
      }),
      ...packageSkills
         .filter((s) => !bundledNames.has(s.name))
         .map((s): ResolvedSkill => ({ ...s, origin: "package" })),
   ];
}
