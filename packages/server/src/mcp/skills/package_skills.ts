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
import { stepsOutside } from "../../service/package_retrieval";

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

/** Largest single skill file served. A markdown guide never needs more. */
export const PACKAGE_SKILL_FILE_MAX_BYTES = 256 * 1024;

/**
 * A file's text, or why it cannot be served. The real path must sit inside the
 * real package root: a symlink in a package can point anywhere, and this text
 * is returned over unauthenticated MCP and REST.
 */
export function readFileInside(
   realRoot: string,
   file: string,
   maxBytes: number,
): { text: string } | { problem: string } {
   try {
      const real = fs.realpathSync(file);
      if (stepsOutside(path.relative(realRoot, real))) {
         return { problem: "resolves outside the package through a link" };
      }
      const { size } = fs.statSync(real);
      if (size > maxBytes) {
         return { problem: `is ${size} bytes, over the ${maxBytes} byte cap` };
      }
      return { text: fs.readFileSync(real, "utf8") };
   } catch (error) {
      return { problem: `cannot be read (${(error as Error).message})` };
   }
}

/**
 * Read the skills under `<package>/<dirRel>/<name>/SKILL.md` plus each skill's
 * optional `reference/*.md`. The package's own `skills/` and an agent's skill
 * roots are both read through here.
 *
 * Parsing is shared with the repo's own skills (`parseSkill`, `parseReference`)
 * so one definition decides what a SKILL.md means. Selection deliberately is
 * NOT shared: `buildSkills` gates on `manifests/publisher-local.json`, which
 * answers "what does this server ship" and has no counterpart in a package. A
 * package ships what it contains, and the repo's `credible-*` exclusion is
 * likewise not applied: it exists for a gitignored install target inside this
 * repo, and silently dropping a customer's skill by name would be a trap.
 */
export function readSkillsDir(
   packagePath: string,
   dirRel: string = PACKAGE_SKILLS_DIR,
): PackageSkills {
   const root = path.join(packagePath, dirRel);
   const skills: SkillEntry[] = [];
   const paths: string[] = [];
   const warnings: string[] = [];

   let realRoot: string;
   let entries: fs.Dirent[];
   try {
      realRoot = fs.realpathSync(packagePath);
      if (!fs.statSync(root).isDirectory()) return { skills, paths, warnings };
      entries = fs.readdirSync(root, { withFileTypes: true });
   } catch {
      // No skills directory is the common case, not a problem to report.
      return { skills, paths, warnings };
   }
   if (stepsOutside(path.relative(realRoot, fs.realpathSync(root)))) {
      warnings.push(
         `Skills directory '${dirRel}' resolves outside the package through a link, so it is not served. Fix: copy the skills into the package.`,
      );
      return { skills, paths, warnings };
   }

   const seen = new Map<string, string>();
   for (const entry of [...entries].sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   )) {
      if (entry.name.startsWith(".")) continue;
      const skillDir = path.join(root, entry.name);
      try {
         if (!fs.statSync(skillDir).isDirectory()) continue;
      } catch {
         continue;
      }
      const rel = `${dirRel}/${entry.name}`;
      const read = readFileInside(
         realRoot,
         path.join(skillDir, "SKILL.md"),
         PACKAGE_SKILL_FILE_MAX_BYTES,
      );
      if ("problem" in read) {
         warnings.push(
            `Package skill '${rel}/SKILL.md' ${read.problem}, so it is not served. Fix: add a readable SKILL.md inside the package, or remove the directory.`,
         );
         continue;
      }

      const skill = parseSkill(read.text, entry.name);
      if (!skill.description) {
         // The description is what a caller reads before deciding to fetch the
         // body, so a skill without one is invisible in every listing.
         warnings.push(
            `Package skill '${skill.name}' has no description in its frontmatter, so nothing tells a caller when to read it. Fix: add a one-line 'description:' to ${rel}/SKILL.md.`,
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
      const references: SkillEntry[] = [];
      const refPaths: string[] = [];
      const served: string[] = [];
      for (const file of refFiles) {
         const relative = `${rel}/reference/${file}`;
         const ref = readFileInside(
            realRoot,
            path.join(skillDir, "reference", file),
            PACKAGE_SKILL_FILE_MAX_BYTES,
         );
         if ("problem" in ref) {
            warnings.push(
               `Package skill reference '${relative}' ${ref.problem}, so it is not served.`,
            );
            continue;
         }
         references.push(parseReference(ref.text, skill.name, file));
         refPaths.push(relative);
         served.push(file);
      }
      if (references.length > 0) {
         skill.body = `${skill.body}\n\n${referencePointer(skill.name, served)}`;
      }
      skills.push(skill, ...references);
      paths.push(`${rel}/SKILL.md`, ...refPaths);
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
