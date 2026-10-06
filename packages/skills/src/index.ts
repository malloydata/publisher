// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The skill files are this package's payload; these exports exist so a consumer
 * can find them without hardcoding a node_modules layout.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import { skillsDir } from "./payload.js";

/** A skill's frontmatter, plus where its files live. */
export interface Skill {
   name: string;
   description: string;
   /** Absolute path to the skill's directory: SKILL.md and any reference/. */
   dir: string;
}

function unquote(value: string): string {
   return value.trim().replace(/^["']|["']$/g, "");
}

/**
 * Read every skill's frontmatter from the shipped files.
 *
 * Parsed on demand rather than baked into a generated manifest, so there is no
 * second copy of this metadata to regenerate or to drift.
 */
export function listSkills(): Skill[] {
   const skills: Skill[] = [];
   for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const dir = path.join(skillsDir, entry.name);
      const skillFile = path.join(dir, "SKILL.md");
      if (!fs.existsSync(skillFile)) continue;
      const markdown = fs
         .readFileSync(skillFile, "utf8")
         .replace(/\r\n/g, "\n");
      const frontmatter = markdown.match(/^---\n([\s\S]*?)\n---/)?.[1] ?? "";
      skills.push({
         name: unquote(frontmatter.match(/^name:\s*(.+)$/m)?.[1] ?? entry.name),
         description: unquote(
            frontmatter.match(/^description:\s*(.+)$/m)?.[1] ?? "",
         ),
         dir,
      });
   }
   // Codepoint order, not localeCompare: the result is compared across
   // platforms and must not depend on the runtime's locale.
   return skills.sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   );
}

/**
 * The installer is re-exported here so `@malloy-publisher/skills` has one entry
 * point: consumers get `skillsDir` to read from and `installSkills` to write
 * with, without reaching into a subpath.
 */
export { skillsDir } from "./payload.js";
export {
   countSkills,
   installSkills,
   isWithinDirectory,
   type SkillInstall,
} from "./install.js";
