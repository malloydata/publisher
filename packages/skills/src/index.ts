// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The skill files are this package's payload; these exports exist so a consumer
 * can find them without hardcoding a node_modules layout.
 */
import { skillsDir } from "./payload.js";
import { readSkills, type Skill } from "./skill_frontmatter.js";

export type { Skill };

/** Read every skill's frontmatter from the shipped files, in name order. */
export function listSkills(): Skill[] {
   return readSkills(skillsDir);
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
