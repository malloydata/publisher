// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What must not reach the published package, in one place.
 *
 * The copy, the pack audit, and the tests all have to agree exactly. When they
 * drift, one of them silently permits what another forbids: a filter that only
 * the copy applies makes the audit demand a file that will never be there, and
 * a filter only the audit applies lets the copy ship it.
 */
import * as path from "node:path";
// By path: the published package cannot depend on the server, and `tsconfig.build.json` never builds scripts/.
import { buildTimeSkillFiles } from "../../server/src/service/skills/skill_files";

/** Path segments of a forward-slash or platform-separated relative path. */
function segments(relative: string): string[] {
   return relative.split(/[\\/]/).filter(Boolean);
}

/**
 * `credible-*` skills are Credible-platform-specific and must never be
 * published (see skills/README.md).
 *
 * Case-insensitive, and matched at every depth: an npm tarball cannot be
 * unpublished, so this errs toward excluding a file it should not rather than
 * shipping one it must not.
 */
export function isCredible(relative: string): boolean {
   return segments(relative).some((segment) =>
      segment.toLowerCase().startsWith("credible-"),
   );
}

/**
 * Editor debris. It must not enter the copy or the audit's expectations: npm
 * strips `npm-debug.log` from every tarball whatever "files" says, so a stray
 * one would be reported as a missing skill file, which is what the audit says
 * when the copy has catastrophically broken.
 *
 * Dotfiles are not listed here: `skillFiles` never returns one.
 */
export function isNeverPacked(relative: string): boolean {
   const name = path.basename(relative);
   return name.endsWith(".orig") || name === "npm-debug.log";
}

/**
 * skills/README.md documents the internal upstream-sync process, which is not
 * what a consumer of this package needs. The package ships its own README.
 */
export function isSourceReadme(relative: string): boolean {
   return relative === "README.md";
}

/** True if a path relative to skills/ must not reach the package. */
export function isExcluded(relative: string): boolean {
   return (
      isCredible(relative) ||
      isNeverPacked(relative) ||
      isSourceReadme(relative)
   );
}

/** A skill's files that reach the package: no skip beyond `isExcluded`, because other clients (Claude Code, `npx skills`) read the folder whole. */
export function packedSkillFiles(skillsDir: string, name: string): string[] {
   return buildTimeSkillFiles(path.join(skillsDir, name)).filter(
      (file) => !isExcluded(`${name}/${file}`),
   );
}
