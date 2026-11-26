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
import {
   buildTimeSkillFiles,
   isCredible,
   isNeverShipped,
} from "../../server/src/service/skills/skill_files";

export { isCredible };

/**
 * skills/README.md documents the internal upstream-sync process, which is not
 * what a consumer of this package needs. The package ships its own README.
 */
export function isSourceReadme(relative: string): boolean {
   return relative === "README.md";
}

/** True if a path relative to skills/ must not reach the package. */
export function isExcluded(relative: string): boolean {
   return isNeverShipped(relative) || isSourceReadme(relative);
}

/** A skill's files that reach the package: no skip beyond `isNeverShipped`, because other clients (Claude Code, `npx skills`) read the folder whole. */
export function packedSkillFiles(skillsDir: string, name: string): string[] {
   return buildTimeSkillFiles(path.join(skillsDir, name));
}
