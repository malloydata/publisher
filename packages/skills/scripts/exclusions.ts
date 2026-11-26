// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** What reaches the published package, in one place: the copy and the pack audit both list a skill's files here, so they cannot drift. */
import * as path from "node:path";
// By path: the published package cannot depend on the server, and `tsconfig.build.json` never builds scripts/.
import { buildTimeSkillFiles } from "../../server/src/service/skills/skill_files";

/** A skill's files that reach the package: no skip beyond `isNeverShipped` (skill_files.ts), because other clients (Claude Code, `npx skills`) read the folder whole. */
export function packedSkillFiles(skillsDir: string, name: string): string[] {
   return buildTimeSkillFiles(path.join(skillsDir, name));
}
