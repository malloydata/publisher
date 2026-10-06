// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { countSkills, skillsDir } from "@malloy-publisher/skills";
import { ScaffoldError } from "./errors";

/**
 * Fail early when the skills package shipped nothing. Installing zero skills used
 * to print `✓ Installed 0 Malloy skills` and exit 0, which reads as success for a
 * workspace whose whole point is that the skills are there.
 *
 * The copier itself lives in `@malloy-publisher/skills` so the `malloy-skills`
 * CLI and this scaffolder install through the same hardened code. This check
 * stays here because it is the one part that raises a user-facing
 * `ScaffoldError`, and because the remedy it names is specific to this package.
 */
export function assertSkillsAvailable(): void {
   if (countSkills(skillsDir) === 0) {
      throw new ScaffoldError(
         `No Malloy skills found in ${skillsDir}. The @malloy-publisher/skills ` +
            `install looks incomplete; reinstall create-malloy-package and run again.`,
      );
   }
}
