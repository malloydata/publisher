// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Writes the two full-view REST replies of the skill-directory fixture
 * (src/service/skills/__fixtures__/skill-directory) into the directory it is
 * given, for a consumer that pins them in a golden check:
 *
 *   bun scripts/generate-skill-directory-replies.ts <out-dir>
 *
 * `source.servedRevision` is different on every run, so a check ignores it.
 */
import fs from "fs";
import path from "path";
import { skillDirectoryReplies } from "../src/test_helpers/skill_directory_replies";

const out = process.argv[2];
if (out === undefined) {
   throw new Error("Usage: generate-skill-directory-replies.ts <out-dir>");
}

const { skillSet, workflow } = await skillDirectoryReplies();
fs.mkdirSync(out, { recursive: true });
for (const [name, reply] of [
   ["publisher_skill_directory_skill_set.json", skillSet],
   ["publisher_skill_directory_definition.json", workflow],
] as const) {
   fs.writeFileSync(
      path.join(out, name),
      `${JSON.stringify(reply, null, 2)}\n`,
   );
   console.log(`Wrote ${path.join(out, name)}`);
}
