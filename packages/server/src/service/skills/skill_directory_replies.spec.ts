// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { expect, it } from "bun:test";
import { SKILL_MD } from "./__fixtures__/skill-directory/fixture";
import { skillDirectoryReplies } from "../../test_helpers/skill_directory_replies";

it("serves the fixture's workflow with its set exactly as the set's own read does, and its SKILL.md as written", async () => {
   const { skillSet, workflow } = await skillDirectoryReplies();
   const { source, ...own } = skillSet;
   // The same bytes, key order included: a consumer pins one and reads either.
   expect(JSON.stringify(workflow.skill_sets[0])).toBe(JSON.stringify(own));
   expect(source?.definitionSha).toBeUndefined();
   expect(workflow.source.definitionSha).toMatch(/^[0-9a-f]{64}$/);
   expect(
      skillSet.skills.find((f) => f.relative_filepath === "pricing/SKILL.md")
         ?.file_contents,
   ).toBe(SKILL_MD);
});
