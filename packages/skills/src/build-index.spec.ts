// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
   INDEX_GROUPS,
   INDEX_NAME,
   buildIndex,
   indexPath,
   listSkillFrontmatter,
   repoSkillsDir,
} from "../scripts/build-index";

describe("skills/malloy/SKILL.md (generated index)", () => {
   it("is in sync with the generator", () => {
      // Regenerate with:
      //   bun run --cwd packages/skills build-index
      expect(fs.readFileSync(indexPath, "utf8")).toBe(
         buildIndex(repoSkillsDir),
      );
   });

   it("groups every skill except the index itself", () => {
      const onDisk = listSkillFrontmatter(repoSkillsDir)
         .map((s) => s.name)
         .filter((name) => name !== INDEX_NAME);
      const grouped = INDEX_GROUPS.flatMap((g) => g.skills);
      expect([...grouped].sort()).toEqual([...onDisk].sort());
   });

   it("fails when a skill is missing from INDEX_GROUPS", () => {
      // A throw here is what keeps a new skill from shipping unlisted. The
      // sync assertion above would pass for free if buildIndex ever stopped
      // checking coverage, so this is the positive control.
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "malloy-index-"));
      fs.cpSync(repoSkillsDir, tmp, { recursive: true });
      fs.mkdirSync(path.join(tmp, "malloy-ungrouped-fixture"));
      fs.writeFileSync(
         path.join(tmp, "malloy-ungrouped-fixture", "SKILL.md"),
         "---\nname: malloy-ungrouped-fixture\ndescription: Fixture.\n---\n",
      );
      try {
         expect(() => buildIndex(tmp)).toThrow(/malloy-ungrouped-fixture/);
      } finally {
         fs.rmSync(tmp, { recursive: true, force: true });
      }
   });
});
