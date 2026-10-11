// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SkillNotFoundError } from "../errors";
import { BUNDLED_SKILLS } from "../mcp/skills/package_skills";
import type { EnvironmentStore } from "../service/environment_store";
import { SkillController } from "./skill.controller";

const bundled = BUNDLED_SKILLS[0]!;

const storeWith = (
   skills: Array<{ name: string; description: string; body: string }>,
): EnvironmentStore =>
   ({
      getEnvironment: async () => ({
         getPackage: async () => ({ listSkills: () => skills }),
      }),
   }) as unknown as EnvironmentStore;

describe("SkillController skills", () => {
   it("lists the bundled set with origin when the package ships none", async () => {
      const listed = await new SkillController(storeWith([])).listSkills(
         "env",
         "pkg",
      );
      expect(listed).toHaveLength(BUNDLED_SKILLS.length);
      expect(listed[0]).toEqual({
         name: bundled.name,
         description: bundled.description,
         origin: "bundled",
      });
      expect(JSON.stringify(listed)).not.toContain('"body"');
   });

   it("lets a package skill replace a bundled one and add a new one, marked by origin", async () => {
      const controller = new SkillController(
         storeWith([
            { name: bundled.name, description: "ours", body: "OUR BODY" },
            { name: "house-rules", description: "new", body: "RULES" },
         ]),
      );
      const listed = await controller.listSkills("env", "pkg");
      expect(listed.find((s) => s.name === bundled.name)).toEqual({
         name: bundled.name,
         description: "ours",
         origin: "package",
      });
      expect(listed.at(-1)).toEqual({
         name: "house-rules",
         description: "new",
         origin: "package",
      });
      expect(await controller.getSkill("env", "pkg", bundled.name)).toEqual({
         name: bundled.name,
         description: "ours",
         content: "OUR BODY",
         origin: "package",
      });
   });

   it("fetches a bundled skill's body and a slash-named reference entry", async () => {
      const controller = new SkillController(storeWith([]));
      const skill = await controller.getSkill("env", "pkg", bundled.name);
      expect(skill).toEqual({
         name: bundled.name,
         description: bundled.description,
         content: bundled.body,
         origin: "bundled",
      });
      const reference = BUNDLED_SKILLS.find((s) => s.name.includes("/"))!;
      expect(
         (await controller.getSkill("env", "pkg", reference.name)).content,
      ).toBe(reference.body);
   });

   it("throws SkillNotFoundError for a name nothing defines", async () => {
      await expect(
         new SkillController(storeWith([])).getSkill("env", "pkg", "nope"),
      ).rejects.toBeInstanceOf(SkillNotFoundError);
   });
});
