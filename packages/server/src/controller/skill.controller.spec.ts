// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SkillNotFoundError } from "../errors";
import {
   BUNDLED_CATALOG,
   type ServedSkill,
} from "../service/skills/package_skills";
import { SkillController } from "./skill.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type { PackageLookup } from "../service/environment_ports";

const bundled = BUNDLED_CATALOG[0]!;
const bundledDescription = bundled.frontmatter.description as string;

/** A package skill whose files are text. */
const packageSkill = (
   name: string,
   description: string,
   files: Record<string, string>,
): ServedSkill => ({
   name,
   description,
   catalog: {
      name,
      frontmatter: { name, description },
      files: Object.fromEntries(
         Object.entries(files).map(([p, f]) => [
            p,
            { digest: "sha256:0", size: f.length },
         ]),
      ),
   },
   read: async (p) => ({ text: files[p]! }),
});

const storeWith = (skills: ServedSkill[]): PackageLookup =>
   fakeOf<PackageLookup>({
      getEnvironment: async () => ({
         getPackage: async () => ({ listSkills: () => skills }),
      }),
   });

describe("SkillController skills", () => {
   it("lists the bundled set with origin when the package ships none", async () => {
      const listed = await new SkillController(storeWith([])).listSkills(
         "env",
         "pkg",
      );
      expect(listed).toHaveLength(BUNDLED_CATALOG.length);
      expect(listed[0]).toEqual({
         name: bundled.name,
         description: bundledDescription.trim(),
         origin: "bundled",
      });
      expect(JSON.stringify(listed)).not.toContain('"content"');
   });

   it("lets a package skill replace a bundled one and add a new one, marked by origin", async () => {
      const controller = new SkillController(
         storeWith([
            packageSkill(bundled.name, "ours", { "SKILL.md": "OUR BODY" }),
            packageSkill("house-rules", "new", { "SKILL.md": "RULES" }),
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
         path: "SKILL.md",
         description: "ours",
         content: "OUR BODY",
         origin: "package",
         frontmatter: { name: bundled.name, description: "ours" },
         files: [{ path: "SKILL.md", size: 8 }],
      });
   });

   it("reads a bundled skill's SKILL.md verbatim, with its frontmatter and every file", async () => {
      const controller = new SkillController(storeWith([]));
      const skill = await controller.getSkill("env", "pkg", bundled.name);
      expect(skill.content).toBe(bundled.bodies["SKILL.md"]);
      expect(skill.path).toBe("SKILL.md");
      expect(skill.origin).toBe("bundled");
      expect(skill.frontmatter).toEqual(bundled.frontmatter);
      expect(skill.files!.map((f) => f.path)).toEqual(
         Object.keys(bundled.files).sort((a, b) =>
            a === "SKILL.md" ? -1 : b === "SKILL.md" ? 1 : a < b ? -1 : 1,
         ),
      );
   });

   it("lists a package skill's other files with their sizes, and serves none of them", async () => {
      const controller = new SkillController(
         storeWith([
            packageSkill("house", "d", {
               "SKILL.md": "S",
               "scripts/run.py": "print('x')\n",
            }),
         ]),
      );
      expect(await controller.getSkill("env", "pkg", "house")).toEqual({
         name: "house",
         path: "SKILL.md",
         description: "d",
         content: "S",
         origin: "package",
         frontmatter: { name: "house", description: "d" },
         files: [
            { path: "SKILL.md", size: 1 },
            { path: "scripts/run.py", size: 11 },
         ],
      });
   });

   it("throws SkillNotFoundError for a name nothing defines", async () => {
      const controller = new SkillController(
         storeWith([packageSkill("house", "d", { "SKILL.md": "S" })]),
      );
      await expect(
         controller.getSkill("env", "pkg", "nope"),
      ).rejects.toBeInstanceOf(SkillNotFoundError);
   });
});
