// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SkillNotFoundError } from "../errors";
import {
   BUNDLED_CATALOG,
   type ServedSkill,
   type SkillFileRead,
} from "../service/skills/package_skills";
import { SkillController } from "./skill.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type { PackageLookup } from "../service/environment_ports";

const bundled = BUNDLED_CATALOG[0]!;
const bundledDescription = bundled.frontmatter.description as string;

/** A package skill whose files are text, or a read with a size (a file with no text). */
const packageSkill = (
   name: string,
   description: string,
   files: Record<string, string | { read: SkillFileRead; size: number }>,
): ServedSkill => ({
   name,
   description,
   catalog: {
      name,
      frontmatter: { name, description },
      files: Object.fromEntries(
         Object.entries(files).map(([p, f]) => [
            p,
            {
               digest: "sha256:0",
               size: typeof f === "string" ? f.length : f.size,
            },
         ]),
      ),
   },
   read: (p) => {
      const file = files[p]!;
      return typeof file === "string" ? { text: file } : file.read;
   },
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
      expect(skill.message).toBeUndefined();
   });

   it("reads any other file by its path, verbatim and with no frontmatter or files", async () => {
      const controller = new SkillController(
         storeWith([
            packageSkill("house", "d", {
               "SKILL.md": "S",
               "scripts/run.py": "print('x')\n",
            }),
         ]),
      );
      expect(
         await controller.getSkill("env", "pkg", "house", "scripts/run.py"),
      ).toEqual({
         name: "house",
         path: "scripts/run.py",
         description: "d",
         content: "print('x')\n",
         origin: "package",
      });
   });

   it("reads a bundled skill's embedded Markdown by its path", async () => {
      const [path, text] = Object.entries(bundled.bodies).find(
         ([p]) => p !== "SKILL.md",
      )!;
      const file = await new SkillController(storeWith([])).getSkill(
         "env",
         "pkg",
         bundled.name,
         path,
      );
      expect(file.content).toBe(text);
   });

   it("answers a file with no text as its size and a message, never bytes", async () => {
      const controller = new SkillController(
         storeWith([
            packageSkill("house", "d", {
               "SKILL.md": "S",
               "assets/logo.png": { read: { binary: true }, size: 68 },
            }),
         ]),
      );
      expect(
         await controller.getSkill("env", "pkg", "house", "assets/logo.png"),
      ).toEqual({
         name: "house",
         path: "assets/logo.png",
         description: "d",
         size: 68,
         message:
            "'assets/logo.png' is a binary file (68 bytes), and only text files are served.",
         origin: "package",
      });
   });

   it("answers 404 for a listed file that changed since the load, as the agent skill-file route does", async () => {
      const controller = new SkillController(
         storeWith([
            packageSkill("house", "d", {
               "SKILL.md": "S",
               "scripts/run.py": { read: { changed: true }, size: 9 },
            }),
         ]),
      );
      const err = await controller
         .getSkill("env", "pkg", "house", "scripts/run.py")
         .catch((e) => e);
      expect(err).toBeInstanceOf(SkillNotFoundError);
      expect(err.message).toContain("changed on disk");
   });

   it("reads a path with a leading ./ as the path without it", async () => {
      const controller = new SkillController(
         storeWith([
            packageSkill("house", "d", { "SKILL.md": "S", "a.md": "A" }),
         ]),
      );
      const file = await controller.getSkill("env", "pkg", "house", "./a.md");
      expect(file).toMatchObject({ path: "a.md", content: "A" });
   });

   it("throws SkillNotFoundError for a name nothing defines, and for a path the skill lacks, naming its files", async () => {
      const controller = new SkillController(
         storeWith([
            packageSkill("house", "d", { "SKILL.md": "S", "a.md": "A" }),
         ]),
      );
      await expect(
         controller.getSkill("env", "pkg", "nope"),
      ).rejects.toBeInstanceOf(SkillNotFoundError);
      const err = await controller
         .getSkill("env", "pkg", "house", "scripts/x.py")
         .catch((e) => e);
      expect(err).toBeInstanceOf(SkillNotFoundError);
      expect(err.message).toContain("'scripts/x.py'");
      expect(err.message).toContain("Its files: SKILL.md, a.md.");
      // The old <skill>/<stem> name is a path the skill does not have.
      await expect(
         controller.getSkill("env", "pkg", "house", "a"),
      ).rejects.toBeInstanceOf(SkillNotFoundError);
   });
});
