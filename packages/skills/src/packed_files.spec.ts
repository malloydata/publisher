// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { packedSkillFiles } from "../scripts/exclusions";

const SKILL_MD = "---\nname: pricing\ndescription: Price a thing.\n---\nbody\n";

describe("packedSkillFiles", () => {
   const tree = (): { root: string; skills: string } => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "packed-"));
      const skill = path.join(root, "skills/pricing");
      fs.mkdirSync(path.join(skill, "scripts"), { recursive: true });
      fs.mkdirSync(path.join(skill, "reference"));
      fs.writeFileSync(path.join(skill, "SKILL.md"), SKILL_MD);
      fs.writeFileSync(path.join(skill, "scripts/run.py"), "print('hi')\n");
      fs.writeFileSync(path.join(skill, ".skillignore"), "*_test.py\n");
      fs.writeFileSync(path.join(skill, "scripts/run_test.py"), "assert 1\n");
      return { root, skills: path.join(root, "skills") };
   };

   it("lists a skill's regular files minus its .skillignore", () => {
      const { root, skills } = tree();
      try {
         expect(packedSkillFiles(skills, "pricing")).toEqual([
            "SKILL.md",
            "scripts/run.py",
         ]);
      } finally {
         fs.rmSync(root, { recursive: true, force: true });
      }
   });

   // A link would be copied as the file it names, which npm pack would then publish.
   const cases: Array<[string, (root: string) => void, RegExp]> = [
      [
         "a file link",
         (root) => {
            fs.writeFileSync(path.join(root, "outside.txt"), "secret\n");
            fs.rmSync(path.join(root, "skills/pricing/scripts/run.py"));
            fs.symlinkSync(
               "../../../outside.txt",
               path.join(root, "skills/pricing/scripts/run.py"),
            );
         },
         /skill 'pricing': 'scripts\/run\.py' is a link/,
      ],
      [
         "a linked Markdown file",
         (root) => {
            fs.writeFileSync(path.join(root, "outside.md"), "# Secret\n");
            fs.symlinkSync(
               "../../../outside.md",
               path.join(root, "skills/pricing/reference/more.md"),
            );
         },
         /skill 'pricing': 'reference\/more\.md' is a link/,
      ],
      [
         "a linked directory",
         (root) => {
            fs.mkdirSync(path.join(root, "outside"));
            fs.symlinkSync(
               "../../outside",
               path.join(root, "skills/pricing/data"),
            );
         },
         /skill 'pricing': 'data' is a link/,
      ],
      [
         "a linked skill root",
         (root) => {
            fs.renameSync(
               path.join(root, "skills/pricing"),
               path.join(root, "elsewhere"),
            );
            fs.symlinkSync("../elsewhere", path.join(root, "skills/pricing"));
         },
         /skill 'pricing' is a link/,
      ],
   ];

   for (const [label, setup, message] of cases) {
      it(`throws for ${label}`, () => {
         const { root, skills } = tree();
         try {
            setup(root);
            expect(() => packedSkillFiles(skills, "pricing")).toThrow(message);
         } finally {
            fs.rmSync(root, { recursive: true, force: true });
         }
      });
   }
});
