// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SkillSetsUnavailableError } from "../../errors";
import { BUNDLED_CATALOG } from "../../service/skills/package_skills";
import { skillsFor } from "./response";

const ctxOf = (defaultSkills: () => unknown) =>
   ({ pkgIndex: { pkg: { defaultSkills } } }) as unknown as Parameters<
      typeof skillsFor
   >[0];

const own = (
   skills: Array<{ name: string; description: string }>,
   overrides: string[] = [],
) => ({ skills, overrides });

describe("skillsFor", () => {
   it("lists the package's advertised skills by name and description", () => {
      const reply = skillsFor(
         ctxOf(() => own([{ name: "house", description: "House rules." }])),
      );
      expect(reply.skills).toEqual([
         { name: "house", description: "House rules." },
      ]);
   });

   it("lists a guide named like a built-in one only where the package overrides it, as the skills routes serve it", () => {
      const builtIn = BUNDLED_CATALOG[0]!.name;
      const skills = [
         { name: builtIn, description: "Ours." },
         { name: "house", description: "House rules." },
      ];
      expect(skillsFor(ctxOf(() => own(skills))).skills).toEqual([
         { name: "house", description: "House rules." },
      ]);
      expect(skillsFor(ctxOf(() => own(skills, [builtIn]))).skills).toEqual([
         { name: builtIn, description: "Ours." },
         { name: "house", description: "House rules." },
      ]);
   });

   it("says the package's guidance could not be read, with the reason, instead of looking empty", () => {
      const reply = skillsFor(
         ctxOf(() => {
            throw new SkillSetsUnavailableError("EACCES on sets/a");
         }),
      );
      expect(reply).toEqual({
         skills_note: expect.stringContaining("EACCES on sets/a"),
      });
      expect(reply).not.toHaveProperty("skills");
   });

   it("lets any other failure through", () => {
      expect(() =>
         skillsFor(
            ctxOf(() => {
               throw new Error("boom");
            }),
         ),
      ).toThrow("boom");
   });
});
