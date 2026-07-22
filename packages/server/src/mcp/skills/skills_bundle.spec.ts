import { describe, expect, it } from "bun:test";
import * as path from "node:path";
import { buildSkills, type SkillEntry } from "./build_skills_bundle";
import bundle from "./skills_bundle.json";

const skills = (
   bundle as {
      skills: { name: string; description: string; body: string }[];
   }
).skills;

/** The repo's top-level skills/, which this bundle is generated from. */
const sourceDir = path.join(
   import.meta.dir,
   "..",
   "..",
   "..",
   "..",
   "..",
   "skills",
);

/**
 * Codepoint order. The committed bundle is sorted with localeCompare, which
 * depends on the runtime's locale, and this suite runs on three platforms.
 * Membership and content are what matter here; file order is cosmetic.
 */
const byName = (a: SkillEntry, b: SkillEntry) =>
   a.name < b.name ? -1 : a.name > b.name ? 1 : 0;

describe("skills_bundle.json (generated dual-channel asset)", () => {
   it("is in sync with skills/", () => {
      // Regenerate with:
      //   bun run src/mcp/skills/build_skills_bundle.ts ../../skills
      expect([...skills].sort(byName)).toEqual(
         [...buildSkills(sourceDir)].sort(byName),
      );
   });

   it("is not empty", () => {
      expect(skills.length).toBeGreaterThan(0);
   });

   it("every skill has a nonempty name, description, and body", () => {
      for (const s of skills) {
         expect(s.name.length).toBeGreaterThan(0);
         expect(s.description.length).toBeGreaterThan(0);
         expect(s.body.length).toBeGreaterThan(0);
      }
   });

   it("skill names are unique", () => {
      const names = skills.map((s) => s.name);
      expect(new Set(names).size).toBe(names.length);
   });

   it("carries no em-dashes (house style)", () => {
      for (const s of skills) {
         expect(s.body).not.toContain("—");
      }
   });
});
