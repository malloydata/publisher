import { describe, expect, it } from "bun:test";
import bundle from "./skills_bundle.json";

const skills = (
   bundle as {
      skills: { name: string; description: string; body: string }[];
   }
).skills;

describe("skills_bundle.json (generated dual-channel asset)", () => {
   it("contains the full ported skill set", () => {
      expect(skills.length).toBeGreaterThanOrEqual(24);
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
