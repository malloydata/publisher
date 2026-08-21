// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import {
   buildSkills,
   isCredible,
   type SkillEntry,
} from "./build_skills_bundle";
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

   /**
    * A skill's reference/ files are its on-demand detail, and this channel is
    * the only way a host without the skills on disk can reach them. Before they
    * were bundled, malloy-review told an agent to load rubrics that, over MCP,
    * did not exist.
    */
   describe("reference files", () => {
      const isReference = (s: SkillEntry) => s.name.includes("/");
      const referenceEntries = skills.filter(isReference);

      it("bundles every reference/*.md in the tree", () => {
         // Mirror the builder's own exclusion. A stray credible-* skill with a
         // reference/ dir is never bundled, so walking without this filter
         // fails on a correct bundle.
         const onDisk = fs
            .readdirSync(sourceDir, { withFileTypes: true })
            .filter((d) => d.isDirectory() && !isCredible(d.name))
            .flatMap((d) => {
               const dir = path.join(sourceDir, d.name, "reference");
               return fs.existsSync(dir)
                  ? fs
                       .readdirSync(dir)
                       .filter((f) => f.endsWith(".md"))
                       .map((f) => `${d.name}/${path.basename(f, ".md")}`)
                  : [];
            });

         expect(onDisk.length).toBeGreaterThan(0);
         expect(referenceEntries.map((s) => s.name).sort()).toEqual(
            onDisk.sort(),
         );
      });

      it("names each one under its parent skill", () => {
         const skillNames = new Set(
            skills.filter((s) => !isReference(s)).map((s) => s.name),
         );
         for (const entry of referenceEntries) {
            expect(skillNames.has(entry.name.split("/")[0])).toBe(true);
         }
      });

      it("gives each one a description drawn from its heading", () => {
         // Reference files carry no frontmatter, so a listing would otherwise
         // show them nameless. Every file in the tree has an H1 to use.
         //
         // Assert against the no-heading fallback rather than a length floor:
         // the fallback is itself long enough to clear any such floor, so a
         // length check alone stays green even if heading extraction breaks
         // entirely and all of a skill's entries collapse to one string.
         for (const entry of referenceEntries) {
            const parent = entry.name.split("/")[0];
            expect(entry.description).toContain("Reference detail for");
            expect(entry.description).not.toBe(
               `Reference detail for the ${parent} skill.`,
            );
         }
      });

      it("tells the parent skill where its reference files went", () => {
         // The parent body points at them by relative path, which resolves for
         // a host reading files off disk and not for one given this as a prompt.
         const parents = new Set(
            referenceEntries.map((s) => s.name.split("/")[0]),
         );
         for (const name of parents) {
            const parent = skills.find((s) => s.name === name);
            expect(parent?.body).toContain("Reference files over MCP");
            expect(parent?.body).toContain(`get the prompt named \`${name}/`);
         }
      });

      it("leaves skills without a reference/ directory untouched", () => {
         const plain = skills.find((s) => s.name === "malloy-getting-started");
         expect(plain?.body).not.toContain("Reference files over MCP");
      });
   });

   // No allowlist: the three reference files that carried the last of the
   // drift were rewritten upstream and copied back, so every entry in the
   // bundle now honours the house style and a new em-dash fails outright.
   //
   // Descriptions count as well as bodies. A SKILL.md's frontmatter
   // description is not part of its body, and it ships as the MCP prompt's
   // description, so it is bundle text like any other.
   it("carries no em-dashes (house style)", () => {
      const hasEmDash = (s: SkillEntry) =>
         s.body.includes("—") || s.description.includes("—");

      // Positive controls, one per field the predicate reads. With the
      // allowlist gone the offenders assertion runs against a clean tree, so
      // it would pass for free if the predicate ever stopped detecting; the
      // deleted allowlist test proved that incidentally, by asserting the
      // listed entries still held one. An empty description (or body) on
      // the other control is load-bearing: it is the half that would stay
      // green if that field were dropped from the predicate.
      expect(
         hasEmDash({ name: "control", description: "", body: "a — b" }),
      ).toBe(true);
      expect(
         hasEmDash({ name: "control2", description: "a — b", body: "" }),
      ).toBe(true);

      const offenders = skills.filter(hasEmDash).map((s) => s.name);
      expect(offenders).toEqual([]);
   });
});
