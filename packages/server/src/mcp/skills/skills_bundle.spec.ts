import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
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
         const onDisk = fs
            .readdirSync(sourceDir, { withFileTypes: true })
            .filter((d) => d.isDirectory())
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
         for (const entry of referenceEntries) {
            expect(entry.description).toContain("Reference detail for");
            expect(entry.description.length).toBeGreaterThan(
               "Reference detail for the  skill.".length,
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

   /**
    * Pre-existing drift, bounded rather than exempted. Every SKILL.md honours
    * the house style; these three reference files never did, because nothing
    * checked them until they entered the bundle. Fixing 23 prose em-dashes in
    * three SHARED skills is an upstream-first change of its own, not something
    * to bundle into the build-time asset that surfaced it.
    *
    * Listed by name so a NEW violation still fails: shrink this list as the
    * files are fixed, and delete it when it is empty.
    */
   const EM_DASH_DRIFT = new Set([
      "malloy-html-data-apps/lazy-load",
      "malloy-html-data-apps/verification-harness",
      "malloy-lookml-review/build-derived-tables",
   ]);

   it("carries no em-dashes (house style)", () => {
      const offenders = skills
         .filter((s) => !EM_DASH_DRIFT.has(s.name) && s.body.includes("—"))
         .map((s) => s.name);
      expect(offenders).toEqual([]);
   });

   it("the em-dash allowlist names only entries that still need fixing", () => {
      // Stops the list outliving the drift: an entry that has been cleaned up,
      // or renamed away, has to leave the list.
      const stillDrifting = skills
         .filter((s) => EM_DASH_DRIFT.has(s.name) && s.body.includes("—"))
         .map((s) => s.name);
      expect([...EM_DASH_DRIFT].sort()).toEqual(stillDrifting.sort());
   });
});
