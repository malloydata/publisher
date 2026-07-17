import { describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import { isCredible, isExcluded } from "../scripts/exclusions";
import { listSkills, skillsDir } from "./index";

/** The repo's top-level skills/, which copy-skills.ts copies into this package. */
const sourceDir = path.join(import.meta.dir, "..", "..", "..", "skills");

/** Every file under a dir, as forward-slash paths relative to it. */
function filesIn(dir: string, prefix = ""): string[] {
   const files: string[] = [];
   for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
         files.push(...filesIn(path.join(dir, entry.name), relative));
      } else {
         files.push(relative);
      }
   }
   return files;
}

function skillNamesIn(dir: string): string[] {
   return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .filter((name) => fs.existsSync(path.join(dir, name, "SKILL.md")))
      .sort();
}

describe("@malloy-publisher/skills", () => {
   it("ships every publishable skill in the repo's skills/ directory", () => {
      const publishable = skillNamesIn(sourceDir).filter(
         (name) => !isExcluded(name),
      );
      expect(skillNamesIn(skillsDir)).toEqual(publishable);
   });

   /**
    * The copy filters `credible-*` out, but that is a backstop. A committed one
    * is the defect itself (skills/README.md), and this package turns skills/
    * into a published artifact that npm cannot unpublish, so say so loudly and
    * point at the stray rather than at the copy.
    */
   it("has no credible-* file committed to skills/", () => {
      expect(filesIn(sourceDir).filter(isCredible)).toEqual([]);
   });

   it("finds a skill for each one it ships", () => {
      expect(listSkills().length).toBe(skillNamesIn(skillsDir).length);
   });

   it("reads a name and description for every skill", () => {
      for (const skill of listSkills()) {
         expect(skill.name.length).toBeGreaterThan(0);
         expect(skill.description.length).toBeGreaterThan(0);
         expect(fs.existsSync(path.join(skill.dir, "SKILL.md"))).toBe(true);
      }
   });

   /**
    * The frontmatter name is what MCP prompts and `skill:` cross-references use;
    * the directory is what this package ships. If they drift, a consumer doing
    * path.join(skillsDir, skill.name) gets ENOENT for a skill that exists.
    */
   it("names each skill after its directory", () => {
      for (const skill of listSkills()) {
         expect(skill.name).toBe(path.basename(skill.dir));
      }
   });

   // The reason this package exists: reference/ files reach no npm consumer
   // today, so the pointers to them in the MCP prompt bodies dangle.
   it("brings each skill's reference/ files along", () => {
      const withReference = skillNamesIn(sourceDir)
         .filter((name) => !isExcluded(name))
         .filter((name) =>
            fs.existsSync(path.join(sourceDir, name, "reference")),
         );
      expect(withReference.length).toBeGreaterThan(0);
      for (const name of withReference) {
         const shipped = fs.readdirSync(
            path.join(skillsDir, name, "reference"),
         );
         const original = fs
            .readdirSync(path.join(sourceDir, name, "reference"))
            .filter((file) => !isExcluded(`${name}/reference/${file}`));
         expect(shipped.sort()).toEqual(original.sort());
      }
   });

   it("leaves out the internal sync README and any credible-* skill", () => {
      const shipped = fs.readdirSync(skillsDir);
      expect(shipped).not.toContain("README.md");
      expect(shipped.filter(isCredible)).toEqual([]);
   });
});
