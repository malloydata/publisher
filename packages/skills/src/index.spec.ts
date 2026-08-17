import { describe, expect, it } from "bun:test";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { isCredible, isExcluded } from "../scripts/exclusions";
import { listSkills, skillsDir } from "./index";

/** The repo's top-level skills/, which copy-skills.ts copies into this package. */
const sourceDir = path.join(import.meta.dir, "..", "..", "..", "skills");

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
    *
    * Asks git, not the filesystem: an uncommitted credible-* skill under
    * skills/ is a supported local state (`skills/credible-*` is gitignored as
    * a local install target), and only a committed one is the defect.
    */
   it("has no credible-* file committed to skills/", () => {
      const committed = execFileSync("git", ["ls-files"], {
         cwd: sourceDir,
         encoding: "utf8",
      })
         .split("\n")
         .filter(Boolean);
      expect(committed.filter(isCredible)).toEqual([]);
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

   /**
    * The stamp is what makes a months-old install identifiable on disk, and it
    * is applied by the copy rather than committed, so nothing else would notice
    * it silently stopping. Counting the key matters as much as reading it: a
    * second `version:` makes the frontmatter a duplicate-key YAML error, which
    * strict hosts reject by dropping the skill entirely.
    */
   it("stamps every shipped skill's frontmatter with the package version", () => {
      const { version } = JSON.parse(
         fs.readFileSync(
            path.join(import.meta.dir, "..", "package.json"),
            "utf8",
         ),
      ) as { version: string };
      const shipped = skillNamesIn(skillsDir);
      expect(shipped.length).toBeGreaterThan(0);
      for (const name of shipped) {
         const text = fs.readFileSync(
            path.join(skillsDir, name, "SKILL.md"),
            "utf8",
         );
         const front = text.slice(4, text.indexOf("\n---", 4));
         expect(front.match(/^version:.*$/gm)).toEqual([`version: ${version}`]);
      }
   });

   /** Pack-time only, so the upstream-sync copies stay byte-identical. */
   it("leaves the repo's source skills unstamped", () => {
      for (const name of skillNamesIn(sourceDir)) {
         const text = fs.readFileSync(
            path.join(sourceDir, name, "SKILL.md"),
            "utf8",
         );
         expect(text.slice(0, text.indexOf("\n---", 4))).not.toMatch(
            /^version:/m,
         );
      }
   });

   it("leaves out the internal sync README and any credible-* skill", () => {
      const shipped = fs.readdirSync(skillsDir);
      expect(shipped).not.toContain("README.md");
      expect(shipped.filter(isCredible)).toEqual([]);
   });
});
