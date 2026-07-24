import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { skillsDir } from "@malloy-publisher/skills";
import { countSkills, installSkills } from "./skills";

/**
 * `.claude/skills/` is shared agent territory, so a symlink can be anywhere in
 * it, not just at the top. fs.cpSync follows one it finds at the destination, so
 * every one of these has to be checked by executing the copy and then reading the
 * file the link points at, which is the only thing that distinguishes "left alone"
 * from "written through" once the run has printed its green count.
 *
 * Names are derived from the shipped payload rather than written down here: a
 * hardcoded skill name fails the day one is renamed, and says nothing about why.
 */
let tmp: string;
let outside: string;
let targetDir: string;

const SKILL_COUNT = countSkills(skillsDir);

/** A shipped skill, and one that ships a reference/ subdirectory. */
function shippedSkills(): { any: string; withReference: string } {
   const names = fs
      .readdirSync(skillsDir, { withFileTypes: true })
      .filter(
         (entry) =>
            entry.isDirectory() &&
            fs.existsSync(path.join(skillsDir, entry.name, "SKILL.md")),
      )
      .map((entry) => entry.name)
      .sort();
   const withReference = names.find((name) =>
      fs.existsSync(path.join(skillsDir, name, "reference")),
   );
   if (names.length === 0 || withReference === undefined) {
      throw new Error(
         `The shipped skills no longer include one with a reference/ ` +
            `subdirectory, so this suite cannot exercise a nested directory.`,
      );
   }
   return { any: names[0], withReference };
}

beforeEach(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-skills-"));
   outside = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-skills-outside-"));
   targetDir = path.join(tmp, ".claude", "skills");
});

afterEach(() => {
   fs.rmSync(tmp, { recursive: true, force: true });
   fs.rmSync(outside, { recursive: true, force: true });
});

describe("installSkills: symlinks inside a skill directory", () => {
   test("a linked SKILL.md is replaced, not the file it points at", () => {
      const { any: skill } = shippedSkills();
      const stolen = path.join(outside, "target.md");
      fs.writeFileSync(stolen, "ORIGINAL\n");
      fs.mkdirSync(path.join(targetDir, skill), { recursive: true });
      const link = path.join(targetDir, skill, "SKILL.md");
      fs.symlinkSync(stolen, link);

      const result = installSkills(targetDir, tmp);

      expect(fs.readFileSync(stolen, "utf8")).toBe("ORIGINAL\n");
      expect(fs.lstatSync(link).isSymbolicLink()).toBe(false);
      expect(fs.readFileSync(link, "utf8")).toBe(
         fs.readFileSync(path.join(skillsDir, skill, "SKILL.md"), "utf8"),
      );
      expect(result.installed).toBe(SKILL_COUNT);
      expect(result.refreshed).toContain(skill);
   });

   test("a linked reference/ is replaced, not written into", () => {
      const { withReference: skill } = shippedSkills();
      const stolenDir = path.join(outside, "refdir");
      fs.mkdirSync(stolenDir);
      fs.writeFileSync(path.join(stolenDir, "keep.txt"), "MINE\n");
      fs.mkdirSync(path.join(targetDir, skill), { recursive: true });
      const link = path.join(targetDir, skill, "reference");
      fs.symlinkSync(stolenDir, link);

      installSkills(targetDir, tmp);

      expect(fs.readdirSync(stolenDir)).toEqual(["keep.txt"]);
      expect(fs.lstatSync(link).isSymbolicLink()).toBe(false);
      expect(fs.readdirSync(link).length).toBeGreaterThan(0);
   });

   test("a link two levels down is not written through either", () => {
      const { withReference: skill } = shippedSkills();
      // The link has to carry the name of a file the copy actually writes, or it
      // is never reached and the test passes without proving anything.
      const shipped = fs.readdirSync(
         path.join(skillsDir, skill, "reference"),
      )[0];
      expect(shipped).toBeDefined();
      const stolen = path.join(outside, "deep.md");
      fs.writeFileSync(stolen, "ORIGINAL\n");
      const nested = path.join(targetDir, skill, "reference");
      fs.mkdirSync(nested, { recursive: true });
      fs.symlinkSync(stolen, path.join(nested, shipped));

      installSkills(targetDir, tmp);

      expect(fs.readFileSync(stolen, "utf8")).toBe("ORIGINAL\n");
   });
});

describe("installSkills: the checks that were already there", () => {
   test("a symlinked skill directory is still left alone", () => {
      const { any: skill } = shippedSkills();
      const source = path.join(outside, "my-skill");
      fs.mkdirSync(source);
      fs.writeFileSync(path.join(source, "SKILL.md"), "# mine\n");
      fs.mkdirSync(targetDir, { recursive: true });
      const link = path.join(targetDir, skill);
      fs.symlinkSync(source, link);

      const result = installSkills(targetDir, tmp);

      expect(fs.readFileSync(path.join(source, "SKILL.md"), "utf8")).toBe(
         "# mine\n",
      );
      expect(fs.lstatSync(link).isSymbolicLink()).toBe(true);
      expect(result.installed).toBe(SKILL_COUNT - 1);
      expect(result.skipped).toContain(skill);
   });

   test("a target that resolves outside the workspace installs nothing", () => {
      fs.symlinkSync(outside, path.join(tmp, ".claude"));

      const result = installSkills(targetDir, tmp);

      expect(result.installed).toBe(0);
      expect(result.refused).toContain("outside the workspace");
      expect(fs.readdirSync(outside)).toEqual([]);
   });

   test("a clean install reports every skill and refreshes none", () => {
      const result = installSkills(targetDir, tmp);

      expect(result.installed).toBe(SKILL_COUNT);
      expect(result.refreshed).toEqual([]);
      expect(result.skipped).toEqual([]);
      expect(countSkills(targetDir)).toBe(SKILL_COUNT);
   });
});
