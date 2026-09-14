// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { type SkillEntry } from "./build_skills_bundle";
import { readPackageSkills, resolveSkills } from "./package_skills";

const skill = (name: string, description = "", body = ""): SkillEntry => ({
   name,
   description,
   body,
});

describe("readPackageSkills", () => {
   let pkg: string;

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-skills-"));
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   const writeSkill = (dir: string, contents: string) => {
      fs.mkdirSync(path.join(pkg, "skills", dir), { recursive: true });
      fs.writeFileSync(path.join(pkg, "skills", dir, "SKILL.md"), contents);
   };

   const writeReference = (dir: string, file: string, contents: string) => {
      const refDir = path.join(pkg, "skills", dir, "reference");
      fs.mkdirSync(refDir, { recursive: true });
      fs.writeFileSync(path.join(refDir, file), contents);
   };

   it("returns nothing, and no warning, for a package with no skills/", () => {
      expect(readPackageSkills(pkg)).toEqual({
         skills: [],
         paths: [],
         warnings: [],
      });
   });

   it("reads a skill's frontmatter and body", () => {
      writeSkill(
         "revenue",
         "---\nname: revenue-rules\ndescription: How revenue is defined here.\n---\n\nUse net_revenue, never gross.\n",
      );
      const { skills, paths, warnings } = readPackageSkills(pkg);
      expect(warnings).toEqual([]);
      expect(skills).toHaveLength(1);
      expect(skills[0]!.name).toBe("revenue-rules");
      expect(skills[0]!.description).toBe("How revenue is defined here.");
      expect(skills[0]!.body).toBe("Use net_revenue, never gross.");
      expect(paths).toEqual(["skills/revenue/SKILL.md"]);
   });

   it("falls back to the directory name when frontmatter omits name", () => {
      writeSkill("house-style", "no frontmatter at all");
      expect(readPackageSkills(pkg).skills[0]!.name).toBe("house-style");
   });

   it("serves reference files as their own entries and points the body at them", () => {
      writeSkill(
         "revenue",
         "---\nname: revenue-rules\ndescription: d\n---\n\nSee reference/margin.md.\n",
      );
      writeReference("revenue", "margin.md", "# Margin\n\nDetail.\n");
      const { skills, paths } = readPackageSkills(pkg);
      expect(skills.map((s) => s.name)).toEqual([
         "revenue-rules",
         "revenue-rules/margin",
      ]);
      // The parent body has to tell an MCP-only caller where the file went,
      // since a relative path means nothing over that channel: the prompt-name
      // template, and which files are actually there.
      expect(skills[0]!.body).toContain("revenue-rules/<name>");
      expect(skills[0]!.body).toContain("Available: margin.");
      expect(paths).toEqual([
         "skills/revenue/SKILL.md",
         "skills/revenue/reference/margin.md",
      ]);
   });

   it("warns, and keeps loading, when a skill directory has no SKILL.md", () => {
      fs.mkdirSync(path.join(pkg, "skills", "empty"), { recursive: true });
      writeSkill("good", "---\nname: good\ndescription: d\n---\n\nbody\n");
      const { skills, warnings } = readPackageSkills(pkg);
      expect(skills.map((s) => s.name)).toEqual(["good"]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("skills/empty");
   });

   it("warns when a skill has no description, since a listing shows nothing else", () => {
      writeSkill("quiet", "---\nname: quiet\n---\n\nbody\n");
      expect(readPackageSkills(pkg).warnings[0]).toContain("no description");
   });

   it("keeps the first of two skills declaring the same name, and warns", () => {
      writeSkill("a-dir", "---\nname: dupe\ndescription: first\n---\n\nA\n");
      writeSkill("b-dir", "---\nname: dupe\ndescription: second\n---\n\nB\n");
      const { skills, warnings } = readPackageSkills(pkg);
      expect(skills).toHaveLength(1);
      expect(skills[0]!.description).toBe("first");
      expect(warnings[0]).toContain("'dupe'");
   });

   it("does not drop a skill for being named credible-*", () => {
      // That exclusion is a rule about this repo's gitignored install target,
      // not about a customer's package. Applying it there would silently drop
      // a skill on the strength of its name.
      writeSkill(
         "credible-house",
         "---\nname: credible-house\ndescription: d\n---\n\nbody\n",
      );
      expect(readPackageSkills(pkg).skills.map((s) => s.name)).toEqual([
         "credible-house",
      ]);
   });

   it("reads skills in a stable order regardless of directory order", () => {
      writeSkill("zebra", "---\nname: zebra\ndescription: d\n---\n\nz\n");
      writeSkill("alpha", "---\nname: alpha\ndescription: d\n---\n\na\n");
      expect(readPackageSkills(pkg).skills.map((s) => s.name)).toEqual([
         "alpha",
         "zebra",
      ]);
   });
});

describe("resolveSkills", () => {
   const bundled = [
      skill("malloy-analysis", "bundled analysis", "BUNDLED ANALYSIS"),
      skill("malloy-phrase-detection", "bundled phrases", "BUNDLED PHRASES"),
   ];

   it("returns the bundled set unchanged when the package ships none", () => {
      const resolved = resolveSkills(bundled, []);
      expect(resolved.map((s) => [s.name, s.origin])).toEqual([
         ["malloy-analysis", "bundled"],
         ["malloy-phrase-detection", "bundled"],
      ]);
   });

   it("shadows a bundled skill of the same name, in place", () => {
      const resolved = resolveSkills(bundled, [
         skill("malloy-phrase-detection", "ours", "OUR PHRASES"),
      ]);
      expect(resolved).toHaveLength(2);
      const shadowed = resolved[1]!;
      expect(shadowed.name).toBe("malloy-phrase-detection");
      expect(shadowed.body).toBe("OUR PHRASES");
      expect(shadowed.origin).toBe("package");
      // The one it did not shadow is untouched.
      expect(resolved[0]!.body).toBe("BUNDLED ANALYSIS");
      expect(resolved[0]!.origin).toBe("bundled");
   });

   it("appends a package skill whose name is new", () => {
      const resolved = resolveSkills(bundled, [
         skill("revenue-rules", "ours", "OURS"),
      ]);
      expect(resolved.map((s) => s.name)).toEqual([
         "malloy-analysis",
         "malloy-phrase-detection",
         "revenue-rules",
      ]);
      expect(resolved[2]!.origin).toBe("package");
   });

   it("shadows and appends in one pass", () => {
      const resolved = resolveSkills(bundled, [
         skill("malloy-analysis", "ours", "OUR ANALYSIS"),
         skill("revenue-rules", "ours", "OURS"),
      ]);
      expect(resolved.map((s) => [s.name, s.origin])).toEqual([
         ["malloy-analysis", "package"],
         ["malloy-phrase-detection", "bundled"],
         ["revenue-rules", "package"],
      ]);
   });

   it("does not mutate the inputs", () => {
      const packageSkills = [skill("malloy-analysis", "ours", "OURS")];
      resolveSkills(bundled, packageSkills);
      expect(bundled[0]!.body).toBe("BUNDLED ANALYSIS");
      expect(
         (bundled[0] as SkillEntry & { origin?: string }).origin,
      ).toBeUndefined();
      expect(
         (packageSkills[0] as SkillEntry & { origin?: string }).origin,
      ).toBeUndefined();
   });
});
