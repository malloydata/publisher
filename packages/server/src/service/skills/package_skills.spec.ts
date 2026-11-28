// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { linkTextKeptVerbatim } from "../../test_helpers/link_text";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { execFileSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";
import { type BundledSkill } from "./build_skills_bundle";
import {
   namedSkillFiles,
   resolveSkills,
   skillFileList,
   skillFilePath,
   type ServedSkill,
} from "./package_skills";
import {
   emptyWorkflows,
   PACKAGE_SKILL_FILE_MAX_BYTES,
   readPackageWorkflows,
   type SetSkill,
} from "./package_workflows";

const digest = "sha256:0";

const bundledSkill = (
   name: string,
   description: string,
   body: string,
   others: Record<string, number> = {},
): BundledSkill => ({
   name,
   frontmatter: { name, description },
   files: {
      "SKILL.md": { digest, size: body.length },
      ...Object.fromEntries(
         Object.entries(others).map(([p, size]) => [p, { digest, size }]),
      ),
   },
   bodies: { "SKILL.md": body },
});

const packageSkill = (
   name: string,
   description: string,
   body: string,
): ServedSkill => ({
   name,
   description,
   catalog: {
      name,
      frontmatter: { name, description },
      files: { "SKILL.md": { digest, size: body.length } },
   },
   read: async () => ({ text: body }),
});

/** The package-relative path of each inline file the skills carry. */
const inlinePaths = (skills: SetSkill[]) =>
   skills.flatMap((s) => s.inline.map((f) => `${s.rel}/${f.relative}`));

describe("a skill set read through a workflow", () => {
   let pkg: string;

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-skills-"));
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
   });

   /** The set at `skills/` as the package declares it default; `warnings` is everything the package publishes. */
   const readSet = () => {
      const result = readPackageWorkflows(
         pkg,
         { skillSets: { default: { path: "skills" } } },
         [],
      );
      return {
         result,
         skills: result.sets.get("default")?.skills ?? [],
         warnings: result.warnings.map((w) => w.message),
      };
   };

   const writeSkill = (dir: string, contents: string) => {
      fs.mkdirSync(path.join(pkg, "skills", dir), { recursive: true });
      fs.writeFileSync(path.join(pkg, "skills", dir, "SKILL.md"), contents);
   };

   const writeReference = (dir: string, file: string, contents: string) => {
      const refDir = path.join(pkg, "skills", dir, "reference");
      fs.mkdirSync(refDir, { recursive: true });
      fs.writeFileSync(path.join(refDir, file), contents);
   };

   it("serves nothing, and flags the skills/ directory nothing reads, when the manifest declares no skill sets", () => {
      writeSkill("real", "---\nname: real\ndescription: d\n---\n\nbody\n");
      expect(readPackageWorkflows(pkg, undefined, [])).toEqual({
         ...emptyWorkflows(),
         contentShas: new Map(),
         skillsDirNotRead: true,
      });
   });

   it("answers the same for a skill directory and for a skill set that go out and back, whether or not the outside directory exists", () => {
      const outside = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-skills-out-"));
      const shown = () => {
         const { result, skills, warnings } = readSet();
         return JSON.stringify({
            workflows: [...result.workflows.keys()],
            skills: skills.map((s) => s.dirName),
            warnings,
         });
      };
      try {
         fs.mkdirSync(path.join(outside, "present"));
         writeSkill("real", "---\nname: real\ndescription: d\n---\n\nbody\n");
         const results: string[] = [];
         for (const which of ["present", "absent_"]) {
            const through = path.join(outside, which);
            // A skill directory that passes out and back into skills/real.
            fs.rmSync(path.join(pkg, "skills", "via"), { force: true });
            fs.symlinkSync(
               `${through}/${path.relative(through, path.join(pkg, "skills", "real"))}`,
               path.join(pkg, "skills", "via"),
            );
            results.push(shown());
         }
         expect(results[0]).toBe(results[1]);
         // Kept text: refused as leading out. Normalized when made (Windows):
         // the link leads to the package's own skills/real and is served.
         if (linkTextKeptVerbatim()) {
            expect(results[0]).toContain("resolves outside the package");
         } else {
            expect(results[0]).not.toContain("resolves outside the package");
         }

         // skills/ itself, out and back into a directory of the package.
         const sets: string[] = [];
         fs.renameSync(path.join(pkg, "skills"), path.join(pkg, "kept"));
         for (const which of ["present", "absent_"]) {
            const through = path.join(outside, which);
            fs.rmSync(path.join(pkg, "skills"), { force: true });
            fs.symlinkSync(
               `${through}/${path.relative(through, path.join(pkg, "kept"))}`,
               path.join(pkg, "skills"),
            );
            sets.push(shown());
         }
         expect(sets[0]).toBe(sets[1]);
         if (linkTextKeptVerbatim()) {
            expect(sets[0]).toContain("resolves outside the package");
         } else {
            expect(sets[0]).not.toContain("resolves outside the package");
         }
      } finally {
         fs.rmSync(outside, { recursive: true, force: true });
      }
   });

   it("refuses a skill set that is a dangling link out of the package, saying it leads outside it", () => {
      fs.symlinkSync("/definitely-absent-skills-dir", path.join(pkg, "skills"));
      const { result, warnings } = readSet();
      expect(result.sets.size).toBe(0);
      expect(warnings.join(" ")).toContain("resolves outside the package");
   });

   it("refuses a skill set that is a dangling link inside the package, saying it does not exist", () => {
      fs.symlinkSync(path.join(pkg, "gone"), path.join(pkg, "skills"));
      const { result, warnings } = readSet();
      expect(result.sets.size).toBe(0);
      expect(warnings.join(" ")).toContain("does not exist");
   });

   describe("files that leave the package", () => {
      let outside: string;

      beforeEach(() => {
         outside = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-outside-"));
         fs.writeFileSync(
            path.join(outside, "secret.md"),
            "---\nname: x\ndescription: leaked\n---\nSECRET",
         );
      });

      afterEach(() => {
         fs.rmSync(outside, { recursive: true, force: true });
      });

      it("does not serve a SKILL.md that is a link out of the package", () => {
         fs.mkdirSync(path.join(pkg, "skills", "bad"), { recursive: true });
         fs.symlinkSync(
            path.join(outside, "secret.md"),
            path.join(pkg, "skills", "bad", "SKILL.md"),
         );
         const { skills, warnings } = readSet();
         expect(skills).toEqual([]);
         expect(warnings.join("\n")).toContain("outside the package");
      });

      it("does not serve a SKILL.md that is a link to a non-Markdown file in the package", () => {
         fs.writeFileSync(path.join(pkg, "publisher.json"), '{"name":"x"}');
         fs.mkdirSync(path.join(pkg, "skills", "bad"), { recursive: true });
         fs.symlinkSync(
            path.join(pkg, "publisher.json"),
            path.join(pkg, "skills", "bad", "SKILL.md"),
         );
         const { skills, warnings } = readSet();
         expect(skills).toEqual([]);
         expect(warnings.join("\n")).toContain("not Markdown");
      });

      it("does not serve a reference file that is a link out", () => {
         writeSkill("ok", "---\nname: ok\ndescription: d\n---\nbody");
         fs.mkdirSync(path.join(pkg, "skills", "ok", "reference"));
         fs.symlinkSync(
            path.join(outside, "secret.md"),
            path.join(pkg, "skills", "ok", "reference", "leak.md"),
         );
         const { skills, warnings } = readSet();
         expect(skills.map((s) => s.served.name)).toEqual(["ok"]);
         expect(Object.keys(skills[0]!.catalog.files)).toEqual(["SKILL.md"]);
         expect(warnings.join("\n")).toContain("outside the package");
      });

      it("refuses a skill set that is a link out", () => {
         fs.mkdirSync(path.join(outside, "bad"));
         fs.writeFileSync(path.join(outside, "bad", "SKILL.md"), "SECRET");
         fs.symlinkSync(outside, path.join(pkg, "skills"));
         const { result, skills, warnings } = readSet();
         expect(result.sets.size).toBe(0);
         expect(skills).toEqual([]);
         expect(warnings.join("\n")).toContain("outside the package");
      });

      it("does not serve a skill directory that is a link out", () => {
         fs.mkdirSync(path.join(outside, "linked"));
         fs.writeFileSync(path.join(outside, "linked", "SKILL.md"), "SECRET");
         fs.mkdirSync(path.join(pkg, "skills"));
         fs.symlinkSync(
            path.join(outside, "linked"),
            path.join(pkg, "skills", "linked"),
         );
         const { skills } = readSet();
         expect(skills).toEqual([]);
      });
   });

   it("refuses a file over the size cap and keeps the rest", () => {
      writeSkill(
         "big",
         `---\nname: big\ndescription: d\n---\n${"x".repeat(PACKAGE_SKILL_FILE_MAX_BYTES)}`,
      );
      writeSkill("small", "---\nname: small\ndescription: d\n---\nok");
      const { skills, warnings } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["small"]);
      expect(warnings.join("\n")).toContain("over the 256 KiB cap");
   });

   it("reads a skill's frontmatter, and serves its SKILL.md verbatim", async () => {
      const text =
         "---\nname: revenue-rules\ndescription: How revenue is defined here.\n---\n\nUse net_revenue, never gross.\n";
      writeSkill("revenue-rules", text);
      const { skills, warnings } = readSet();
      expect(warnings).toEqual([]);
      expect(skills).toHaveLength(1);
      expect(skills[0]!.served.name).toBe("revenue-rules");
      expect(skills[0]!.served.description).toBe(
         "How revenue is defined here.",
      );
      expect(skills[0]!.catalog.frontmatter).toEqual({
         name: "revenue-rules",
         description: "How revenue is defined here.",
      });
      expect(await skills[0]!.served.read("SKILL.md")).toEqual({ text });
      expect(inlinePaths(skills)).toEqual(["skills/revenue-rules/SKILL.md"]);
   });

   it("falls back to the directory name when frontmatter omits name", () => {
      writeSkill("house-style", "no frontmatter at all");
      expect(readSet().skills[0]!.served.name).toBe("house-style");
   });

   it("serves a reference file by its path, and the skill's own text with nothing added", async () => {
      const text =
         "---\nname: revenue-rules\ndescription: d\n---\n\nSee reference/margin.md.\n";
      writeSkill("revenue-rules", text);
      writeReference("revenue-rules", "margin.md", "# Margin\n\nDetail.\n");
      const { skills } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["revenue-rules"]);
      expect(Object.keys(skills[0]!.catalog.files)).toEqual([
         "SKILL.md",
         "reference/margin.md",
      ]);
      expect(await skills[0]!.served.read("SKILL.md")).toEqual({ text });
      expect(await skills[0]!.served.read("reference/margin.md")).toEqual({
         text: "# Margin\n\nDetail.\n",
      });
      expect(inlinePaths(skills)).toEqual([
         "skills/revenue-rules/SKILL.md",
         "skills/revenue-rules/reference/margin.md",
      ]);
   });

   it("warns, and keeps loading, when a skill directory has no SKILL.md", () => {
      fs.mkdirSync(path.join(pkg, "skills", "empty"), { recursive: true });
      writeSkill("good", "---\nname: good\ndescription: d\n---\n\nbody\n");
      const { skills, warnings } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["good"]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("skills/empty");
      expect(warnings[0]).toContain("has no SKILL.md");
   });

   it("does not serve a skill whose SKILL.md is not UTF-8 text, and names it", () => {
      writeSkill("good", "---\nname: good\ndescription: d\n---\n\nbody\n");
      fs.mkdirSync(path.join(pkg, "skills", "latin"), { recursive: true });
      fs.writeFileSync(
         path.join(pkg, "skills", "latin", "SKILL.md"),
         Buffer.concat([
            Buffer.from("---\nname: latin\ndescription: d\n---\n\ncaf"),
            Buffer.from([0xe9]),
         ]),
      );
      const { skills, warnings } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["good"]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("skills/latin/SKILL.md");
      expect(warnings[0]).toContain("not UTF-8 text");
   });

   it("warns when a skill has no description, since a listing shows nothing else", () => {
      writeSkill("quiet", "---\nname: quiet\n---\n\nbody\n");
      expect(readSet().warnings[0]).toContain("no description");
   });

   it("does not serve a skill whose frontmatter name is not its directory name", () => {
      writeSkill("a-dir", "---\nname: other\ndescription: d\n---\n\nA\n");
      writeSkill("b-dir", "---\nname: b-dir\ndescription: d\n---\n\nB\n");
      const { skills, warnings } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["b-dir"]);
      expect(inlinePaths(skills)).toEqual(["skills/b-dir/SKILL.md"]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("'other'");
      expect(warnings[0]).toContain("Fix:");
   });

   it("lists a plain file named reference like any other file, with no warning", () => {
      writeSkill("odd", "---\nname: odd\ndescription: d\n---\n\nbody\n");
      fs.writeFileSync(path.join(pkg, "skills", "odd", "reference"), "x");
      const { skills, warnings } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["odd"]);
      expect(Object.keys(skills[0]!.catalog.files)).toEqual([
         "SKILL.md",
         "reference",
      ]);
      expect(warnings).toEqual([]);
   });

   it.skipIf(process.platform === "win32" || process.getuid?.() === 0)(
      "warns, and still serves the skill, when reference is unreadable",
      () => {
         writeSkill("odd", "---\nname: odd\ndescription: d\n---\n\nbody\n");
         const ref = path.join(pkg, "skills", "odd", "reference");
         fs.mkdirSync(ref);
         fs.chmodSync(ref, 0o000);
         try {
            const { skills, warnings } = readSet();
            expect(skills.map((s) => s.served.name)).toEqual(["odd"]);
            expect(warnings.join("\n")).toContain("cannot be listed");
         } finally {
            fs.chmodSync(ref, 0o755);
         }
      },
   );

   it("serves a skill that only warns, and sets aside only the one that is wrong", () => {
      writeSkill("quiet", "---\nname: quiet\n---\n\nbody\n");
      writeSkill("a-dir", "---\nname: other\ndescription: d\n---\n\nA\n");
      const { skills, warnings } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["quiet"]);
      expect(warnings).toHaveLength(2);
   });

   it("does not list a reference/ directory that is a link out of the package", () => {
      writeSkill("linked", "---\nname: linked\ndescription: d\n---\n\nbody\n");
      const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-ref-out-"));
      fs.writeFileSync(path.join(outsideDir, "secret-name.md"), "SECRET");
      fs.symlinkSync(
         outsideDir,
         path.join(pkg, "skills", "linked", "reference"),
      );
      const { skills, warnings } = readSet();
      expect(skills.map((s) => s.served.name)).toEqual(["linked"]);
      expect(inlinePaths(skills)).toHaveLength(1);
      expect(warnings.join("\n")).toContain("outside the package");
      expect(warnings.join("\n")).not.toContain("secret-name");
      fs.rmSync(outsideDir, { recursive: true, force: true });
   });

   // Git Bash's mkfifo makes an MSYS pipe that native Windows reads block on.
   it.skipIf(process.platform === "win32")(
      "does not read a skill file that is not a regular file",
      () => {
         fs.mkdirSync(path.join(pkg, "skills", "pipe"), { recursive: true });
         execFileSync("mkfifo", [path.join(pkg, "skills", "pipe", "SKILL.md")]);
         const { skills, warnings } = readSet();
         expect(skills).toEqual([]);
         expect(warnings.join("\n")).toContain("not a regular file");
      },
   );

   it("does not drop a skill for being named credible-*", () => {
      // That exclusion is a rule about this repo's gitignored install target,
      // not about a customer's package. Applying it there would silently drop
      // a skill on the strength of its name.
      writeSkill(
         "credible-house",
         "---\nname: credible-house\ndescription: d\n---\n\nbody\n",
      );
      expect(readSet().skills.map((s) => s.served.name)).toEqual([
         "credible-house",
      ]);
   });

   it("reads skills in a stable order regardless of directory order", () => {
      writeSkill("zebra", "---\nname: zebra\ndescription: d\n---\n\nz\n");
      writeSkill("alpha", "---\nname: alpha\ndescription: d\n---\n\na\n");
      expect(readSet().skills.map((s) => s.served.name)).toEqual([
         "alpha",
         "zebra",
      ]);
   });
});

describe("resolveSkills", () => {
   const bundled = [
      bundledSkill("malloy-analysis", "bundled analysis", "BUNDLED ANALYSIS"),
      bundledSkill("malloy-phrase-detection", "bundled phrases", "PHRASES"),
   ];

   it("returns the bundled set unchanged when the package ships none", async () => {
      const resolved = resolveSkills(bundled, []);
      expect(resolved.map((s) => [s.name, s.origin])).toEqual([
         ["malloy-analysis", "bundled"],
         ["malloy-phrase-detection", "bundled"],
      ]);
      expect(resolved[0]!.description).toBe("bundled analysis");
      expect(await resolved[0]!.read("SKILL.md")).toEqual({
         text: "BUNDLED ANALYSIS",
      });
   });

   it("shadows a bundled skill of the same name, in place", async () => {
      const resolved = resolveSkills(bundled, [
         packageSkill("malloy-phrase-detection", "ours", "OUR PHRASES"),
      ]);
      expect(resolved).toHaveLength(2);
      const shadowed = resolved[1]!;
      expect(shadowed.name).toBe("malloy-phrase-detection");
      expect(await shadowed.read("SKILL.md")).toEqual({ text: "OUR PHRASES" });
      expect(shadowed.origin).toBe("package");
      // The one it did not shadow is untouched.
      expect(await resolved[0]!.read("SKILL.md")).toEqual({
         text: "BUNDLED ANALYSIS",
      });
      expect(resolved[0]!.origin).toBe("bundled");
   });

   it("appends a package skill whose name is new", () => {
      const resolved = resolveSkills(bundled, [
         packageSkill("revenue-rules", "ours", "OURS"),
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
         packageSkill("malloy-analysis", "ours", "OUR ANALYSIS"),
         packageSkill("revenue-rules", "ours", "OURS"),
      ]);
      expect(resolved.map((s) => [s.name, s.origin])).toEqual([
         ["malloy-analysis", "package"],
         ["malloy-phrase-detection", "bundled"],
         ["revenue-rules", "package"],
      ]);
   });

   it("drops a shadowed bundled skill's other files, since nothing is merged", () => {
      const resolved = resolveSkills(
         [
            bundledSkill("malloy-review", "bundled", "B", {
               "reference/x.md": 3,
            }),
         ],
         [packageSkill("malloy-review", "ours", "OURS")],
      );
      expect(Object.keys(resolved[0]!.catalog.files)).toEqual(["SKILL.md"]);
   });

   it("serves a bundled skill's Markdown, and names the package to install for any other file", async () => {
      const [skill] = resolveSkills(
         [bundledSkill("pricing", "d", "B", { "scripts/run.py": 120 })],
         [],
      );
      const read = await skill!.read("scripts/run.py");
      expect(read).toEqual({
         message: expect.stringContaining("@malloy-publisher/skills"),
      });
      expect(JSON.stringify(read)).toContain("120 bytes");
   });

   it("does not mutate the inputs", () => {
      const packageSkills = [packageSkill("malloy-analysis", "ours", "OURS")];
      resolveSkills(bundled, packageSkills);
      expect((bundled[0] as { origin?: string }).origin).toBeUndefined();
      expect((packageSkills[0] as { origin?: string }).origin).toBeUndefined();
   });
});

describe("a path of a skill", () => {
   const [skill] = resolveSkills(
      [
         bundledSkill("pricing", "d", "B", {
            "scripts/run.py": 1,
            "references/a.md": 2,
            "NOTES.md": 3,
         }),
      ],
      [],
   );

   it("defaults to SKILL.md when blank, and drops a leading ./ but never trims a name", () => {
      expect(skillFilePath(undefined)).toBe("SKILL.md");
      expect(skillFilePath(null)).toBe("SKILL.md");
      expect(skillFilePath("  ")).toBe("SKILL.md");
      expect(skillFilePath("./scripts/run.py")).toBe("scripts/run.py");
      expect(skillFilePath("././references/a.md")).toBe("references/a.md");
      // A listed name may end in a space, and a trimmed path would never match it.
      expect(skillFilePath("scripts/run.py ")).toBe("scripts/run.py ");
      expect(skillFilePath("scripts/../scripts/run.py")).toBe(
         "scripts/../scripts/run.py",
      );
   });

   it("lists SKILL.md first, then by path", () => {
      expect(skillFileList(skill!).map((f) => f.path)).toEqual([
         "SKILL.md",
         "NOTES.md",
         "references/a.md",
         "scripts/run.py",
      ]);
   });

   it("names the first 25 files and counts the rest", () => {
      const many = Object.fromEntries(
         Array.from({ length: 30 }, (_, i) => [
            `f${String(i).padStart(2, "0")}.md`,
            1,
         ]),
      );
      const [big] = resolveSkills([bundledSkill("big", "d", "B", many)], []);
      const named = namedSkillFiles(big!);
      expect(named.startsWith("SKILL.md, f00.md, f01.md")).toBe(true);
      expect(named).toContain("(and 6 more; see `files` on SKILL.md");
      expect(named).not.toContain("f29.md");
   });
});
