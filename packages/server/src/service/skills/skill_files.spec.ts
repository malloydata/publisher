// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { execFileSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";
import {
   ASSET_MAX_DEPTH,
   ASSET_MAX_ENTRIES,
   mayChangeSkill,
   mayChangeSkillExactly,
   newWalkBudget,
   REFERENCE_MAX_EXAMINED,
   skillFiles,
   type SkillFilesResult,
} from "./skill_files";

describe("skillFiles", () => {
   let dir: string;

   beforeEach(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "skill-files-"));
   });

   afterEach(() => {
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const write = (rel: string, contents = "x") => {
      const file = path.join(dir, rel);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, contents);
   };
   const skillignore = (...lines: string[]) =>
      write(".skillignore", lines.join("\n"));

   it("returns every file, sorted, with no warning and no skillignore", () => {
      write("SKILL.md");
      write("scripts/run.py");
      write("references/a.md");
      write("a.txt");
      expect(skillFiles(dir)).toEqual({
         files: ["SKILL.md", "a.txt", "references/a.md", "scripts/run.py"],
         warnings: [],
         skillignore: null,
      });
   });

   it("excludes dot names, node_modules, __pycache__ and *.pyc, and `!` cannot bring one back", () => {
      write("SKILL.md");
      write(".env");
      write(".DS_Store");
      write(".git/x");
      write("sub/.hidden/y.txt");
      write("node_modules/a.js");
      write("__pycache__/m.pyc");
      write("x.pyc");
      write("pkg/mod.PYC");
      skillignore("!.env", "!.git/x", "!node_modules/a.js", "!x.pyc");
      const { files, warnings } = skillFiles(dir);
      expect(files).toEqual(["SKILL.md"]);
      expect(warnings).toEqual([]);
   });

   it("applies gitignore rules, and keeps SKILL.md with a warning when a rule matches it", () => {
      write("SKILL.md");
      write("a_test.py");
      write("b.py");
      write("fixtures/x.csv");
      write("fixtures/keep.csv");
      skillignore("*_test.py", "fixtures/*", "!fixtures/keep.csv", "SKILL.md");
      const { files, warnings } = skillFiles(dir);
      expect(files).toEqual(["SKILL.md", "b.py", "fixtures/keep.csv"]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("SKILL.md");
   });

   it("stays quiet for an allowlist that re-includes SKILL.md", () => {
      write("SKILL.md");
      write("a.md");
      write("scripts/run.py");
      skillignore("*", "!SKILL.md");
      expect(skillFiles(dir)).toEqual({
         files: ["SKILL.md"],
         warnings: [],
         skillignore: "*\n!SKILL.md",
      });
   });

   it("matches case-insensitively", () => {
      write("SKILL.md");
      write("run.py");
      write("Docs/Notes.TXT");
      skillignore("*.PY", "docs/", "*.txt");
      expect(skillFiles(dir).files).toEqual(["SKILL.md"]);
   });

   it("reads only the root .skillignore", () => {
      write("SKILL.md");
      write("sub/.skillignore", "*");
      write("sub/a.txt");
      expect(skillFiles(dir).files).toEqual(["SKILL.md", "sub/a.txt"]);
   });

   it("prunes an ignored directory: its children are neither listed nor counted", () => {
      write("SKILL.md");
      write("kept/z.txt");
      // More children than the entry limit: walking them would trip it.
      for (let i = 0; i < ASSET_MAX_ENTRIES + 500; i++) {
         write(`fixtures/n${i}.csv`);
      }
      skillignore("fixtures/");
      expect(skillFiles(dir)).toMatchObject({
         files: ["SKILL.md", "kept/z.txt"],
         warnings: [],
      });
   });

   it.skipIf(process.getuid?.() === 0)(
      "does not open an ignored directory",
      () => {
         write("SKILL.md");
         write("fixtures/x.csv");
         skillignore("fixtures/");
         const fixtures = path.join(dir, "fixtures");
         fs.chmodSync(fixtures, 0o000);
         try {
            expect(skillFiles(dir)).toMatchObject({
               files: ["SKILL.md"],
               warnings: [],
            });
         } finally {
            fs.chmodSync(fixtures, 0o755);
         }
      },
   );

   it("counts ignored entries toward the entry limit", () => {
      for (let i = 0; i < ASSET_MAX_ENTRIES; i++) write(`n${i}.tmp`);
      write("zz.txt");
      skillignore("*.tmp");
      const { files, warnings } = skillFiles(dir);
      expect(files).toEqual([]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("more than 1000 entries");
   });

   it("stops at the entry limit with a warning and keeps what it found", () => {
      for (let i = 0; i < ASSET_MAX_ENTRIES + 5; i++) {
         write(`f${String(i).padStart(4, "0")}.txt`);
      }
      const { files, warnings } = skillFiles(dir);
      expect(files).toHaveLength(ASSET_MAX_ENTRIES);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("'f1000.txt'");
   });

   it("does not descend past the depth limit, and warns", () => {
      const nested = (levels: number) =>
         Array.from({ length: levels }, (_, i) => `d${i}`).join("/");
      write(`${nested(ASSET_MAX_DEPTH - 1)}/ok.txt`);
      write(`${nested(ASSET_MAX_DEPTH)}/too_deep.txt`);
      const { files, warnings } = skillFiles(dir);
      expect(files).toEqual([`${nested(ASSET_MAX_DEPTH - 1)}/ok.txt`]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain(`'${nested(ASSET_MAX_DEPTH)}'`);
   });

   describe(".skillignore bounds", () => {
      it("skips a pattern with more than two stars", () => {
         write("SKILL.md");
         write("a1b2c3d.txt");
         skillignore("*a*1*b*2*c*3*d.txt");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", "a1b2c3d.txt"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("more than 2 '*'");
      });

      it("applies a run of whole-segment `**` as one, with no warning", () => {
         write("SKILL.md");
         write("x/y/z/w.txt");
         skillignore("**/**/**/w.txt", `a/${"**/".repeat(65)}b`);
         expect(skillFiles(dir)).toMatchObject({
            files: ["SKILL.md"],
            warnings: [],
         });
      });

      it("skips a pattern with more than two whole-segment `**` that are not in one run", () => {
         write("SKILL.md");
         write("a/b/c/d/e/f/w.txt");
         skillignore("a/**/b/**/c/**/w.txt");
         const { files, warnings } = skillFiles(dir);
         expect(files).toContain("a/b/c/d/e/f/w.txt");
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("more than 2 '*'");
      });

      it.skipIf(process.platform === "win32")(
         "does not count an escaped star, which matches a literal one",
         () => {
            write("a*b*c*d.txt");
            write("axbxcxd.txt");
            skillignore("a\\*b\\*c\\*d.txt");
            expect(skillFiles(dir)).toMatchObject({
               files: ["axbxcxd.txt"],
               warnings: [],
            });
         },
      );

      it("warns on a leading ./", () => {
         write("SKILL.md");
         write("x");
         skillignore("./x");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", "x"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("./x");
      });

      it("skips patterns past the 50th, naming how many", () => {
         write("SKILL.md");
         write("p0.txt");
         write("p55.txt");
         const lines = Array.from({ length: 60 }, (_, i) => `p${i}.txt`);
         skillignore(...lines);
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", "p55.txt"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("more than 50 patterns");
         expect(warnings[0]).toContain("last 10");
         expect(warnings[0]).toContain("p50.txt");
      });

      it("counts neither comments nor blank lines toward the 50", () => {
         write("SKILL.md");
         write("a.txt");
         skillignore(
            ...Array.from({ length: 300 }, () => "# note"),
            "",
            "a.txt",
         );
         expect(skillFiles(dir)).toMatchObject({
            files: ["SKILL.md"],
            warnings: [],
         });
      });

      it("skips the patterns that bring the total past 1,024 characters, naming how many", () => {
         write("SKILL.md");
         write("a0.txt");
         write("a10.txt");
         write("a11.txt");
         write("a12.txt");
         const pad = (i: number) => `a${i}.txt`.padEnd(100, "z");
         skillignore(...Array.from({ length: 13 }, (_, i) => pad(i)));
         // Each line is 100 characters, so ten fit and the 11th brings the sum to 1,100.
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual([
            "SKILL.md",
            "a0.txt",
            "a10.txt",
            "a11.txt",
            "a12.txt",
         ]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("total more than 1024 characters");
         expect(warnings[0]).toContain("last 3");
      });

      it("counts the characters of a kept pattern only, not a comment or a skipped line", () => {
         write("SKILL.md");
         write("a.txt");
         skillignore(
            ...Array.from({ length: 40 }, () => `# ${"c".repeat(100)}`),
            "z".repeat(201),
            "a.txt",
         );
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("over 200 characters");
      });

      it("skips a pattern over 200 characters", () => {
         write("SKILL.md");
         write("a.txt");
         skillignore(`${"a".repeat(201)}`, "a.txt");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("over 200 characters");
      });

      it("ignores a .skillignore over 16 KiB, with a warning", () => {
         write("SKILL.md");
         write("a.txt");
         write(".skillignore", `a.txt\n${"# pad\n".repeat(3000)}`);
         const result = skillFiles(dir);
         expect(result.files).toEqual(["SKILL.md", "a.txt"]);
         expect(result.skillignore).toBeNull();
         expect(result.warnings).toHaveLength(1);
         expect(result.warnings[0]).toContain("16 KiB cap");
      });

      it("ignores a .skillignore that is a symlink, with a warning", () => {
         write("SKILL.md");
         write("a.txt");
         const outside = fs.mkdtempSync(
            path.join(os.tmpdir(), "skill-files-out-"),
         );
         const target = path.join(outside, "patterns");
         fs.writeFileSync(target, "a.txt");
         try {
            fs.symlinkSync(target, path.join(dir, ".skillignore"));
            const result = skillFiles(dir);
            expect(result.files).toEqual(["SKILL.md", "a.txt"]);
            expect(result.skillignore).toBeNull();
            expect(result.warnings).toHaveLength(1);
            expect(result.warnings[0]).toContain("link");
         } finally {
            fs.rmSync(outside, { recursive: true, force: true });
         }
      });

      it("strips a leading BOM before parsing", () => {
         write("SKILL.md");
         write("a.txt");
         write(".skillignore", "\uFEFFa.txt\n");
         const result = skillFiles(dir);
         expect(result.files).toEqual(["SKILL.md"]);
         expect(result.warnings).toEqual([]);
      });

      it("handles CRLF line endings", () => {
         write("SKILL.md");
         write("a.txt");
         write("b.txt");
         write(".skillignore", "a.txt\r\nb.txt\r\n");
         expect(skillFiles(dir).files).toEqual(["SKILL.md"]);
      });

      it("returns the applied text, so an edit can move a hash", () => {
         write("SKILL.md");
         skillignore("# only a comment");
         expect(skillFiles(dir).skillignore).toBe("# only a comment");
      });
   });

   describe("symlinks", () => {
      it("never descends into a symlinked directory and returns the link as a candidate", () => {
         write("SKILL.md");
         write("real/inner.txt");
         fs.symlinkSync(path.join(dir, "real"), path.join(dir, "alias"));
         fs.symlinkSync(path.join(dir, "SKILL.md"), path.join(dir, "link.md"));
         const { files } = skillFiles(dir);
         expect(files).toEqual([
            "SKILL.md",
            "alias",
            "link.md",
            "real/inner.txt",
         ]);
      });

      it("applies the ignore rules to a symlink like any other entry", () => {
         write("SKILL.md");
         write("real/inner.txt");
         fs.symlinkSync(path.join(dir, "real"), path.join(dir, "alias"));
         skillignore("alias");
         expect(skillFiles(dir).files).toEqual(["SKILL.md", "real/inner.txt"]);
      });
   });

   it("warns, and returns no files, for a directory that cannot be listed", () => {
      const result = skillFiles(path.join(dir, "missing"));
      expect(result.files).toEqual([]);
      expect(result.warnings).toHaveLength(1);
      expect(result.warnings[0]).toContain("ENOENT");
   });

   it("matches a full-depth walk against patterns sharing a prefix, at the star cap, within a second", () => {
      const deep = Array.from({ length: 6 }, (_, i) => `dir-number-${i}`);
      for (let f = 0; f < 10; f++) write(`a/${deep.join("/")}/f${f}.txt`);
      write("SKILL.md");
      skillignore(
         ...Array.from({ length: 30 }, (_, i) =>
            i % 2 === 0
               ? `a/${"**/".repeat(8)}b${i}`
               : `a/**/${deep[i % 6]}/**/z${i}`,
         ),
      );
      const started = performance.now();
      const result = skillFiles(dir);
      const elapsed = performance.now() - started;
      expect(result.warnings).toEqual([]);
      expect(result.files).toHaveLength(11);
      expect(elapsed).toBeLessThan(1000);
   });

   describe("path length", () => {
      it("keeps a path of 200 characters and skips one of 201, with one warning naming the first", () => {
         write("SKILL.md");
         const keep = `d/${"k".repeat(198)}`;
         const drop = `d/${"z".repeat(199)}`;
         write(keep);
         write(drop);
         write(`d/${"y".repeat(199)}.txt`);
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", keep]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("2 paths are over 200 characters");
         expect(warnings[0]).toContain('from "d/yyyy');
      });

      it("does not descend into a directory whose own path is over 200 characters", () => {
         write("SKILL.md");
         write(`${"d".repeat(201)}/inner.txt`);
         write("ok.txt");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", "ok.txt"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("1 path is over 200 characters");
      });

      it("measures the path from the skill, so a deep directory under assets/ counts its whole chain", () => {
         write("SKILL.md");
         const chain = Array.from({ length: 5 }, (_, i) =>
            `c${i}`.padEnd(40, "n"),
         );
         write(`assets/${chain.join("/")}/f.txt`);
         expect(skillFiles(dir).files).toEqual(["SKILL.md"]);
      });

      it("still counts a Markdown file of reference/ with a long path toward its cap, as the listing does", () => {
         for (let i = 0; i < ASSET_MAX_ENTRIES + 100; i++) {
            write(
               `reference/${String(i).padStart(4, "0").padEnd(195, "n")}.md`,
            );
         }
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual([]);
         expect(
            warnings.filter((w) => w.includes("Markdown files")),
         ).toHaveLength(1);
         expect(
            warnings.filter((w) => w.includes("over 200 characters")),
         ).toHaveLength(1);
      });
   });

   // Skipped on Windows, whose MAX_PATH and slower filesystem fail the fixtures; the bounds are platform-independent.
   describe.skipIf(process.platform === "win32")("total matching work", () => {
      const walkSeconds = (run: () => SkillFilesResult) => {
         const started = performance.now();
         const result = run();
         return { result, elapsed: performance.now() - started };
      };
      /** A name that brings `prefix` plus it to exactly 200 characters. */
      const padded = (prefix: string, label: string, i: number, ext: string) =>
         `${label}${String(i).padStart(4, "0")}`.padEnd(
            200 - prefix.length - ext.length,
            "n",
         ) + ext;
      const nested = (base: string, levels: number) =>
         Array.from({ length: levels }, (_, l) =>
            `${base}${l}`.padEnd(20, "n"),
         ).join("/");
      /** Files at the bound: `flat` directly in assets/ and the root, `deep` under 7 and 6 nested directories. */
      const buildTree = (layout: "flat" | "deep") => {
         write("SKILL.md");
         for (let i = 0; i < ASSET_MAX_ENTRIES + 100; i++) {
            write(`reference/${padded("reference/", "r", i, ".md")}`);
         }
         const assets =
            layout === "deep" ? `assets/${nested("a", 7)}/` : "assets/";
         const other = layout === "deep" ? `o/${nested("o", 6)}/` : "";
         for (let i = 0; i <= ASSET_MAX_ENTRIES; i++) {
            write(`${assets}${padded(assets, "a", i, ".dat")}`);
            write(`${other}${padded(other, "x", i, ".dat")}`);
         }
      };
      // The matching rules come first, as `ignore` tests last to first, so every later rule runs on every path.
      // 48 slow rules fill both caps (50 patterns, under 1,024 characters); the 149 after them are skipped.
      const shapes: Record<string, string> = {
         "basename `*` and 17 `?`": `*${"?".repeat(17)}zq`,
         "slash `**/*` and 14 `?`": `**/*${"?".repeat(14)}k`,
         "anchored `assets/**/*` and 8 `?`": `assets/**/*${"?".repeat(8)}k`,
      };

      it.each(["flat", "deep"] as const)(
         "matches the most paths the walk allows, at the path bound, against the most patterns of each slow shape within a second (%s)",
         (layout) => {
            buildTree(layout);
            for (const [shape, slow] of Object.entries(shapes)) {
               skillignore(
                  "reference/*.md",
                  "*.dat",
                  ...Array.from({ length: 48 }, () => slow),
                  ...Array.from({ length: 149 }, () => `*${"?".repeat(77)}zq`),
               );
               const { result, elapsed } = walkSeconds(() =>
                  skillFiles(dir, { budget: newWalkBudget("package") }),
               );
               if (elapsed >= 1000) {
                  throw new Error(`${shape} took ${elapsed.toFixed(0)} ms`);
               }
               expect(result.files).toEqual(["SKILL.md"]);
               expect(
                  result.warnings.filter((w) =>
                     w.startsWith("skill walk stopped"),
                  ),
               ).toHaveLength(3);
            }
         },
         60_000,
      );

      it("bounds paths of a thousand characters under assets/ and elsewhere, where a slash pattern backtracks across every segment", () => {
         write("SKILL.md");
         const chain = (base: string, levels: number, tag: string) => {
            const dirs = Array.from({ length: levels }, (_, l) =>
               `${tag}${l}`.padEnd(245, "n"),
            ).join("/");
            for (let i = 0; i < ASSET_MAX_ENTRIES; i++) {
               write(
                  `${base}/${dirs}/${`${tag}f${String(i).padStart(4, "0")}`.padEnd(244, "n")}.dat`,
               );
            }
         };
         chain("assets", 7, "a");
         chain("o", 6, "o");
         skillignore(
            "*.dat",
            ...Array.from({ length: 49 }, () => `**/*${"?".repeat(14)}k`),
         );
         const { result, elapsed } = walkSeconds(() =>
            skillFiles(dir, { budget: newWalkBudget("agent") }),
         );
         expect(elapsed).toBeLessThan(1000);
         expect(result.files).toEqual(["SKILL.md"]);
         expect(result.warnings).toHaveLength(1);
         expect(result.warnings[0]).toContain(
            "2 paths are over 200 characters",
         );
      }, 60_000);

      it("bounds ten thousand ignored Markdown files in reference/ against two hundred slow patterns", () => {
         write("SKILL.md");
         for (let i = 0; i < REFERENCE_MAX_EXAMINED; i++) {
            write(`reference/${padded("reference/", "", i, ".md")}`, "x");
         }
         skillignore(
            "reference/*",
            ...Array.from(
               { length: 199 },
               (_, i) =>
                  `*${"?".repeat(77)}${String.fromCharCode(97 + (i % 26))}${String.fromCharCode(97 + ((i >> 2) % 26))}`,
            ),
         );
         const { result, elapsed } = walkSeconds(() => skillFiles(dir));
         expect(elapsed).toBeLessThan(1000);
         expect(result.files).toEqual(["SKILL.md"]);
         expect(result.warnings).toHaveLength(3);
         expect(result.warnings[0]).toContain("more than 50 patterns");
         expect(result.warnings[1]).toContain("more than 1024 characters");
         expect(result.warnings[2]).toContain(
            `reference/ holds more than ${ASSET_MAX_ENTRIES} Markdown files`,
         );
      }, 60_000);
   });

   describe("SKILL.md", () => {
      it("is kept even when a folder sorting before it fills the entry limit", () => {
         write("SKILL.md");
         for (let i = 0; i <= ASSET_MAX_ENTRIES; i++) write(`Assets/f${i}.txt`);
         const { files, warnings } = skillFiles(dir);
         expect(files).toContain("SKILL.md");
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("more than 1000 entries");
      });

      it("gets no special case when it is a directory", () => {
         write("SKILL.md/x");
         write("a.txt");
         skillignore("SKILL.md");
         expect(skillFiles(dir)).toMatchObject({
            files: ["a.txt"],
            warnings: [],
         });
      });

      it("warns when a lowercase pattern matches it", () => {
         write("SKILL.md");
         skillignore("skill.md");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md"]);
         expect(warnings).toHaveLength(1);
      });
   });

   describe("entry budgets", () => {
      const fill = (folder: string, count: number) => {
         for (let i = 0; i < count; i++) write(`${folder}/f${i}.txt`);
      };

      it("walks SKILL.md, reference/ and assets/ before the rest, so a bound cuts the rest first", () => {
         fill("Examples", ASSET_MAX_ENTRIES);
         write("reference/a.md");
         write("assets/b.txt");
         write("SKILL.md");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(
            expect.arrayContaining([
               "SKILL.md",
               "reference/a.md",
               "assets/b.txt",
            ]),
         );
         expect(
            files.filter((f) => f.startsWith("Examples/")).length,
         ).toBeLessThan(ASSET_MAX_ENTRIES);
         expect(warnings).toHaveLength(1);
      });

      it("charges entries below assets/ and every other entry to separate budgets", () => {
         fill("assets", ASSET_MAX_ENTRIES);
         fill("other", ASSET_MAX_ENTRIES - 1);
         const budget = newWalkBudget("package");
         const { files, warnings } = skillFiles(dir, { budget });
         expect(files).toHaveLength(2 * ASSET_MAX_ENTRIES - 1);
         expect(warnings).toEqual([]);
         expect(budget.assets.left).toBe(0);
         expect(budget.other.left).toBe(0);
      });

      it("takes from a budget shared by several walks, and says where each is cut", () => {
         const budget = newWalkBudget("package");
         for (const skill of ["a", "b"]) {
            fs.mkdirSync(path.join(dir, skill));
            for (let i = 0; i < 600; i++) {
               fs.writeFileSync(path.join(dir, skill, `f${i}.txt`), "x");
            }
         }
         const first = skillFiles(path.join(dir, "a"), { budget });
         const second = skillFiles(path.join(dir, "b"), { budget });
         expect(first.files).toHaveLength(600);
         expect(first.warnings).toEqual([]);
         expect(second.files).toHaveLength(ASSET_MAX_ENTRIES - 600);
         expect(second.warnings).toHaveLength(1);
         expect(second.warnings[0]).toContain("more than 1000 entries");
      });

      it("says the shared budget was already spent when a walk took nothing from it", () => {
         const budget = newWalkBudget("package");
         const sizes = { a: 600, b: 400, c: 5 };
         const [, , third] = Object.entries(sizes).map(([skill, count]) => {
            fs.mkdirSync(path.join(dir, skill));
            for (let i = 0; i < count; i++) {
               fs.writeFileSync(path.join(dir, skill, `f${i}.txt`), "x");
            }
            return skillFiles(path.join(dir, skill), { budget });
         });
         expect(third!.files).toEqual([]);
         expect(third!.warnings).toHaveLength(1);
         expect(third!.warnings[0]).toContain(
            `the package's walk budget of ${ASSET_MAX_ENTRIES} entries was already spent on earlier skills`,
         );
      });

      it("takes SKILL.md and a Markdown file directly in reference/ from neither shared budget, even with nothing left", () => {
         write("SKILL.md");
         write("reference/a.md");
         write("reference/data.csv");
         write("reference/sub/b.md");
         write("assets/c.txt");
         write("other.txt");
         const { files, warnings } = skillFiles(dir, {
            budget: {
               assets: { left: 0 },
               other: { left: 0 },
               owner: "package",
            },
         });
         expect(files).toEqual(["SKILL.md", "reference/a.md"]);
         expect(warnings).toHaveLength(2);
      });

      describe("references/ is read as reference/ is", () => {
         const inReferences = (files: string[]) =>
            files.filter((f) => f.startsWith("references/"));

         it("serves at most 1,000 Markdown files of its own, whatever the shared budget has left", () => {
            for (let i = 0; i < ASSET_MAX_ENTRIES + 200; i++) {
               write(`references/r${i}.md`);
            }
            const { files, warnings } = skillFiles(dir, {
               budget: {
                  assets: { left: 0 },
                  other: { left: 0 },
                  owner: "package",
               },
            });
            expect(inReferences(files)).toHaveLength(ASSET_MAX_ENTRIES);
            expect(warnings).toHaveLength(1);
            expect(warnings[0]).toContain(
               `references/ holds more than ${ASSET_MAX_ENTRIES} Markdown files`,
            );
         });

         it("follows a link to a directory the caller resolves, for its Markdown alone", () => {
            write("shared/a.md");
            write("shared/data.csv");
            fs.symlinkSync(
               path.join(dir, "shared"),
               path.join(dir, "references"),
               "dir",
            );
            const { files } = skillFiles(dir, {
               followReference: (target) => target,
            });
            expect(inReferences(files)).toEqual(["references/a.md"]);
         });

         it("does not count what it holds besides Markdown", () => {
            for (let i = 0; i < ASSET_MAX_ENTRIES; i++) {
               write(`references/fig${i}.png`);
            }
            write("references/overview.md");
            const { files, warnings } = skillFiles(dir);
            expect(files.filter((f) => f.endsWith(".md"))).toEqual([
               "references/overview.md",
            ]);
            expect(warnings).toEqual([]);
         });
      });

      describe("reference/", () => {
         const fillReference = (folder: string, count: number) => {
            for (let i = 0; i < count; i++) write(`${folder}/r${i}.md`);
         };
         const inReference = (files: string[]) =>
            files.filter((f) => f.startsWith("reference/"));
         const link = () =>
            fs.symlinkSync(
               path.join(dir, "shared"),
               path.join(dir, "reference"),
               "dir",
            );
         // How many directory entries `run` reads through opendir, which only a reference/ listing does.
         const listingReads = (run: () => void) => {
            let reads = 0;
            const open = fs.opendirSync;
            const spy = spyOn(fs, "opendirSync").mockImplementation(((
               ...args: Parameters<typeof fs.opendirSync>
            ) => {
               const opened = open(...args);
               const read = opened.readSync.bind(opened);
               opened.readSync = () => {
                  reads++;
                  return read();
               };
               return opened;
            }) as typeof fs.opendirSync);
            try {
               run();
            } finally {
               spy.mockRestore();
            }
            return reads;
         };

         it("serves at most 1,000 Markdown files of its own, whatever the shared budget has left", () => {
            fillReference("reference", ASSET_MAX_ENTRIES + 200);
            const { files, warnings } = skillFiles(dir, {
               budget: {
                  assets: { left: 0 },
                  other: { left: 0 },
                  owner: "package",
               },
            });
            expect(inReference(files)).toHaveLength(ASSET_MAX_ENTRIES);
            expect(warnings).toHaveLength(1);
            expect(warnings[0]).toContain(
               `reference/ holds more than ${ASSET_MAX_ENTRIES} Markdown files`,
            );
         });

         it("does not count what it holds besides Markdown, so a thousand figures cannot cost it overview.md", () => {
            for (let i = 0; i < ASSET_MAX_ENTRIES; i++) {
               write(`reference/fig${i}.png`);
            }
            write("reference/overview.md");
            const { files, warnings } = skillFiles(dir);
            expect(files.filter((f) => f.endsWith(".md"))).toEqual([
               "reference/overview.md",
            ]);
            expect(warnings).toEqual([]);
         });

         it("does not count what a link holds besides Markdown, so a thousand figures cannot cost it z.md", () => {
            for (let i = 0; i < ASSET_MAX_ENTRIES; i++) {
               write(`shared/fig${i}.png`);
            }
            write("shared/z.md");
            link();
            const { files, warnings } = skillFiles(dir, {
               followReference: (target) => target,
            });
            expect(inReference(files)).toEqual(["reference/z.md"]);
            expect(warnings.some((w) => w.includes("Markdown files"))).toBe(
               false,
            );
         });

         it("does not count a dot Markdown file, and reads on past a thousand of them", () => {
            for (let i = 0; i < 1200; i++) write(`reference/.h${i}.md`);
            write("reference/z.md");
            let files: string[] = [];
            let warnings: string[] = [];
            const reads = listingReads(() => {
               ({ files, warnings } = skillFiles(dir));
            });
            expect(inReference(files)).toEqual(["reference/z.md"]);
            expect(warnings).toEqual([]);
            expect(reads).toBeGreaterThanOrEqual(1201);
         });

         it("counts a Markdown file .skillignore matches, so a thousand ignored ones end the listing", () => {
            for (let i = 0; i < ASSET_MAX_ENTRIES + 100; i++) {
               write(`reference/skip${i}.md`);
            }
            skillignore("reference/skip*.md");
            let files: string[] = [];
            let warnings: string[] = [];
            const reads = listingReads(() => {
               ({ files, warnings } = skillFiles(dir));
            });
            expect(inReference(files)).toEqual([]);
            expect(warnings).toHaveLength(1);
            expect(warnings[0]).toContain(
               `reference/ holds more than ${ASSET_MAX_ENTRIES} Markdown files`,
            );
            expect(reads).toBeGreaterThanOrEqual(ASSET_MAX_ENTRIES);
            expect(reads).toBeLessThanOrEqual(ASSET_MAX_ENTRIES + 1);
         });

         it("counts an ignored file of a linked reference/ the same way", () => {
            for (let i = 0; i < ASSET_MAX_ENTRIES + 100; i++) {
               write(`shared/skip${i}.md`);
            }
            link();
            skillignore("reference/skip*.md");
            let warnings: string[] = [];
            const reads = listingReads(() => {
               ({ warnings } = skillFiles(dir, {
                  followReference: (target) => target,
               }));
            });
            expect(warnings.some((w) => w.includes("Markdown files"))).toBe(
               true,
            );
            expect(reads).toBeLessThanOrEqual(ASSET_MAX_ENTRIES + 1);
         });

         it("lists a link to a directory of five thousand Markdown files for no more than 1,001 entries", () => {
            fillReference("shared", 5000);
            link();
            let files: string[] = [];
            const reads = listingReads(() => {
               ({ files } = skillFiles(dir, {
                  followReference: (target) => target,
               }));
            });
            expect(inReference(files)).toHaveLength(ASSET_MAX_ENTRIES);
            // The spy must see the listing, or the bound below is vacuous.
            expect(reads).toBeGreaterThanOrEqual(ASSET_MAX_ENTRIES);
            expect(reads).toBeLessThanOrEqual(ASSET_MAX_ENTRIES + 1);
         });

         describe("past REFERENCE_MAX_EXAMINED entries of any kind", () => {
            const fillOther = (count: number) => {
               fs.mkdirSync(path.join(dir, "shared"), { recursive: true });
               for (let i = 0; i < count; i++) {
                  fs.writeFileSync(path.join(dir, "shared", `n${i}.txt`), "x");
               }
            };
            const listLink = () => {
               let warnings: string[] = [];
               const reads = listingReads(() => {
                  ({ warnings } = skillFiles(dir, {
                     followReference: (target) => target,
                  }));
               });
               return {
                  reads,
                  stopped: warnings.filter((w) =>
                     w.includes(
                        `reference/ holds more than ${REFERENCE_MAX_EXAMINED} entries`,
                     ),
                  ),
               };
            };

            it("lists a link to twenty thousand other files and one Markdown file for no more than 10,001 entries, and says so", () => {
               fillOther(2 * REFERENCE_MAX_EXAMINED);
               write("shared/z.md");
               link();
               const { reads, stopped } = listLink();
               // The spy must see the listing, or the bound below is vacuous.
               expect(reads).toBeGreaterThanOrEqual(REFERENCE_MAX_EXAMINED);
               expect(reads).toBeLessThanOrEqual(REFERENCE_MAX_EXAMINED + 1);
               expect(stopped).toEqual([
                  `skill walk stopped in 'reference': reference/ holds more than ${REFERENCE_MAX_EXAMINED} entries, so the rest are not read`,
               ]);
            });

            it("says nothing about a directory of exactly that many", () => {
               fillOther(REFERENCE_MAX_EXAMINED);
               link();
               expect(listLink().stopped).toEqual([]);
            });
         });
      });

      it("counts the depth below assets/ from assets/, and from the skill elsewhere", () => {
         const nested = (root: string, levels: number) =>
            [root, ...Array.from({ length: levels }, (_, i) => `d${i}`)].join(
               "/",
            );
         write(`${nested("assets", ASSET_MAX_DEPTH - 1)}/ok.txt`);
         write(`${nested("assets", ASSET_MAX_DEPTH)}/deep.txt`);
         write(`${nested("scripts", ASSET_MAX_DEPTH - 2)}/ok.txt`);
         write(`${nested("scripts", ASSET_MAX_DEPTH - 1)}/deep.txt`);
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual([
            `${nested("assets", ASSET_MAX_DEPTH - 1)}/ok.txt`,
            `${nested("scripts", ASSET_MAX_DEPTH - 2)}/ok.txt`,
         ]);
         expect(warnings).toHaveLength(2);
      });

      it("reads a root reference/ that is a link only when told where it leads, and then only its flat Markdown", () => {
         write("shared/a.md");
         write("shared/b.txt");
         write("shared/sub/c.md");
         fs.symlinkSync(
            path.join(dir, "shared"),
            path.join(dir, "reference"),
            "dir",
         );
         expect(skillFiles(dir).files).toEqual([
            "reference",
            "shared/a.md",
            "shared/b.txt",
            "shared/sub/c.md",
         ]);
         const followed = skillFiles(dir, {
            followReference: (link) =>
               link === path.join(dir, "reference") ? link : undefined,
         });
         expect(followed.files).toEqual([
            "reference/a.md",
            "shared/a.md",
            "shared/b.txt",
            "shared/sub/c.md",
         ]);
      });

      it("applies .skillignore to the Markdown it reads through a reference/ link", () => {
         write("shared/a.md");
         write("shared/b.md");
         fs.symlinkSync(
            path.join(dir, "shared"),
            path.join(dir, "reference"),
            "dir",
         );
         skillignore("reference/b.md");
         const { files } = skillFiles(dir, {
            followReference: (link) => link,
         });
         expect(files.filter((f) => f.startsWith("reference/"))).toEqual([
            "reference/a.md",
         ]);
      });
   });

   describe("star cap boundary", () => {
      it.skipIf(process.platform === "win32")(
         "counts each star between escapes as its own run",
         () => {
            write("abcd.txt");
            skillignore("*\\a*\\a*\\a*d.txt");
            const { files, warnings } = skillFiles(dir);
            expect(files).toEqual(["abcd.txt"]);
            expect(warnings).toHaveLength(1);
            expect(warnings[0]).toContain("more than 2 '*'");
         },
      );

      it("does not count a star inside a bracket expression, which is a literal", () => {
         write("abacad.txt");
         skillignore("[a*]b[a*]c[a*]d.txt");
         expect(skillFiles(dir)).toMatchObject({ files: [], warnings: [] });
      });

      it("applies a pattern with two single stars, with no warning", () => {
         write("abc.txt");
         skillignore("a*b*c.txt");
         expect(skillFiles(dir)).toMatchObject({ files: [], warnings: [] });
      });

      it("skips a pattern with three single stars, with one warning", () => {
         write("abcd.txt");
         skillignore("a*b*c*d.txt");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["abcd.txt"]);
         expect(warnings).toHaveLength(1);
      });

      it("counts a `**` inside a segment as one star, and a whole-segment one as another", () => {
         write("abcd.txt");
         write("x/y/w.txt");
         skillignore("a**b**c**d.txt", "!**/keep.txt", "x/**/y/**");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["abcd.txt"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("a**b**c**d.txt");
      });
   });

   describe("gitignore semantics", () => {
      it("cannot re-include a file under an ignored directory", () => {
         write("fixtures/keep.csv");
         write("fixtures/other.csv");
         skillignore("fixtures/", "!fixtures/keep.csv");
         expect(skillFiles(dir).files).toEqual([]);
      });

      it("re-includes a file when the rule is `fixtures/**`", () => {
         write("fixtures/keep.csv");
         write("fixtures/other.csv");
         skillignore("fixtures/**", "!fixtures/keep.csv");
         expect(skillFiles(dir).files).toEqual(["fixtures/keep.csv"]);
      });

      it("refuses a .skillignore that is a directory, with a warning", () => {
         write("a.txt");
         write(".skillignore/inner", "a.txt");
         const result = skillFiles(dir);
         expect(result.files).toEqual(["a.txt"]);
         expect(result.skillignore).toBeNull();
         expect(result.warnings).toHaveLength(1);
      });

      it.skipIf(process.platform === "win32")(
         "refuses a .skillignore that is a FIFO, with a warning",
         () => {
            write("a.txt");
            execFileSync("mkfifo", [path.join(dir, ".skillignore")]);
            const result = skillFiles(dir);
            expect(result.files).toEqual(["a.txt"]);
            expect(result.skillignore).toBeNull();
            expect(result.warnings).toHaveLength(1);
         },
      );

      it("reads the .skillignore through one descriptor, so a swap after a check cannot reach a read that follows a link or blocks", () => {
         write("a.txt");
         skillignore("a.txt");
         const spies = [
            spyOn(fs, "lstatSync"),
            spyOn(fs, "readFileSync"),
            spyOn(fs, "openSync"),
         ];
         try {
            expect(skillFiles(dir).files).toEqual([]);
            const named = (spy: (typeof spies)[number]) =>
               spy.mock.calls.filter(([file]) =>
                  String(file).endsWith(".skillignore"),
               );
            expect(named(spies[0]!)).toEqual([]);
            expect(named(spies[1]!)).toEqual([]);
            const opens = named(spies[2]!);
            expect(opens).toHaveLength(1);
            const flags = Number(opens[0]![1]);
            expect(flags & (fs.constants.O_NOFOLLOW ?? 0)).toBe(
               fs.constants.O_NOFOLLOW ?? 0,
            );
            expect(flags & (fs.constants.O_NONBLOCK ?? 0)).toBe(
               fs.constants.O_NONBLOCK ?? 0,
            );
         } finally {
            for (const spy of spies) spy.mockRestore();
         }
      });
   });
});

describe("mayChangeSkill", () => {
   let dir: string;

   beforeEach(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "may-change-skill-"));
   });

   afterEach(() => {
      fs.rmSync(dir, { recursive: true, force: true });
   });

   const write = (rel: string) => {
      const file = path.join(dir, rel);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, "x");
   };

   it("rules a path out exactly when the walk does not keep it, so a reload follows what is served", async () => {
      const paths = [
         "SKILL.md",
         "reference/glossary.md",
         "reference/Draft.MD",
         "scripts/run.py",
         "scripts/__pycache__/run.pyc",
         "scripts/run.PYC",
         "node_modules/pkg/index.js",
         ".env",
         ".git/config",
         "assets/index.html",
         "assets/tmp/cache.json",
         "data/raw.log",
         "data/Raw.CSV",
         "notes/todo.txt",
         "notes/keep.txt",
      ];
      for (const rel of paths) write(rel);
      fs.writeFileSync(
         path.join(dir, ".skillignore"),
         "assets/tmp/\n*.log\n*.csv\nnotes/*\n!notes/keep.txt\n",
      );
      const served = new Set(skillFiles(dir).files);
      expect(served.has("scripts/run.py")).toBe(true);
      expect(served.has("data/raw.log")).toBe(false);
      for (const rel of paths) {
         expect([rel, await mayChangeSkillExactly(dir, rel)]).toEqual([
            rel,
            served.has(rel),
         ]);
      }
   });

   it("by name alone, rules out only the names no walk keeps, and reads no .skillignore", async () => {
      fs.writeFileSync(path.join(dir, ".skillignore"), "*.log\n");
      expect(mayChangeSkill("data/raw.log")).toBe(true);
      expect(await mayChangeSkillExactly(dir, "data/raw.log")).toBe(false);
      expect(mayChangeSkill("a/__pycache__/b.txt")).toBe(false);
      expect(mayChangeSkill(".env")).toBe(false);
   });

   it("always allows SKILL.md, which the walk keeps whatever .skillignore matches", async () => {
      write("SKILL.md");
      write("reference/glossary.md");
      write("notes.md");
      fs.writeFileSync(
         path.join(dir, ".skillignore"),
         "*.md\n!reference/*.md\n",
      );
      const served = new Set(skillFiles(dir).files);
      expect(served.has("SKILL.md")).toBe(true);
      expect(served.has("notes.md")).toBe(false);
      expect(await mayChangeSkillExactly(dir, "SKILL.md")).toBe(true);
      expect(await mayChangeSkillExactly(dir, "notes.md")).toBe(false);
      expect(await mayChangeSkillExactly(dir, "reference/glossary.md")).toBe(
         true,
      );
   });

   it("always allows an edit to the .skillignore itself, but not to one in a subfolder", async () => {
      fs.writeFileSync(path.join(dir, ".skillignore"), "*\n");
      expect(await mayChangeSkillExactly(dir, ".skillignore")).toBe(true);
      expect(await mayChangeSkillExactly(dir, "sub/.skillignore")).toBe(false);
   });

   it("applies no .skillignore over 16 KiB, and none that is a link, as a walk does not", async () => {
      fs.writeFileSync(
         path.join(dir, ".skillignore"),
         `*.log\n${"# pad\n".repeat(3000)}`,
      );
      expect(await mayChangeSkillExactly(dir, "data/raw.log")).toBe(true);
      fs.rmSync(path.join(dir, ".skillignore"));
      fs.writeFileSync(path.join(dir, "real-ignore"), "*.log\n");
      fs.symlinkSync(
         path.join(dir, "real-ignore"),
         path.join(dir, ".skillignore"),
      );
      expect(await mayChangeSkillExactly(dir, "data/raw.log")).toBe(true);
   });

   it.skipIf(process.platform === "win32")(
      "applies no .skillignore that is a FIFO, and does not wait on it",
      async () => {
         execFileSync("mkfifo", [path.join(dir, ".skillignore")]);
         expect(await mayChangeSkillExactly(dir, "data/raw.log")).toBe(true);
      },
      5000,
   );

   it("reads the .skillignore through one descriptor, with no check of the path before it", async () => {
      fs.writeFileSync(path.join(dir, ".skillignore"), "*.log\n");
      const spies = [
         spyOn(fs.promises, "lstat"),
         spyOn(fs.promises, "readFile"),
         spyOn(fs.promises, "open"),
      ];
      try {
         expect(await mayChangeSkillExactly(dir, "data/raw.log")).toBe(false);
         const named = (spy: (typeof spies)[number]) =>
            spy.mock.calls.filter(([file]) =>
               String(file).endsWith(".skillignore"),
            );
         expect(named(spies[0]!)).toEqual([]);
         expect(named(spies[1]!)).toEqual([]);
         expect(named(spies[2]!)).toHaveLength(1);
      } finally {
         for (const spy of spies) spy.mockRestore();
      }
   });
});
