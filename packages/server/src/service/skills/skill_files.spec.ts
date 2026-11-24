// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { execFileSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";
import { ASSET_MAX_DEPTH, ASSET_MAX_ENTRIES, skillFiles } from "./skill_files";

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
      it("skips a pattern with more than two single stars, but not `**`", () => {
         write("SKILL.md");
         write("a1b2c3d.txt");
         write("x/y/z/w.txt");
         skillignore("*a*1*b*2*c*3*d.txt", "**/**/**/w.txt");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", "a1b2c3d.txt"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("single '*'");
      });

      it("warns on a leading ./", () => {
         write("SKILL.md");
         write("x");
         skillignore("./x");
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", "x"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("./x");
      });

      it("skips patterns past the 200th, naming how many", () => {
         write("SKILL.md");
         write("p0.txt");
         write("p205.txt");
         const lines = Array.from({ length: 210 }, (_, i) => `p${i}.txt`);
         skillignore(...lines);
         const { files, warnings } = skillFiles(dir);
         expect(files).toEqual(["SKILL.md", "p205.txt"]);
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain("last 10");
         expect(warnings[0]).toContain("p200.txt");
      });

      it("counts neither comments nor blank lines toward the 200", () => {
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
         expect(result.warnings[0]).toContain("16384 byte cap");
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

   it("matches a skill at the entry limit against 200 patterns within a second", () => {
      for (let d = 0; d < 2; d++) {
         for (let f = 0; f < 490; f++) write(`dir${d}/file${f}.txt`);
      }
      write("SKILL.md");
      skillignore(
         ...Array.from({ length: 200 }, (_, i) =>
            i % 2 === 0 ? `*_gen${i}*.tmp` : `!keep${i}/**/*.md`,
         ),
      );
      const started = performance.now();
      const result = skillFiles(dir);
      const elapsed = performance.now() - started;
      expect(result.files).toHaveLength(981);
      expect(elapsed).toBeLessThan(1000);
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

   describe("star cap boundary", () => {
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

      it("counts a `**` inside a segment as a single star, but not a whole-segment one", () => {
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
   });
});
