// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { parseFrontmatter } from "../../server/src/service/skills/frontmatter";
import { readFrontmatter, readSkills } from "./skill_frontmatter";

describe("readFrontmatter", () => {
   it("reads a plain name and description", () => {
      expect(
         readFrontmatter("name: my-skill\ndescription: Does a thing.", "dir"),
      ).toEqual({ name: "my-skill", description: "Does a thing." });
   });

   it("reads a block scalar description to its true length", () => {
      const text =
         "name: my-skill\ndescription: >-\n  First line\n  second line";
      expect(readFrontmatter(text, "dir").description).toBe(
         "First line second line",
      );
   });

   it("unescapes a YAML single-quoted description the line read would keep doubled", () => {
      expect(
         readFrontmatter("name: x\ndescription: 'the model''s file'", "dir")
            .description,
      ).toBe("the model's file");
   });

   it("falls back to a line read when the YAML does not parse", () => {
      expect(
         readFrontmatter("name: x\ndescription: Use when: a thing", "dir"),
      ).toEqual({ name: "x", description: "Use when: a thing" });
   });

   it("falls back to a line read when the name breaks the spec's character set", () => {
      expect(
         readFrontmatter("name: My_Skill\ndescription: d", "My_Skill"),
      ).toEqual({ name: "My_Skill", description: "d" });
      expect(readFrontmatter("name: a--b\ndescription: d", "dir").name).toBe(
         "a--b",
      );
   });

   it("takes the directory name when none is declared", () => {
      expect(readFrontmatter("description: d", "from-dir")).toEqual({
         name: "from-dir",
         description: "d",
      });
      expect(readFrontmatter("", "from-dir")).toEqual({
         name: "from-dir",
         description: "",
      });
   });
});

const anchors = Array.from(
   { length: 10_700 },
   (_, i) => `a${i}: &k${i} x\nb${i}: *k${i}`,
).join("\n");
const anchorsLed = (lead: string) =>
   Array.from(
      { length: 10_700 },
      (_, i) => `a${i}: &${lead}k${i} x\nb${i}: *${lead}k${i}`,
   ).join("\n");
const wide = `[${Array.from({ length: 300 }, (_, i) => i).join(", ")}]`;
const refs = Array.from({ length: 99 }, (_, i) => `r${i}: *l`).join("\n");

/** A folded description whose YAML reading ("one two") differs from the line read (">-"), padded by a comment to `bytes`. */
const atCap = (bytes: number) => {
   const head = "name: x\ndescription: >-\n  one\n  two\n#";
   return head + "c".repeat(bytes - Buffer.byteLength(head));
};

/** [label, SKILL.md text, directory name, expected name, expected description] */
const PARITY: [string, string, string, string, string][] = [
   ["a bare name", "---\nname:\ndescription: foo\n---\n", "dir", "dir", "foo"],
   ["a null name", "---\nname: ~\ndescription: d\n---\n", "dir", "dir", "d"],
   [
      "an empty description",
      "---\nname: x\ndescription:\n---\n",
      "dir",
      "x",
      "",
   ],
   [
      "a null description",
      "---\nname: x\ndescription: ~\n---\n",
      "dir",
      "x",
      "",
   ],
   [
      "a folded block scalar",
      "---\nname: x\ndescription: >-\n  one\n  two\n---\n",
      "dir",
      "x",
      "one two",
   ],
   [
      "a literal block scalar",
      "---\nname: x\ndescription: |\n  one\n  two\n---\n",
      "dir",
      "x",
      "one\ntwo",
   ],
   ["no frontmatter", "# Just a body\n", "dir", "dir", ""],
   [
      "YAML that does not parse",
      "---\nname: x\ndescription: Use when: a thing\n---\n",
      "dir",
      "x",
      "Use when: a thing",
   ],
   [
      "a name outside the spec",
      "---\nname: a--b\ndescription: d\n---\n",
      "dir",
      "a--b",
      "d",
   ],
   [
      "10,700 distinct anchors",
      `---\nname: x\ndescription: d\n${anchors}\n---\n`,
      "dir",
      "x",
      "d",
   ],
   [
      "10,700 anchors with names led by U+00A0",
      `---\nname: x\ndescription: d\n${anchorsLed("\u00a0")}\n---\n`,
      "dir",
      "x",
      "d",
   ],
   [
      "10,700 anchors with names led by U+3000",
      `---\nname: x\ndescription: d\n${anchorsLed("\u3000")}\n---\n`,
      "dir",
      "x",
      "d",
   ],
   [
      "an empty name read line by line after the anchor bound",
      `---\nname:\ndescription: foo\n${anchors}\n---\n`,
      "dir",
      "dir",
      "foo",
   ],
   [
      "an empty name read line by line from invalid YAML",
      "---\nname:\ndescription: Use when: a thing\n---\n",
      "dir",
      "dir",
      "Use when: a thing",
   ],
   [
      "21,400 plain keys",
      `---\nname: x\ndescription: d\n${Array.from({ length: 21_400 }, (_, i) => `a${i}: x`).join("\n")}\n---\n`,
      "dir",
      "x",
      "d",
   ],
   [
      "10,700 empty-name pairs",
      `---\nname: x\ndescription: d\n${Array.from({ length: 10_700 }, (_, i) => `a${i}: & x\nb${i}: *`).join("\n")}\n---\n`,
      "dir",
      "x",
      "d",
   ],
   [
      "exactly 4 KiB, read as YAML",
      `---\n${atCap(4096)}\n---\n`,
      "dir",
      "x",
      "one two",
   ],
   [
      "one byte over 4 KiB, read line by line",
      `---\n${atCap(4097)}\n---\n`,
      "dir",
      "x",
      ">-",
   ],
   [
      "CRLF line endings",
      "---\r\nname: x\r\ndescription: d\r\n---\r\nBody\r\n",
      "dir",
      "x",
      "d",
   ],
   [
      "a leading BOM, which both read past",
      "\uFEFF---\nname: x\ndescription: d\n---\n",
      "dir",
      "x",
      "d",
   ],
   [
      "an anchored list referenced 99 times",
      `---\nname: x\ndescription: d\nl: &l ${wide}\n${refs}\n---\n`,
      "dir",
      "x",
      "d",
   ],
   [
      "a name and description that start past the first 4 KiB of an oversized frontmatter, which the line read never reaches",
      `---\nx-pad: ${"p".repeat(5000)}\nname: x\ndescription: late\n---\n`,
      "dir",
      "dir",
      "",
   ],
];

describe("the server's and the package's frontmatter readers", () => {
   let root: string;
   beforeEach(() => {
      root = fs.mkdtempSync(path.join(os.tmpdir(), "skills-parity-"));
   });
   afterEach(() => {
      fs.rmSync(root, { recursive: true, force: true });
   });

   it.each(PARITY)(
      "agree on %s",
      (_label, markdown, dir, name, description) => {
         // The package reads a file through its directory reader, BOM and CRLF included.
         fs.mkdirSync(path.join(root, dir));
         fs.writeFileSync(path.join(root, dir, "SKILL.md"), markdown);
         expect(
            readSkills(root).map((s) => ({
               name: s.name,
               description: s.description,
            })),
         ).toEqual([{ name, description }]);
         const server = parseFrontmatter(markdown, dir);
         expect({ name: server.name, description: server.description }).toEqual(
            {
               name,
               description,
            },
         );
      },
   );
});
