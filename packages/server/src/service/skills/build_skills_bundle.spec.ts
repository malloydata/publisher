// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { buildSkills, flatSkills, parseSkill } from "./build_skills_bundle";
import { unquote } from "./frontmatter";

const SKILL_MD =
   "---\nname: pricing\ndescription: Price a thing.\n---\n<!--\nSPDX-License-Identifier: MIT\n-->\n# Pricing\n\nRun `scripts/run.py`.\n";
const BINARY = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0, 1, 2, 3]);

describe("buildSkills", () => {
   let root: string;
   let skillsDir: string;
   const write = (file: string, data: string | Buffer) => {
      const full = path.join(skillsDir, file);
      fs.mkdirSync(path.dirname(full), { recursive: true });
      fs.writeFileSync(full, data);
   };

   beforeAll(() => {
      root = fs.mkdtempSync(path.join(os.tmpdir(), "bundle-"));
      skillsDir = path.join(root, "skills");
      fs.mkdirSync(path.join(root, "manifests"));
      fs.writeFileSync(
         path.join(root, "manifests", "publisher-local.json"),
         JSON.stringify({ auto_discovered: ["pricing"], supporting: [] }),
      );
      write("pricing/SKILL.md", SKILL_MD);
      write("pricing/reference/rules.md", "# Rules\n\nDetail.\n");
      write("pricing/references/more.md", "# More\n");
      write("pricing/scripts/run.py", "print('hi')\n");
      write("pricing/scripts/run_test.py", "assert True\n");
      write("pricing/assets/logo.png", BINARY);
      write("pricing/assets/notes.md", "# Notes\n");
      write("pricing/.skillignore", "*_test.py\n");
      write("pricing/.env", "SECRET=1\n");
      // Neither is in the manifest, so neither is bundled.
      write("other/SKILL.md", "---\nname: other\ndescription: x\n---\nbody\n");
      write("credible-local/SKILL.md", "---\nname: credible-local\n---\n");
   });
   afterAll(() => fs.rmSync(root, { recursive: true, force: true }));

   it("lists every file skillFiles returns, with the digest and size of its raw bytes", () => {
      const [skill] = buildSkills(skillsDir);
      expect(Object.keys(skill!.files)).toEqual([
         "SKILL.md",
         "assets/logo.png",
         "assets/notes.md",
         "reference/rules.md",
         "references/more.md",
         "scripts/run.py",
      ]);
      expect(skill!.files["assets/logo.png"]).toEqual({
         digest: `sha256:${createHash("sha256").update(BINARY).digest("hex")}`,
         size: BINARY.length,
      });
   });

   it("embeds the verbatim text of Markdown files outside assets/ only, and the parsed frontmatter", () => {
      const [skill] = buildSkills(skillsDir);
      expect(skill!.name).toBe("pricing");
      expect(skill!.frontmatter).toEqual({
         name: "pricing",
         description: "Price a thing.",
      });
      expect(skill!.bodies).toEqual({
         "SKILL.md": SKILL_MD,
         "reference/rules.md": "# Rules\n\nDetail.\n",
         "references/more.md": "# More\n",
      });
   });

   it("leaves out what .skillignore names and every dotfile", () => {
      const [skill] = buildSkills(skillsDir);
      expect(Object.keys(skill!.files)).not.toContain("scripts/run_test.py");
      expect(Object.keys(skill!.files)).not.toContain(".env");
      expect(Object.keys(skill!.files)).not.toContain(".skillignore");
   });

   it("bundles only the skills the manifest names", () => {
      expect(buildSkills(skillsDir).map((s) => s.name)).toEqual(["pricing"]);
   });

   it("fails rather than bundle a skill whose walk or frontmatter had a finding", () => {
      write("pricing/.skillignore", "*_test.py\n./scripts/run.py\n");
      expect(() => buildSkills(skillsDir)).toThrow(/pricing.*starts with/);
      write("pricing/.skillignore", "*_test.py\n");
      write("pricing/SKILL.md", "---\nname: [\n---\nbody\n");
      expect(() => buildSkills(skillsDir)).toThrow(/not valid YAML/);
      write("pricing/SKILL.md", SKILL_MD);
   });

   it("flattens to the entries get_skill reads: a body with a pointer section, and one per reference file", () => {
      const flat = flatSkills(buildSkills(skillsDir));
      expect(flat.map((s) => s.name)).toEqual(["pricing", "pricing/rules"]);
      expect(flat[0]!.body).toContain("# Pricing");
      expect(flat[0]!.body).not.toContain("SPDX-License-Identifier");
      expect(flat[0]!.body).toContain("Reference files over MCP");
      expect(flat[1]!.body).toBe("# Rules\n\nDetail.");
   });
});

describe("buildSkills refuses a link, which would be followed to whatever it names", () => {
   // Each case builds a skill `pricing` in `skills/` whose one odd entry is a link, beside a target outside it.
   const cases: Array<[string, (root: string) => void, RegExp]> = [
      [
         "a file link",
         (root) => {
            fs.writeFileSync(path.join(root, "outside.txt"), "secret\n");
            fs.symlinkSync(
               "../../../outside.txt",
               path.join(root, "skills/pricing/scripts/run.py"),
            );
         },
         /skill 'pricing': 'scripts\/run\.py' is a link/,
      ],
      [
         "a linked Markdown file",
         (root) => {
            fs.writeFileSync(path.join(root, "outside.md"), "# Secret\n");
            fs.symlinkSync(
               "../../../outside.md",
               path.join(root, "skills/pricing/reference/more.md"),
            );
         },
         /skill 'pricing': 'reference\/more\.md' is a link/,
      ],
      [
         "a linked directory",
         (root) => {
            fs.mkdirSync(path.join(root, "outside"));
            fs.symlinkSync(
               "../../outside",
               path.join(root, "skills/pricing/data"),
            );
         },
         /skill 'pricing': 'data' is a link/,
      ],
      [
         "a linked skill root",
         (root) => {
            fs.renameSync(
               path.join(root, "skills/pricing"),
               path.join(root, "elsewhere"),
            );
            fs.symlinkSync("../elsewhere", path.join(root, "skills/pricing"));
         },
         /skill 'pricing' is a link/,
      ],
   ];

   for (const [label, setup, message] of cases) {
      it(`throws for ${label}`, () => {
         const root = fs.mkdtempSync(path.join(os.tmpdir(), "bundle-link-"));
         try {
            fs.mkdirSync(path.join(root, "manifests"));
            fs.writeFileSync(
               path.join(root, "manifests", "publisher-local.json"),
               JSON.stringify({ auto_discovered: ["pricing"], supporting: [] }),
            );
            const skill = path.join(root, "skills/pricing");
            fs.mkdirSync(path.join(skill, "scripts"), { recursive: true });
            fs.mkdirSync(path.join(skill, "reference"));
            fs.writeFileSync(path.join(skill, "SKILL.md"), SKILL_MD);
            setup(root);
            expect(() => buildSkills(path.join(root, "skills"))).toThrow(
               message,
            );
         } finally {
            fs.rmSync(root, { recursive: true, force: true });
         }
      });
   }
});

describe("build_skills_bundle parseSkill", () => {
   it("parses name, description, and body from frontmatter", () => {
      const md =
         "---\nname: malloy-queries\ndescription: Query patterns.\n---\n# Body\n\nText.";
      const s = parseSkill(md, "dir");
      expect(s.name).toBe("malloy-queries");
      expect(s.description).toBe("Query patterns.");
      expect(s.body).toBe("# Body\n\nText.");
   });

   it("keeps colons in the description (line-based, not split on colon)", () => {
      const md =
         "---\nname: x\ndescription: Use when: a thing happens.\n---\nbody";
      expect(parseSkill(md, "dir").description).toBe(
         "Use when: a thing happens.",
      );
   });

   it("tolerates CRLF line endings", () => {
      const md = "---\r\nname: x\r\ndescription: d\r\n---\r\nbody line";
      const s = parseSkill(md, "dir");
      expect(s.name).toBe("x");
      expect(s.description).toBe("d");
      expect(s.body).toBe("body line");
   });

   it("falls back to the dir name and empty description when frontmatter is missing", () => {
      const s = parseSkill("# Just a body, no frontmatter", "fallback-name");
      expect(s.name).toBe("fallback-name");
      expect(s.description).toBe("");
      expect(s.body).toBe("# Just a body, no frontmatter");
   });

   it("strips surrounding quotes from values", () => {
      const md = "---\nname: \"quoted-name\"\ndescription: 'q'\n---\nb";
      const s = parseSkill(md, "dir");
      expect(s.name).toBe("quoted-name");
      expect(s.description).toBe("q");
   });
});

describe("build_skills_bundle unquote", () => {
   it("removes a single layer of surrounding quotes", () => {
      expect(unquote('"hi"')).toBe("hi");
      expect(unquote("'hi'")).toBe("hi");
      expect(unquote("plain")).toBe("plain");
   });
});
