// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import {
   buildSkills,
   flatSkills,
   isCredible,
   type BundledSkill,
   type SkillEntry,
} from "./build_skills_bundle";
import { skillFiles } from "./skill_files";
import bundle from "./skills_bundle.json";

const skills = (bundle as unknown as { skills: BundledSkill[] }).skills;

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
 * Codepoint order, matching the builder's own sort. Both sides are sorted before
 * comparing so the sync test stays about membership and content rather than order;
 * the order test below covers order separately.
 */
const byCodepoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

const byName = (a: { name: string }, b: { name: string }) =>
   byCodepoint(a.name, b.name);

/** The flat entries the MCP prompts and get_skill still read. */
const flat = flatSkills(skills);

describe("skills_bundle.json (generated dual-channel asset)", () => {
   it("is in sync with skills/", () => {
      // Regenerate with:
      //   bun run src/service/skills/build_skills_bundle.ts ../../skills
      expect([...skills].sort(byName)).toEqual(
         [...buildSkills(sourceDir)].sort(byName),
      );
   });

   it("is not empty", () => {
      expect(skills.length).toBeGreaterThan(0);
   });

   /**
    * The bundle is committed indented, which is a merge property rather than a
    * style preference. One line per entry field means two branches editing
    * different skills touch different lines, so git merges them cleanly; while
    * the file was minified it was a single line, and every pair of concurrent
    * skills edits collided on it.
    *
    * Read off disk rather than through the import above: the sync test compares
    * parsed JSON, so it stays green if a regeneration ever re-minifies the file.
    *
    * The indent width is 3 rather than 2 so the file matches tabWidth in
    * packages/server/.prettierrc. At any other width prettier reformats it, so a
    * contributor with format-on-save would fail this test by opening the file.
    *
    * Still CRLF-normalized, though the sibling .gitattributes now pins eol=lf and
    * should make that unreachable. It is two lines of insurance against that file
    * being dropped, not a claim that CRLF is expected.
    */
   it("is committed indented, one entry per line", () => {
      const raw = fs
         .readFileSync(path.join(import.meta.dir, "skills_bundle.json"), "utf8")
         .replace(/\r\n/g, "\n");

      // Read the width from prettier's config rather than restating it. There
      // were three copies of "3": the builder's JSON.stringify indent, this
      // matcher, and tabWidth in packages/server/.prettierrc. Nothing failed
      // when they disagreed, because prettier:check globs only ts and tsx and
      // never looks at a .json, so a tabWidth edit alone reintroduced the
      // format-on-save trap with the guard unable to see it. Reading it here
      // means that edit fails immediately, at the config change.
      const prettierrc = JSON.parse(
         fs.readFileSync(
            path.join(import.meta.dir, "..", "..", "..", ".prettierrc"),
            "utf8",
         ),
      ) as { tabWidth?: number };
      expect(typeof prettierrc.tabWidth).toBe("number");

      // Entry keys sit three levels deep: root object, skills array, entry.
      const indent = " ".repeat((prettierrc.tabWidth as number) * 3);

      // Needs no positive control: if this pattern stopped matching, the count
      // would be 0 rather than quietly staying green.
      const nameLines = raw
         .split("\n")
         .filter((l) => l.startsWith(`${indent}"name": `));
      expect(nameLines.length).toBe(skills.length);

      expect(raw.endsWith("\n")).toBe(true);
   });

   /**
    * Entry order decides line positions in a committed file, so it has to be the
    * same on every contributor's machine. The builder sorts by codepoint for that
    * reason: localeCompare follows the runtime's locale, and under cs-CZ the `ch`
    * digraph sorts after `h`, which moves malloy-charts and makes a regeneration
    * emit a reordering diff unrelated to the skill that actually changed.
    *
    * Both halves are about the committed file, not the comparator: no test can
    * catch a localeCompare revert on a machine whose locale agrees with codepoint
    * order, which includes en-US and therefore CI. The second assertion is the
    * one that narrows it, by failing as soon as the builder's order and the
    * committed order disagree. The sync test cannot see either problem, because
    * it sorts both sides before comparing.
    */
   it("is committed in codepoint order, and in the order the builder emits", () => {
      const names = skills.map((s) => s.name);
      expect(names).toEqual([...names].sort(byCodepoint));
      expect(names).toEqual(buildSkills(sourceDir).map((s) => s.name));
   });

   it("every skill has a name, a description in its frontmatter, and a SKILL.md", () => {
      for (const s of skills) {
         expect(s.name.length).toBeGreaterThan(0);
         expect(String(s.frontmatter.description).length).toBeGreaterThan(0);
         expect(s.bodies["SKILL.md"]!.length).toBeGreaterThan(0);
      }
   });

   it("skill names are unique", () => {
      const names = skills.map((s) => s.name);
      expect(new Set(names).size).toBe(names.length);
   });

   // The bundle must stay near the Markdown it embeds: the tree is 4.7 MB because of scripts, tests and
   // workbooks, and none of them may be embedded.
   it("stays within 2 MB", () => {
      const bytes = fs.statSync(
         path.join(import.meta.dir, "skills_bundle.json"),
      ).size;
      expect(bytes).toBeLessThan(2_000_000);
   });

   describe("a skill is its whole directory", () => {
      const dirOf = (s: BundledSkill) => path.join(sourceDir, s.name);

      it("serves SKILL.md verbatim, licence header and frontmatter included, with no pointer section", () => {
         for (const s of skills) {
            expect(s.bodies["SKILL.md"]).toBe(
               fs.readFileSync(path.join(dirOf(s), "SKILL.md"), "utf8"),
            );
            expect(s.bodies["SKILL.md"]).not.toContain(
               "Reference files over MCP",
            );
         }
         const withHeader = skills.filter((s) =>
            s.bodies["SKILL.md"]!.includes("SPDX-License-Identifier"),
         );
         expect(withHeader.length).toBeGreaterThan(0);
      });

      it("lists every file skillFiles returns, with the digest and size of its raw bytes", () => {
         for (const s of skills) {
            const found = skillFiles(dirOf(s));
            expect(found.warnings).toEqual([]);
            expect(Object.keys(s.files)).toEqual(found.files);
            for (const [file, entry] of Object.entries(s.files)) {
               const bytes = fs.readFileSync(path.join(dirOf(s), file));
               expect(entry).toEqual({
                  digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
                  size: bytes.length,
               });
            }
         }
      });

      it("embeds the text of every Markdown file and of nothing else", () => {
         for (const s of skills) {
            expect(Object.keys(s.bodies)).toEqual(
               Object.keys(s.files).filter((f) => f.endsWith(".md")),
            );
            for (const [file, text] of Object.entries(s.bodies)) {
               expect(text).toBe(
                  fs.readFileSync(path.join(dirOf(s), file), "utf8"),
               );
            }
         }
         // Positive controls: the tree does hold scripts and binaries, and they are listed.
         const listed = skills.flatMap((s) => Object.keys(s.files));
         expect(listed.some((f) => f.endsWith(".py"))).toBe(true);
         expect(listed.some((f) => f.endsWith(".md"))).toBe(true);
      });

      it("keeps the files a .skillignore names out of the manifest, while the tree holds them", () => {
         const tests = (dir: string): string[] =>
            fs
               .readdirSync(dir, { withFileTypes: true })
               .flatMap((e) =>
                  e.isDirectory()
                     ? tests(path.join(dir, e.name))
                     : e.name.endsWith("_test.py")
                       ? [e.name]
                       : [],
               );
         const onDisk = skills.flatMap((s) => tests(dirOf(s)));
         expect(onDisk.length).toBeGreaterThan(0);
         const listed = skills.flatMap((s) => Object.keys(s.files));
         expect(listed.filter((f) => f.endsWith("_test.py"))).toEqual([]);
      });

      it("keeps malloy-excel-review's fixtures/README.md and drops the rest of fixtures/", () => {
         const excel = skills.find((s) => s.name === "malloy-excel-review")!;
         expect(
            Object.keys(excel.files).filter((f) => /fixture/i.test(f)),
         ).toEqual(["fixtures/README.md"]);
         expect(excel.bodies["fixtures/README.md"]).toContain(
            "build_fixture.py",
         );
         // Positive control: the tree does hold them.
         expect(
            fs.existsSync(
               path.join(
                  sourceDir,
                  "malloy-excel-review/fixtures/fixture.xlsx",
               ),
            ),
         ).toBe(true);
      });
   });

   /**
    * A skill's reference/ files are its on-demand detail, and this channel is
    * the only way a host without the skills on disk can reach them. Before they
    * were bundled, malloy-review told an agent to load rubrics that, over MCP,
    * did not exist.
    */
   describe("reference files, as the flat entries", () => {
      const isReference = (s: SkillEntry) => s.name.includes("/");
      const referenceEntries = flat.filter(isReference);

      it("bundles every reference/*.md in the tree", () => {
         // Mirror the builder's own exclusion. A stray credible-* skill with a
         // reference/ dir is never bundled, so walking without this filter
         // fails on a correct bundle.
         const onDisk = fs
            .readdirSync(sourceDir, { withFileTypes: true })
            .filter((d) => d.isDirectory() && !isCredible(d.name))
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
            flat.filter((s) => !isReference(s)).map((s) => s.name),
         );
         for (const entry of referenceEntries) {
            expect(skillNames.has(entry.name.split("/")[0])).toBe(true);
         }
      });

      it("gives each one a description drawn from its heading", () => {
         // Reference files carry no frontmatter, so a listing would otherwise
         // show them nameless. Every file in the tree has an H1 to use.
         //
         // Assert against the no-heading fallback rather than a length floor:
         // the fallback is itself long enough to clear any such floor, so a
         // length check alone stays green even if heading extraction breaks
         // entirely and all of a skill's entries collapse to one string.
         for (const entry of referenceEntries) {
            const parent = entry.name.split("/")[0];
            expect(entry.description).toContain("Reference detail for");
            expect(entry.description).not.toBe(
               `Reference detail for the ${parent} skill.`,
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
            const parent = flat.find((s) => s.name === name);
            expect(parent?.body).toContain("Reference files over MCP");
            expect(parent?.body).toContain(`get the prompt named \`${name}/`);
         }
      });

      it("leaves skills without a reference/ directory untouched", () => {
         const plain = flat.find((s) => s.name === "malloy-getting-started");
         expect(plain?.body).not.toContain("Reference files over MCP");
      });
   });

   // No allowlist: the three reference files that carried the last of the
   // drift were rewritten upstream and copied back, so every entry in the
   // bundle now honours the house style and a new em-dash fails outright.
   //
   // Descriptions count as well as bodies. A SKILL.md's frontmatter
   // description is not part of its body, and it ships as the MCP prompt's
   // description, so it is bundle text like any other.
   it("carries no em-dashes (house style)", () => {
      const hasEmDash = (s: SkillEntry) =>
         s.body.includes("—") || s.description.includes("—");

      // Positive controls, one per field the predicate reads. With the
      // allowlist gone the offenders assertion runs against a clean tree, so
      // it would pass for free if the predicate ever stopped detecting; the
      // deleted allowlist test proved that incidentally, by asserting the
      // listed entries still held one. An empty description (or body) on
      // the other control is load-bearing: it is the half that would stay
      // green if that field were dropped from the predicate.
      expect(
         hasEmDash({ name: "control", description: "", body: "a — b" }),
      ).toBe(true);
      expect(
         hasEmDash({ name: "control2", description: "a — b", body: "" }),
      ).toBe(true);

      const offenders = flat.filter(hasEmDash).map((s) => s.name);
      expect(offenders).toEqual([]);
   });
});
