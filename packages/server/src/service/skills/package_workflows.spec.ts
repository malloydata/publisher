// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { linkTextKeptVerbatim } from "../../test_helpers/link_text";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import { createHash } from "crypto";
import path from "path";
import { FCJ_MAX_CHARS } from "../../retrieval/search/pages";
import {
   advertisedSkills,
   emptyWorkflows,
   readPackageWorkflows,
   resolveSkills,
   WORKFLOW_INSTRUCTIONS_MAX_BYTES,
   workflowSkillSet,
   type PackageWorkflows,
} from "./package_skills";

const isWindows = process.platform === "win32";
const isRoot = process.getuid?.() === 0;
// macOS and Windows default to case-insensitive filesystems, where two names that differ only by case cannot coexist.
const caseSensitiveFs = (() => {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "case-probe-"));
   try {
      fs.writeFileSync(path.join(dir, "A"), "");
      return !fs.existsSync(path.join(dir, "a"));
   } finally {
      fs.rmSync(dir, { recursive: true, force: true });
   }
})();

const MAIN = "workflows/analyst/skill_sets/main";
const SECOND = "workflows/analyst/skill_sets/second";
const LINKED = "workflows/analyst/linked";
const CONVENTIONS = `${MAIN}/conventions`;
const INSTRUCTIONS = "workflows/analyst/instructions.md";
/** A file of the full view; the view's own type is the union with the summary's. */
type FullFile = { relative_filepath: string; file_contents: string };
const sha = (data: string | Buffer) =>
   createHash("sha256").update(data).digest("hex");

describe("readPackageWorkflows", () => {
   let pkg: string;
   let outside: string;

   beforeEach(() => {
      pkg = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-workflows-"));
      outside = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-workflows-out-"));
   });

   afterEach(() => {
      fs.rmSync(pkg, { recursive: true, force: true });
      fs.rmSync(outside, { recursive: true, force: true });
   });

   const write = (rel: string, text: string | Buffer) => {
      fs.mkdirSync(path.dirname(path.join(pkg, rel)), { recursive: true });
      fs.writeFileSync(path.join(pkg, rel), text);
   };

   const writeSkill = (setPath: string, name: string, body = "body") =>
      write(
         `${setPath}/${name}/SKILL.md`,
         `---\nname: ${name}\ndescription: d\n---\n\n${body}\n`,
      );

   /** A complete, valid workflow on disk; returns its manifest entry. */
   const analyst = (extra: Record<string, unknown> = {}) => {
      write(INSTRUCTIONS, "Be careful with revenue.");
      write(
         `${CONVENTIONS}/SKILL.md`,
         "---\nname: conventions\ndescription: House rules.\nallowed-tools: Bash\nx-owner: me\n---\n\nUse net revenue.\n",
      );
      write(
         `${CONVENTIONS}/reference/glossary.md`,
         "# Glossary\n\nnet = after refunds\n",
      );
      return {
         description: "Answers revenue questions",
         instructions: INSTRUCTIONS,
         skill_sets: [MAIN],
         ...extra,
      };
   };

   const read = (workflows: unknown) => readPackageWorkflows(pkg, workflows);

   /** Asserts the workflow is refused with one warning in the shape Credible parses; returns that warning. */
   const refused = (workflows: unknown, name = "analyst") => {
      const result = read(workflows);
      expect(result.workflows.has(name)).toBe(false);
      expect(result.warnings).toHaveLength(1);
      expect(
         result.warnings[0]!.startsWith(`Workflow '${name}' is not served: `),
      ).toBe(true);
      expect(
         result.warnings[0]!.endsWith(
            `. Fix: correct the 'workflows.${name}' entry in publisher.json and the files it names.`,
         ),
      ).toBe(true);
      return result.warnings[0]!;
   };

   /** The files of one set of a served workflow, as the wire carries them. */
   const viewOf = (result: PackageWorkflows, index = 0, name = "analyst") => {
      const set = workflowSkillSet(
         result.workflows.get(name)!.sets[index]!,
         "full",
      );
      return { ...set, skills: set.skills as FullFile[] };
   };

   describe("a workflow", () => {
      it("resolves to the contract shape", () => {
         const result = read({ analyst: analyst() });
         expect(result.warnings).toEqual([]);
         const workflow = result.workflows.get("analyst")!;
         expect(workflow).toMatchObject({
            name: "analyst",
            description: "Answers revenue questions",
            instructions: {
               path: INSTRUCTIONS,
               text: "Be careful with revenue.",
            },
            warnings: [],
         });
         expect(workflow.definitionSha).toMatch(/^[0-9a-f]{64}$/);
         expect(workflow.sets).toHaveLength(1);
         expect(workflow.sets[0]!.set).toMatchObject({
            name: "main",
            path: MAIN,
            findings: [],
         });
         expect(workflow.sets[0]!.set.digest).toMatch(/^[0-9a-f]{64}$/);
         expect(result.sets.get(MAIN)).toBe(workflow.sets[0]!.set);
         expect(result.paths.sort()).toEqual([
            INSTRUCTIONS,
            `${CONVENTIONS}/SKILL.md`,
            `${CONVENTIONS}/reference/glossary.md`,
         ]);
         expect(result.watched).toEqual([INSTRUCTIONS, MAIN]);
         expect(result.skillsDirNotRead).toBe(false);
      });

      it("re-emits SKILL.md with only name and description and serves references raw, sorted", () => {
         const { skills } = viewOf(read({ analyst: analyst() }));
         expect(skills.map((s) => s.relative_filepath)).toEqual([
            "conventions/SKILL.md",
            "conventions/reference/glossary.md",
         ]);
         expect(skills[0]!.file_contents).toBe(
            '---\nname: "conventions"\ndescription: "House rules."\n---\n\nUse net revenue.\n',
         );
         expect(skills[1]!.file_contents).toBe(
            "# Glossary\n\nnet = after refunds\n",
         );
      });

      it("summarizes a skill with its parsed name and description, not the quoted emission", () => {
         write(INSTRUCTIONS, "i");
         write(
            `${MAIN}/quoted/SKILL.md`,
            '---\nname: quoted\ndescription: Say "net" and use C:\\data\n---\n\nbody\n',
         );
         const result = read({
            analyst: { instructions: INSTRUCTIONS, skill_sets: [MAIN] },
         });
         const full = viewOf(result).skills[0]!;
         expect(
            workflowSkillSet(
               result.workflows.get("analyst")!.sets[0]!,
               "summary",
            ).skills,
         ).toEqual([
            {
               relative_filepath: "quoted/SKILL.md",
               name: "quoted",
               description: 'Say "net" and use C:\\data',
               bytes: Buffer.byteLength(full.file_contents),
            },
         ]);
      });

      it("ignores x- keys on the workflow", () => {
         const result = read({
            analyst: analyst({ "x-owner": "data team", "x-note": 1 }),
         });
         expect(result.warnings).toEqual([]);
         expect(result.workflows.has("analyst")).toBe(true);
      });

      it("serves a workflow with only instructions, or only skill sets, and needs no description", () => {
         write(INSTRUCTIONS, "brief");
         writeSkill(MAIN, "conventions");
         const result = read({
            brief: { instructions: INSTRUCTIONS },
            skills: { skill_sets: [MAIN] },
         });
         expect(result.warnings).toEqual([]);
         const brief = result.workflows.get("brief")!;
         expect(brief.sets).toEqual([]);
         expect(brief.description).toBeUndefined();
         expect(result.workflows.get("skills")!.instructions).toBeUndefined();
      });

      it("refuses a workflow that declares nothing to serve", () => {
         expect(refused({ analyst: {} })).toContain(
            "it declares nothing to serve: add 'instructions' or 'skill_sets'",
         );
         expect(
            refused({ analyst: { description: "d", skill_sets: [] } }),
         ).toContain("it declares nothing to serve");
      });

      it("refuses only the bad workflow and keeps the others", () => {
         const good = analyst();
         const result = read({ analyst: good, "Bad Name": good, other: good });
         expect([...result.workflows.keys()]).toEqual(["analyst", "other"]);
         expect(result.warnings).toHaveLength(1);
         expect(result.warnings[0]).toContain(
            "Workflow 'Bad Name' is not served",
         );
      });

      it("serves a workflow at the package root next to the package's own publisher.json", () => {
         write("publisher.json", "{}");
         write("brief.md", "hello");
         const result = read({ root: { instructions: "brief.md" } });
         expect(result.workflows.get("root")!.instructions!.text).toBe("hello");
      });
   });

   describe("manifest shape", () => {
      it("returns nothing and no warning when the manifest declares no workflows", () => {
         expect(read(undefined)).toEqual(emptyWorkflows());
      });

      it.each([[[]], ["x"], [5], [null]])(
         "warns and serves nothing when workflows is %p",
         (value) => {
            const result = read(value);
            expect(result.workflows.size).toBe(0);
            expect(result.warnings).toHaveLength(1);
            expect(result.warnings[0]).toContain("must be an object");
            expect(result.warnings[0]).toContain("Fix:");
         },
      );

      it.each([
         ["Upper"],
         ["-lead"],
         ["trail-"],
         ["dou--ble"],
         ["under_score"],
         ["a".repeat(65)],
         [""],
      ])("refuses a workflow named %p", (name) => {
         const result = read({ [name]: analyst() });
         expect(result.workflows.size).toBe(0);
         expect(result.warnings).toHaveLength(1);
         expect(result.warnings[0]).toContain(
            "its name must be 1-64 characters of lowercase letters, digits and single hyphens",
         );
      });

      it("serves a workflow named for its 64 characters", () => {
         expect(
            read({ [`a${"b".repeat(63)}`]: analyst() }).workflows.size,
         ).toBe(1);
      });

      it("does not treat __proto__ as a workflow", () => {
         const raw = JSON.parse(`{"__proto__": {"description": "d"}}`);
         expect(read(raw).workflows.size).toBe(0);
      });

      it.each([
         ["model", "sonnet"],
         ["budget", { turns: 5 }],
         ["schedules", []],
         ["skills", ["x"]],
         ["tools", ["Bash"]],
         ["mcp-servers", {}],
         ["hooks", {}],
         ["permission-mode", "bypass"],
         ["surprise", 1],
      ])("refuses a workflow that sets %p, naming it", (key, value) => {
         const warning = refused({ analyst: analyst({ [key]: value }) });
         expect(warning).toContain(`'${key}'`);
         expect(warning).toContain(
            "Allowed keys: description, instructions, skill_sets",
         );
      });

      it.each([
         ["description", ""],
         ["description", "  "],
         ["description", 3],
         ["instructions", 3],
         ["instructions", ""],
         ["instructions", { path: "x" }],
         ["instructions", ["x.md"]],
         ["skill_sets", "workflows/analyst/skill_sets/main"],
         ["skill_sets", {}],
         ["skill_sets", [null]],
         ["skill_sets", [{}]],
         ["skill_sets", [["x"]]],
         ["skill_sets", [""]],
      ])("refuses a workflow whose %s is %p", (key, value) => {
         refused({ analyst: analyst({ [key]: value }) });
      });

      it.each([[[1]], ["x"], [null]])(
         "refuses a workflow whose entry is %p",
         (entry) => {
            expect(refused({ analyst: entry })).toContain(
               "its entry must be an object",
            );
         },
      );

      it("never throws on hostile values", () => {
         const hostile: unknown[] = [
            { a: null },
            { a: [] },
            { a: { instructions: { path: "x" } } },
            { a: { instructions: ["x.md"] } },
            { a: { instructions: "x.md", skill_sets: [null] } },
            { a: { instructions: "x.md", skill_sets: [{}] } },
            { a: { instructions: "x.md", skill_sets: "x" } },
            { a: { skill_sets: [5] } },
            { a: { skill_sets: [["x"]] } },
            { a: { description: { d: 1 }, skill_sets: ["x"] } },
         ];
         for (const raw of hostile) {
            const result = read(raw);
            expect(result.workflows.size).toBe(0);
            expect(result.warnings.length).toBeGreaterThan(0);
         }
      });
   });

   describe("instructions", () => {
      it.each([
         ["an absolute path", "/etc/passwd.md"],
         ["a parent escape", "../outside.md"],
         ["a NUL byte", "workflows/a\0.md"],
         ["a non-markdown file", "publisher.json"],
         ["a data file", "data.csv"],
         ["a missing file", "workflows/analyst/missing.md"],
         ["a backslash path", "workflows\\analyst\\instructions.md"],
      ])(
         "refuses a workflow whose instructions is %s",
         (_label, instructions) => {
            write("publisher.json", "{}");
            write("data.csv", "a,b");
            refused({ analyst: analyst({ instructions }) });
         },
      );

      it("names the backslash", () => {
         expect(
            refused({
               analyst: analyst({ skill_sets: ["workflows\\analyst"] }),
            }),
         ).toContain("backslash");
      });

      it("refuses instructions that are a link out of the package", () => {
         const entry = analyst();
         fs.writeFileSync(path.join(outside, "secret.md"), "SECRET");
         fs.rmSync(path.join(pkg, INSTRUCTIONS));
         fs.symlinkSync(
            path.join(outside, "secret.md"),
            path.join(pkg, INSTRUCTIONS),
         );
         expect(refused({ analyst: entry })).toContain("outside the package");
      });

      it("refuses instructions that are a .md link to publisher.json", () => {
         write("publisher.json", '{"name":"x"}');
         const entry = analyst();
         fs.rmSync(path.join(pkg, INSTRUCTIONS));
         fs.symlinkSync(
            path.join(pkg, "publisher.json"),
            path.join(pkg, INSTRUCTIONS),
         );
         expect(refused({ analyst: entry })).toContain("not Markdown");
      });

      it("serves instructions at the 16 KiB cap and refuses one byte over", () => {
         const entry = analyst();
         write(INSTRUCTIONS, "x".repeat(WORKFLOW_INSTRUCTIONS_MAX_BYTES));
         expect(read({ analyst: entry }).workflows.size).toBe(1);
         write(INSTRUCTIONS, "x".repeat(WORKFLOW_INSTRUCTIONS_MAX_BYTES + 1));
         expect(refused({ analyst: entry })).toContain("byte cap");
      });

      it("refuses instructions beside a nested publisher.json", () => {
         const entry = analyst();
         write("workflows/analyst/publisher.json", "{}");
         expect(refused({ analyst: entry })).toContain(
            `'instructions' '${INSTRUCTIONS}' sits under a nested publisher.json at 'workflows/analyst'`,
         );
      });

      it("hashes the same files for ./ and doubled slashes in a path", () => {
         const entry = analyst();
         const plain = read({ analyst: entry });
         const dotted = read({
            analyst: {
               ...entry,
               instructions: "./workflows//analyst/instructions.md",
               skill_sets: [`${MAIN}/`],
            },
         });
         expect(dotted.paths.sort()).toEqual(plain.paths.sort());
         expect(dotted.workflows.get("analyst")!.definitionSha).toBe(
            plain.workflows.get("analyst")!.definitionSha,
         );
      });
   });

   describe("skill_sets", () => {
      it.each([
         ["missing", "nope"],
         ["absolute", "/tmp"],
         ["the package root", "."],
         ["a parent escape", "../x"],
         ["a file", INSTRUCTIONS],
      ])("refuses a workflow whose skill set entry is %s", (_label, entry) => {
         refused({ analyst: analyst({ skill_sets: [entry] }) });
      });

      it("says a missing skill set does not exist", () => {
         expect(
            refused({ analyst: analyst({ skill_sets: ["nope"] }) }),
         ).toContain("skill set directory 'nope' does not exist");
      });

      it("refuses a workflow whose skill set is a link out of the package, the same whether or not the outside directory exists", () => {
         const answers: string[] = [];
         fs.mkdirSync(path.join(outside, "present"));
         for (const which of ["present", "absent_"]) {
            fs.rmSync(path.join(pkg, "workflows"), {
               recursive: true,
               force: true,
            });
            write(INSTRUCTIONS, "Be careful.");
            // Out through <outside>/<which> and back into the package's own
            // workflows/analyst/real directory.
            const through = path.join(outside, which);
            fs.mkdirSync(path.join(pkg, "workflows/analyst/real"), {
               recursive: true,
            });
            fs.symlinkSync(
               `${through}/${path.relative(through, path.join(pkg, "workflows/analyst/real"))}`,
               path.join(pkg, LINKED),
            );
            const workflows = {
               analyst: {
                  instructions: INSTRUCTIONS,
                  skill_sets: [LINKED],
               },
            };
            if (linkTextKeptVerbatim()) {
               answers.push(refused(workflows));
            } else {
               // Normalized when made (Windows), the link leads to the
               // workflow's own real/ inside the package and is served.
               const result = read(workflows);
               expect(result.workflows.has("analyst")).toBe(true);
               answers.push(JSON.stringify(result.warnings));
            }
         }
         expect(answers[0]).toBe(answers[1]);
         if (linkTextKeptVerbatim()) {
            expect(answers[0]).toContain("resolves outside the package");
         } else {
            expect(answers[0]).not.toContain("resolves outside the package");
         }
      });

      it("refuses a workflow whose skill set directory is a link out of the package", () => {
         const entry = analyst();
         fs.mkdirSync(path.join(outside, "s"));
         writeSkill(outside, "s");
         fs.rmSync(path.join(pkg, MAIN), { recursive: true });
         fs.symlinkSync(outside, path.join(pkg, MAIN));
         expect(refused({ analyst: entry })).toContain("outside the package");
      });

      it("does not say whether a publisher.json exists beyond a link out of the package", () => {
         const entry = analyst();
         fs.writeFileSync(path.join(outside, "publisher.json"), "{}");
         fs.rmSync(path.join(pkg, MAIN), { recursive: true });
         fs.symlinkSync(outside, path.join(pkg, MAIN));
         const warning = refused({ analyst: entry });
         expect(warning).toContain("outside the package");
         expect(warning).not.toContain("nested");
      });

      it("sets aside a skill directory that is a link out, and serves the workflow", () => {
         const entry = analyst();
         fs.mkdirSync(path.join(outside, "linked"));
         fs.writeFileSync(
            path.join(outside, "linked", "SKILL.md"),
            "---\nname: linked\ndescription: leaked\n---\nSECRET",
         );
         fs.symlinkSync(
            path.join(outside, "linked"),
            path.join(pkg, MAIN, "linked"),
         );
         const result = read({ analyst: entry });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(
            viewOf(result).skills.map((s) => s.relative_filepath),
         ).not.toContainEqual(expect.stringContaining("linked"));
         expect(result.warnings.join("\n")).toContain(
            `Skill set '${MAIN}' skill finding: Package skill '${MAIN}/linked/SKILL.md' resolves outside the package through a link, so it is not served.`,
         );
      });

      it("serves a set that is a link to a directory inside the package, without walking beyond it for a publisher.json", () => {
         const entry = analyst();
         writeSkill("shared", "conventions");
         write("shared/conventions/other/publisher.json", "{}");
         fs.rmSync(path.join(pkg, MAIN), { recursive: true });
         fs.symlinkSync(path.join(pkg, "shared"), path.join(pkg, MAIN));
         // Nothing below the skill's own folders is read for one, so this one is skipped.
         expect(read({ analyst: entry }).workflows.has("analyst")).toBe(true);
      });

      it("refuses a workflow with a nested publisher.json at the set root, in a skill, in reference/ and below assets/", () => {
         const entry = analyst();
         for (const [where, found] of [
            [`${MAIN}/publisher.json`, "publisher.json"],
            [`${CONVENTIONS}/publisher.json`, "conventions/publisher.json"],
            [
               `${CONVENTIONS}/reference/publisher.json`,
               "conventions/reference/publisher.json",
            ],
            [
               `${CONVENTIONS}/assets/a/Publisher.JSON`,
               "conventions/assets/a/Publisher.JSON",
            ],
         ] as const) {
            write(where, "{}");
            expect(refused({ analyst: entry })).toContain(
               `skill set directory '${MAIN}' contains a nested publisher.json at '${found}'`,
            );
            fs.rmSync(path.join(pkg, where));
         }
         expect(read({ analyst: entry }).workflows.has("analyst")).toBe(true);
      });

      it("refuses a workflow whose skill set sits under a nested publisher.json, whatever the depth", () => {
         const entry = analyst({ instructions: undefined });
         for (const above of ["workflows/analyst", "workflows"]) {
            write(`${above}/publisher.json`, "{}");
            expect(refused({ analyst: entry })).toContain(
               `skill set directory '${MAIN}' sits under a nested publisher.json at '${above}'`,
            );
            fs.rmSync(path.join(pkg, above, "publisher.json"));
         }
      });

      it("serves a set below the package's own publisher.json", () => {
         const entry = analyst();
         write("publisher.json", "{}");
         expect(read({ analyst: entry }).workflows.has("analyst")).toBe(true);
      });

      it("refuses a nested publisher.json behind a linked skill directory", () => {
         write(
            "shared/linked/SKILL.md",
            "---\nname: linked\ndescription: d\n---\n\nbody\n",
         );
         write("shared/linked/publisher.json", "{}");
         const entry = analyst();
         fs.symlinkSync(
            path.join(pkg, "shared/linked"),
            path.join(pkg, MAIN, "linked"),
         );
         expect(refused({ analyst: entry })).toContain("nested publisher.json");
      });

      it("skips a publisher.json anywhere else below a skill, with the files a skill may not hold", () => {
         const entry = analyst();
         write(`${CONVENTIONS}/templates/publisher.json`, "{}");
         const result = read({ analyst: entry });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.warnings.join("\n")).toContain(
            `${CONVENTIONS}/templates/publisher.json`,
         );
      });

      it("refuses a workflow that lists more than eight skill sets, and serves eight", () => {
         const names = Array.from({ length: 9 }, (_, i) => `s${i}`);
         for (const name of names) writeSkill(`sets/${name}`, "k");
         const sets = names.map((name) => `sets/${name}`);
         expect(refused({ analyst: { skill_sets: sets } })).toContain(
            "'skill_sets' lists 9 directories, over the limit of 8",
         );
         const eight = read({ analyst: { skill_sets: sets.slice(0, 8) } });
         expect(eight.workflows.get("analyst")!.sets).toHaveLength(8);
      });

      it("refuses a set whose folder is not a valid name", () => {
         writeSkill("sets/Bad_Name", "k");
         expect(
            refused({ analyst: { skill_sets: ["sets/Bad_Name"] } }),
         ).toContain(
            "skill set directory 'sets/Bad_Name' is named 'Bad_Name', which must be 1-64 characters of lowercase letters, digits and single hyphens",
         );
      });

      it("refuses two sets with the same folder name, and the same path twice", () => {
         writeSkill("a/main", "one");
         writeSkill("b/main", "two");
         expect(
            refused({ analyst: { skill_sets: ["a/main", "b/main"] } }),
         ).toContain(
            "skill set directories 'a/main' and 'b/main' are both named 'main'",
         );
         expect(
            refused({ analyst: { skill_sets: ["a/main", "./a//main"] } }),
         ).toContain("skill set directory 'a/main' is listed twice");
      });

      it("lets two workflows list sets that share a folder name", () => {
         writeSkill("a/main", "one");
         writeSkill("b/main", "two");
         const result = read({
            one: { skill_sets: ["a/main"] },
            two: { skill_sets: ["b/main"] },
         });
         expect(result.warnings).toEqual([]);
         expect([...result.sets.keys()]).toEqual(["a/main", "b/main"]);
      });
   });

   describe("a skill in a set", () => {
      it("is served from the first set that holds its name, with a warning on the workflow", () => {
         const entry = analyst();
         writeSkill(SECOND, "conventions", "second copy");
         writeSkill(SECOND, "extra");
         const result = read({
            analyst: { ...entry, skill_sets: [MAIN, SECOND] },
         });
         const workflow = result.workflows.get("analyst")!;
         expect(workflow.warnings).toEqual([
            `Workflow 'analyst' lists skill 'conventions' in both '${MAIN}' and '${SECOND}'; the copy in '${MAIN}', listed first, is the one served.`,
         ]);
         expect(result.warnings).toEqual(workflow.warnings);
         expect(
            workflow.sets.map(({ skills }) => skills.map((s) => s.dirName)),
         ).toEqual([["conventions"], ["extra"]]);
         expect(
            viewOf(result, 0).skills.map((s) => s.file_contents)[0],
         ).toContain("Use net revenue.");
         // The set itself still holds both, for a workflow that lists it first.
         expect(result.sets.get(SECOND)!.skills.map((s) => s.dirName)).toEqual([
            "conventions",
            "extra",
         ]);
      });

      it("keeps its own set per file path: sets are listed in the order the workflow lists them", () => {
         const entry = analyst();
         writeSkill(SECOND, "aaa");
         const result = read({
            analyst: { ...entry, skill_sets: [SECOND, MAIN] },
         });
         expect(
            result.workflows.get("analyst")!.sets.map((s) => s.set.path),
         ).toEqual([SECOND, MAIN]);
         expect(
            viewOf(result, 0).skills.map((s) => s.relative_filepath),
         ).toEqual(["aaa/SKILL.md"]);
      });

      it("is set aside, with a finding, when it has no readable SKILL.md, and the workflow is served", () => {
         const entry = analyst();
         fs.mkdirSync(path.join(pkg, MAIN, "empty"));
         const result = read({ analyst: entry });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.warnings).toHaveLength(1);
         expect(
            result.warnings[0]!.startsWith(
               `Skill set '${MAIN}' skill finding: Package skill '${MAIN}/empty/SKILL.md'`,
            ),
         ).toBe(true);
      });

      it("is served, flagged, when it has no description", () => {
         write(`${MAIN}/quiet/SKILL.md`, "---\nname: quiet\n---\n\nbody\n");
         const result = read({ analyst: analyst() });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(
            viewOf(result).skills.map((s) => s.relative_filepath),
         ).toContain("quiet/SKILL.md");
         expect(result.warnings.join("\n")).toContain("no description");
         expect(result.warnings.join("\n")).toContain("'quiet'");
      });

      // chmod revokes nothing on Windows or for root.
      it.skipIf(isWindows || isRoot)(
         "is served, flagged, when it has a directory that cannot be listed",
         () => {
            writeSkill(MAIN, "odd");
            const sealed = path.join(pkg, MAIN, "odd/scripts");
            fs.mkdirSync(sealed);
            fs.chmodSync(sealed, 0o000);
            try {
               const result = read({ analyst: analyst() });
               expect(result.workflows.has("analyst")).toBe(true);
               expect(result.warnings.join("\n")).toContain("cannot be listed");
            } finally {
               fs.chmodSync(sealed, 0o755);
            }
         },
      );

      it("is set aside when its name is not its directory name", () => {
         write(
            `${MAIN}/other-dir/SKILL.md`,
            "---\nname: elsewhere\ndescription: d\n---\n\nbody\n",
         );
         const result = read({ analyst: analyst() });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(
            viewOf(result).skills.map((s) => s.relative_filepath),
         ).not.toContain("other-dir/SKILL.md");
         expect(result.warnings.join("\n")).toContain("'elsewhere'");
      });

      it("serves a set that holds no skills, with a finding", () => {
         fs.mkdirSync(path.join(pkg, MAIN), { recursive: true });
         write(INSTRUCTIONS, "i");
         const result = read({
            analyst: { instructions: INSTRUCTIONS, skill_sets: [MAIN] },
         });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.warnings).toEqual([
            `Skill set '${MAIN}' skill finding: the skill set holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry.`,
         ]);
      });

      it("never refuses a workflow, whatever its skills hold", () => {
         const entry = analyst();
         write(`${CONVENTIONS}/scripts/bad name.py`, "x");
         write(`${CONVENTIONS}/a..b.md`, "x");
         write(`${CONVENTIONS}/m.malloy`, "x");
         fs.mkdirSync(path.join(pkg, MAIN, "empty"));
         expect(read({ analyst: entry }).workflows.has("analyst")).toBe(true);
      });
   });

   describe("inline Markdown", () => {
      const inlineOf = (result: PackageWorkflows) =>
         viewOf(result).skills.map((s) => s.relative_filepath);
      const assetsOf = (result: PackageWorkflows) =>
         (viewOf(result).assets ?? []).map((a) => a.relative_filepath);

      it.each([
         ["a space", "reference/has space.md", "its name is outside"],
         ["a percent sign", "reference/50%-rule.md", "its name is outside"],
         ["'..'", "reference/a..b.md", "its name has '..' in it"],
      ])(
         "does not serve a reference whose name has %s, and names the file",
         (_label, rel, why) => {
            write(`${CONVENTIONS}/${rel}`, "# R\n");
            const result = read({ analyst: analyst() });
            expect(inlineOf(result)).not.toContain(`conventions/${rel}`);
            expect(assetsOf(result)).not.toContain(`conventions/${rel}`);
            expect(result.warnings).toContain(
               `Skill set '${MAIN}' skill finding: Package skill file '${CONVENTIONS}/${rel}' ${why}${why.endsWith("outside") ? " [A-Za-z0-9_][A-Za-z0-9._-]*" : ""}, so it is not served.`,
            );
         },
      );

      it.skipIf(!caseSensitiveFs)(
         "does not serve a reference that differs from another file only by case",
         () => {
            write(`${CONVENTIONS}/reference/Dup.md`, "# One\n");
            write(`${CONVENTIONS}/reference/dup.md`, "# Two\n");
            const result = read({ analyst: analyst() });
            expect(
               inlineOf(result).filter((f) => /reference\/dup\.md$/i.test(f)),
            ).toHaveLength(1);
            expect(result.warnings.join("\n")).toContain(
               "its name differs from another file's only by case, so it is not served",
            );
         },
      );

      it("gives a Markdown file too large for the set's budget one warning, never two", () => {
         // Linked to a directory in the package, so it is outside the skill and cannot be listed either.
         write("shared-ref/big.md", `#\n${"x".repeat(300 * 1024)}`);
         const entry = analyst();
         fs.rmSync(path.join(pkg, CONVENTIONS, "reference"), {
            recursive: true,
         });
         fs.symlinkSync(
            path.join(pkg, "shared-ref"),
            path.join(pkg, CONVENTIONS, "reference"),
         );
         const result = read({ analyst: entry });
         const about = result.warnings.filter((w) => w.includes("big.md"));
         expect(about).toHaveLength(1);
         expect(inlineOf(result)).not.toContain("conventions/reference/big.md");
      });

      it("names the cap that applied: the file's own, or the set's budget", () => {
         write(
            `${CONVENTIONS}/references/huge.md`,
            `#\n${"x".repeat(300 * 1024)}`,
         );
         for (let i = 0; i < 3; i++) {
            write(
               `${CONVENTIONS}/references/part${i}.md`,
               `#\n${"y".repeat(100 * 1024)}`,
            );
         }
         const warnings = read({ analyst: analyst() }).warnings.join("\n");
         expect(warnings).toMatch(
            /references\/huge\.md' is \d+ bytes, over the 262144 byte per-file cap, so it is listed instead of inlined/,
         );
         expect(warnings).toContain(
            "would take the skill set's Markdown over the 262144 byte cap, so it is listed instead of inlined",
         );
      });
   });

   describe("a file get_skill cannot return whole", () => {
      const tooLong = "x".repeat(FCJ_MAX_CHARS + 1);
      const warning = (file: string) =>
         `Package skill file '${CONVENTIONS}/${file}' is over the ${FCJ_MAX_CHARS}-character reply budget of get_skill, so get_skill cannot return it whole`;

      it("is warned of once, for SKILL.md, an inline reference and a listed text file", () => {
         write(
            `${MAIN}/long/SKILL.md`,
            `---\nname: long\ndescription: d\n---\n\n${tooLong}\n`,
         );
         write(`${CONVENTIONS}/references/long.md`, tooLong);
         write(`${CONVENTIONS}/assets/long.txt`, tooLong);
         write(`${CONVENTIONS}/scripts/short.py`, "print()\n");
         const result = read({ analyst: analyst() });
         const findings = result.sets.get(MAIN)!.findings;
         for (const file of ["references/long.md", "assets/long.txt"]) {
            expect(
               findings.filter((f) => f.includes(warning(file))),
            ).toHaveLength(1);
         }
         expect(
            findings.filter((f) =>
               f.includes(
                  `Package skill file '${MAIN}/long/SKILL.md' is over the ${FCJ_MAX_CHARS}-character reply budget`,
               ),
            ),
         ).toHaveLength(1);
         expect(findings.join("\n")).not.toContain("short.py");
         // Still served: the warning is for the author.
         expect(result.workflows.has("analyst")).toBe(true);
      });
   });

   describe("digests", () => {
      const twoWorkflows = (extra: Record<string, unknown> = {}) => {
         const entry = analyst();
         writeSkill(SECOND, "extra");
         write("workflows/other/instructions.md", "other brief");
         return {
            analyst: { ...entry, skill_sets: [MAIN, SECOND] },
            other: {
               instructions: "workflows/other/instructions.md",
               skill_sets: [SECOND],
            },
            ...extra,
         };
      };
      const digests = (result: PackageWorkflows) =>
         Object.fromEntries(
            [...result.sets].map(([setPath, set]) => [setPath, set.digest]),
         );
      const definition = (result: PackageWorkflows, name = "analyst") =>
         result.workflows.get(name)!.definitionSha;

      it("is stable across reads, and moves a workflow's definitionSha on every part of the definition", () => {
         const entry = analyst();
         const base = definition(read({ analyst: entry }));
         expect(definition(read({ analyst: entry }))).toBe(base);

         expect(
            definition(read({ analyst: { ...entry, description: "other" } })),
         ).not.toBe(base);
         expect(
            definition(read({ analyst: { ...entry, skill_sets: [] } })),
         ).not.toBe(base);
         expect(
            definition(
               read({ analyst: { ...entry, instructions: undefined } }),
            ),
         ).not.toBe(base);
         expect(
            definition(read({ analyst: { ...entry, "x-note": "n" } })),
         ).toBe(base);

         write(INSTRUCTIONS, "changed");
         const afterInstructions = definition(read({ analyst: entry }));
         expect(afterInstructions).not.toBe(base);
         write(
            `${CONVENTIONS}/SKILL.md`,
            "---\nname: conventions\ndescription: d\n---\nz",
         );
         expect(definition(read({ analyst: entry }))).not.toBe(
            afterInstructions,
         );
      });

      it("moves definitionSha with the instructions path, not only its text", () => {
         const entry = analyst();
         write("workflows/analyst/other.md", "Be careful with revenue.");
         expect(definition(read({ analyst: entry }))).not.toBe(
            definition(
               read({
                  analyst: {
                     ...entry,
                     instructions: "workflows/analyst/other.md",
                  },
               }),
            ),
         );
      });

      it("moves a set's digest on an edit to a file it serves, and no other set's", () => {
         const workflows = twoWorkflows();
         const before = digests(read(workflows));
         write(`${SECOND}/extra/notes.txt`, "added");
         const after = digests(read(workflows));
         expect(after[MAIN]).toBe(before[MAIN]);
         expect(after[SECOND]).not.toBe(before[SECOND]);
      });

      it("does not move a set's digest for an edit to a workflow's instructions or to its other sets", () => {
         const workflows = twoWorkflows();
         const before = read(workflows);
         write(INSTRUCTIONS, "edited brief");
         write("workflows/other/instructions.md", "edited other brief");
         writeSkill(SECOND, "extra", "edited");
         const after = read(workflows);
         expect(after.sets.get(MAIN)!.digest).toBe(
            before.sets.get(MAIN)!.digest,
         );
         // The other set did change, and so did the workflow that lists it.
         expect(after.sets.get(SECOND)!.digest).not.toBe(
            before.sets.get(SECOND)!.digest,
         );
         expect(definition(after, "other")).not.toBe(
            definition(before, "other"),
         );
      });

      it("moves no set's digest when workflows are reordered, and a set shared by two workflows is read once", () => {
         const workflows = twoWorkflows();
         const forward = read(workflows);
         const backward = read({
            other: workflows.other,
            analyst: workflows.analyst,
         });
         expect(digests(backward)).toEqual(digests(forward));
         expect(definition(backward)).toBe(definition(forward));
         expect(forward.sets.size).toBe(2);
         expect(forward.workflows.get("analyst")!.sets[1]!.set).toBe(
            forward.workflows.get("other")!.sets[0]!.set,
         );
      });

      it("moves a workflow's definitionSha, and no set's digest, when it lists its sets in another order", () => {
         const workflows = twoWorkflows();
         const forward = read(workflows);
         const swapped = read({
            ...workflows,
            analyst: { ...workflows.analyst, skill_sets: [SECOND, MAIN] },
         });
         expect(digests(swapped)).toEqual(digests(forward));
         expect(definition(swapped)).not.toBe(definition(forward));
      });

      it("is per workflow: editing one workflow's instructions does not move another's definitionSha", () => {
         const workflows = twoWorkflows();
         const before = definition(read(workflows), "other");
         write(INSTRUCTIONS, "edited");
         expect(definition(read(workflows), "other")).toBe(before);
      });

      it("moves a set's digest when a listed file changes and not otherwise", () => {
         const entry = analyst();
         const digest = () => read({ analyst: entry }).sets.get(MAIN)!.digest;
         const bare = digest();
         write(`${CONVENTIONS}/assets/a.txt`, "one");
         const one = digest();
         expect(one).not.toBe(bare);
         expect(digest()).toBe(one);
         write(`${CONVENTIONS}/assets/a.txt`, "two");
         expect(digest()).not.toBe(one);
      });
   });

   describe("advertised skills", () => {
      it("are the default workflow's, each name from the first set that holds it", () => {
         const entry = analyst();
         writeSkill(SECOND, "conventions", "second copy");
         writeSkill(SECOND, "extra");
         const result = read({
            default: { skill_sets: [MAIN, SECOND] },
            analyst: entry,
         });
         expect(advertisedSkills(result).map((s) => s.name)).toEqual([
            "conventions",
            "extra",
         ]);
         expect(advertisedSkills(result)[0]!.read("SKILL.md")).toEqual({
            text: expect.stringContaining("Use net revenue."),
         });
      });

      it("are none when no workflow is named default, whatever else the package declares", () => {
         const result = read({ analyst: analyst() });
         expect(advertisedSkills(result)).toEqual([]);
         expect(advertisedSkills(emptyWorkflows())).toEqual([]);
      });

      it("are none for a default workflow that lists no skill set, or is refused", () => {
         write(INSTRUCTIONS, "i");
         expect(
            advertisedSkills(read({ default: { instructions: INSTRUCTIONS } })),
         ).toEqual([]);
         expect(
            advertisedSkills(read({ default: { skill_sets: ["nope"] } })),
         ).toEqual([]);
      });

      it("shadow a bundled skill of the same name through resolveSkills", () => {
         writeSkill(MAIN, "malloy-analysis", "OUR ANALYSIS");
         const resolved = resolveSkills(
            [],
            advertisedSkills(read({ default: { skill_sets: [MAIN] } })),
         );
         expect(resolved.map((s) => [s.name, s.origin])).toEqual([
            ["malloy-analysis", "package"],
         ]);
      });
   });

   describe("the root skills/ directory", () => {
      const skillsDirNotRead = (workflows: unknown) =>
         read(workflows).skillsDirNotRead;

      it("is flagged when it exists and no workflow lists it", () => {
         writeSkill("skills", "old");
         expect(skillsDirNotRead(undefined)).toBe(true);
         expect(skillsDirNotRead({ analyst: analyst() })).toBe(true);
      });

      it("is not flagged when a workflow lists it, or a set under it", () => {
         writeSkill("skills", "old");
         expect(skillsDirNotRead({ default: { skill_sets: ["skills"] } })).toBe(
            false,
         );
         writeSkill("skills/sub", "new");
         expect(
            skillsDirNotRead({ analyst: { skill_sets: ["skills/sub"] } }),
         ).toBe(false);
         expect(
            skillsDirNotRead({ analyst: { skill_sets: ["./skills//"] } }),
         ).toBe(false);
      });

      it("is not flagged when there is none", () => {
         expect(skillsDirNotRead(undefined)).toBe(false);
         expect(skillsDirNotRead({ analyst: analyst() })).toBe(false);
      });

      it("is not flagged for a file named skills", () => {
         write("skills", "not a directory");
         expect(skillsDirNotRead(undefined)).toBe(false);
      });
   });

   describe("assets", () => {
      const ASSETS = `${CONVENTIONS}/assets`;
      const MAX = 1024 * 1024;
      const LISTED = "conventions/assets";
      const listed = (result: PackageWorkflows) =>
         (viewOf(result).assets ?? []).map((a) => a.relative_filepath);
      const warned = (result: PackageWorkflows, ...needles: string[]) =>
         result.warnings.some((w) => needles.every((n) => w.includes(n)));
      /** A strict finding sets the skill aside with a finding the service matches on, and serves the workflow. */
      const setAside = (needle: string) => {
         const result = read({ analyst: analyst() });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.workflows.get("analyst")!.sets[0]!.skills).toEqual([]);
         expect(result.warnings.join("\n")).toContain(
            `Skill set '${MAIN}' skill finding: Package skill '${CONVENTIONS}' is not served: `,
         );
         expect(result.warnings.join("\n")).toContain(needle);
      };

      it("lists text assets with their size and sha, and ignores dotfiles", () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         write(`${ASSETS}/css/app.css`, "body{}");
         write(`${ASSETS}/.hidden/x.txt`, "ignored");
         write(`${ASSETS}/.DS_Store`, "ignored");
         const result = read({ analyst: analyst() });

         expect(result.warnings).toEqual([]);
         expect(viewOf(result).assets).toEqual([
            {
               relative_filepath: `${LISTED}/css/app.css`,
               bytes: 6,
               sha256: sha("body{}"),
            },
            {
               relative_filepath: `${LISTED}/index.html`,
               bytes: 11,
               sha256: sha("<h1>hi</h1>"),
            },
         ]);
         expect(result.paths).toContain(`${ASSETS}/css/app.css`);
         expect(result.contentShas.get(`${ASSETS}/index.html`)).toBe(
            sha("<h1>hi</h1>"),
         );
         // Assets are listed, never served as skill text.
         expect(
            viewOf(result).skills.some((f) =>
               f.relative_filepath.includes("assets"),
            ),
         ).toBe(false);
      });

      it("serves a set with no assets, or an empty assets directory, with no assets key", () => {
         fs.mkdirSync(path.join(pkg, ASSETS), { recursive: true });
         const result = read({ analyst: analyst() });
         expect(result.warnings).toEqual([]);
         expect("assets" in viewOf(result)).toBe(false);
         // Only inline Markdown is covered: no asset was listed.
         expect(
            [...result.contentShas.keys()].every((k) => k.endsWith(".md")),
         ).toBe(true);
      });

      it("accepts a file one byte under 1 MiB and refuses one at 1 MiB", () => {
         write(`${ASSETS}/under.txt`, Buffer.alloc(MAX - 1, "a"));
         write(`${ASSETS}/exact.txt`, Buffer.alloc(MAX, "a"));
         const result = read({ analyst: analyst() });
         expect(listed(result)).toEqual([`${LISTED}/under.txt`]);
         expect(warned(result, "exact.txt", `${MAX} byte file cap`)).toBe(true);
      });

      describe("skipped, with the skill still served", () => {
         it("lists non-UTF-8 and NUL-bearing files too, hashing their raw bytes", () => {
            const png = Buffer.from([0x89, 0x50, 0xff, 0xfe]);
            const nul = Buffer.from("a\0b");
            write(`${ASSETS}/logo.png`, png);
            write(`${ASSETS}/nul.txt`, nul);
            write(`${ASSETS}/ok.txt`, "fine");
            const result = read({ analyst: analyst() });
            expect(result.warnings).toEqual([]);
            expect(viewOf(result).assets).toEqual([
               {
                  relative_filepath: `${LISTED}/logo.png`,
                  bytes: 4,
                  sha256: sha(png),
               },
               {
                  relative_filepath: `${LISTED}/nul.txt`,
                  bytes: 3,
                  sha256: sha(nul),
               },
               {
                  relative_filepath: `${LISTED}/ok.txt`,
                  bytes: 4,
                  sha256: sha("fine"),
               },
            ]);
         });

         it("keeps a leading BOM so the listed sha is the file's", () => {
            const bytes = Buffer.from("\uFEFFhello");
            write(`${ASSETS}/bom.txt`, bytes);
            expect(
               viewOf(read({ analyst: analyst() })).assets![0]!.sha256,
            ).toBe(sha(bytes));
         });

         it("skips Malloy files in any case", () => {
            write(`${ASSETS}/m.malloy`, "source: x is a");
            write(`${ASSETS}/n.MALLOYNB`, "{}");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([]);
            expect(warned(result, "m.malloy", "Malloy")).toBe(true);
            expect(warned(result, "n.MALLOYNB", "Malloy")).toBe(true);
         });

         it("skips a bad segment name and a path over 200 characters", () => {
            write(`${ASSETS}/has space.txt`, "x");
            write(`${ASSETS}/_ok-1.2.txt`, "x");
            write(`${ASSETS}/-dash.txt`, "x");
            write(`${ASSETS}/${"a".repeat(200)}.txt`, "x");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([`${LISTED}/_ok-1.2.txt`]);
            expect(warned(result, "has space.txt", "name")).toBe(true);
            expect(warned(result, "-dash.txt", "name")).toBe(true);
            expect(warned(result, "200 characters")).toBe(true);
         });

         it("stops at 200 files and 4 MiB per skill set", () => {
            for (let i = 0; i < 201; i++) write(`${ASSETS}/f${i}.txt`, "x");
            const files = read({ analyst: analyst() });
            expect(listed(files)).toHaveLength(200);
            expect(
               warned(
                  files,
                  "the skill set already has 200 assets, the per-set file cap",
               ),
            ).toBe(true);

            fs.rmSync(path.join(pkg, ASSETS), { recursive: true });
            for (let i = 0; i < 5; i++) {
               write(`${ASSETS}/big${i}.txt`, Buffer.alloc(900_000, "a"));
            }
            const bytes = read({ analyst: analyst() });
            expect(listed(bytes)).toHaveLength(4);
            expect(warned(bytes, "big4.txt", "per-set cap")).toBe(true);
         });

         it("walks eight levels and warns about a ninth", () => {
            const seven = Array.from({ length: 7 }, (_, i) => `d${i}`).join(
               "/",
            );
            write(`${ASSETS}/${seven}/ok.txt`, "x");
            write(`${ASSETS}/${seven}/d7/deep.txt`, "x");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([`${LISTED}/${seven}/ok.txt`]);
            expect(warned(result, "deeper than 8")).toBe(true);
         });

         // Empty directories fill the entry cap without reaching the file caps.
         const fillEntries = (dirs: number) => {
            write(`${ASSETS}/a.txt`, "x");
            for (let i = 0; i < dirs; i++) {
               fs.mkdirSync(
                  path.join(pkg, ASSETS, `d${String(i).padStart(4, "0")}`),
                  { recursive: true },
               );
            }
            write(`${ASSETS}/z.txt`, "x");
         };

         it("walks exactly 1,000 entries per skill set and stays served past that", () => {
            fillEntries(998);
            const atCap = read({ analyst: analyst() });
            expect(listed(atCap)).toEqual([
               `${LISTED}/a.txt`,
               `${LISTED}/z.txt`,
            ]);
            expect(warned(atCap, "1000 entries")).toBe(false);

            write(`${ASSETS}/zz.txt`, "x");
            const over = read({ analyst: analyst() });
            expect(over.workflows.has("analyst")).toBe(true);
            expect(listed(over)).toEqual([
               `${LISTED}/a.txt`,
               `${LISTED}/z.txt`,
            ]);
            expect(warned(over, "zz.txt", "1000 entries")).toBe(true);
         });

         it("lists a large file with a NUL in its head, and hashes it without reading it as text", () => {
            const big = Buffer.alloc(900_000, "a");
            big[10] = 0;
            write(`${ASSETS}/blob.txt`, big);
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual([`${LISTED}/blob.txt`]);
            expect(result.contentShas.get(`${ASSETS}/blob.txt`)).toBe(sha(big));
         });

         // chmod revokes nothing on Windows or for root.
         it.skipIf(isWindows || isRoot)(
            "skips, rather than sets the skill aside for, an unreadable file under a path containing 'link'",
            () => {
               write(`${ASSETS}/link/x.txt`, "x");
               fs.chmodSync(path.join(pkg, ASSETS, "link/x.txt"), 0);
               write(`${ASSETS}/ok.txt`, "fine");
               const result = read({ analyst: analyst() });
               expect(result.workflows.has("analyst")).toBe(true);
               expect(listed(result)).toEqual([`${LISTED}/ok.txt`]);
               expect(warned(result, "link/x.txt", "cannot be read")).toBe(
                  true,
               );
            },
         );

         it("serves a skill named like an Object.prototype key", () => {
            write(
               `${MAIN}/__proto__/SKILL.md`,
               "---\nname: __proto__\ndescription: d\n---\n\nbody\n",
            );
            write(`${MAIN}/__proto__/assets/a.txt`, "x");
            const result = read({ analyst: analyst() });
            expect(listed(result)).toEqual(["__proto__/assets/a.txt"]);
         });

         it("names the first twenty skips and counts the rest", () => {
            for (let i = 0; i < 30; i++) write(`${ASSETS}/m${i}.malloy`, "x");
            const result = read({ analyst: analyst() });
            expect(
               result.warnings.filter((w) => w.includes(".malloy")),
            ).toHaveLength(20);
            expect(warned(result, "10 further assets")).toBe(true);
         });
      });

      describe("sets the skill aside, and serves the workflow", () => {
         it("for a symlinked file, with the file named", () => {
            write("outside.txt", "x");
            fs.mkdirSync(path.join(pkg, ASSETS), { recursive: true });
            fs.symlinkSync(
               path.join(pkg, "outside.txt"),
               path.join(pkg, ASSETS, "link.txt"),
            );
            setAside("link.txt");
         });

         it("for a symlinked file that points outside the package", () => {
            fs.writeFileSync(path.join(outside, "secret.txt"), "s");
            fs.mkdirSync(path.join(pkg, ASSETS), { recursive: true });
            fs.symlinkSync(
               path.join(outside, "secret.txt"),
               path.join(pkg, ASSETS, "secret.txt"),
            );
            setAside("secret.txt");
         });

         it("for a symlink in a nested directory", () => {
            write(`${ASSETS}/sub/real.txt`, "x");
            fs.symlinkSync(
               path.join(pkg, ASSETS, "sub/real.txt"),
               path.join(pkg, ASSETS, "sub/alias.txt"),
            );
            setAside("alias.txt");
         });

         it("when assets is itself a symlink out of the package", () => {
            analyst();
            fs.mkdirSync(path.join(outside, "files"));
            fs.writeFileSync(path.join(outside, "files/x.txt"), "x");
            fs.symlinkSync(path.join(outside, "files"), path.join(pkg, ASSETS));
            setAside("conventions/assets");
         });

         it("when assets is itself a symlink within the package", () => {
            analyst();
            write("elsewhere/x.txt", "x");
            fs.symlinkSync(path.join(pkg, "elsewhere"), path.join(pkg, ASSETS));
            setAside("conventions/assets");
         });

         it("for a nested SKILL.md, in any case", () => {
            for (const name of ["SKILL.md", "skill.md"]) {
               fs.rmSync(path.join(pkg, ASSETS), {
                  recursive: true,
                  force: true,
               });
               write(`${ASSETS}/sub/${name}`, "{}");
               setAside(name);
            }
         });

         it("on '..' in a name", () => {
            write(`${ASSETS}/a..b.txt`, "x");
            setAside("a..b.txt");
         });

         it.skipIf(!caseSensitiveFs)(
            "on two assets that differ only by case",
            () => {
               write(`${ASSETS}/Logo.svg`, "x");
               write(`${ASSETS}/logo.svg`, "y");
               setAside("only by case");
            },
         );

         it.skipIf(!caseSensitiveFs)(
            "on two directories that differ only by case",
            () => {
               write(`${ASSETS}/Dir/a.txt`, "x");
               write(`${ASSETS}/dir/b.txt`, "y");
               setAside("only by case");
            },
         );

         it("and keeps the set's other skills", () => {
            writeSkill(MAIN, "zebra");
            write(`${ASSETS}/a..b.txt`, "x");
            const result = read({ analyst: analyst() });
            expect(
               result.workflows
                  .get("analyst")!
                  .sets[0]!.skills.map((s) => s.dirName),
            ).toEqual(["zebra"]);
         });
      });

      describe("refuses the workflow", () => {
         it("for a publisher.json below assets/, in any case", () => {
            for (const name of ["publisher.json", "Publisher.JSON"]) {
               fs.rmSync(path.join(pkg, ASSETS), {
                  recursive: true,
                  force: true,
               });
               write(`${ASSETS}/sub/${name}`, "{}");
               expect(refused({ analyst: analyst() })).toContain(
                  `nested publisher.json at 'conventions/assets/sub/${name}'`,
               );
            }
         });
      });

      describe("package budget", () => {
         const writeAssets = (setPath: string, files: number) => {
            writeSkill(setPath, "s");
            for (let i = 0; i < files; i++) {
               write(
                  `${setPath}/s/assets/f${i}.txt`,
                  Buffer.alloc(1_000_000, "a"),
               );
            }
         };

         it("charges each distinct set once, in sorted path order, whichever workflow lists it", () => {
            writeAssets("sets/a", 4);
            writeAssets("sets/b", 4);
            writeAssets("sets/c", 4);
            const result = read({
               // Listed out of order, and `d` shares c's set, which is read once.
               c: { skill_sets: ["sets/c"] },
               b: { skill_sets: ["sets/b"] },
               a: { skill_sets: ["sets/a"] },
               d: { skill_sets: ["sets/c"] },
            });
            const files = (setPath: string) =>
               result.sets.get(setPath)!.skills.flatMap((s) => s.listed);

            expect(files("sets/a")).toHaveLength(4);
            expect(files("sets/b")).toHaveLength(4);
            expect(files("sets/c")).toHaveLength(0);
            expect(
               result.warnings.some(
                  (w) =>
                     w.startsWith("Skill set 'sets/c' skill finding:") &&
                     w.includes("8388608 byte per-package cap"),
               ),
            ).toBe(true);
            expect(result.sets.size).toBe(3);
         });

         it("does not read or charge the set of a workflow that is refused", () => {
            writeAssets("sets/a", 4);
            writeAssets("sets/b", 4);
            writeAssets("sets/c", 4);
            const result = read({
               a: { skill_sets: ["sets/a"], surprise: true },
               b: { skill_sets: ["sets/b"] },
               c: { skill_sets: ["sets/c"] },
            });
            expect(result.workflows.has("a")).toBe(false);
            expect(result.sets.has("sets/a")).toBe(false);
            const files = (setPath: string) =>
               result.sets.get(setPath)!.skills.flatMap((s) => s.listed);
            expect(files("sets/b")).toHaveLength(4);
            expect(files("sets/c")).toHaveLength(4);
         });
      });
   });
});
