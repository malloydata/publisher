// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { linkTextKeptVerbatim } from "../../test_helpers/link_text";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs";
import os from "os";
import { createHash } from "crypto";
import path from "path";
import { FCJ_MAX_CHARS } from "../../constants";
import * as revision from "../package_revision";
import {
   boundedDeclaration,
   MANIFEST_WORKFLOWS_MAX_BYTES,
} from "../package_revision";
import {
   advertisedSkills,
   resolveSkills,
   workflowSkillSet,
} from "./package_skills";
import {
   adoptWorkflows,
   emptyWorkflows,
   readPackageWorkflows,
   SKILL_FINDINGS_MAX,
   SKILLS_PER_SET_MAX,
   WORKFLOW_INSTRUCTIONS_MAX_BYTES,
   workflowFileServed,
   workflowsToWire,
   SKILL_FILE_SKIPPED,
   SKILL_FINDING,
   SKILL_SHADOWED,
   WORKFLOW_NOT_SERVED,
   type PackageWorkflows,
} from "./package_workflows";

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
/** The text of each warning, which most tests read as prose. */
const texts = (warnings: Array<{ message: string }>) =>
   warnings.map((w) => w.message);

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
         skillSets: [MAIN],
         ...extra,
      };
   };

   const read = (workflows: unknown) => readPackageWorkflows(pkg, workflows);

   /** Asserts the workflow is refused with one `workflow-not-served` warning; returns its text. */
   const refused = (workflows: unknown, name = "analyst") => {
      const result = read(workflows);
      expect(result.workflows.has(name)).toBe(false);
      expect(result.warnings).toHaveLength(1);
      const [warning] = result.warnings;
      expect(warning).toMatchObject({
         code: WORKFLOW_NOT_SERVED,
         subject: name,
         severity: "warn",
      });
      expect(
         warning!.message.startsWith(`Workflow '${name}' is not served: `),
      ).toBe(true);
      expect(
         warning!.message.endsWith(
            `. Fix: correct the 'workflows.${name}' entry in publisher.json and the files it names.`,
         ),
      ).toBe(true);
      return warning!.message;
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
         expect(texts(result.warnings)).toEqual([]);
         const workflow = result.workflows.get("analyst")!;
         expect(workflow).toMatchObject({
            name: "analyst",
            description: "Answers revenue questions",
            instructions: {
               path: INSTRUCTIONS,
               text: "Be careful with revenue.",
            },
            shadowing: [],
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
            analyst: { instructions: INSTRUCTIONS, skillSets: [MAIN] },
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
         expect(texts(result.warnings)).toEqual([]);
         expect(result.workflows.has("analyst")).toBe(true);
      });

      it("serves a workflow with only instructions, or only skill sets, and needs no description", () => {
         write(INSTRUCTIONS, "brief");
         writeSkill(MAIN, "conventions");
         const result = read({
            brief: { instructions: INSTRUCTIONS },
            skills: { skillSets: [MAIN] },
         });
         expect(texts(result.warnings)).toEqual([]);
         const brief = result.workflows.get("brief")!;
         expect(brief.sets).toEqual([]);
         expect(brief.description).toBeUndefined();
         expect(result.workflows.get("skills")!.instructions).toBeUndefined();
      });

      it("refuses a workflow that declares nothing to serve", () => {
         expect(refused({ analyst: {} })).toContain(
            "it declares nothing to serve: add 'instructions' or 'skillSets'",
         );
         expect(
            refused({ analyst: { description: "d", skillSets: [] } }),
         ).toContain("it declares nothing to serve");
      });

      it("refuses only the bad workflow and keeps the others", () => {
         const good = analyst();
         const result = read({ analyst: good, "Bad Name": good, other: good });
         expect([...result.workflows.keys()]).toEqual(["analyst", "other"]);
         expect(texts(result.warnings)).toHaveLength(1);
         expect(texts(result.warnings)[0]).toContain(
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
            expect(texts(result.warnings)).toHaveLength(1);
            expect(texts(result.warnings)[0]).toContain("must be an object");
            expect(texts(result.warnings)[0]).toContain("Fix:");
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
         expect(texts(result.warnings)).toHaveLength(1);
         expect(texts(result.warnings)[0]).toContain(
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
         ["skill_sets", [MAIN]],
      ])("refuses a workflow that sets %p, naming it", (key, value) => {
         const warning = refused({ analyst: analyst({ [key]: value }) });
         expect(warning).toContain(`'${key}'`);
         expect(warning).toContain(
            "Allowed keys: description, instructions, skillSets",
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
         ["skillSets", "workflows/analyst/skill_sets/main"],
         ["skillSets", {}],
         ["skillSets", [null]],
         ["skillSets", [{}]],
         ["skillSets", [["x"]]],
         ["skillSets", [""]],
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
            { a: { instructions: "x.md", skillSets: [null] } },
            { a: { instructions: "x.md", skillSets: [{}] } },
            { a: { instructions: "x.md", skillSets: "x" } },
            { a: { skillSets: [5] } },
            { a: { skillSets: [["x"]] } },
            { a: { description: { d: 1 }, skillSets: ["x"] } },
         ];
         for (const raw of hostile) {
            const result = read(raw);
            expect(result.workflows.size).toBe(0);
            expect(texts(result.warnings).length).toBeGreaterThan(0);
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

      it("refuses instructions that are not UTF-8 text, as Markdown is", () => {
         const entry = analyst();
         fs.writeFileSync(
            path.join(pkg, INSTRUCTIONS),
            Buffer.from([0x62, 0xff, 0xfe, 0x62]),
         );
         expect(refused({ analyst: entry })).toContain("is not UTF-8 text");
      });

      it("names the backslash", () => {
         expect(
            refused({
               analyst: analyst({ skillSets: ["workflows\\analyst"] }),
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
         expect(refused({ analyst: entry })).toContain(
            `is ${WORKFLOW_INSTRUCTIONS_MAX_BYTES + 1} bytes, over the 16 KiB cap`,
         );
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
               skillSets: [`${MAIN}/`],
            },
         });
         expect(dotted.paths.sort()).toEqual(plain.paths.sort());
         expect(dotted.workflows.get("analyst")!.definitionSha).toBe(
            plain.workflows.get("analyst")!.definitionSha,
         );
      });
   });

   describe("skillSets", () => {
      it.each([
         ["missing", "nope"],
         ["absolute", "/tmp"],
         ["the package root", "."],
         ["a parent escape", "../x"],
         ["a file", INSTRUCTIONS],
      ])("refuses a workflow whose skill set entry is %s", (_label, entry) => {
         refused({ analyst: analyst({ skillSets: [entry] }) });
      });

      it("says a missing skill set does not exist", () => {
         expect(
            refused({ analyst: analyst({ skillSets: ["nope"] }) }),
         ).toContain("skill set directory 'nope' does not exist");
      });

      it("says a skill set that is a file is not a folder, not that it does not exist", () => {
         const why = refused({
            analyst: analyst({ skillSets: [INSTRUCTIONS] }),
         });
         expect(why).toContain(
            `skill set directory '${INSTRUCTIONS}' is not a folder`,
         );
         expect(why).not.toContain("does not exist");
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
                  skillSets: [LINKED],
               },
            };
            if (linkTextKeptVerbatim()) {
               answers.push(refused(workflows));
            } else {
               // Normalized when made (Windows), the link leads to the
               // workflow's own real/ inside the package and is served.
               const result = read(workflows);
               expect(result.workflows.has("analyst")).toBe(true);
               answers.push(JSON.stringify(texts(result.warnings)));
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
         expect(texts(result.warnings).join("\n")).toContain(
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

      it("refuses a workflow with a nested publisher.json at the set root", () => {
         const entry = analyst();
         write(`${MAIN}/publisher.json`, "{}");
         expect(refused({ analyst: entry })).toContain(
            `skill set directory '${MAIN}' contains a nested publisher.json at 'publisher.json'`,
         );
         fs.rmSync(path.join(pkg, MAIN, "publisher.json"));
         expect(read({ analyst: entry }).workflows.has("analyst")).toBe(true);
      });

      it.each([
         ["its directory", "publisher.json"],
         ["reference/", "reference/publisher.json"],
         ["references/", "references/publisher.json"],
         ["a folder below assets/", "assets/a/Publisher.JSON"],
      ])(
         "sets a skill aside for a nested publisher.json in %s, and serves every workflow that lists the set",
         (_where, inside) => {
            const entry = analyst();
            writeSkill(MAIN, "bad");
            write(`${MAIN}/bad/${inside}`, "{}");
            const result = read({
               default: { skillSets: [MAIN] },
               analyst: entry,
            });
            expect([...result.workflows.keys()]).toEqual([
               "default",
               "analyst",
            ]);
            expect(advertisedSkills(result).map((s) => s.name)).toEqual([
               "conventions",
            ]);
            expect(
               viewOf(result).skills.map((s) => s.relative_filepath),
            ).not.toContainEqual(expect.stringContaining("bad/"));
            expect(texts(result.warnings)).toEqual([
               `Skill set '${MAIN}' skill finding: Package skill '${MAIN}/bad' is not served: '${MAIN}/bad/${inside}' is a publisher.json, which would make its directory a second package root; remove it or rename it.`,
            ]);
         },
      );

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

      it("sets aside a linked skill directory that holds a publisher.json, and serves the workflow", () => {
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
         const result = read({ analyst: entry });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(texts(result.warnings)).toEqual([
            `Skill set '${MAIN}' skill finding: Package skill '${MAIN}/linked' is not served: '${MAIN}/linked/publisher.json' is a publisher.json, which would make its directory a second package root; remove it or rename it.`,
         ]);
      });

      it("skips a publisher.json anywhere else below a skill, with the files a skill may not hold", () => {
         const entry = analyst();
         write(`${CONVENTIONS}/templates/publisher.json`, "{}");
         const result = read({ analyst: entry });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(texts(result.warnings).join("\n")).toContain(
            `${CONVENTIONS}/templates/publisher.json`,
         );
      });

      it("refuses a workflow that lists more than eight skill sets, and serves eight", () => {
         const names = Array.from({ length: 9 }, (_, i) => `s${i}`);
         for (const name of names) writeSkill(`sets/${name}`, "k");
         const sets = names.map((name) => `sets/${name}`);
         expect(refused({ analyst: { skillSets: sets } })).toContain(
            "'skillSets' lists 9 directories, over the limit of 8",
         );
         const eight = read({ analyst: { skillSets: sets.slice(0, 8) } });
         expect(eight.workflows.get("analyst")!.sets).toHaveLength(8);
      });

      it("refuses a set whose folder is not a valid name", () => {
         writeSkill("sets/Bad_Name", "k");
         expect(
            refused({ analyst: { skillSets: ["sets/Bad_Name"] } }),
         ).toContain(
            "skill set directory 'sets/Bad_Name' is named 'Bad_Name', which must be 1-64 characters of lowercase letters, digits and single hyphens",
         );
      });

      it("accepts a set folder name of 64 characters and refuses one of 65", () => {
         const fits = "a".repeat(64);
         const over = "a".repeat(65);
         writeSkill(`sets/${fits}`, "k");
         writeSkill(`sets/${over}`, "k");
         expect(
            read({ analyst: { skillSets: [`sets/${fits}`] } }).workflows.has(
               "analyst",
            ),
         ).toBe(true);
         expect(
            refused({ analyst: { skillSets: [`sets/${over}`] } }),
         ).toContain(
            `skill set directory 'sets/${over}' is named '${over}', which must be 1-64 characters`,
         );
      });

      it("refuses two sets with the same folder name, and the same path twice", () => {
         writeSkill("a/main", "one");
         writeSkill("b/main", "two");
         expect(
            refused({ analyst: { skillSets: ["a/main", "b/main"] } }),
         ).toContain(
            "skill set directories 'a/main' and 'b/main' are both named 'main'",
         );
         expect(
            refused({ analyst: { skillSets: ["a/main", "./a//main"] } }),
         ).toContain("skill set directory 'a/main' is listed twice");
      });

      it("lets two workflows list sets that share a folder name", () => {
         writeSkill("a/main", "one");
         writeSkill("b/main", "two");
         const result = read({
            one: { skillSets: ["a/main"] },
            two: { skillSets: ["b/main"] },
         });
         expect(texts(result.warnings)).toEqual([]);
         expect([...result.sets.keys()]).toEqual(["a/main", "b/main"]);
      });

      it("publishes a shared set's finding once, however many workflows list it, served or refused", () => {
         writeSkill(MAIN, "conventions");
         write(`${MAIN}/conventions/scripts/a..b.py`, "x = 1");
         const result = read({
            one: { skillSets: [MAIN] },
            two: { skillSets: [MAIN] },
            three: { description: "", skillSets: [MAIN] },
         });
         expect([...result.workflows.keys()]).toEqual(["one", "two"]);
         expect(
            texts(result.warnings).filter((w) =>
               w.startsWith(`Skill set '${MAIN}' skill finding:`),
            ),
         ).toHaveLength(1);
      });

      it("watches every set path and instructions file a workflow declares, refused or not, so fixing it recompiles", () => {
         write(INSTRUCTIONS, "i");
         const result = read({
            analyst: {
               instructions: INSTRUCTIONS,
               skillSets: [MAIN],
               surprise: 1,
            },
            broken: {
               instructions: "workflows/broken/instructions.md",
               skillSets: [
                  "workflows/broken/skill_sets/main",
                  "../out",
                  "/abs",
               ],
            },
            junk: { skillSets: "not a list" },
         });
         expect(result.workflows.size).toBe(0);
         expect([...result.watched].sort()).toEqual(
            [
               INSTRUCTIONS,
               MAIN,
               "workflows/broken/instructions.md",
               "workflows/broken/skill_sets/main",
            ].sort(),
         );
      });
   });

   describe("per-path checks", () => {
      /** How often `run` asks the filesystem the named questions. */
      const calls = (
         names: Array<
            "readdirSync" | "statSync" | "lstatSync" | "realpathSync"
         >,
         run: () => void,
      ) => {
         const spies = names.map((name) => spyOn(fs, name));
         try {
            run();
            return spies.reduce((sum, spy) => sum + spy.mock.calls.length, 0);
         } finally {
            for (const spy of spies) spy.mockRestore();
         }
      };

      it("looks up each skill set directory once, whatever number of workflows lists it", () => {
         writeSkill("sets/a/b/c", "one");
         writeSkill("sets/d/e/f", "two");
         const workflows = (n: number) =>
            Object.fromEntries(
               Array.from({ length: n }, (_, i) => [
                  `w${i}`,
                  { skillSets: ["sets/a/b/c", "sets/d/e/f"] },
               ]),
            );
         const all = [
            "readdirSync",
            "statSync",
            "lstatSync",
            "realpathSync",
         ] as const;
         const one = calls([...all], () => read(workflows(1)));
         expect(one).toBeGreaterThan(0);
         expect(calls([...all], () => read(workflows(50)))).toBe(one);
      });

      it("looks in each directory above an instructions file for a publisher.json once", () => {
         const workflows = (n: number) =>
            Object.fromEntries(
               Array.from({ length: n }, (_, i) => {
                  write(`briefs/deep/w${i}.md`, "brief");
                  return [`w${i}`, { instructions: `briefs/deep/w${i}.md` }];
               }),
            );
         const one = calls(["readdirSync"], () => read(workflows(1)));
         expect(one).toBeGreaterThan(0);
         expect(calls(["readdirSync"], () => read(workflows(50)))).toBe(one);
      });
   });

   describe("a set that cannot be read", () => {
      let boom: ReturnType<typeof spyOn> | undefined;
      afterEach(() => boom?.mockRestore());

      /** Nothing is served and nothing is refused: the walk says it could not finish, naming why. */
      const expectUnavailable = (result: PackageWorkflows, reason: string) => {
         expect(result.unavailable?.reason).toContain(reason);
         expect(result.workflows.size).toBe(0);
         expect(result.sets.size).toBe(0);
         expect(result.paths).toEqual([]);
         expect(texts(result.warnings)).toEqual([]);
         expect(result.skillsDirNotRead).toBe(false);
      };

      for (const [label, mode] of [
         ["no access at all", 0o000],
         ["no right to list it", 0o311],
      ] as const) {
         it.skipIf(isWindows || isRoot)(
            `is unavailable, not refused, when a set's folder cannot be listed, with ${label}`,
            () => {
               analyst();
               writeSkill(SECOND, "extra");
               fs.chmodSync(path.join(pkg, SECOND), mode);
               try {
                  const result = read({
                     fine: { skillSets: [MAIN] },
                     broken: { skillSets: [SECOND] },
                  });
                  expectUnavailable(result, "EACCES");
                  // Still watched, so fixing the folder reloads the package.
                  expect(result.watched).toEqual([MAIN, SECOND]);
               } finally {
                  fs.chmodSync(path.join(pkg, SECOND), 0o755);
               }
            },
         );
      }

      it.skipIf(isWindows || isRoot)(
         "is unavailable when a set sits under a folder it can walk through but not list, since a publisher.json there cannot be ruled out",
         () => {
            writeSkill(MAIN, "one");
            const above = path.join(pkg, "workflows/analyst/skill_sets");
            fs.chmodSync(above, 0o311);
            try {
               expectUnavailable(
                  read({ broken: { skillSets: [MAIN] } }),
                  "EACCES",
               );
            } finally {
               fs.chmodSync(above, 0o755);
            }
         },
      );

      it("is unavailable, naming the set, when reading it throws, and never throws itself", () => {
         analyst();
         writeSkill(SECOND, "extra");
         const real = revision.canonicalSha;
         boom = spyOn(revision, "canonicalSha").mockImplementation(
            (value: unknown) => {
               if ((value as { path?: string }).path === SECOND) {
                  throw new Error("injected failure");
               }
               return real(value);
            },
         );
         const result = read({
            fine: { skillSets: [MAIN] },
            broken: { skillSets: [SECOND] },
            both: { skillSets: [MAIN, SECOND] },
         });
         expectUnavailable(
            result,
            `skill set directory '${SECOND}' cannot be read (injected failure)`,
         );
      });

      it("is unavailable when the walk itself throws, whatever it was reading", () => {
         const hostile = new Proxy(
            {},
            {
               ownKeys() {
                  throw new Error("injected walk failure");
               },
            },
         );
         const result = read(hostile);
         expect(result.unavailable).toEqual({
            reason: "injected walk failure",
            declared: [],
         });
         expect(result.workflows.size).toBe(0);
      });

      it.skipIf(isWindows || isRoot)(
         "is unavailable, not refused, when an instructions file cannot be read",
         () => {
            const entry = analyst();
            const file = path.join(pkg, INSTRUCTIONS);
            fs.chmodSync(file, 0o000);
            try {
               const result = read({ analyst: entry });
               expectUnavailable(result, "EACCES");
               expect(result.unavailable?.declared).toEqual(["analyst"]);
               expect(result.watched).toEqual([INSTRUCTIONS, MAIN]);
            } finally {
               fs.chmodSync(file, 0o644);
            }
         },
      );

      it.skipIf(isWindows || isRoot)(
         "skips a skill whose SKILL.md cannot be read with a finding, and still serves the workflow",
         () => {
            const entry = analyst();
            writeSkill(MAIN, "readable");
            const file = path.join(pkg, CONVENTIONS, "SKILL.md");
            fs.chmodSync(file, 0o000);
            try {
               const result = read({ analyst: entry });
               expect(result.unavailable).toBeUndefined();
               expect([...result.workflows.keys()]).toEqual(["analyst"]);
               expect(
                  result.sets.get(MAIN)!.skills.map((s) => s.dirName),
               ).toEqual(["readable"]);
               expect(texts(result.warnings)).toHaveLength(1);
               expect(texts(result.warnings)[0]).toContain("is not served");
               expect(texts(result.warnings)[0]).toContain(
                  "cannot be read (EACCES",
               );
            } finally {
               fs.chmodSync(file, 0o644);
            }
         },
      );

      it("still refuses an instructions file that is not there, as the author's mistake", () => {
         analyst();
         const warning = refused({
            analyst: { instructions: "workflows/analyst/missing.md" },
         });
         expect(warning).toContain("missing.md");
      });

      it("is unavailable, not refused, when a set's folder fails to be examined with anything but absence", () => {
         analyst();
         const real = fs.statSync;
         const failing = spyOn(fs, "statSync").mockImplementation(((
            target: fs.PathLike,
            ...rest: unknown[]
         ) => {
            if (String(target).endsWith(MAIN)) {
               throw Object.assign(new Error("EIO: i/o error"), {
                  code: "EIO",
               });
            }
            return (real as (...a: unknown[]) => unknown)(target, ...rest);
         }) as typeof fs.statSync);
         try {
            expectUnavailable(read({ analyst: analyst() }), "EIO");
         } finally {
            failing.mockRestore();
         }
      });

      it("names only the workflows that passed declaration, when what failed is a set read", () => {
         analyst();
         writeSkill(SECOND, "extra");
         const real = revision.canonicalSha;
         boom = spyOn(revision, "canonicalSha").mockImplementation(
            (value: unknown) => {
               if ((value as { path?: string }).path === SECOND) {
                  throw new Error("injected failure");
               }
               return real(value);
            },
         );
         const result = read({
            refused: { surprise: 1 },
            fine: { skillSets: [MAIN] },
            broken: { skillSets: [SECOND] },
         });
         expect(result.unavailable?.declared).toEqual(["fine", "broken"]);
      });

      it("names only the workflows that passed declaration, when what failed is a file the declaration reads", () => {
         const entry = analyst();
         const real = fs.readFileSync;
         const failing = spyOn(fs, "readFileSync").mockImplementation(((
            target: fs.PathOrFileDescriptor,
            ...rest: unknown[]
         ) => {
            if (String(target).endsWith(INSTRUCTIONS)) {
               throw Object.assign(new Error("EIO: i/o error"), {
                  code: "EIO",
               });
            }
            return (real as (...a: unknown[]) => unknown)(target, ...rest);
         }) as typeof fs.readFileSync);
         try {
            const result = read({ refused: { surprise: 1 }, analyst: entry });
            expect(result.unavailable?.declared).toEqual(["analyst"]);
         } finally {
            failing.mockRestore();
         }
      });

      it("is unavailable, not absent, when the package's own folder cannot be resolved", () => {
         const result = readPackageWorkflows(path.join(pkg, "gone"), {
            analyst: { instructions: INSTRUCTIONS },
         });
         expect(result.unavailable?.declared).toEqual(["analyst"]);
         expect(result.unavailable?.reason).toContain("ENOENT");
         expect(result.workflows.size).toBe(0);
      });

      it.skipIf(isWindows)(
         "refuses a workflow whose instructions path is a link loop, as the author's mistake",
         () => {
            analyst();
            const dir = path.join(pkg, "workflows/analyst");
            fs.symlinkSync("b.md", path.join(dir, "a.md"));
            fs.symlinkSync("a.md", path.join(dir, "b.md"));
            const result = read({
               analyst: { instructions: "workflows/analyst/a.md" },
            });
            expect(result.unavailable).toBeUndefined();
            expect(texts(result.warnings)).toHaveLength(1);
            expect(texts(result.warnings)[0]).toContain("a.md");
         },
      );

      it("refuses a workflow whose instructions name is over-long, as the author's mistake", () => {
         analyst();
         const result = read({
            analyst: {
               instructions: `workflows/analyst/${"x".repeat(300)}.md`,
            },
         });
         expect(result.unavailable).toBeUndefined();
         expect(texts(result.warnings)).toHaveLength(1);
      });

      it.each([
         ["ELOOP", "is part of a link loop"],
         ["ENAMETOOLONG", "has a name that is too long"],
      ])(
         "refuses a workflow, never the whole walk, naming the cause, when its set's folder fails to be examined with %s",
         (code, cause) => {
            const entry = analyst();
            const real = fs.statSync;
            const failing = spyOn(fs, "statSync").mockImplementation(((
               target: fs.PathLike,
               ...rest: unknown[]
            ) => {
               if (String(target).endsWith(MAIN)) {
                  throw Object.assign(new Error(`${code}: injected`), { code });
               }
               return (real as (...a: unknown[]) => unknown)(target, ...rest);
            }) as typeof fs.statSync);
            try {
               const result = read({ analyst: entry });
               expect(result.unavailable).toBeUndefined();
               expect(texts(result.warnings)).toHaveLength(1);
               expect(texts(result.warnings)[0]).toContain(
                  `skill set directory '${MAIN}' ${cause}`,
               );
               expect(texts(result.warnings)[0]).not.toContain(
                  "does not exist",
               );
            } finally {
               failing.mockRestore();
            }
         },
      );

      it("is not unavailable for what the author wrote wrong: that refuses the workflow with a warning", () => {
         analyst();
         const result = read({ broken: { skillSets: ["nowhere"] } });
         expect(result.unavailable).toBeUndefined();
         expect(texts(result.warnings)).toHaveLength(1);
         expect(texts(result.warnings)[0]).toContain("does not exist");
      });
   });

   describe("a set only a refused workflow lists", () => {
      const THIRD = "workflows/analyst/skill_sets/third";

      it("is not read: it is not served, hashed, reported or charged to the package", () => {
         analyst();
         // An empty set has a finding of its own, which would be reported if it were read.
         fs.mkdirSync(path.join(pkg, THIRD), { recursive: true });
         const result = read({
            fine: { skillSets: [MAIN] },
            broken: { skillSets: [THIRD], "not-a-key": 1 },
         });
         expect([...result.workflows.keys()]).toEqual(["fine"]);
         expect([...result.sets.keys()]).toEqual([MAIN]);
         expect(result.paths.some((p) => p.startsWith(THIRD))).toBe(false);
         expect(texts(result.warnings)).toHaveLength(1);
         expect(texts(result.warnings)[0]).toStartWith(
            "Workflow 'broken' is not served: it sets 'not-a-key'",
         );
      });
   });

   describe("a skill in a set", () => {
      it("is served from the first set that holds its name, with a warning on the workflow", () => {
         const entry = analyst();
         writeSkill(SECOND, "conventions", "second copy");
         writeSkill(SECOND, "extra");
         const result = read({
            analyst: { ...entry, skillSets: [MAIN, SECOND] },
         });
         const workflow = result.workflows.get("analyst")!;
         expect(workflow.shadowing).toEqual([
            {
               code: SKILL_SHADOWED,
               subject: "analyst",
               severity: "warn",
               message: `Workflow 'analyst' lists skill 'conventions' in both '${MAIN}' and '${SECOND}'; the copy in '${MAIN}', listed first, is the one served.`,
            },
         ]);
         expect(result.warnings).toEqual(workflow.shadowing);
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
            analyst: { ...entry, skillSets: [SECOND, MAIN] },
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
         expect(texts(result.warnings)).toHaveLength(1);
         expect(
            texts(result.warnings)[0]!.startsWith(
               `Skill set '${MAIN}' skill finding: Package skill '${MAIN}/empty' has no SKILL.md, so it is not served. Fix: add ${MAIN}/empty/SKILL.md, or remove the directory.`,
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
         expect(texts(result.warnings).join("\n")).toContain("no description");
         expect(texts(result.warnings).join("\n")).toContain("'quiet'");
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
               expect(texts(result.warnings).join("\n")).toContain(
                  "cannot be listed",
               );
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
         expect(texts(result.warnings).join("\n")).toContain("'elsewhere'");
      });

      it("serves a set that holds no skills, with a finding", () => {
         fs.mkdirSync(path.join(pkg, MAIN), { recursive: true });
         write(INSTRUCTIONS, "i");
         const result = read({
            analyst: { instructions: INSTRUCTIONS, skillSets: [MAIN] },
         });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(texts(result.warnings)).toEqual([
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
            expect(texts(result.warnings)).toContain(
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
            expect(texts(result.warnings).join("\n")).toContain(
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
         const about = texts(result.warnings).filter((w) =>
            w.includes("big.md"),
         );
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
         const warnings = texts(read({ analyst: analyst() }).warnings).join(
            "\n",
         );
         expect(warnings).toMatch(
            /references\/huge\.md' is \d+ bytes, over the 256 KiB per-file cap, so it is listed instead of inlined/,
         );
         expect(warnings).toContain(
            "would take the skill set's Markdown over the 256 KiB cap, so it is listed instead of inlined",
         );
      });
   });

   describe("a file get_skill cannot return whole", () => {
      const tooLong = "x".repeat(FCJ_MAX_CHARS + 1);
      const warning = (file: string) =>
         `Package skill file '${CONVENTIONS}/${file}' is over the ${FCJ_MAX_CHARS}-character reply budget of get_skill, so get_skill cannot return it whole`;

      const writeLong = () => {
         write(
            `${MAIN}/long/SKILL.md`,
            `---\nname: long\ndescription: d\n---\n\n${tooLong}\n`,
         );
         write(`${CONVENTIONS}/references/long.md`, tooLong);
         write(`${CONVENTIONS}/assets/long.txt`, tooLong);
         write(`${CONVENTIONS}/scripts/short.py`, "print()\n");
      };

      it("is warned of once, for SKILL.md, an inline reference and a listed text file, in a set the default workflow lists", () => {
         writeLong();
         const result = read({ default: analyst() });
         const findings = texts(result.sets.get(MAIN)!.findings);
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
         expect(result.workflows.has("default")).toBe(true);
      });

      it("is warned of when the default workflow lists the set though another workflow lists it too", () => {
         writeLong();
         const result = read({
            analyst: analyst(),
            default: { skillSets: [MAIN] },
         });
         expect(
            texts(result.sets.get(MAIN)!.findings).filter((f) =>
               f.includes(warning("assets/long.txt")),
            ),
         ).toHaveLength(1);
      });

      it("is not warned of for a set only another workflow lists, which get_skill never serves", () => {
         writeLong();
         const result = read({ analyst: analyst() });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(texts(result.warnings).join("\n")).not.toContain("get_skill");
      });
   });

   describe("digests", () => {
      const twoWorkflows = (extra: Record<string, unknown> = {}) => {
         const entry = analyst();
         writeSkill(SECOND, "extra");
         write("workflows/other/instructions.md", "other brief");
         return {
            analyst: { ...entry, skillSets: [MAIN, SECOND] },
            other: {
               instructions: "workflows/other/instructions.md",
               skillSets: [SECOND],
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

      /** The `files` map of the set at `setPath` as it is hashed, with the set's workflow reads as served. */
      const hashedFiles = (workflows: unknown, setPath: string) => {
         const real = revision.canonicalSha;
         const payloads: Array<{
            path?: string;
            skills?: Array<{ files: Record<string, string> }>;
         }> = [];
         const spy = spyOn(revision, "canonicalSha").mockImplementation(
            (value: unknown) => {
               payloads.push(value as (typeof payloads)[number]);
               return real(value);
            },
         );
         try {
            const result = read(workflows);
            const hashed = payloads.find((p) => p.path === setPath)!;
            return { result, files: hashed.skills![0]!.files };
         } finally {
            spy.mockRestore();
         }
      };

      it("hashes a SKILL.md as the workflow serves it, so the digest pins what the reply carries", () => {
         // Its frontmatter carries keys the re-emission drops.
         const { result, files } = hashedFiles({ analyst: analyst() }, MAIN);
         const served = viewOf(result).skills.find(
            (f) => f.relative_filepath === "conventions/SKILL.md",
         )!.file_contents;
         expect(served).not.toContain("allowed-tools");
         expect(files["SKILL.md"]).toBe(sha(served));
         expect(files["reference/glossary.md"]).toBe(
            sha("# Glossary\n\nnet = after refunds\n"),
         );
      });

      it("does not move for an edit the served bytes do not carry, and moves for one they do", () => {
         const entry = analyst();
         const before = read({ analyst: entry }).sets.get(MAIN)!.digest;
         write(
            `${CONVENTIONS}/SKILL.md`,
            "---\nname: conventions\ndescription: House rules.\nallowed-tools: Read\nx-owner: you\n---\n\nUse net revenue.\n",
         );
         expect(read({ analyst: entry }).sets.get(MAIN)!.digest).toBe(before);
         write(
            `${CONVENTIONS}/SKILL.md`,
            "---\nname: conventions\ndescription: House rules.\n---\n\nUse gross revenue.\n",
         );
         expect(read({ analyst: entry }).sets.get(MAIN)!.digest).not.toBe(
            before,
         );
      });

      it("is stable across reads, and moves a workflow's definitionSha on every part of the definition", () => {
         const entry = analyst();
         const base = definition(read({ analyst: entry }));
         expect(definition(read({ analyst: entry }))).toBe(base);

         expect(
            definition(read({ analyst: { ...entry, description: "other" } })),
         ).not.toBe(base);
         expect(
            definition(read({ analyst: { ...entry, skillSets: [] } })),
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
            analyst: { ...workflows.analyst, skillSets: [SECOND, MAIN] },
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
      it("are the default workflow's, each name from the first set that holds it", async () => {
         const entry = analyst();
         writeSkill(SECOND, "conventions", "second copy");
         writeSkill(SECOND, "extra");
         const result = read({
            default: { skillSets: [MAIN, SECOND] },
            analyst: entry,
         });
         expect(advertisedSkills(result).map((s) => s.name)).toEqual([
            "conventions",
            "extra",
         ]);
         expect(await advertisedSkills(result)[0]!.read("SKILL.md")).toEqual({
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
            advertisedSkills(read({ default: { skillSets: ["nope"] } })),
         ).toEqual([]);
      });

      it("shadow a bundled skill of the same name through resolveSkills", () => {
         writeSkill(MAIN, "malloy-analysis", "OUR ANALYSIS");
         const resolved = resolveSkills(
            [],
            advertisedSkills(read({ default: { skillSets: [MAIN] } })),
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
         expect(skillsDirNotRead({ default: { skillSets: ["skills"] } })).toBe(
            false,
         );
         writeSkill("skills/sub", "new");
         expect(
            skillsDirNotRead({ analyst: { skillSets: ["skills/sub"] } }),
         ).toBe(false);
         expect(
            skillsDirNotRead({ analyst: { skillSets: ["./skills//"] } }),
         ).toBe(false);
      });

      it("is flagged when the only workflow that lists it is refused, since nothing reads it, and not when a served one does too", () => {
         writeSkill("skills", "old");
         expect(
            skillsDirNotRead({
               default: { description: "", skillSets: ["skills"] },
            }),
         ).toBe(true);
         expect(
            skillsDirNotRead({
               broken: { description: "", skillSets: ["skills"] },
               default: { skillSets: ["skills"] },
            }),
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

      it.skipIf(isWindows)(
         "is not looked through when it is a link out of the package, whether or not the target exists",
         () => {
            // What lies outside decides nothing: both targets answer the same.
            fs.mkdirSync(path.join(outside, "real"));
            fs.symlinkSync(
               path.join(outside, "real"),
               path.join(pkg, "skills"),
            );
            const toExisting = skillsDirNotRead(undefined);
            fs.rmSync(path.join(pkg, "skills"));
            fs.symlinkSync(
               path.join(outside, "missing"),
               path.join(pkg, "skills"),
            );
            const toMissing = skillsDirNotRead(undefined);
            expect([toExisting, toMissing]).toEqual([false, false]);
         },
      );

      it.skipIf(isWindows)(
         "is flagged when it is a link to a folder inside the package",
         () => {
            writeSkill("old-skills", "old");
            fs.symlinkSync(
               path.join(pkg, "old-skills"),
               path.join(pkg, "skills"),
            );
            expect(skillsDirNotRead(undefined)).toBe(true);
         },
      );
   });

   describe("warning codes and subjects", () => {
      const BIG = "x".repeat(257 * 1024);
      /** The one finding of the default workflow's set that mentions `needle`. */
      const findingAbout = (needle: string) => {
         const found = read({ default: analyst() })
            .sets.get(MAIN)!
            .findings.filter((f) => f.message.includes(needle));
         expect(found).toHaveLength(1);
         return found[0]!;
      };

      it.each([
         [
            "a skill with no description",
            SKILL_FINDING,
            "no description",
            () =>
               write(`${MAIN}/quiet/SKILL.md`, "---\nname: quiet\n---\n\nb\n"),
         ],
         [
            "a directory with no SKILL.md",
            SKILL_FINDING,
            "has no SKILL.md",
            () =>
               fs.mkdirSync(path.join(pkg, MAIN, "empty"), { recursive: true }),
         ],
         [
            "a Markdown file listed in place of inlined",
            SKILL_FINDING,
            "so it is listed instead of inlined",
            () => write(`${CONVENTIONS}/docs/big.md`, BIG),
         ],
         [
            "a skill set aside for a publisher.json inside it",
            SKILL_FINDING,
            "is not served: ",
            () => {
               write(`${MAIN}/aaa/publisher.json`, "{}");
               write(
                  `${MAIN}/aaa/SKILL.md`,
                  "---\nname: aaa\ndescription: d\n---\n\nb\n",
               );
            },
         ],
         [
            "a reference named like a file the host reads with authority",
            SKILL_FILE_SKIPPED,
            "references/AGENTS.md",
            () => write(`${CONVENTIONS}/references/AGENTS.md`, "x"),
         ],
         [
            "a skipped asset",
            SKILL_FILE_SKIPPED,
            "scripts/publisher.json",
            () => write(`${CONVENTIONS}/scripts/publisher.json`, "{}"),
         ],
         [
            "a file over the reply budget of get_skill",
            SKILL_FILE_SKIPPED,
            "reply budget",
            () =>
               write(
                  `${CONVENTIONS}/references/long.md`,
                  "x".repeat(FCJ_MAX_CHARS + 1),
               ),
         ],
      ])("%s is %s of its set", (_label, code, needle, setup) => {
         setup();
         expect(findingAbout(needle)).toMatchObject({
            code,
            subject: MAIN,
            severity: "warn",
         });
         expect(findingAbout(needle).message).toStartWith(
            `Skill set '${MAIN}' skill finding: `,
         );
      });

      it("makes a set with no skills, and one past the skill cap, findings of the set", () => {
         write(INSTRUCTIONS, "i");
         fs.mkdirSync(path.join(pkg, SECOND), { recursive: true });
         for (let i = 0; i <= SKILLS_PER_SET_MAX; i++) {
            writeSkill(MAIN, `s${String(i).padStart(3, "0")}`);
         }
         const result = read({
            analyst: { instructions: INSTRUCTIONS, skillSets: [MAIN, SECOND] },
         });
         expect(result.sets.get(SECOND)!.findings).toEqual([
            {
               code: SKILL_FINDING,
               subject: SECOND,
               severity: "warn",
               message: `Skill set '${SECOND}' skill finding: the skill set holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry.`,
            },
         ]);
         expect(result.sets.get(MAIN)!.findings).toMatchObject([
            { code: SKILL_FINDING, subject: MAIN },
         ]);
         expect(result.sets.get(MAIN)!.findings[0]!.message).toContain(
            `holds more than ${SKILLS_PER_SET_MAX} skill directories`,
         );
      });

      it("counts the skipped assets past the named ones as skipped files", () => {
         for (let i = 0; i < 21; i++) {
            write(`${CONVENTIONS}/scripts/m${i}.malloy`, "x");
         }
         const counted = findingAbout("further assets are not served");
         expect(counted).toMatchObject({
            code: SKILL_FILE_SKIPPED,
            subject: MAIN,
         });
      });

      it("keeps each set's findings on the workflow that reads it whole, beside its shadowing findings", () => {
         writeSkill(SECOND, "conventions", "second copy");
         write(`${SECOND}/conventions/scripts/CLAUDE.md`, "x");
         const result = read({
            analyst: { ...analyst(), skillSets: [MAIN, SECOND] },
         });
         const workflow = result.workflows.get("analyst")!;
         expect(workflow.shadowing.map((w) => [w.code, w.subject])).toEqual([
            [SKILL_SHADOWED, "analyst"],
         ]);
         expect(result.warnings.map((w) => [w.code, w.subject]).sort()).toEqual(
            [
               [SKILL_SHADOWED, "analyst"],
               [SKILL_FILE_SKIPPED, SECOND],
            ].sort(),
         );
      });
   });

   describe("a folder that is a skill, not a set", () => {
      const SKILL_DIR = "sets/margin";

      const listedAsSet = (setPath: string) => {
         write(
            `${setPath}/SKILL.md`,
            "---\nname: margin\ndescription: d\n---\n",
         );
         write(`${setPath}/references/a.md`, "# A\n");
         write(`${setPath}/scripts/run.py`, "print()\n");
         write(INSTRUCTIONS, "i");
         return read({
            analyst: { instructions: INSTRUCTIONS, skillSets: [setPath] },
         });
      };

      it("says it is a skill and names its parent folder, with no finding for each of its folders", () => {
         const result = listedAsSet(SKILL_DIR);
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.sets.get(SKILL_DIR)!.skills).toEqual([]);
         expect(result.sets.get(SKILL_DIR)!.findings).toEqual([
            {
               code: SKILL_FINDING,
               subject: SKILL_DIR,
               severity: "warn",
               message: `Skill set '${SKILL_DIR}' skill finding: this is a skill, not a skill set (it holds a SKILL.md), so none of it is served. Fix: list its parent folder 'sets'.`,
            },
         ]);
      });

      it("asks for a folder of its own when the skill sits at the package root's top level", () => {
         const result = listedAsSet("margin");
         expect(result.sets.get("margin")!.findings[0]!.message).toContain(
            "Fix: move it into a folder and list that folder.",
         );
      });
   });

   describe("a workflows value too large to read", () => {
      it("says it is too large, not that it must be an object", () => {
         const result = read(
            boundedDeclaration({
               big: "x".repeat(MANIFEST_WORKFLOWS_MAX_BYTES),
            }).value,
         );
         expect(result.workflows.size).toBe(0);
         expect(result.warnings).toEqual([
            {
               severity: "warn",
               message:
                  "Package manifest 'workflows' is too deeply nested or over 1 MiB, so no workflow is served. Fix: shorten it; a real declaration is a few KiB.",
            },
         ]);
      });
   });

   describe("assets", () => {
      const ASSETS = `${CONVENTIONS}/assets`;
      const MAX = 1024 * 1024;
      const LISTED = "conventions/assets";
      const listed = (result: PackageWorkflows) =>
         (viewOf(result).assets ?? []).map((a) => a.relative_filepath);
      const warned = (result: PackageWorkflows, ...needles: string[]) =>
         texts(result.warnings).some((w) =>
            needles.every((n) => w.includes(n)),
         );
      /** A strict finding sets the skill aside with a finding the service matches on, and serves the workflow. */
      const setAside = (needle: string) => {
         const result = read({ analyst: analyst() });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.workflows.get("analyst")!.sets[0]!.skills).toEqual([]);
         expect(texts(result.warnings).join("\n")).toContain(
            `Skill set '${MAIN}' skill finding: Package skill '${CONVENTIONS}' is not served: `,
         );
         expect(texts(result.warnings).join("\n")).toContain(needle);
      };

      it("lists text assets with their size and sha, and ignores dotfiles", () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         write(`${ASSETS}/css/app.css`, "body{}");
         write(`${ASSETS}/.hidden/x.txt`, "ignored");
         write(`${ASSETS}/.DS_Store`, "ignored");
         const result = read({ analyst: analyst() });

         expect(texts(result.warnings)).toEqual([]);
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
         expect(texts(result.warnings)).toEqual([]);
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
         expect(
            warned(result, "exact.txt", "at or over the 1 MiB file cap"),
         ).toBe(true);
      });

      describe("skipped, with the skill still served", () => {
         it("lists non-UTF-8 and NUL-bearing files too, hashing their raw bytes", () => {
            const png = Buffer.from([0x89, 0x50, 0xff, 0xfe]);
            const nul = Buffer.from("a\0b");
            write(`${ASSETS}/logo.png`, png);
            write(`${ASSETS}/nul.txt`, nul);
            write(`${ASSETS}/ok.txt`, "fine");
            const result = read({ analyst: analyst() });
            expect(texts(result.warnings)).toEqual([]);
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

         it("holds the 4 MiB to the skill set, not to each skill in it", () => {
            const entry = analyst();
            writeSkill(MAIN, "second");
            for (const skill of ["conventions", "second"]) {
               for (let i = 0; i < 3; i++) {
                  write(
                     `${MAIN}/${skill}/assets/big${i}.txt`,
                     Buffer.alloc(900_000, "a"),
                  );
               }
            }
            const result = read({ analyst: entry });
            const files = result.sets
               .get(MAIN)!
               .skills.flatMap((s) => s.listed.map((a) => a.relative_filepath));
            expect(files).toEqual([
               "conventions/assets/big0.txt",
               "conventions/assets/big1.txt",
               "conventions/assets/big2.txt",
               "second/assets/big0.txt",
            ]);
            expect(
               warned(result, "second/assets/big1.txt", "per-set cap"),
            ).toBe(true);
            expect(
               warned(result, "second/assets/big2.txt", "per-set cap"),
            ).toBe(true);
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
               texts(result.warnings).filter((w) => w.includes(".malloy")),
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

         it("for a publisher.json below assets/, in any case", () => {
            for (const name of ["publisher.json", "Publisher.JSON"]) {
               fs.rmSync(path.join(pkg, ASSETS), {
                  recursive: true,
                  force: true,
               });
               write(`${ASSETS}/sub/${name}`, "{}");
               setAside(`conventions/assets/sub/${name}`);
            }
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

         describe("without spending the set's inline budget on its SKILL.md", () => {
            const bigSkill = (name: string, kib: number) =>
               write(
                  `${MAIN}/${name}/SKILL.md`,
                  `---\nname: ${name}\ndescription: d\n---\n\n${"x".repeat(kib * 1024)}\n`,
               );

            it("for a publisher.json inside it, which a later skill's SKILL.md would otherwise be refused over", () => {
               analyst();
               bigSkill("aaa", 200);
               write(`${MAIN}/aaa/publisher.json`, "{}");
               bigSkill("zzz", 100);
               const result = read({ analyst: analyst() });
               expect(
                  viewOf(result).skills.map((f) => f.relative_filepath),
               ).toContain("zzz/SKILL.md");
               const findings = texts(result.sets.get(MAIN)!.findings).join(
                  "\n",
               );
               expect(findings).toContain(
                  `Package skill '${MAIN}/aaa' is not served`,
               );
               expect(findings).not.toContain("over the 256 KiB cap");
            });

            it("for a strict rule met while its files are read, which a later skill's reference would otherwise be listed over", () => {
               analyst();
               bigSkill("aaa", 200);
               write(`${MAIN}/aaa/assets/a..b.txt`, "x");
               bigSkill("zzz", 1);
               write(
                  `${MAIN}/zzz/reference/part.md`,
                  `#\n${"y".repeat(100 * 1024)}`,
               );
               const result = read({ analyst: analyst() });
               const view = viewOf(result);
               expect(view.skills.map((f) => f.relative_filepath)).toContain(
                  "zzz/reference/part.md",
               );
               expect(
                  (view.assets ?? []).map((a) => a.relative_filepath),
               ).not.toContain("zzz/reference/part.md");
               expect(
                  texts(result.sets.get(MAIN)!.findings).join("\n"),
               ).toContain(`Package skill '${MAIN}/aaa' is not served`);
            });
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
               c: { skillSets: ["sets/c"] },
               b: { skillSets: ["sets/b"] },
               a: { skillSets: ["sets/a"] },
               d: { skillSets: ["sets/c"] },
            });
            const files = (setPath: string) =>
               result.sets.get(setPath)!.skills.flatMap((s) => s.listed);

            expect(files("sets/a")).toHaveLength(4);
            expect(files("sets/b")).toHaveLength(4);
            expect(files("sets/c")).toHaveLength(0);
            expect(
               texts(result.warnings).some(
                  (w) =>
                     w.startsWith("Skill set 'sets/c' skill finding:") &&
                     w.includes("8 MiB per-package cap"),
               ),
            ).toBe(true);
            expect(result.sets.size).toBe(3);
         });

         it("moves no set's digest when workflows are reordered at the package cap, where charging order decides what is listed", () => {
            writeAssets("sets/a", 4);
            writeAssets("sets/b", 4);
            writeAssets("sets/c", 4);
            const flow = (setPath: string) => ({ skillSets: [setPath] });
            const digests = (result: PackageWorkflows) =>
               Object.fromEntries(
                  [...result.sets].map(([setPath, set]) => [
                     setPath,
                     set.digest,
                  ]),
               );
            const forward = read({
               a: flow("sets/a"),
               b: flow("sets/b"),
               c: flow("sets/c"),
            });
            const backward = read({
               c: flow("sets/c"),
               b: flow("sets/b"),
               a: flow("sets/a"),
            });
            // c is the set the cap cuts off; first-come charging would cut a or b instead.
            expect(
               forward.sets.get("sets/c")!.skills.flatMap((s) => s.listed),
            ).toHaveLength(0);
            expect(digests(backward)).toEqual(digests(forward));
         });

         it("does not read or charge the set of a workflow that is refused", () => {
            writeAssets("sets/a", 4);
            writeAssets("sets/b", 4);
            writeAssets("sets/c", 4);
            const result = read({
               a: { skillSets: ["sets/a"], surprise: true },
               b: { skillSets: ["sets/b"] },
               c: { skillSets: ["sets/c"] },
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

   describe("a set's bounds", () => {
      const SET = "sets/many";

      it("serves the first skills by name up to the cap, and names the rest once", () => {
         for (let i = 0; i < SKILLS_PER_SET_MAX + 3; i++) {
            writeSkill(SET, `s${String(i).padStart(3, "0")}`);
         }
         const result = read({ default: { skillSets: [SET] } });
         const set = result.sets.get(SET)!;
         expect(set.skills.map((s) => s.dirName)).toEqual(
            Array.from(
               { length: SKILLS_PER_SET_MAX },
               (_, i) => `s${String(i).padStart(3, "0")}`,
            ),
         );
         expect(texts(result.warnings)).toEqual([
            `Skill set '${SET}' skill finding: the skill set holds more than ${SKILLS_PER_SET_MAX} skill directories, so the first ${SKILLS_PER_SET_MAX} by name are served and the rest are not read. Fix: move some to another skill set.`,
         ]);
      });

      it("serves a set of exactly the cap whole, with no finding", () => {
         for (let i = 0; i < SKILLS_PER_SET_MAX; i++) {
            writeSkill(SET, `s${String(i).padStart(3, "0")}`);
         }
         const result = read({ default: { skillSets: [SET] } });
         expect(result.sets.get(SET)!.skills).toHaveLength(SKILLS_PER_SET_MAX);
         expect(texts(result.warnings)).toEqual([]);
      });

      it("names at most the findings cap of a set's skill findings, and counts the rest", () => {
         const extra = 5;
         for (let i = 0; i < SKILL_FINDINGS_MAX + extra; i++) {
            write(
               `${SET}/bad${String(i).padStart(2, "0")}/SKILL.md`,
               "no frontmatter\n",
            );
         }
         writeSkill(SET, "ok");
         const result = read({ default: { skillSets: [SET] } });
         expect(texts(result.warnings)).toHaveLength(SKILL_FINDINGS_MAX + 1);
         expect(texts(result.warnings).at(-1)).toBe(
            `Skill set '${SET}' skill finding: ${extra} further skill findings are not named, for the same kinds of reasons.`,
         );
         // A skill with a finding is still served.
         expect(result.sets.get(SET)!.skills).toHaveLength(
            SKILL_FINDINGS_MAX + extra + 1,
         );
      });

      it("caps each set's findings on its own", () => {
         for (const set of ["sets/a", "sets/b"]) {
            for (let i = 0; i < SKILL_FINDINGS_MAX + 1; i++) {
               write(
                  `${set}/${set.slice(-1)}${String(i).padStart(2, "0")}/SKILL.md`,
                  "no frontmatter\n",
               );
            }
         }
         const result = read({
            default: { skillSets: ["sets/a", "sets/b"] },
         });
         expect(texts(result.warnings)).toHaveLength(
            2 * (SKILL_FINDINGS_MAX + 1),
         );
      });
   });

   describe("the shas a walk ships", () => {
      it("has one for every path it read, an instructions file and a .skillignore included", () => {
         const declared = { analyst: analyst() };
         write(`${CONVENTIONS}/.skillignore`, "notes.txt\n");
         write(`${CONVENTIONS}/notes.txt`, "left out");
         write(`${CONVENTIONS}/assets/logo.bin`, Buffer.from([0, 255, 1]));
         const result = read(declared);
         for (const p of result.paths) {
            expect(result.contentShas.has(p)).toBe(true);
         }
         expect(result.contentShas.get(INSTRUCTIONS)).toBe(
            sha("Be careful with revenue."),
         );
         expect(result.contentShas.get(`${CONVENTIONS}/.skillignore`)).toBe(
            sha("notes.txt\n"),
         );
         expect(result.contentShas.get(`${CONVENTIONS}/assets/logo.bin`)).toBe(
            sha(Buffer.from([0, 255, 1])),
         );
         expect(result.paths).not.toContain(`${CONVENTIONS}/notes.txt`);
      });

      it("moves with an edit to the .skillignore, since it changes what the set serves", () => {
         const declared = { analyst: analyst() };
         write(`${CONVENTIONS}/.skillignore`, "a.txt\n");
         const before = read(declared).contentShas.get(
            `${CONVENTIONS}/.skillignore`,
         );
         write(`${CONVENTIONS}/.skillignore`, "a.txt\nb.txt\n");
         expect(
            read(declared).contentShas.get(`${CONVENTIONS}/.skillignore`),
         ).not.toBe(before);
      });
   });

   describe("across the worker boundary", () => {
      const declared = () => ({
         one: analyst(),
         two: { description: "Two", skillSets: [MAIN] },
      });

      /** What a worker ships: the walk without its readers, cloned. */
      const shipped = (walk: PackageWorkflows) =>
         adoptWorkflows(pkg, structuredClone(workflowsToWire(walk)));

      it("keeps the maps, the digests and the own keys named like Object.prototype members", () => {
         for (const name of ["__proto__", "constructor", "toString"]) {
            write(`${CONVENTIONS}/${name}`, `root ${name}\n`);
         }
         const walked = read(declared());
         const adopted = shipped(walked);

         expect([...adopted.workflows.keys()]).toEqual(["one", "two"]);
         expect([...adopted.sets.keys()]).toEqual([MAIN]);
         expect(adopted.contentShas).toEqual(walked.contentShas);
         expect(adopted.paths).toEqual(walked.paths);
         expect(adopted.watched).toEqual(walked.watched);
         expect(adopted.warnings).toEqual(walked.warnings);
         expect(adopted.sets.get(MAIN)!.digest).toBe(
            walked.sets.get(MAIN)!.digest,
         );
         const files = adopted.sets.get(MAIN)!.skills[0]!.catalog.files;
         for (const name of ["__proto__", "constructor", "toString"]) {
            expect(Object.hasOwn(files, name)).toBe(true);
         }
         expect(Object.hasOwn(files, "valueOf")).toBe(false);
         expect(files).toEqual(walked.sets.get(MAIN)!.skills[0]!.catalog.files);
      });

      it("keeps one skill shared by every set and workflow that holds it, with its reader rebuilt", async () => {
         const adopted = shipped(read(declared()));
         const fromSet = adopted.sets.get(MAIN)!.skills[0]!;
         for (const workflow of adopted.workflows.values()) {
            expect(workflow.sets[0]!.skills[0]).toBe(fromSet);
         }
         expect(await fromSet.served.read("SKILL.md")).toEqual(
            await read(declared())
               .sets.get(MAIN)!
               .skills[0]!.served.read("SKILL.md"),
         );
      });

      it("answers a binary listed file from its listing, without reading the disk", async () => {
         write(`${CONVENTIONS}/assets/logo.bin`, Buffer.from([0, 255, 1]));
         const adopted = shipped(read(declared()));
         const skill = adopted.sets.get(MAIN)!.skills[0]!;
         const reads = (
            [
               "readFileSync",
               "openSync",
               "statSync",
               "lstatSync",
               "realpathSync",
            ] as const
         ).map((fn) => spyOn(fs, fn));
         const open = spyOn(fs.promises, "open");
         try {
            expect(await skill.served.read("assets/logo.bin")).toEqual({
               binary: true,
            });
            expect(reads.map((spy) => spy.mock.calls.length)).toEqual([
               0, 0, 0, 0, 0,
            ]);
            expect(open).not.toHaveBeenCalled();
         } finally {
            for (const spy of reads) spy.mockRestore();
            open.mockRestore();
         }
      });

      it("carries a walk that could not finish, with the names the manifest declares", () => {
         const walked = read({
            ...declared(),
            [`${"x"}`]: new Proxy(
               {},
               {
                  ownKeys() {
                     throw new Error("injected walk failure");
                  },
               },
            ),
         });
         expect(walked.unavailable?.declared).toEqual(["one", "two", "x"]);
         expect(shipped(walked).unavailable).toEqual(walked.unavailable);
      });
   });
});

describe("workflowFileServed", () => {
   const watched = [INSTRUCTIONS, MAIN];
   const served = (relative: string) =>
      workflowFileServed("/nowhere", watched, relative, false);

   it("serves an instructions file and any file a skill of a set could serve", () => {
      expect(served(INSTRUCTIONS)).toBe(true);
      expect(served(`${CONVENTIONS}/SKILL.md`)).toBe(true);
      expect(served(`${CONVENTIONS}/scripts/run.py`)).toBe(true);
   });

   it("serves a manifest the package's own declares workflows in, and one at or above a set or an instructions file, which refuses its workflows", () => {
      expect(served("publisher.json")).toBe(true);
      expect(served(`${MAIN}/publisher.json`)).toBe(true);
      expect(served("workflows/analyst/skill_sets/publisher.json")).toBe(true);
      expect(served("workflows/analyst/publisher.json")).toBe(true);
      // Beside, not at or above, any watched path.
      expect(served("workflows/other/publisher.json")).toBe(false);
      expect(served(`${MAIN}2/publisher.json`)).toBe(false);
   });

   it("leaves out a file loose in a set's folder, a dot-named folder, a name no walk keeps, and a file outside every path", () => {
      expect(served(`${MAIN}/README.md`)).toBe(false);
      expect(served(`${MAIN}/.git/config`)).toBe(false);
      expect(served(`${CONVENTIONS}/.env`)).toBe(false);
      expect(served(`${CONVENTIONS}/scripts/__pycache__/run.pyc`)).toBe(false);
      expect(served("scripts/run.py")).toBe(false);
      // A sibling whose name only starts like the set's is not under it.
      expect(served(`${MAIN}2/conventions/SKILL.md`)).toBe(false);
   });
});
