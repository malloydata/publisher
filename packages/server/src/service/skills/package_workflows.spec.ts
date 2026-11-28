// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { linkTextKeptVerbatim } from "../../test_helpers/link_text";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs";
import os from "os";
import { createHash } from "crypto";
import path from "path";
import { FCJ_MAX_CHARS } from "../../constants";
import { expectWarning } from "../../test_helpers/expect_warning";
import * as revision from "../package_revision";
import {
   boundedDeclaration,
   MANIFEST_DECLARATION_MAX_BYTES,
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
   servedContentShas,
   SKILL_FINDINGS_MAX,
   SKILL_SET_INLINE_MAX_FILES,
   SKILLS_PER_SET_MAX,
   WORKFLOW_INSTRUCTIONS_MAX_BYTES,
   workflowFileServed,
   workflowsToWire,
   SKILL_FILE_SKIPPED,
   SKILL_FINDING,
   SKILL_SET_NOT_SERVED,
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
/** The names the fixtures give the sets at MAIN and SECOND, unlike their folders' own. */
const HOUSE = "house";
const EXTRA = "extra";
/** The set the package always selects; its skills are the package's own. */
const DEFAULT = "default";
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

   /** A workflow with instructions, which every workflow needs, listing `names`. */
   const listing = (...names: string[]) => {
      write(INSTRUCTIONS, "Be careful with revenue.");
      return { instructions: INSTRUCTIONS, skillSets: names };
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
         skillSets: [HOUSE],
         ...extra,
      };
   };

   /** The sets of `KNOWN` that a workflow lists, declared: a fixture lists names, and the map follows. */
   const KNOWN: Record<string, string> = { [HOUSE]: MAIN, [EXTRA]: SECOND };
   const declaring = (workflows: unknown) => {
      const listed = new Set<unknown>();
      try {
         for (const entry of Object.values(workflows as object)) {
            const names = (entry as { skillSets?: unknown })?.skillSets;
            if (Array.isArray(names)) names.forEach((n) => listed.add(n));
         }
      } catch {
         // Not a map of workflows: nothing it lists is declared.
      }
      const known = Object.entries(KNOWN).filter(([name]) => listed.has(name));
      return known.length === 0
         ? undefined
         : Object.fromEntries(known.map(([name, at]) => [name, { path: at }]));
   };

   /** Reads the manifest declaring `workflows`, and `skillSets` or else the sets of `KNOWN` they list. */
   const read = (
      workflows: unknown,
      skillSets: unknown = declaring(workflows),
      bundled: readonly string[] = [],
   ) =>
      readPackageWorkflows(
         pkg,
         boundedDeclaration({ skillSets, workflows }).value,
         bundled,
      );

   /** Asserts the workflow is refused with one `workflow-not-served` warning, and nothing else is said; returns its text. */
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

   /** Asserts the set is refused with one `skill-set-not-served` warning on its name; returns its text. */
   const refusedSet = (
      skillSets: unknown,
      workflows: unknown = undefined,
      name = HOUSE,
   ) => {
      const result = read(workflows, skillSets);
      expect(result.sets.has(name)).toBe(false);
      const found = result.warnings.filter(
         (w) => w.code === SKILL_SET_NOT_SERVED && w.subject === name,
      );
      expect(found).toHaveLength(1);
      expect(found[0]!.severity).toBe("warn");
      expect(found[0]!.message).toStartWith(
         `Skill set '${name}' is not served: `,
      );
      expect(found[0]!.message).toEndWith(
         `. Fix: correct the 'skillSets.${name}' entry in publisher.json and the folder it names.`,
      );
      return found[0]!.message;
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
            name: HOUSE,
            path: MAIN,
            overrides: [],
            findings: [],
         });
         expect(workflow.sets[0]!.set.digest).toMatch(/^[0-9a-f]{64}$/);
         expect(result.sets.get(HOUSE)).toBe(workflow.sets[0]!.set);
         expect([...result.contentShas.keys()].sort()).toEqual([
            INSTRUCTIONS,
            `${CONVENTIONS}/SKILL.md`,
            `${CONVENTIONS}/reference/glossary.md`,
         ]);
         expect(result.watched).toEqual([INSTRUCTIONS, MAIN]);
         expect(result.skillsDirNotRead).toBe(false);
      });

      it("serves SKILL.md as written, with the keys it carries, and references raw, sorted", () => {
         const { skills } = viewOf(read({ analyst: analyst() }));
         expect(skills.map((s) => s.relative_filepath)).toEqual([
            "conventions/SKILL.md",
            "conventions/reference/glossary.md",
         ]);
         expect(skills[0]!.file_contents).toBe(
            "---\nname: conventions\ndescription: House rules.\nallowed-tools: Bash\nx-owner: me\n---\n\nUse net revenue.\n",
         );
         expect(skills[1]!.file_contents).toBe(
            "# Glossary\n\nnet = after refunds\n",
         );
      });

      it("summarizes a skill with its parsed name and description and the size of the bytes as written", () => {
         write(INSTRUCTIONS, "i");
         write(
            `${MAIN}/quoted/SKILL.md`,
            '---\nname: quoted\ndescription: Say "net" and use C:\\data\n---\n\nbody\n',
         );
         const result = read({ analyst: listing(HOUSE) });
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

      it("serves a workflow with only instructions, listing no set and needing no description", () => {
         write(INSTRUCTIONS, "brief");
         writeSkill(MAIN, "conventions");
         const result = read({
            brief: { instructions: INSTRUCTIONS },
            listed: { instructions: INSTRUCTIONS, skillSets: [HOUSE] },
            empty: { instructions: INSTRUCTIONS, skillSets: [] },
         });
         expect(texts(result.warnings)).toEqual([]);
         const brief = result.workflows.get("brief")!;
         expect(brief.sets).toEqual([]);
         expect(brief.description).toBeUndefined();
         expect(result.workflows.get("empty")!.sets).toEqual([]);
      });

      it("refuses a workflow without instructions, whatever else it declares", () => {
         writeSkill(MAIN, "one");
         expect(refused({ analyst: {} })).toContain(
            "'instructions' is required",
         );
         expect(
            refused({
               analyst: { description: "d", skillSets: [HOUSE] },
            }),
         ).toContain("'instructions' is required");
      });

      it("serves a workflow named default as an ordinary workflow, with no skill of the package's", () => {
         writeSkill(MAIN, "conventions");
         const result = read({ default: listing(HOUSE) });
         expect(result.workflows.has("default")).toBe(true);
         expect(advertisedSkills(result)).toEqual([]);
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
         expect(result.workflows.get("root")!.instructions.text).toBe("hello");
      });
   });

   describe("manifest shape", () => {
      it("returns nothing and no warning when the manifest declares neither skillSets nor workflows", () => {
         expect(readPackageWorkflows(pkg, undefined, [])).toEqual({
            ...emptyWorkflows(),
            contentShas: new Map(),
         });
      });

      it.each([[[]], ["x"], [5], [null]])(
         "refuses the whole declaration, naming the key, when workflows is %p",
         (value) => {
            const result = read(value);
            expect(result.workflows.size).toBe(0);
            expect(texts(result.warnings)).toHaveLength(1);
            expect(texts(result.warnings)[0]).toContain(
               "'workflows' must be an object",
            );
            expect(texts(result.warnings)[0]).toContain("Fix:");
            expect(result.declarationRefusal).toBe(texts(result.warnings)[0]!);
         },
      );

      it.each([[[]], ["x"], [5], [null]])(
         "refuses every set and workflow, naming the key, when skillSets is %p",
         (value) => {
            const result = read({ analyst: analyst() }, value);
            expect(result.workflows.size).toBe(0);
            expect(result.sets.size).toBe(0);
            expect(texts(result.warnings)).toHaveLength(1);
            expect(texts(result.warnings)[0]).toContain(
               "'skillSets' must be an object",
            );
            expect(texts(result.warnings)[0]).toContain(
               "so no skill set or workflow is served",
            );
            expect(result.declarationRefusal).toBe(texts(result.warnings)[0]!);
         },
      );

      it("refuses the whole declaration for a 33rd skill set or a 65th workflow, and serves 32 and 64", () => {
         const sets = (n: number) =>
            Object.fromEntries(
               Array.from({ length: n }, (_, i) => [`s${i}`, { path: MAIN }]),
            );
         write(INSTRUCTIONS, "i");
         const workflows = (n: number) =>
            Object.fromEntries(
               Array.from({ length: n }, (_, i) => [
                  `w${i}`,
                  { instructions: INSTRUCTIONS },
               ]),
            );
         const over = read(workflows(1), sets(33));
         expect(over.sets.size + over.workflows.size).toBe(0);
         expect(over.declarationRefusal).toContain(
            "'skillSets' declares 33 skill sets, over the limit of 32",
         );
         const overWorkflows = read(workflows(65), {});
         expect(overWorkflows.workflows.size).toBe(0);
         expect(overWorkflows.declarationRefusal).toContain(
            "'workflows' declares 65 workflows, over the limit of 64",
         );
         expect(read(workflows(64), {}).workflows.size).toBe(64);
         // Thirty-two sets at one path are all refused for it, but the declaration stands.
         expect(read(undefined, sets(32)).declarationRefusal).toBeUndefined();
      });

      it("names no whole-declaration refusal for a workflow or set refused alone, or for a served one", () => {
         expect(read({ bad: "nope" }).declarationRefusal).toBeUndefined();
         expect(read({}).declarationRefusal).toBeUndefined();
         expect(
            read(undefined, { bad: "nope" }).declarationRefusal,
         ).toBeUndefined();
         expect(
            readPackageWorkflows(pkg, undefined, []).declarationRefusal,
         ).toBeUndefined();
      });

      it.each([
         ["Upper"],
         ["-lead"],
         ["trail-"],
         ["dou--ble"],
         ["under_score"],
         ["a".repeat(65)],
         [""],
      ])("refuses a workflow named %p, and a skill set named so", (name) => {
         const result = read({ [name]: analyst() });
         expect(result.workflows.size).toBe(0);
         expect(texts(result.warnings)).toHaveLength(1);
         expect(texts(result.warnings)[0]).toContain(
            "its name must be 1-64 characters of lowercase letters, digits and single hyphens",
         );
         expect(
            refusedSet({ [name]: { path: MAIN } }, undefined, name),
         ).toContain(
            "its name must be 1-64 characters of lowercase letters, digits and single hyphens",
         );
      });

      it("serves a workflow and a set named for their 64 characters", () => {
         const name = `a${"b".repeat(63)}`;
         expect(read({ [name]: analyst() }).workflows.size).toBe(1);
         expect(read(undefined, { [name]: { path: MAIN } }).sets.size).toBe(1);
      });

      it("does not treat __proto__ as a workflow or a skill set", () => {
         const raw = JSON.parse(`{"__proto__": {"description": "d"}}`);
         expect(read(raw).workflows.size).toBe(0);
         const set = JSON.parse(`{"__proto__": {"path": "${MAIN}"}}`);
         expect(read(undefined, set).sets.size).toBe(0);
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
         ["skill_sets", [HOUSE]],
      ])("refuses a workflow that sets %p, naming it", (key, value) => {
         const warning = refused({ analyst: analyst({ [key]: value }) });
         expect(warning).toContain(`'${key}'`);
         expect(warning).toContain(
            "Allowed keys: description, instructions, skillSets",
         );
      });

      it.each([
         ["model", "sonnet"],
         ["instructions", "x.md"],
         ["skillSets", [HOUSE]],
         ["surprise", 1],
      ])("refuses a skill set that sets %p, naming it", (key, value) => {
         analyst();
         const why = refusedSet({
            house: { path: MAIN, [key]: value },
         });
         expect(why).toContain(`'${key}'`);
         expect(why).toContain("Allowed keys: path, description, overrides");
      });

      it("ignores x- keys on a skill set, and a skill set's other served keys", () => {
         analyst();
         const result = read(undefined, {
            house: { path: MAIN, "x-owner": "data team", description: "d" },
         });
         expect(texts(result.warnings)).toEqual([]);
         expect(result.sets.get(HOUSE)!.description).toBe("d");
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
         ["skillSets", ["workflows/analyst/skill_sets/main"]],
      ])("refuses a workflow whose %s is %p", (key, value) => {
         refused({ analyst: analyst({ [key]: value }) });
      });

      it.each([
         ["path", undefined, "'path' is required"],
         ["path", "", "'path' must be a non-empty package-relative path"],
         ["path", 3, "'path' must be a non-empty package-relative path"],
         ["path", ["x"], "'path' must be a non-empty package-relative path"],
         ["description", "", "'description' must be a non-empty string"],
         ["description", 3, "'description' must be a non-empty string"],
      ])("refuses a skill set whose %s is %p", (key, value, why) => {
         analyst();
         expect(refusedSet({ house: { path: MAIN, [key]: value } })).toContain(
            why,
         );
      });

      it.each([[[1]], ["x"], [null]])(
         "refuses a workflow, or a skill set, whose entry is %p",
         (entry) => {
            expect(refused({ analyst: entry })).toContain(
               "its entry must be an object",
            );
            expect(refusedSet({ house: entry })).toContain(
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
         for (const raw of [
            { a: null },
            { a: [] },
            { a: { path: { x: 1 } } },
            { a: { path: MAIN, overrides: { x: 1 } } },
            { a: { path: MAIN, description: [] } },
         ]) {
            expect(() => read(undefined, raw)).not.toThrow();
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

      it("names the backslash in a skill set's path", () => {
         expect(
            refusedSet({ house: { path: "workflows\\analyst" } }),
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
         const result = read({ analyst: { instructions: entry.instructions } });
         expectWarning(
            result.warnings,
            WORKFLOW_NOT_SERVED,
            "analyst",
            `'instructions' '${INSTRUCTIONS}' sits under a nested publisher.json at 'workflows/analyst'`,
         );
      });

      it("hashes the same files for ./ and doubled slashes in a path", () => {
         const entry = analyst();
         const plain = read({ analyst: entry });
         const dotted = read(
            {
               analyst: {
                  ...entry,
                  instructions: "./workflows//analyst/instructions.md",
               },
            },
            { house: { path: `${MAIN}/` } },
         );
         expect(texts(dotted.warnings)).toEqual([]);
         expect([...dotted.contentShas].sort()).toEqual(
            [...plain.contentShas].sort(),
         );
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
      ])("refuses a skill set whose path is %s", (_label, at) => {
         refusedSet({ house: { path: at } });
      });

      it("refuses every workflow that lists a refused set, naming why, and serves the ones that do not", () => {
         analyst();
         const result = read(
            {
               lists: listing(HOUSE),
               plain: listing(),
            },
            { house: { path: "nope" } },
         );
         expect([...result.workflows.keys()]).toEqual(["plain"]);
         expectWarning(
            result.warnings,
            SKILL_SET_NOT_SERVED,
            HOUSE,
            "skill set directory 'nope' does not exist",
         );
         expectWarning(
            result.warnings,
            WORKFLOW_NOT_SERVED,
            "lists",
            "it lists the skill set 'house', which is not served: skill set directory 'nope' does not exist",
         );
      });

      it("says a missing skill set does not exist", () => {
         expect(refusedSet({ house: { path: "nope" } })).toContain(
            "skill set directory 'nope' does not exist",
         );
      });

      it("says a skill set that is a file is not a folder, not that it does not exist", () => {
         analyst();
         const why = refusedSet({ house: { path: INSTRUCTIONS } });
         expect(why).toContain(
            `skill set directory '${INSTRUCTIONS}' is not a folder`,
         );
         expect(why).not.toContain("does not exist");
      });

      it("refuses a workflow that lists an undeclared name, naming the declared sets and saying where to declare it", () => {
         analyst();
         const why = refused({
            analyst: { instructions: INSTRUCTIONS, skillSets: ["nope"] },
         });
         expect(why).toContain("it lists the skill set 'nope'");
         expect(why).toContain("the top-level 'skillSets' does not declare");
         expect(why).toContain("declare it there with a 'path'");
         const withSets = read(
            { analyst: { instructions: INSTRUCTIONS, skillSets: ["nope"] } },
            { house: { path: MAIN }, extra: { path: SECOND } },
         );
         expectWarning(
            withSets.warnings,
            WORKFLOW_NOT_SERVED,
            "analyst",
            "(declared: 'house', 'extra')",
         );
      });

      it("refuses a workflow that lists a set by its folder path, the way an earlier release did", () => {
         analyst();
         expect(
            refused({
               analyst: { instructions: INSTRUCTIONS, skillSets: [MAIN] },
            }),
         ).toContain(`it lists the skill set '${MAIN}'`);
      });

      it("refuses a workflow that lists a name twice, and serves one that lists it once", () => {
         analyst();
         expect(refused({ analyst: listing(HOUSE, HOUSE) })).toContain(
            "'skillSets' lists 'house' twice",
         );
         expect(read({ analyst: listing(HOUSE) }).workflows.size).toBe(1);
      });

      it("refuses a workflow that lists more than eight skill sets, and serves eight", () => {
         const names = Array.from({ length: 9 }, (_, i) => `set${i}`);
         for (let i = 0; i < 9; i++) writeSkill(`sets/s${i}`, "k");
         const sets = Object.fromEntries(
            names.map((name, i) => [name, { path: `sets/s${i}` }]),
         );
         const nine = read({ analyst: listing(...names) }, sets);
         expect(nine.workflows.has("analyst")).toBe(false);
         expectWarning(
            nine.warnings,
            WORKFLOW_NOT_SERVED,
            "analyst",
            "'skillSets' lists 9 skill sets, over the limit of 8",
         );
         const eight = read({ analyst: listing(...names.slice(0, 8)) }, sets);
         expect(eight.workflows.get("analyst")!.sets).toHaveLength(8);
      });

      it("refuses a set declared at the same path as another, and both of them", () => {
         analyst();
         writeSkill(SECOND, "x");
         const result = read(
            { analyst: listing(HOUSE) },
            {
               house: { path: MAIN },
               twin: { path: `./${MAIN}//` },
               other: { path: SECOND },
            },
         );
         expect([...result.sets.keys()]).toEqual(["other"]);
         for (const [name, twin] of [
            [HOUSE, "twin"],
            ["twin", HOUSE],
         ]) {
            expect(
               expectWarning(
                  result.warnings,
                  SKILL_SET_NOT_SERVED,
                  name,
                  `its path '${MAIN}' is also the path of '${twin}'`,
               ).message,
            ).toContain("two sets cannot read one folder");
         }
         // The workflow that lists one of them is refused for it.
         expectWarning(
            result.warnings,
            WORKFLOW_NOT_SERVED,
            "analyst",
            "it lists the skill set 'house', which is not served",
         );
      });

      it("serves a set whose folder name is not a valid name, and one of any name the manifest gives", () => {
         writeSkill("sets/Bad_Name", "k");
         const result = read(undefined, { fine: { path: "sets/Bad_Name" } });
         expect(texts(result.warnings)).toEqual([]);
         expect(result.sets.get("fine")!.path).toBe("sets/Bad_Name");
      });

      it("lets two sets share a folder name, and serves each under its declared name", () => {
         writeSkill("a/main", "one");
         writeSkill("b/main", "two");
         const result = read(
            {
               one: listing("first"),
               two: listing("second"),
            },
            { first: { path: "a/main" }, second: { path: "b/main" } },
         );
         expect(texts(result.warnings)).toEqual([]);
         expect([...result.sets.keys()]).toEqual(["first", "second"]);
      });

      it("serves a set listed by two workflows once, under its declared name", () => {
         analyst();
         const result = read({ one: listing(HOUSE), two: listing(HOUSE) });
         expect([...result.sets.keys()]).toEqual([HOUSE]);
         expect(result.workflows.get("one")!.sets[0]!.set).toBe(
            result.workflows.get("two")!.sets[0]!.set,
         );
         expect(result.workflows.get("one")!.sets[0]!.set.name).toBe(HOUSE);
      });

      it("refuses a set whose path is a link out of the package, the same whether or not the outside directory exists", () => {
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
               analyst: { instructions: INSTRUCTIONS, skillSets: [HOUSE] },
            };
            const result = read(workflows, { house: { path: LINKED } });
            if (linkTextKeptVerbatim()) {
               expect(result.workflows.has("analyst")).toBe(false);
               answers.push(
                  expectWarning(
                     result.warnings,
                     SKILL_SET_NOT_SERVED,
                     HOUSE,
                     "resolves outside the package",
                  ).message,
               );
            } else {
               // Normalized when made (Windows), the link leads to the
               // workflow's own real/ inside the package and is served.
               expect(result.workflows.has("analyst")).toBe(true);
               answers.push(JSON.stringify(texts(result.warnings)));
            }
         }
         expect(answers[0]).toBe(answers[1]);
         if (!linkTextKeptVerbatim()) {
            expect(answers[0]).not.toContain("resolves outside the package");
         }
      });

      it("refuses a set whose directory is a link out of the package", () => {
         analyst();
         fs.mkdirSync(path.join(outside, "s"));
         writeSkill(outside, "s");
         fs.rmSync(path.join(pkg, MAIN), { recursive: true });
         fs.symlinkSync(outside, path.join(pkg, MAIN));
         expect(refusedSet({ house: { path: MAIN } })).toContain(
            "outside the package",
         );
      });

      it("does not say whether a publisher.json exists beyond a link out of the package", () => {
         analyst();
         fs.writeFileSync(path.join(outside, "publisher.json"), "{}");
         fs.rmSync(path.join(pkg, MAIN), { recursive: true });
         fs.symlinkSync(outside, path.join(pkg, MAIN));
         const why = refusedSet({ house: { path: MAIN } });
         expect(why).toContain("outside the package");
         expect(why).not.toContain("nested");
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
         expectWarning(
            result.warnings,
            SKILL_FINDING,
            HOUSE,
            `Package skill '${MAIN}/linked/SKILL.md' resolves outside the package through a link, so it is not served.`,
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

      it("refuses a set with a nested publisher.json at its root, and serves it once that is gone", () => {
         const entry = analyst();
         write(`${MAIN}/publisher.json`, "{}");
         expect(refusedSet({ house: { path: MAIN } })).toContain(
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
            analyst();
            writeSkill(MAIN, "bad");
            write(`${MAIN}/bad/${inside}`, "{}");
            const result = read(
               { first: listing("default"), second: listing("default") },
               { default: { path: MAIN } },
            );
            expect([...result.workflows.keys()]).toEqual(["first", "second"]);
            expect(advertisedSkills(result).map((s) => s.name)).toEqual([
               "conventions",
            ]);
            expect(
               viewOf(result, 0, "first").skills.map(
                  (s) => s.relative_filepath,
               ),
            ).not.toContainEqual(expect.stringContaining("bad/"));
            expect(texts(result.warnings)).toHaveLength(1);
            expectWarning(
               result.warnings,
               SKILL_FINDING,
               "default",
               `Package skill '${MAIN}/bad' is not served: '${MAIN}/bad/${inside}' is a publisher.json, which would make its directory a second package root; remove it or rename it.`,
            );
         },
      );

      it("refuses a set that sits under a nested publisher.json, whatever the depth", () => {
         analyst();
         for (const above of ["workflows/analyst", "workflows"]) {
            write(`${above}/publisher.json`, "{}");
            expect(refusedSet({ house: { path: MAIN } })).toContain(
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
         expect(texts(result.warnings)).toHaveLength(1);
         expectWarning(
            result.warnings,
            SKILL_FINDING,
            HOUSE,
            `Package skill '${MAIN}/linked' is not served: '${MAIN}/linked/publisher.json' is a publisher.json`,
         );
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

      it("publishes a set's finding once, however many workflows list it, served or refused", () => {
         writeSkill(MAIN, "conventions");
         write(`${MAIN}/conventions/scripts/a..b.py`, "x = 1");
         const result = read({
            one: listing(HOUSE),
            two: listing(HOUSE),
            three: { description: "", ...listing(HOUSE) },
         });
         expect([...result.workflows.keys()]).toEqual(["one", "two"]);
         expect(
            texts(result.warnings).filter((w) =>
               w.startsWith(`Skill set '${HOUSE}' skill finding:`),
            ),
         ).toHaveLength(1);
      });

      it("watches every set path and instructions file the manifest declares, refused or not, so fixing it recompiles", () => {
         write(INSTRUCTIONS, "i");
         const result = read(
            {
               analyst: {
                  instructions: INSTRUCTIONS,
                  skillSets: [HOUSE],
                  surprise: 1,
               },
               broken: {
                  instructions: "workflows/broken/instructions.md",
                  skillSets: ["nope"],
               },
               junk: { skillSets: "not a list" },
            },
            {
               house: { path: MAIN },
               broken: {
                  path: "workflows/broken/skill_sets/main",
                  surprise: 1,
               },
               outside: { path: "../out" },
               absolute: { path: "/abs" },
            },
         );
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
         write("brief.md", "brief");
         const sets = {
            alpha: { path: "sets/a/b/c" },
            delta: { path: "sets/d/e/f" },
         };
         const workflows = (n: number, listed: string[]) =>
            Object.fromEntries(
               Array.from({ length: n }, (_, i) => [
                  `w${i}`,
                  { instructions: "brief.md", skillSets: listed },
               ]),
            );
         const all = [
            "readdirSync",
            "statSync",
            "lstatSync",
            "realpathSync",
         ] as const;
         // The same fifty workflows, each reading its instructions, with and without the sets they list.
         const without = calls([...all], () => read(workflows(50, []), sets));
         const listed = calls([...all], () =>
            read(workflows(50, ["alpha", "delta"]), sets),
         );
         expect(listed).toBe(without);
         expect(without).toBeGreaterThan(0);
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
         expect(result.contentShas.size).toBe(0);
         expect(texts(result.warnings)).toEqual([]);
         expect(result.skillsDirNotRead).toBe(false);
      };

      /** Makes reading the set that holds the skill `extra` throw, as the walk of its digest would. */
      const failReadingExtra = () => {
         const real = revision.canonicalSha;
         boom = spyOn(revision, "canonicalSha").mockImplementation(
            (value: unknown) => {
               const skills = (value as { skills?: Array<{ name: string }> })
                  .skills;
               if (skills?.some((skill) => skill.name === "extra")) {
                  throw new Error("injected failure");
               }
               return real(value);
            },
         );
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
                     fine: listing(HOUSE),
                     broken: listing(EXTRA),
                  });
                  expectUnavailable(result, "EACCES");
                  expect(result.unavailable?.declaredSets).toEqual([
                     HOUSE,
                     EXTRA,
                  ]);
                  // Still watched, so fixing the folder reloads the package.
                  expect(result.watched).toEqual([INSTRUCTIONS, MAIN, SECOND]);
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
               expectUnavailable(read({ broken: listing(HOUSE) }), "EACCES");
            } finally {
               fs.chmodSync(above, 0o755);
            }
         },
      );

      it("is unavailable, naming the set, when reading it throws, and never throws itself", () => {
         analyst();
         writeSkill(SECOND, "extra");
         failReadingExtra();
         const result = read({
            fine: listing(HOUSE),
            broken: listing(EXTRA),
            both: listing(HOUSE, EXTRA),
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
         // Handed over as it is: boundedDeclaration would call it unreadable instead.
         const result = readPackageWorkflows(pkg, { workflows: hostile }, []);
         expect(result.unavailable).toEqual({
            reason: "injected walk failure",
            declared: [],
            declaredSets: [],
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
               expect(result.unavailable?.declaredSets).toEqual([HOUSE]);
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
                  result.sets.get(HOUSE)!.skills.map((s) => s.dirName),
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

      it("names only the workflows and sets that passed declaration, when what failed is a set read", () => {
         analyst();
         writeSkill(SECOND, "extra");
         failReadingExtra();
         const result = read(
            {
               refused: { surprise: 1 },
               fine: listing(HOUSE),
               broken: listing(EXTRA),
            },
            {
               house: { path: MAIN },
               extra: { path: SECOND },
               "Not A Name": { path: MAIN },
            },
         );
         expect(result.unavailable?.declared).toEqual(["fine", "broken"]);
         expect(result.unavailable?.declaredSets).toEqual([HOUSE, EXTRA]);
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
         const result = readPackageWorkflows(
            path.join(pkg, "gone"),
            {
               skillSets: { house: { path: MAIN } },
               workflows: { analyst: { instructions: INSTRUCTIONS } },
            },
            [],
         );
         expect(result.unavailable?.declared).toEqual(["analyst"]);
         expect(result.unavailable?.declaredSets).toEqual([HOUSE]);
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
         "refuses a set and the workflow that lists it, never the whole walk, naming the cause, when its folder fails to be examined with %s",
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
               expect(texts(result.warnings)).toHaveLength(2);
               for (const [kind, subject] of [
                  [SKILL_SET_NOT_SERVED, HOUSE],
                  [WORKFLOW_NOT_SERVED, "analyst"],
               ]) {
                  const found = expectWarning(
                     result.warnings,
                     kind!,
                     subject,
                     `skill set directory '${MAIN}' ${cause}`,
                  );
                  expect(found.message).not.toContain("does not exist");
               }
            } finally {
               failing.mockRestore();
            }
         },
      );

      it("is not unavailable for what the author wrote wrong: that refuses the set and its workflow with a warning each", () => {
         const result = read(
            { broken: listing(HOUSE) },
            { house: { path: "nowhere" } },
         );
         expect(result.unavailable).toBeUndefined();
         expect(texts(result.warnings)).toHaveLength(2);
         expectWarning(
            result.warnings,
            SKILL_SET_NOT_SERVED,
            HOUSE,
            "does not exist",
         );
      });
   });

   describe("a set no workflow lists", () => {
      const THIRD = "workflows/analyst/skill_sets/third";

      it("is read, watched and served like any other, whether no workflow lists it or only a refused one does", () => {
         analyst();
         writeSkill(THIRD, "stray");
         const result = read(
            {
               fine: listing(HOUSE),
               broken: { ...listing("third"), "not-a-key": 1 },
            },
            {
               house: { path: MAIN },
               third: { path: THIRD },
               lone: { path: SECOND },
            },
         );
         writeSkill(SECOND, "lonely");
         const again = read(
            { fine: listing(HOUSE) },
            {
               house: { path: MAIN },
               third: { path: THIRD },
               lone: { path: SECOND },
            },
         );
         expect([...result.workflows.keys()]).toEqual(["fine"]);
         expect([...again.sets.keys()]).toEqual([HOUSE, "lone", "third"]);
         expect(texts(again.warnings)).toEqual([]);
         expect(
            again.sets.get("third")!.skills.map((skill) => skill.dirName),
         ).toEqual(["stray"]);
         expect(again.watched).toContain(THIRD);
         expect(again.watched).toContain(SECOND);
         expect(again.contentShas.has(`${THIRD}/stray/SKILL.md`)).toBe(true);
         expect(again.contentShas.has(`${SECOND}/lonely/SKILL.md`)).toBe(true);
         // The one only a refused workflow lists is served too.
         expect(result.sets.has("third")).toBe(true);
         expectWarning(
            result.warnings,
            WORKFLOW_NOT_SERVED,
            "broken",
            "it sets 'not-a-key'",
         );
      });
   });

   describe("a skill in a set", () => {
      it("is served from the first set that holds its name, with a warning on the workflow", () => {
         const entry = analyst();
         writeSkill(SECOND, "conventions", "second copy");
         writeSkill(SECOND, "extra");
         const result = read({
            analyst: { ...entry, skillSets: [HOUSE, EXTRA] },
         });
         const workflow = result.workflows.get("analyst")!;
         expect(workflow.shadowing).toEqual([
            {
               code: SKILL_SHADOWED,
               subject: "analyst",
               severity: "warn",
               message: `Workflow 'analyst' lists skill 'conventions' in both '${HOUSE}' and '${EXTRA}'; the copy in '${HOUSE}', listed first, is the one served. Fix: rename one of the two skills, or list only one of the sets.`,
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
         expect(result.sets.get(EXTRA)!.skills.map((s) => s.dirName)).toEqual([
            "conventions",
            "extra",
         ]);
      });

      it("keeps its own set per file path: sets are listed in the order the workflow lists them", () => {
         const entry = analyst();
         writeSkill(SECOND, "aaa");
         const result = read({
            analyst: { ...entry, skillSets: [EXTRA, HOUSE] },
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
               `Skill set '${HOUSE}' skill finding: Package skill '${MAIN}/empty' has no SKILL.md, so it is not served. Fix: add ${MAIN}/empty/SKILL.md, or remove the directory.`,
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
            analyst: { instructions: INSTRUCTIONS, skillSets: [HOUSE] },
         });
         expect(result.workflows.has("analyst")).toBe(true);
         expect(texts(result.warnings)).toEqual([
            `Skill set '${HOUSE}' skill finding: the skill set holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry.`,
         ]);
      });

      it("reads the frontmatter of a SKILL.md that starts with a byte-order mark, and serves it as written", () => {
         const raw =
            "\uFEFF---\r\nname: bom\r\ndescription: d\r\n---\r\n\r\nbody\r\n";
         write(`${MAIN}/bom/SKILL.md`, raw);
         const result = read({ analyst: listing(HOUSE) });
         expect(texts(result.warnings)).toEqual([]);
         const served = viewOf(result).skills.find(
            (f) => f.relative_filepath === "bom/SKILL.md",
         )!;
         expect(served.file_contents).toBe(raw);
         expect(result.sets.get(HOUSE)!.skills[0]!.meta).toEqual({
            name: "bom",
            description: "d",
         });
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
               `Skill set '${HOUSE}' skill finding: Package skill file '${CONVENTIONS}/${rel}' ${why}${why.endsWith("outside") ? " [A-Za-z0-9_][A-Za-z0-9._-]*" : ""}, so it is not served.`,
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

      it("serves a set's inline files up to the cap, its SKILL.md files counted, and skips the rest naming it", () => {
         // Two skills, so no one references/ folder reaches its own 1000-file walk cap.
         const each = Math.ceil(SKILL_SET_INLINE_MAX_FILES / 2) + 2;
         const past = 2 + 2 * each - SKILL_SET_INLINE_MAX_FILES;
         for (const skill of ["aaa", "bbb"]) {
            writeSkill(MAIN, skill);
            for (let i = 0; i < each; i++) {
               write(
                  `${MAIN}/${skill}/references/r${String(i).padStart(5, "0")}.md`,
                  "#\n",
               );
            }
         }
         const result = read({ analyst: listing(HOUSE) });
         expect(viewOf(result).skills).toHaveLength(SKILL_SET_INLINE_MAX_FILES);
         const skipped = result.sets
            .get(HOUSE)!
            .findings.filter((f) => f.code === SKILL_FILE_SKIPPED);
         expect(skipped).toHaveLength(past);
         for (const found of skipped) {
            expect(found.message).toContain(
               `would take the skill set over its ${SKILL_SET_INLINE_MAX_FILES} inline Markdown files, so it is not served`,
            );
         }
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

      it("is warned of once, for SKILL.md, an inline reference and a listed text file, in the default set", () => {
         writeLong();
         const result = read(
            { analyst: { ...analyst(), skillSets: [DEFAULT] } },
            { default: { path: MAIN } },
         );
         const findings = texts(result.sets.get(DEFAULT)!.findings);
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

      it("is warned of for the default set though no workflow lists it", () => {
         writeLong();
         analyst();
         const result = read(
            { analyst: { instructions: INSTRUCTIONS } },
            { default: { path: MAIN } },
         );
         expect(
            texts(result.sets.get(DEFAULT)!.findings).filter((f) =>
               f.includes(warning("assets/long.txt")),
            ),
         ).toHaveLength(1);
      });

      it("is not warned of for a set that is not the default, which get_skill never serves", () => {
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
            analyst: { ...entry, skillSets: [HOUSE, EXTRA] },
            other: {
               instructions: "workflows/other/instructions.md",
               skillSets: [EXTRA],
            },
            ...extra,
         };
      };
      const digests = (result: PackageWorkflows) =>
         Object.fromEntries(
            [...result.sets].map(([name, set]) => [name, set.digest]),
         );
      const definition = (result: PackageWorkflows, name = "analyst") =>
         result.workflows.get(name)!.definitionSha;
      const houseDigest = (
         entry: unknown,
         skillSets: unknown = { house: { path: MAIN } },
         name = HOUSE,
      ) => read({ analyst: entry }, skillSets).sets.get(name)!.digest;

      /** The `files` map the digest of a set is taken over for its skill `skill`, as hashed. */
      const hashedFiles = (workflows: unknown, skill: string) => {
         const real = revision.canonicalSha;
         const payloads: Array<{
            skills?: Array<{ name: string; files: Record<string, string> }>;
         }> = [];
         const spy = spyOn(revision, "canonicalSha").mockImplementation(
            (value: unknown) => {
               payloads.push(value as (typeof payloads)[number]);
               return real(value);
            },
         );
         try {
            read(workflows);
            return payloads
               .flatMap((p) => p.skills ?? [])
               .find((s) => s.name === skill)!.files;
         } finally {
            spy.mockRestore();
         }
      };

      it("hashes every file a set serves as written, a SKILL.md with the keys its frontmatter carries included", () => {
         const entry = analyst();
         const files = hashedFiles({ analyst: entry }, "conventions");
         const raw = fs.readFileSync(
            path.join(pkg, CONVENTIONS, "SKILL.md"),
            "utf8",
         );
         expect(raw).toContain("allowed-tools");
         expect(files["SKILL.md"]).toBe(`sha256:${sha(raw)}`);
         expect(files["reference/glossary.md"]).toBe(
            `sha256:${sha("# Glossary\n\nnet = after refunds\n")}`,
         );
      });

      it("moves for an edit to any byte a set serves, one to a frontmatter key only the author reads included", () => {
         const entry = analyst();
         const before = houseDigest(entry);
         write(
            `${CONVENTIONS}/SKILL.md`,
            "---\nname: conventions\ndescription: House rules.\nallowed-tools: Read\nx-owner: me\n---\n\nUse net revenue.\n",
         );
         const afterTools = houseDigest(entry);
         expect(afterTools).not.toBe(before);
         write(
            `${CONVENTIONS}/SKILL.md`,
            "---\nname: conventions\ndescription: House rules.\nallowed-tools: Read\nx-owner: you\n---\n\nUse net revenue.\n",
         );
         expect(houseDigest(entry)).not.toBe(afterTools);
         write(
            `${CONVENTIONS}/SKILL.md`,
            "---\nname: conventions\ndescription: House rules.\nallowed-tools: Read\nx-owner: you\n---\n\nUse gross revenue.\n",
         );
         expect(houseDigest(entry)).not.toBe(afterTools);
      });

      it("does not cover a set's name, path or description", () => {
         const entry = analyst();
         const base = houseDigest(entry);
         fs.cpSync(path.join(pkg, MAIN), path.join(pkg, SECOND), {
            recursive: true,
         });
         expect(
            houseDigest(
               entry,
               { renamed: { path: SECOND, description: "described" } },
               "renamed",
            ),
         ).toBe(base);
      });

      it("moves with the set's overrides, whatever order they were written in", () => {
         const entry = analyst();
         const none = houseDigest(entry);
         const one = houseDigest(entry, {
            house: { path: MAIN, overrides: ["a"] },
         });
         expect(one).not.toBe(none);
         const sorted = houseDigest(entry, {
            house: { path: MAIN, overrides: ["a", "b"] },
         });
         expect(sorted).not.toBe(one);
         expect(
            houseDigest(entry, {
               house: { path: MAIN, overrides: ["b", "a"] },
            }),
         ).toBe(sorted);
         // A bad value is read as empty.
         expect(
            houseDigest(entry, {
               house: { path: MAIN, overrides: ["a", "a"] },
            }),
         ).toBe(none);
      });

      it("moves with a .skillignore, which changes what the set serves", () => {
         const entry = analyst();
         const before = houseDigest(entry);
         write(`${CONVENTIONS}/.skillignore`, "reference/\n");
         expect(houseDigest(entry)).not.toBe(before);
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

      it("moves definitionSha with the instructions text, not the path it was read from", () => {
         const entry = analyst();
         write("workflows/analyst/other.md", "Be careful with revenue.");
         const base = definition(read({ analyst: entry }));
         expect(
            definition(
               read({
                  analyst: {
                     ...entry,
                     instructions: "workflows/analyst/other.md",
                  },
               }),
            ),
         ).toBe(base);
         write("workflows/analyst/other.md", "Be careful with margin.");
         expect(
            definition(
               read({
                  analyst: {
                     ...entry,
                     instructions: "workflows/analyst/other.md",
                  },
               }),
            ),
         ).not.toBe(base);
      });

      it("moves definitionSha with a listed set's name, though the set's own digest stays", () => {
         const entry = analyst();
         const named = read({ analyst: entry });
         const renamed = read(
            { analyst: { ...entry, skillSets: ["renamed"] } },
            { renamed: { path: MAIN } },
         );
         expect(renamed.sets.get("renamed")!.digest).toBe(
            named.sets.get(HOUSE)!.digest,
         );
         expect(definition(renamed)).not.toBe(definition(named));
      });

      it("moves a set's digest on an edit to a file it serves, and no other set's", () => {
         const workflows = twoWorkflows();
         const before = digests(read(workflows));
         write(`${SECOND}/extra/notes.txt`, "added");
         const after = digests(read(workflows));
         expect(after[HOUSE]).toBe(before[HOUSE]);
         expect(after[EXTRA]).not.toBe(before[EXTRA]);
      });

      it("does not move a set's digest for an edit to a workflow's instructions or to its other sets", () => {
         const workflows = twoWorkflows();
         const before = read(workflows);
         write(INSTRUCTIONS, "edited brief");
         write("workflows/other/instructions.md", "edited other brief");
         writeSkill(SECOND, "extra", "edited");
         const after = read(workflows);
         expect(after.sets.get(HOUSE)!.digest).toBe(
            before.sets.get(HOUSE)!.digest,
         );
         // The other set did change, and so did the workflow that lists it.
         expect(after.sets.get(EXTRA)!.digest).not.toBe(
            before.sets.get(EXTRA)!.digest,
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
            analyst: { ...workflows.analyst, skillSets: [EXTRA, HOUSE] },
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
         const digest = () => read({ analyst: entry }).sets.get(HOUSE)!.digest;
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
      it("are the default set's, whichever workflows list it", async () => {
         analyst();
         writeSkill(MAIN, "extra");
         const result = read(undefined, { default: { path: MAIN } });
         expect(advertisedSkills(result).map((s) => s.name)).toEqual([
            "conventions",
            "extra",
         ]);
         expect(await advertisedSkills(result)[0]!.read("SKILL.md")).toEqual({
            text: expect.stringContaining("Use net revenue."),
         });
      });

      it("serve a SKILL.md as it was written, frontmatter and all", async () => {
         analyst();
         const result = read(undefined, { default: { path: MAIN } });
         const written = fs.readFileSync(
            path.join(pkg, CONVENTIONS, "SKILL.md"),
            "utf8",
         );
         expect(await advertisedSkills(result)[0]!.read("SKILL.md")).toEqual({
            text: written,
         });
      });

      it("are none when no set is named default, whatever else the package declares", () => {
         const result = read({ analyst: analyst() });
         expect(advertisedSkills(result)).toEqual([]);
         expect(advertisedSkills(emptyWorkflows())).toEqual([]);
      });

      it("are none for a default set that is refused, or a workflow named default", () => {
         writeSkill(MAIN, "one");
         expect(
            advertisedSkills(read(undefined, { default: { path: "nope" } })),
         ).toEqual([]);
         expect(advertisedSkills(read({ default: listing(HOUSE) }))).toEqual(
            [],
         );
      });

      it("shadow a bundled skill of the same name through resolveSkills", () => {
         writeSkill(MAIN, "malloy-analysis", "OUR ANALYSIS");
         const resolved = resolveSkills(
            [],
            advertisedSkills(read(undefined, { default: { path: MAIN } })),
         );
         expect(resolved.map((s) => [s.name, s.origin])).toEqual([
            ["malloy-analysis", "package"],
         ]);
      });
   });

   describe("a bad overrides value", () => {
      const withOverrides = (overrides: unknown, workflows?: unknown) =>
         read(workflows ?? { analyst: analyst() }, {
            house: { path: MAIN, overrides },
         });

      it.each([
         ["a value that is not a list", { x: 1 }],
         ["65 names", Array.from({ length: 65 }, (_, i) => `skill-${i}`)],
         ["an entry that is not a skill name", ["Bad_Name"]],
         ["a name listed twice", ["a", "a"]],
      ])(
         "is read as empty, with one finding, and the set is served: %s",
         (_label, overrides) => {
            const none = read(
               { analyst: analyst() },
               { house: { path: MAIN } },
            );
            const result = withOverrides(overrides);
            expect(result.sets.has(HOUSE)).toBe(true);
            expect(result.workflows.has("analyst")).toBe(true);
            expect(result.sets.get(HOUSE)!.overrides).toEqual([]);
            expect(result.sets.get(HOUSE)!.digest).toBe(
               none.sets.get(HOUSE)!.digest,
            );
            expectWarning(result.warnings, SKILL_FINDING, HOUSE, "'overrides'");
            expect(result.warnings).toHaveLength(1);
         },
      );

      it("says it once however many workflows list the set", () => {
         const result = withOverrides(["a", "a"], {
            analyst: analyst(),
            reviewer: listing(HOUSE),
         });
         expect([...result.workflows.keys()]).toEqual(["analyst", "reviewer"]);
         expectWarning(result.warnings, SKILL_FINDING, HOUSE, "'overrides'");
         expect(result.warnings).toHaveLength(1);
      });
   });

   describe("the shas the source content sha covers", () => {
      const walked = () => {
         write(`${CONVENTIONS}/scripts/check.py`, "print('check')\n");
         write(`${CONVENTIONS}/.skillignore`, "scratch/\n");
         return read({ analyst: analyst() });
      };
      const SERVED = [
         `${CONVENTIONS}/SKILL.md`,
         `${CONVENTIONS}/reference/glossary.md`,
         `${CONVENTIONS}/scripts/check.py`,
         `${CONVENTIONS}/.skillignore`,
         INSTRUCTIONS,
      ];

      it("holds a sha for each inline file, listed file, applied .skillignore and instructions file the walk serves", () => {
         const result = walked();
         expect(texts(result.warnings)).toEqual([]);
         const shas = servedContentShas(result);
         for (const served of SERVED) expect(shas.has(served)).toBe(true);
      });

      it.each(SERVED)(
         "refuses a walk that serves %s without a sha, rather than hash it as absent",
         (served) => {
            const result = walked();
            result.contentShas.delete(served);
            expect(() => servedContentShas(result)).toThrow(`'${served}'`);
         },
      );
   });

   describe("a skill named like a bundled skill", () => {
      const BUNDLED = ["malloy-analysis"];

      it("is served, with a finding naming how to say it is meant to replace the bundled one", () => {
         writeSkill(MAIN, "malloy-analysis");
         const result = read(undefined, { house: { path: MAIN } }, BUNDLED);
         expect(result.sets.get(HOUSE)!.skills.map((s) => s.dirName)).toEqual([
            "malloy-analysis",
         ]);
         expect(texts(result.warnings)).toHaveLength(1);
         expectWarning(
            result.warnings,
            SKILL_FINDING,
            HOUSE,
            `Package skill '${MAIN}/malloy-analysis' is named like a bundled skill. Fix: add 'malloy-analysis' to 'skillSets.house.overrides' if it is meant to replace the bundled one, or rename the skill`,
         );
      });

      it("is quiet when the set's overrides name it, and for a skill no bundled one shares its name with", () => {
         writeSkill(MAIN, "malloy-analysis");
         writeSkill(MAIN, "ours");
         const result = read(
            undefined,
            { house: { path: MAIN, overrides: ["malloy-analysis"] } },
            BUNDLED,
         );
         expect(texts(result.warnings)).toEqual([]);
         expect(result.sets.get(HOUSE)!.skills).toHaveLength(2);
      });

      it("is not told of without the bundled names, which only the main thread knows", () => {
         writeSkill(MAIN, "malloy-analysis");
         expect(
            texts(read(undefined, { house: { path: MAIN } }).warnings),
         ).toEqual([]);
      });
   });

   describe("the root skills/ directory", () => {
      const skillsDirNotRead = (skillSets: unknown, workflows?: unknown) =>
         read(workflows, skillSets).skillsDirNotRead;

      it("is flagged when it exists and no set is declared at or under it", () => {
         writeSkill("skills", "old");
         expect(skillsDirNotRead(undefined)).toBe(true);
         expect(skillsDirNotRead(undefined, { analyst: analyst() })).toBe(true);
      });

      it("is not flagged when a set is declared at it, or under it", () => {
         writeSkill("skills", "old");
         expect(skillsDirNotRead({ default: { path: "skills" } })).toBe(false);
         writeSkill("skills/sub", "new");
         expect(skillsDirNotRead({ nested: { path: "skills/sub" } })).toBe(
            false,
         );
         expect(skillsDirNotRead({ legacy: { path: "./skills//" } })).toBe(
            false,
         );
      });

      it("is not flagged when the set declared at it is refused, since the refusal says so", () => {
         writeSkill("skills", "old");
         const result = read(undefined, {
            default: { path: "skills", surprise: 1 },
         });
         expect(result.skillsDirNotRead).toBe(false);
         expectWarning(
            result.warnings,
            SKILL_SET_NOT_SERVED,
            DEFAULT,
            "it sets 'surprise'",
         );
      });

      it("is not flagged when there is none", () => {
         expect(skillsDirNotRead(undefined)).toBe(false);
         expect(skillsDirNotRead(undefined, { analyst: analyst() })).toBe(
            false,
         );
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
      /** The one finding of the default set that mentions `needle`. */
      const findingAbout = (needle: string) => {
         analyst();
         const found = read(undefined, { default: { path: MAIN } })
            .sets.get(DEFAULT)!
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
            subject: DEFAULT,
            severity: "warn",
         });
         expect(findingAbout(needle).message).toStartWith(
            `Skill set '${DEFAULT}' skill finding: `,
         );
      });

      it("makes a set with no skills, and one past the skill cap, findings of the set", () => {
         write(INSTRUCTIONS, "i");
         fs.mkdirSync(path.join(pkg, SECOND), { recursive: true });
         for (let i = 0; i <= SKILLS_PER_SET_MAX; i++) {
            writeSkill(MAIN, `s${String(i).padStart(3, "0")}`);
         }
         const result = read({
            analyst: { instructions: INSTRUCTIONS, skillSets: [HOUSE, EXTRA] },
         });
         expect(result.sets.get(EXTRA)!.findings).toEqual([
            {
               code: SKILL_FINDING,
               subject: EXTRA,
               severity: "warn",
               message: `Skill set '${EXTRA}' skill finding: the skill set holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry.`,
            },
         ]);
         expect(result.sets.get(HOUSE)!.findings).toMatchObject([
            { code: SKILL_FINDING, subject: HOUSE },
         ]);
         expect(result.sets.get(HOUSE)!.findings[0]!.message).toContain(
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
            subject: DEFAULT,
         });
      });

      it("keeps each set's findings on the workflow that reads it whole, beside its shadowing findings", () => {
         writeSkill(SECOND, "conventions", "second copy");
         write(`${SECOND}/conventions/scripts/CLAUDE.md`, "x");
         const result = read({
            analyst: { ...analyst(), skillSets: [HOUSE, EXTRA] },
         });
         const workflow = result.workflows.get("analyst")!;
         expect(workflow.shadowing.map((w) => [w.code, w.subject])).toEqual([
            [SKILL_SHADOWED, "analyst"],
         ]);
         expect(result.warnings.map((w) => [w.code, w.subject]).sort()).toEqual(
            [
               [SKILL_SHADOWED, "analyst"],
               [SKILL_FILE_SKIPPED, EXTRA],
            ].sort(),
         );
      });
   });

   describe("a folder that is a skill, not a set", () => {
      const SKILL_DIR = "sets/margin";
      const SET = "skillish";

      const listedAsSet = (setPath: string) => {
         write(
            `${setPath}/SKILL.md`,
            "---\nname: margin\ndescription: d\n---\n",
         );
         write(`${setPath}/references/a.md`, "# A\n");
         write(`${setPath}/scripts/run.py`, "print()\n");
         write(INSTRUCTIONS, "i");
         return read(
            { analyst: { instructions: INSTRUCTIONS, skillSets: [SET] } },
            { [SET]: { path: setPath } },
         );
      };

      it("says it is a skill and names its parent folder, with no finding for each of its folders", () => {
         const result = listedAsSet(SKILL_DIR);
         expect(result.workflows.has("analyst")).toBe(true);
         expect(result.sets.get(SET)!.skills).toEqual([]);
         expect(result.sets.get(SET)!.findings).toEqual([
            {
               code: SKILL_FINDING,
               subject: SET,
               severity: "warn",
               message: `Skill set '${SET}' skill finding: this is a skill, not a skill set (it holds a SKILL.md), so none of it is served. Fix: list its parent folder 'sets', or, if its subfolders are the skills, remove its SKILL.md.`,
            },
         ]);
      });

      it("asks for a folder of its own when the skill sits at the package root's top level", () => {
         const result = listedAsSet("margin");
         expect(result.sets.get(SET)!.findings[0]!.message).toContain(
            "Fix: move it into a folder and list that folder, or, if its subfolders are the skills, remove its SKILL.md.",
         );
      });
   });

   describe("a declaration too large to read", () => {
      const message =
         "Package manifest 'skillSets' and 'workflows' are too deeply nested or over 1 MiB together, so no skill set or workflow is served. Fix: shorten them; a real declaration is a few KiB.";

      it.each([
         [
            "workflows alone",
            { workflows: { big: "x".repeat(MANIFEST_DECLARATION_MAX_BYTES) } },
         ],
         [
            "skillSets alone",
            { skillSets: { big: "x".repeat(MANIFEST_DECLARATION_MAX_BYTES) } },
         ],
         [
            "the two together, each under the cap",
            {
               skillSets: { a: "x".repeat(MANIFEST_DECLARATION_MAX_BYTES / 2) },
               workflows: { b: "x".repeat(MANIFEST_DECLARATION_MAX_BYTES / 2) },
            },
         ],
      ])(
         "says it is too large, not that it must be an object, for %s",
         (_label, declared) => {
            const result = readPackageWorkflows(
               pkg,
               boundedDeclaration(declared).value,
               [],
            );
            expect(result.workflows.size).toBe(0);
            expect(result.sets.size).toBe(0);
            expect(result.warnings).toEqual([{ severity: "warn", message }]);
            expect(result.declarationRefusal).toBe(message);
         },
      );
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
            `Skill set '${HOUSE}' skill finding: Package skill '${CONVENTIONS}' is not served: `,
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
         expect([...result.contentShas.keys()]).toContain(
            `${ASSETS}/css/app.css`,
         );
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
               .get(HOUSE)!
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
               const findings = texts(result.sets.get(HOUSE)!.findings).join(
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
                  texts(result.sets.get(HOUSE)!.findings).join("\n"),
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
         // Named so that charging by folder would cut another set than charging by name does.
         const SETS = {
            gamma: { path: "sets/a" },
            beta: { path: "sets/b" },
            alpha: { path: "sets/c" },
         };
         const files = (result: PackageWorkflows, name: string) =>
            result.sets.get(name)!.skills.flatMap((s) => s.listed);
         const writeThree = () => {
            writeAssets("sets/a", 4);
            writeAssets("sets/b", 4);
            writeAssets("sets/c", 4);
         };

         it("charges each set once, in name order, whichever workflow lists it, and refuses the one that crosses the cap whole", () => {
            writeThree();
            const result = read(
               {
                  // Listed out of order, and no workflow lists beta, which is charged all the same.
                  one: listing("gamma"),
                  two: listing("gamma", "alpha"),
                  three: listing("alpha"),
               },
               SETS,
            );
            expect(files(result, "alpha")).toHaveLength(4);
            expect(files(result, "beta")).toHaveLength(4);
            expect([...result.sets.keys()]).toEqual(["alpha", "beta"]);
            expectWarning(
               result.warnings,
               SKILL_SET_NOT_SERVED,
               "gamma",
               "8 MiB per-package cap",
            );
            // Every workflow that lists the refused set is refused with it; the rest are whole.
            for (const name of ["one", "two"]) {
               expectWarning(
                  result.warnings,
                  WORKFLOW_NOT_SERVED,
                  name,
                  "it lists the skill set 'gamma', which is not served",
               );
            }
            expect([...result.workflows.keys()]).toEqual(["three"]);
         });

         it("moves no set's digest when workflows are reordered at the cap, and renaming the sets moves which folder is cut", () => {
            writeThree();
            const digests = (result: PackageWorkflows) =>
               Object.fromEntries(
                  [...result.sets].map(([name, set]) => [name, set.digest]),
               );
            const forward = read(
               {
                  a: listing("gamma"),
                  b: listing("beta"),
                  c: listing("alpha"),
               },
               SETS,
            );
            const backward = read(
               {
                  c: listing("alpha"),
                  b: listing("beta"),
                  a: listing("gamma"),
               },
               SETS,
            );
            expect(forward.sets.has("gamma")).toBe(false);
            expect(digests(backward)).toEqual(digests(forward));
            const swapped = read(
               { c: listing("alpha", "beta", "gamma") },
               {
                  alpha: { path: "sets/a" },
                  beta: { path: "sets/b" },
                  gamma: { path: "sets/c" },
               },
            );
            expect([...forward.sets.values()].map((set) => set.path)).toEqual([
               "sets/c",
               "sets/b",
            ]);
            expect([...swapped.sets.values()].map((set) => set.path)).toEqual([
               "sets/a",
               "sets/b",
            ]);
            // The folder that was served under another name is the one cut now.
            expectWarning(
               forward.warnings,
               SKILL_SET_NOT_SERVED,
               "gamma",
               "8 MiB per-package cap",
            );
            expectWarning(
               swapped.warnings,
               SKILL_SET_NOT_SERVED,
               "gamma",
               "8 MiB per-package cap",
            );
         });

         it("does not read or charge a set that its own declaration refuses", () => {
            writeThree();
            const result = read(
               { w: listing("alpha", "beta", "gamma") },
               {
                  alpha: { path: "sets/c", surprise: true },
                  beta: { path: "sets/b" },
                  gamma: { path: "sets/a" },
               },
            );
            expect(result.workflows.has("w")).toBe(false);
            expect(result.sets.has("alpha")).toBe(false);
            expect(files(result, "beta")).toHaveLength(4);
            expect(files(result, "gamma")).toHaveLength(4);
         });
      });
   });

   describe("a set's bounds", () => {
      const AT = "sets/many";
      const SET = "crowd";
      const crowd = () => read(undefined, { [SET]: { path: AT } });

      it("serves the first skills by name up to the cap, and names the rest once", () => {
         for (let i = 0; i < SKILLS_PER_SET_MAX + 3; i++) {
            writeSkill(AT, `s${String(i).padStart(3, "0")}`);
         }
         const result = crowd();
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
            writeSkill(AT, `s${String(i).padStart(3, "0")}`);
         }
         const result = crowd();
         expect(result.sets.get(SET)!.skills).toHaveLength(SKILLS_PER_SET_MAX);
         expect(texts(result.warnings)).toEqual([]);
      });

      it("names at most the findings cap of a set's skill findings, and counts the rest", () => {
         const extra = 5;
         for (let i = 0; i < SKILL_FINDINGS_MAX + extra; i++) {
            write(
               `${AT}/bad${String(i).padStart(2, "0")}/SKILL.md`,
               "no frontmatter\n",
            );
         }
         writeSkill(AT, "ok");
         const result = crowd();
         expect(texts(result.warnings)).toHaveLength(SKILL_FINDINGS_MAX + 1);
         expect(texts(result.warnings).at(-1)).toBe(
            `Skill set '${SET}' skill finding: ${extra} further skill findings are not named, for the same kinds of reasons.`,
         );
         // A skill with a finding is still served.
         expect(result.sets.get(SET)!.skills).toHaveLength(
            SKILL_FINDINGS_MAX + extra + 1,
         );
      });

      it("caps the skipped files and the other findings of a set separately, each count line carrying the code of what it elides", () => {
         const extra = 3;
         const pad = (i: number) => String(i).padStart(2, "0");
         for (let i = 0; i < SKILL_FINDINGS_MAX + extra; i++) {
            write(`${AT}/bad${pad(i)}/SKILL.md`, "no frontmatter\n");
         }
         writeSkill(AT, "ok");
         for (let i = 0; i < SKILL_FINDINGS_MAX + extra; i++) {
            write(`${AT}/ok/docs/d${pad(i)}/CLAUDE.md`, "x");
         }
         const findings = crowd().sets.get(SET)!.findings;
         const of = (code: string) => findings.filter((f) => f.code === code);
         expect(of(SKILL_FINDING)).toHaveLength(SKILL_FINDINGS_MAX + 1);
         expect(of(SKILL_FILE_SKIPPED)).toHaveLength(SKILL_FINDINGS_MAX + 1);
         expect(of(SKILL_FINDING).at(-1)!.message).toContain(
            `${extra} further skill findings are not named`,
         );
         expect(of(SKILL_FILE_SKIPPED).at(-1)!.message).toContain(
            `${extra} further skipped files are not named`,
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
         const result = read(undefined, {
            first: { path: "sets/a" },
            second: { path: "sets/b" },
         });
         expect(texts(result.warnings)).toHaveLength(
            2 * (SKILL_FINDINGS_MAX + 1),
         );
      });
   });

   describe("the shas a walk ships", () => {
      it("has one for every file it read, an instructions file and a .skillignore included, and none for a file it left out", () => {
         const declared = { analyst: analyst() };
         write(`${CONVENTIONS}/.skillignore`, "notes.txt\n");
         write(`${CONVENTIONS}/notes.txt`, "left out");
         write(`${CONVENTIONS}/assets/logo.bin`, Buffer.from([0, 255, 1]));
         const result = read(declared);
         expect([...result.contentShas.keys()].sort()).toEqual(
            [
               INSTRUCTIONS,
               `${CONVENTIONS}/.skillignore`,
               `${CONVENTIONS}/SKILL.md`,
               `${CONVENTIONS}/assets/logo.bin`,
               `${CONVENTIONS}/reference/glossary.md`,
            ].sort(),
         );
         expect(result.contentShas.get(INSTRUCTIONS)).toBe(
            sha("Be careful with revenue."),
         );
         expect(result.contentShas.get(`${CONVENTIONS}/.skillignore`)).toBe(
            sha("notes.txt\n"),
         );
         expect(result.contentShas.get(`${CONVENTIONS}/assets/logo.bin`)).toBe(
            sha(Buffer.from([0, 255, 1])),
         );
         expect(result.contentShas.get(`${CONVENTIONS}/SKILL.md`)).toBe(
            sha(fs.readFileSync(path.join(pkg, CONVENTIONS, "SKILL.md"))),
         );
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
         two: {
            description: "Two",
            instructions: INSTRUCTIONS,
            skillSets: [HOUSE],
         },
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
         expect([...adopted.sets.keys()]).toEqual([HOUSE]);
         expect(adopted.watched).toEqual(walked.watched);
         expect(adopted.warnings).toEqual(walked.warnings);
         expect(adopted.sets.get(HOUSE)!.digest).toBe(
            walked.sets.get(HOUSE)!.digest,
         );
         const files = adopted.sets.get(HOUSE)!.skills[0]!.catalog.files;
         for (const name of ["__proto__", "constructor", "toString"]) {
            expect(Object.hasOwn(files, name)).toBe(true);
         }
         expect(Object.hasOwn(files, "valueOf")).toBe(false);
         expect(files).toEqual(
            walked.sets.get(HOUSE)!.skills[0]!.catalog.files,
         );
      });

      it("ships no sha: those stay in the worker, which hashes them into the one it ships", () => {
         const walked = read(declared());
         expect(walked.contentShas.size).toBeGreaterThan(0);
         const wire = workflowsToWire(walked);
         expect("contentShas" in wire).toBe(false);
         expect("paths" in wire).toBe(false);
         const adopted = shipped(walked);
         expect("contentShas" in adopted).toBe(false);
         expect("paths" in adopted).toBe(false);
      });

      it("keeps the refusal of a whole declaration", () => {
         const walked = read("nope");
         expect(walked.declarationRefusal).toContain("must be an object");
         expect(shipped(walked).declarationRefusal).toBe(
            walked.declarationRefusal,
         );
      });

      it("keeps one skill shared by every set and workflow that holds it, with its reader rebuilt", async () => {
         const adopted = shipped(read(declared()));
         const fromSet = adopted.sets.get(HOUSE)!.skills[0]!;
         for (const workflow of adopted.workflows.values()) {
            expect(workflow.sets[0]!.skills[0]).toBe(fromSet);
         }
         expect(await fromSet.served.read("SKILL.md")).toEqual(
            await read(declared())
               .sets.get(HOUSE)!
               .skills[0]!.served.read("SKILL.md"),
         );
      });

      it("answers a binary listed file from its listing, without reading the disk", async () => {
         write(`${CONVENTIONS}/assets/logo.bin`, Buffer.from([0, 255, 1]));
         const adopted = shipped(read(declared()));
         const skill = adopted.sets.get(HOUSE)!.skills[0]!;
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
         const hostile = new Proxy(
            {},
            {
               ownKeys() {
                  throw new Error("injected walk failure");
               },
            },
         );
         // Handed over as it is: boundedDeclaration would call it unreadable instead.
         const walked = readPackageWorkflows(
            pkg,
            {
               skillSets: { house: { path: MAIN } },
               workflows: { ...declared(), x: hostile },
            },
            [],
         );
         expect(walked.unavailable?.declared).toEqual(["one", "two", "x"]);
         expect(walked.unavailable?.declaredSets).toEqual([HOUSE]);
         expect(shipped(walked).unavailable).toEqual(walked.unavailable);
      });
   });
});

describe("workflowFileServed", () => {
   const watched = [INSTRUCTIONS, MAIN];
   const served = (relative: string) =>
      workflowFileServed("/nowhere", watched, relative);

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
