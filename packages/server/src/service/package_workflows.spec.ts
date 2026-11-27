// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Package workflows end to end through `Package.create`, which sends publisher.json
 * through the package-load worker pool, the path production takes. A mock of
 * the worker would hide a handoff that forgets `workflows`.
 */
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import { createHash } from "crypto";
import * as fs from "fs";
import fsSync from "fs";
import * as os from "os";
import * as path from "path";
import { SkillController } from "../controller/skill.controller";
import { skillsFor } from "../retrieval/search/response";
import { resetDeprecatedUsageMetricsForTest } from "../deprecated_usage_metrics";
import {
   PackageManifestError,
   WorkflowNotFoundError,
   WorkflowsUnavailableError,
} from "../errors";
import { registerGetSkillTool } from "../mcp/tools/get_skill_tool";
import { registerGetWorkflowTool } from "../mcp/tools/get_workflow_tool";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
   type LoadPackageJob,
   type LoadPackageOutcome,
} from "../package_load/package_load_pool";
import { PackageLoadProtocolError } from "../package_load/protocol_decode";
import { servingIdentityOutcome } from "../test_helpers/serving_identity";
import { fakeOf } from "../test_helpers/store_fakes";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { compileSource, type CompileSourceHost } from "./compile_source";
import type { PackageLookup } from "./environment_ports";
import { Package } from "./package";
import { canonicalSha, computeSourceContentSha } from "./package_revision";
import { BUNDLED_CATALOG } from "./skills/package_skills";
import {
   readPackageWorkflows,
   SKILLS_PER_SET_MAX,
} from "./skills/package_workflows";

const MAIN = "workflows/analyst/skill_sets/main";
const SECOND = "workflows/analyst/skill_sets/second";
const LISTED = "conventions/assets";
const ASSETS = `${MAIN}/conventions/assets`;

describe("package workflows", () => {
   let dir: string;
   let duckdb: { close: () => Promise<void> };
   let malloyConfig: import("@malloydata/malloy").MalloyConfig;

   beforeAll(async () => {
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
   });

   beforeEach(async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-workflows-"));
      fs.writeFileSync(
         path.join(dir, "model.malloy"),
         `source: t is duckdb.sql("select 1 as id")\n`,
      );
      write("workflows/analyst/instructions.md", "Be careful with revenue.");
      write(
         `${MAIN}/conventions/SKILL.md`,
         "---\nname: conventions\ndescription: House rules.\n---\n\nUse net revenue.\n",
      );
      write("workflows/other/instructions.md", "Other brief.");
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const conn = new DuckDBConnection("duckdb", ":memory:");
      duckdb = conn;
      malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", conn]]), "duckdb"),
      );
   });

   afterEach(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   function write(rel: string, text: string) {
      fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
      fs.writeFileSync(path.join(dir, rel), text);
   }

   const sha = (text: string) =>
      createHash("sha256").update(text).digest("hex");

   const analyst = () => ({
      description: "Answers revenue questions",
      instructions: "workflows/analyst/instructions.md",
      skill_sets: [MAIN],
   });
   const other = () => ({
      description: "Other",
      instructions: "workflows/other/instructions.md",
   });

   function manifest(extra: Record<string, unknown>) {
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", version: "1", ...extra }),
      );
   }

   const load = () => Package.create("env", "pkg", dir, malloyConfig);

   /** A workflow skill file as the tests that are not about a changed file expect it: served, or not served at all. */
   const fileOf = async (
      pkg: Package,
      workflow: string,
      file: string,
      skillSet?: string,
   ) => {
      const read = await pkg.getWorkflowSkillFile(workflow, file, skillSet);
      expect(read !== undefined && "changed" in read).toBe(false);
      return read as Exclude<typeof read, { changed: true }>;
   };

   const storeFor = (pkg: Package): PackageLookup =>
      fakeOf<PackageLookup>({
         getEnvironment: async () => ({
            getPackage: async () => pkg,
         }),
      });

   type Handler = (params: unknown) => Promise<{
      isError?: boolean;
      content: Array<{ resource?: { text: string } }>;
   }>;

   type ToolReply = Awaited<ReturnType<Handler>>;

   const toolFor = (pkg: Package): Handler => {
      let handler: Handler | undefined;
      let description = "";
      registerGetWorkflowTool(
         {
            registerTool: (
               _n: string,
               { description: d }: { description: string },
               h: Handler,
            ) => {
               description = d;
               handler = h;
            },
         } as never,
         storeFor(pkg),
      );
      expect(description).toContain("ONLY WHEN THE USER NAMES IT");
      return handler!;
   };
   const warningsOf = (pkg: Package) =>
      (pkg.getPackageMetadata().warnings ?? []).map((w) => w.message ?? "");

   it("carries workflows through the worker, the lookup and the listing", async () => {
      manifest({ workflows: { analyst: analyst(), other: other() } });
      const pkg = await load();

      expect(pkg.listWorkflows()).toEqual([
         {
            name: "analyst",
            description: "Answers revenue questions",
            has_instructions: true,
            skill_sets: [
               {
                  name: "main",
                  path: MAIN,
                  digest: expect.stringMatching(/^[0-9a-f]{64}$/),
               },
            ],
         },
         {
            name: "other",
            description: "Other",
            has_instructions: true,
            skill_sets: [],
         },
      ]);
      const metadata = pkg.getPackageMetadata();
      expect(metadata).not.toHaveProperty("workflows");
      expect(metadata).not.toHaveProperty("agents");
      expect(metadata.warnings ?? []).toEqual([]);

      expect(pkg.getWorkflow("analyst")).toEqual({
         name: "analyst",
         description: "Answers revenue questions",
         has_instructions: true,
         instructions: "Be careful with revenue.",
         skill_sets: [
            {
               name: "main",
               path: MAIN,
               digest: expect.stringMatching(/^[0-9a-f]{64}$/),
               skills: [
                  {
                     relative_filepath: "conventions/SKILL.md",
                     file_contents:
                        '---\nname: "conventions"\ndescription: "House rules."\n---\n\nUse net revenue.\n',
                  },
               ],
            },
         ],
         warnings: [],
         source: {
            environment: "env",
            package: "pkg",
            sourceContentSha: pkg.getSourceContentSha(),
            definitionSha: expect.stringMatching(/^[0-9a-f]{64}$/),
            servedRevision: pkg.getServedRevision(),
         },
      });
      expect(pkg.getWorkflow("missing")).toBeUndefined();
      expect(pkg.workflowPaths()).toEqual([
         "workflows/analyst/instructions.md",
         MAIN,
         "workflows/other/instructions.md",
      ]);
   });

   it("leaves a package without workflows untouched: nothing listed, no warnings", async () => {
      manifest({});
      const pkg = await load();
      expect(pkg.getPackageMetadata().warnings ?? []).toEqual([]);
      expect(pkg.listWorkflows()).toEqual([]);
      expect(pkg.listSkills()).toEqual([]);
      expect(pkg.workflowPaths()).toEqual([]);
   });

   it("serves the package and the good workflows when one is bad", async () => {
      manifest({
         workflows: {
            analyst: analyst(),
            bad: { ...other(), tools: ["Bash"] },
            "Also Bad": other(),
         },
      });
      const pkg = await load();
      expect(pkg.listWorkflows().map((w) => w.name)).toEqual(["analyst"]);
      const warnings = warningsOf(pkg);
      expect(warnings).toHaveLength(2);
      expect(warnings.join("\n")).toContain("Workflow 'bad' is not served");
      expect(warnings.join("\n")).toContain("Fix:");
   });

   it("watches the instructions and skill sets of a workflow it refused, so fixing one reloads the package", async () => {
      manifest({ workflows: { broken: { ...analyst(), surprise: true } } });
      const pkg = await load();
      expect(pkg.listWorkflows()).toEqual([]);
      expect([...pkg.workflowPaths()].sort()).toEqual(
         ["workflows/analyst/instructions.md", MAIN].sort(),
      );
   });

   it("lets a hostile workflows value reach the main thread without failing the worker", async () => {
      for (const workflows of [
         "text",
         7,
         null,
         [1, 2],
         { a: { description: { x: 1 }, instructions: [] } },
         JSON.parse('{"__proto__":{"description":"d","instructions":"x.md"}}'),
      ]) {
         manifest({ workflows });
         let error: unknown;
         let pkg: Package | undefined;
         try {
            pkg = await load();
         } catch (e) {
            error = e;
         }
         // A package must load; if anything throws it can only be the author's error type.
         expect(
            error === undefined || error instanceof PackageManifestError,
         ).toBe(true);
         expect(pkg?.listWorkflows() ?? []).toEqual([]);
      }
   });

   // Raw text: JSON.stringify of this value overflows the stack, which is the point.
   const writeDeepWorkflows = () => {
      const n = 50_000;
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         `{"name":"pkg","workflows":{"a":{"description":"d","instructions":"x.md","x-deep":${"[".repeat(n)}${"]".repeat(n)}}}}`,
      );
   };

   it("serves a package whose workflows value is nested past the stack", async () => {
      writeDeepWorkflows();
      const pkg = await load();
      expect(pkg.listWorkflows()).toEqual([]);
      expect(pkg.getSourceContentSha()).toMatch(/^[0-9a-f]{64}$/);
   });

   it("finishes a reload whose workflows value is nested past the stack", async () => {
      manifest({ workflows: { analyst: analyst() } });
      const pkg = await load();
      writeDeepWorkflows();
      await pkg.reloadAllModels({});
      expect(pkg.listWorkflows()).toEqual([]);
      expect(pkg.getSourceContentSha()).toMatch(/^[0-9a-f]{64}$/);
   });

   describe("serving identity", () => {
      const shaOf = async () => (await load()).getSourceContentSha();

      beforeEach(() => manifest({ workflows: { analyst: analyst() } }));

      it("moves on an instructions or skill edit", async () => {
         const base = await shaOf();
         write("workflows/analyst/instructions.md", "edited");
         const afterInstructions = await shaOf();
         expect(afterInstructions).not.toBe(base);
         write(
            `${MAIN}/conventions/SKILL.md`,
            "---\nname: conventions\ndescription: House rules.\n---\n\nEdited.\n",
         );
         expect(await shaOf()).not.toBe(afterInstructions);
      });

      it("moves on a description or skill-set-list edit, and on a refused workflow's edit", async () => {
         const base = await shaOf();
         manifest({
            workflows: {
               analyst: { ...analyst(), description: "Other words" },
            },
         });
         const afterDescription = await shaOf();
         expect(afterDescription).not.toBe(base);
         manifest({
            workflows: { analyst: { ...analyst(), skill_sets: [] } },
         });
         const afterSets = await shaOf();
         expect(afterSets).not.toBe(afterDescription);
         manifest({
            workflows: {
               analyst: { ...analyst(), skill_sets: [] },
               refused: { surprise: 1 },
            },
         });
         expect(await shaOf()).not.toBe(afterSets);
      });

      it("does not move on a version bump or an unrelated manifest key", async () => {
         const base = await shaOf();
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({
               name: "pkg",
               version: "2",
               description: "changed",
               somethingElse: { nested: true },
               workflows: { analyst: analyst() },
            }),
         );
         expect(await shaOf()).toBe(base);
      });

      it("keeps one workflow's definitionSha when another is edited", async () => {
         manifest({ workflows: { analyst: analyst(), other: other() } });
         const before = (await load()).getWorkflow("other")!.source
            .definitionSha;
         write("workflows/analyst/instructions.md", "edited");
         expect((await load()).getWorkflow("other")!.source.definitionSha).toBe(
            before,
         );
      });

      it("keeps a set's digest when another set or a workflow's instructions is edited", async () => {
         write(
            `${SECOND}/extra/SKILL.md`,
            "---\nname: extra\ndescription: d\n---\n\nbody\n",
         );
         manifest({
            workflows: {
               analyst: { ...analyst(), skill_sets: [MAIN, SECOND] },
            },
         });
         const digests = async () =>
            Object.fromEntries(
               (await load())
                  .listWorkflows()[0]!
                  .skill_sets.map((s) => [s.name, s.digest]),
            );
         const before = await digests();
         write("workflows/analyst/instructions.md", "edited");
         write(`${SECOND}/extra/notes.md`, "# Notes\n");
         const after = await digests();
         expect(after.main).toBe(before.main);
         expect(after.second).not.toBe(before.second);
      });

      it("tells a package with no workflows key from one with an empty one", async () => {
         manifest({});
         const withoutKey = await shaOf();
         manifest({ workflows: {} });
         expect(await shaOf()).not.toBe(withoutKey);
         manifest({ description: "x" });
         expect(await shaOf()).toBe(withoutKey);
      });
   });

   describe("reload and metadata PATCH", () => {
      it("picks up a workflow edit on reload and re-pins the sha", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         const before = pkg.getSourceContentSha();
         const beforeDefinition =
            pkg.getWorkflow("analyst")!.source.definitionSha;

         write("workflows/analyst/instructions.md", "after reload");
         manifest({
            workflows: { analyst: analyst(), other: other() },
         });
         await pkg.reloadAllModels({});

         expect(pkg.getWorkflow("analyst")!.instructions).toBe("after reload");
         expect(pkg.getWorkflow("analyst")!.source.definitionSha).not.toBe(
            beforeDefinition,
         );
         expect(pkg.getWorkflow("other")).toBeDefined();
         expect(pkg.getSourceContentSha()).not.toBe(before);
         expect(pkg.getWorkflow("analyst")!.source.sourceContentSha).toBe(
            pkg.getSourceContentSha(),
         );
      });

      it("finishes a reload, with a warn finding, when a skill's frontmatter is not valid YAML", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         const before = pkg.getSourceContentSha();

         write(
            `${MAIN}/odd/SKILL.md`,
            "---\nname: odd\ndescription: Use when: x\n---\n\nbody\n",
         );
         await pkg.reloadAllModels({});

         expect(
            pkg
               .getWorkflow("analyst")!
               .skill_sets[0]!.skills.map((f) => f.relative_filepath),
         ).toContain("odd/SKILL.md");
         expect(pkg.getSourceContentSha()).not.toBe(before);
         expect(
            pkg
               .getPackageMetadata()
               .warnings?.filter((w) => w.message?.includes("not valid YAML")),
         ).toEqual([
            expect.objectContaining({
               severity: "warn",
               message: expect.stringContaining(
                  `Skill set '${MAIN}' skill finding:`,
               ),
            }),
         ]);
      });

      it("serves a workflow whose skill has no description and reports it as a warn finding", async () => {
         write(`${MAIN}/quiet/SKILL.md`, "---\nname: quiet\n---\n\nbody\n");
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         expect(pkg.getWorkflow("analyst")).toBeDefined();
         const finding = pkg
            .getPackageMetadata()
            .warnings?.find((w) => w.message?.includes("no description"));
         expect(finding?.severity).toBe("warn");
         expect(pkg.getWorkflow("analyst")!.warnings).toEqual([
            expect.stringContaining("no description"),
         ]);
      });

      it("drops a workflow on reload when the manifest removes it", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         manifest({});
         await pkg.reloadAllModels({});
         expect(pkg.listWorkflows()).toEqual([]);
      });

      it("keeps workflows across a metadata PATCH", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         // Environment.updatePackage replaces the stored metadata exactly like this.
         pkg.setPackageMetadata({ name: "pkg", description: "patched" });
         expect(pkg.getPackageMetadata().description).toBe("patched");
         expect(pkg.listWorkflows().map((w) => w.name)).toEqual(["analyst"]);
      });
   });

   describe("the default workflow's skills", () => {
      const skillsOf = (pkg: Package) => pkg.listSkills().map((s) => s.name);

      it("are the package's advertised skills, first set winning a repeated name", async () => {
         write(
            `${SECOND}/conventions/SKILL.md`,
            "---\nname: conventions\ndescription: Second copy.\n---\n\nbody\n",
         );
         write(
            `${SECOND}/extra/SKILL.md`,
            "---\nname: extra\ndescription: d\n---\n\nbody\n",
         );
         manifest({
            workflows: { default: { skill_sets: [MAIN, SECOND] } },
         });
         const pkg = await load();
         expect(skillsOf(pkg)).toEqual(["conventions", "extra"]);
         expect(pkg.listSkills()[0]!.description).toBe("House rules.");
         expect(pkg.getWorkflow("default")!.warnings).toEqual([
            expect.stringContaining(
               "lists skill 'conventions' in both 'workflows/analyst/skill_sets/main' and 'workflows/analyst/skill_sets/second'",
            ),
         ]);
      });

      it("are none for a package with no default workflow", async () => {
         manifest({ workflows: { analyst: analyst() } });
         expect(skillsOf(await load())).toEqual([]);
      });

      it("follow a reload", async () => {
         manifest({ workflows: { default: { skill_sets: [MAIN] } } });
         const pkg = await load();
         expect(skillsOf(pkg)).toEqual(["conventions"]);
         manifest({ workflows: { other: other() } });
         await pkg.reloadAllModels({});
         expect(skillsOf(pkg)).toEqual([]);
      });
   });

   describe("what is no longer read", () => {
      let harness: MetricsHarness;
      const used = (feature: string) =>
         harness.collectCounter("publisher_deprecated_usage_total", {
            feature,
         });
      const codes = (pkg: Package) =>
         (pkg.getPackageMetadata().warnings ?? []).map((w) => w.code);

      beforeEach(async () => {
         harness = await startMetricsHarness();
         resetDeprecatedUsageMetricsForTest();
      });

      afterEach(async () => {
         resetDeprecatedUsageMetricsForTest();
         await harness.shutdown();
      });

      it("warns of an agents key, with its code, and counts the package once per load", async () => {
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         expect(pkg.listWorkflows()).toEqual([]);
         expect(
            (pkg.getPackageMetadata().warnings ?? []).filter(
               (w) => w.code === "PACKAGE_AGENTS_NOT_READ",
            ),
         ).toEqual([
            {
               code: "PACKAGE_AGENTS_NOT_READ",
               message: '"agents" is no longer read; declare "workflows"',
               severity: "warn",
            },
         ]);
         expect(await used("agents")).toBe(1);
         await pkg.reloadAllModels({});
         expect(await used("agents")).toBe(2);
      });

      it("warns of a root skills/ directory no workflow lists, with its code, and counts it once per load", async () => {
         write(
            "skills/old/SKILL.md",
            "---\nname: old\ndescription: d\n---\n\nbody\n",
         );
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         expect(pkg.listSkills()).toEqual([]);
         expect(
            (pkg.getPackageMetadata().warnings ?? []).filter(
               (w) => w.code === "PACKAGE_SKILLS_DIR_NOT_READ",
            ),
         ).toEqual([
            {
               code: "PACKAGE_SKILLS_DIR_NOT_READ",
               message:
                  'skills/ is no longer read; move each skill into a skill set and list it under "workflows.default"',
               severity: "warn",
            },
         ]);
         expect(await used("skills_dir")).toBe(1);
         await pkg.reloadAllModels({});
         expect(await used("skills_dir")).toBe(2);
      });

      it("still warns of a root skills/ directory, and counts it, when the only workflow that lists it is refused", async () => {
         write(
            "skills/old/SKILL.md",
            "---\nname: old\ndescription: d\n---\n\nbody\n",
         );
         manifest({
            workflows: { default: { description: "", skill_sets: ["skills"] } },
         });
         const pkg = await load();
         expect(pkg.listSkills()).toEqual([]);
         expect(codes(pkg)).toContain("PACKAGE_SKILLS_DIR_NOT_READ");
         expect(await used("skills_dir")).toBe(1);
      });

      it("says nothing of skills/ when the default workflow lists it", async () => {
         write(
            "skills/old/SKILL.md",
            "---\nname: old\ndescription: d\n---\n\nbody\n",
         );
         manifest({ workflows: { default: { skill_sets: ["skills"] } } });
         const pkg = await load();
         expect(pkg.listSkills().map((s) => s.name)).toEqual(["old"]);
         expect(codes(pkg)).not.toContain("PACKAGE_SKILLS_DIR_NOT_READ");
         expect(await used("skills_dir")).toBe(0);
      });

      it("counts neither for a package that declares workflows and has no skills/", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         expect(codes(pkg)).toEqual([]);
         expect(await used("agents")).toBe(0);
         expect(await used("skills_dir")).toBe(0);
      });
   });

   describe("a skill set picked alone", () => {
      const DUPLICATE = `Workflow 'analyst' lists skill 'conventions' in both '${MAIN}' and '${SECOND}'; the copy in '${MAIN}', listed first, is the one served.`;

      beforeEach(() => {
         write(
            `${SECOND}/extra/SKILL.md`,
            "---\nname: extra\ndescription: d\n---\n",
         );
         write(`${SECOND}/extra/scripts/a..b.py`, "x");
         // A name both sets hold is a shadowing finding on the workflow.
         write(
            `${SECOND}/conventions/SKILL.md`,
            "---\nname: conventions\ndescription: Second copy.\n---\n",
         );
         manifest({
            workflows: {
               analyst: { ...analyst(), skill_sets: [MAIN, SECOND] },
            },
         });
      });

      it("returns that set and no instructions, with the workflow's pin", async () => {
         const pkg = await load();
         const full = pkg.getWorkflow("analyst")!;
         const picked = pkg.getWorkflow("analyst", "full", "main")!;
         expect(picked.instructions).toBeUndefined();
         expect(picked.has_instructions).toBe(true);
         expect(picked.skill_sets).toEqual([full.skill_sets[0]]);
         expect(picked.source).toEqual(full.source);
      });

      it("carries that set's findings, not another set's and not the shadowing ones", async () => {
         const pkg = await load();
         const second = pkg.getWorkflow("analyst", "full", "second")!;
         expect(second.warnings.join("\n")).toContain("a..b.py");
         // This reply serves the set's own copy of the name, so the copy in the first-listed set is not the one served.
         expect(second.warnings).not.toContain(DUPLICATE);
         const main = pkg.getWorkflow("analyst", "full", "main")!;
         expect(main.warnings).toEqual([]);
         expect(pkg.getWorkflow("analyst")!.warnings).toContain(DUPLICATE);
         expect(pkg.getWorkflow("analyst")!.warnings.join("\n")).toContain(
            "a..b.py",
         );
      });

      it("is not served for a name no set of the workflow has", async () => {
         const pkg = await load();
         expect(pkg.getWorkflow("analyst", "full", "nope")).toBeUndefined();
         expect(pkg.getWorkflow("other", "full", "main")).toBeUndefined();
      });

      const paths = (set: { skills: Array<{ relative_filepath: string }> }) =>
         set.skills.map((f) => f.relative_filepath);

      it("serves the picked set whole, though the whole workflow serves a name from the first-listed set only", async () => {
         const pkg = await load();
         expect(paths(pkg.getWorkflow("analyst")!.skill_sets[1]!)).toEqual([
            "extra/SKILL.md",
         ]);
         for (const view of ["full", "summary"] as const) {
            const picked = pkg.getWorkflow("analyst", view, "second")!;
            expect(paths(picked.skill_sets[0]!)).toEqual([
               "conventions/SKILL.md",
               "extra/SKILL.md",
            ]);
         }
         const [conventions] = pkg.getWorkflow("analyst", "full", "second")!
            .skill_sets[0]!.skills as Array<{ file_contents: string }>;
         expect(conventions!.file_contents).toContain("Second copy.");
      });

      it("keeps the picked set's skills when an earlier set gains a skill it holds, as its digest does", async () => {
         fs.rmSync(path.join(dir, MAIN, "conventions"), { recursive: true });
         write(
            `${MAIN}/other/SKILL.md`,
            "---\nname: other\ndescription: d\n---\n",
         );
         const before = (await load()).getWorkflow("analyst", "full", "second")!
            .skill_sets[0]!;
         expect(paths(before)).toEqual([
            "conventions/SKILL.md",
            "extra/SKILL.md",
         ]);

         write(
            `${MAIN}/conventions/SKILL.md`,
            "---\nname: conventions\ndescription: Now in main.\n---\n",
         );
         const after = (await load()).getWorkflow("analyst", "full", "second")!
            .skill_sets[0]!;
         expect(after).toEqual(before);
         expect(after.digest).toBe(before.digest);
      });

      it("reads a file inside the picked set, where the whole workflow serves the first-listed copy", async () => {
         const pkg = await load();
         const file = "conventions/SKILL.md";
         expect((await fileOf(pkg, "analyst", file))!.file_contents).toContain(
            "Use net revenue.",
         );
         const picked = (await fileOf(pkg, "analyst", file, "second"))!;
         expect(picked.file_contents).toContain("Second copy.");
         expect(picked.source).toEqual(pkg.getWorkflow("analyst")!.source);
         expect(
            await fileOf(pkg, "analyst", "extra/SKILL.md", "second"),
         ).toBeDefined();
         // A name the picked set does not hold is not served, though another set of the workflow does.
         expect(
            await fileOf(pkg, "analyst", "extra/SKILL.md", "main"),
         ).toBeUndefined();
         expect(await fileOf(pkg, "analyst", file, "nope")).toBeUndefined();
         expect(await fileOf(pkg, "other", file, "main")).toBeUndefined();
      });
   });

   describe("skill assets", () => {
      beforeEach(() => manifest({ workflows: { analyst: analyst() } }));

      it("keeps a workflow without assets free of an assets key", async () => {
         const workflow = (await load()).getWorkflow("analyst")!;
         expect("assets" in workflow.skill_sets[0]!).toBe(false);
      });

      it("lists assets in the full view and drops them from the summary", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         write(`${ASSETS}/css/app.css`, "body{}");
         const pkg = await load();

         const full = pkg.getWorkflow("analyst")!;
         expect(full.skill_sets[0]!.assets).toEqual([
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
         expect(JSON.stringify(full)).not.toContain("<h1>hi</h1>");

         const summary = pkg.getWorkflow("analyst", "summary")!;
         expect(summary.skill_sets[0]).not.toHaveProperty("assets");
         expect(summary.source).toEqual(full.source);
      });

      it("serves a listed asset through the skill-file lookup, REST and MCP, and nothing unlisted", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         write(
            `${ASSETS}/unlisted.malloy`,
            `source: x is duckdb.sql("select 1 as id")\n`,
         );
         write("secret.txt", "outside");
         const pkg = await load();
         const listed = `${LISTED}/index.html`;
         const expected = {
            relative_filepath: listed,
            file_contents: "<h1>hi</h1>",
            source: pkg.getWorkflow("analyst")!.source,
         };

         expect(await fileOf(pkg, "analyst", listed)).toEqual(expected);
         const store = fakeOf<PackageLookup>({
            getEnvironment: async () => ({ getPackage: async () => pkg }),
         });
         expect(
            await new SkillController(store).getWorkflowSkillFile(
               "env",
               "pkg",
               "analyst",
               listed,
            ),
         ).toEqual(expected);
         type Handler = (p: unknown) => Promise<{
            content: Array<{ resource?: { text: string } }>;
         }>;
         let handler: Handler | undefined;
         registerGetWorkflowTool(
            {
               registerTool: (_n: string, _config: unknown, h: Handler) => {
                  handler = h;
               },
            } as never,
            store,
         );
         const viaMcp = JSON.parse(
            (
               await handler!({
                  scopes: [{ environment: "env", package: "pkg" }],
                  workflow_name: "analyst",
                  skill_path: listed,
               })
            ).content[0]!.resource!.text,
         );
         expect(viaMcp.skillFile).toEqual(expected);

         for (const bad of [
            `${LISTED}/unlisted.malloy`,
            `${LISTED}/missing.html`,
            `${LISTED}/../../../../../secret.txt`,
            "conventions/assets",
         ]) {
            expect(await fileOf(pkg, "analyst", bad)).toBeUndefined();
         }
      });

      it("serves a file by its skill and path across the workflow's sets", async () => {
         write(
            `${SECOND}/extra/SKILL.md`,
            "---\nname: extra\ndescription: d\n---\n\nbody\n",
         );
         write(`${SECOND}/extra/scripts/run.py`, "print('hi')\n");
         manifest({
            workflows: {
               analyst: { ...analyst(), skill_sets: [MAIN, SECOND] },
            },
         });
         const pkg = await load();
         expect(
            (await fileOf(pkg, "analyst", "extra/scripts/run.py"))
               ?.file_contents,
         ).toBe("print('hi')\n");
         expect(
            (await fileOf(pkg, "analyst", "conventions/SKILL.md"))
               ?.file_contents,
         ).toContain("Use net revenue.");
         expect(await fileOf(pkg, "nope", "extra/SKILL.md")).toBe(undefined);
      });

      it("serves a skill name two sets hold from the first-listed set, and the shadowed copy's files are unreachable", async () => {
         write(
            `${SECOND}/conventions/SKILL.md`,
            "---\nname: conventions\ndescription: d\n---\n\nSecond copy.\n",
         );
         write(
            `${SECOND}/conventions/reference/only-second.md`,
            "# Only here\n",
         );
         const served = async (sets: string[]) => {
            manifest({
               workflows: { analyst: { ...analyst(), skill_sets: sets } },
            });
            return load();
         };

         const first = await served([MAIN, SECOND]);
         const skill = async (pkg: Package) =>
            (await fileOf(pkg, "analyst", "conventions/SKILL.md"))
               ?.file_contents ?? "";
         expect(await skill(first)).toContain("Use net revenue.");
         expect(await skill(first)).not.toContain("Second copy.");
         expect(
            await fileOf(
               first,
               "analyst",
               "conventions/reference/only-second.md",
            ),
         ).toBeUndefined();

         const reversed = await served([SECOND, MAIN]);
         expect(await skill(reversed)).toContain("Second copy.");
         expect(
            (
               await fileOf(
                  reversed,
                  "analyst",
                  "conventions/reference/only-second.md",
               )
            )?.file_contents,
         ).toBe("# Only here\n");
      });

      it("answers a listed asset that changed since the load as changed, on every surface, though it keeps its size", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         const listed = `${LISTED}/index.html`;
         write(`${ASSETS}/index.html`, "<h1>ho</h1>");
         expect(await pkg.getWorkflowSkillFile("analyst", listed)).toEqual({
            changed: true,
         });
         const store = fakeOf<PackageLookup>({
            getEnvironment: async () => ({ getPackage: async () => pkg }),
         });
         await expect(
            new SkillController(store).getWorkflowSkillFile(
               "env",
               "pkg",
               "analyst",
               listed,
            ),
         ).rejects.toThrow("changed on disk after the package loaded");
         type Handler = (p: unknown) => Promise<{
            content: Array<{ resource?: { text: string } }>;
         }>;
         let handler: Handler | undefined;
         registerGetWorkflowTool(
            {
               registerTool: (_n: string, _config: unknown, h: Handler) => {
                  handler = h;
               },
            } as never,
            store,
         );
         const viaMcp = JSON.parse(
            (
               await handler!({
                  scopes: [{ environment: "env", package: "pkg" }],
                  workflow_name: "analyst",
                  skill_path: listed,
               })
            ).content[0]!.resource!.text,
         );
         // Not `workflow: null`, which the run-workflow recipe reads as "the name did not match".
         expect("workflow" in viaMcp).toBe(false);
         expect(viaMcp.skillFile.relative_filepath).toBe(listed);
         expect(viaMcp.skillFile.message).toContain(
            "changed on disk after the package",
         );
         expect(viaMcp.skillFile.message).not.toContain("Fix:");
         expect(viaMcp.message).toBeUndefined();
      });

      it("reads a path that starts with ./ as the file it names", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         const plain = await fileOf(pkg, "analyst", `${LISTED}/index.html`);
         expect(plain).toBeDefined();
         expect(await fileOf(pkg, "analyst", `./${LISTED}/index.html`)).toEqual(
            plain,
         );
         expect(
            (await fileOf(pkg, "analyst", "./conventions/SKILL.md"))
               ?.relative_filepath,
         ).toBe("conventions/SKILL.md");
      });

      it("refuses to serve a listed asset that became a link after the load", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         write("secret.txt", "outside");
         const pkg = await load();
         fs.rmSync(path.join(dir, ASSETS, "index.html"));
         fs.symlinkSync(
            path.join(dir, "secret.txt"),
            path.join(dir, ASSETS, "index.html"),
         );
         expect(
            await pkg.getWorkflowSkillFile("analyst", `${LISTED}/index.html`),
         ).toEqual({ changed: true });
      });

      it("sets the skill aside and says why when an asset is a link, and still serves the workflow", async () => {
         write("secret.txt", "outside");
         fs.mkdirSync(path.join(dir, ASSETS), { recursive: true });
         fs.symlinkSync(
            path.join(dir, "secret.txt"),
            path.join(dir, ASSETS, "link.txt"),
         );
         const pkg = await load();
         expect(pkg.getWorkflow("analyst")!.skill_sets[0]!.skills).toEqual([]);
         const warning = warningsOf(pkg).find((m) => m.includes("link.txt"));
         expect(warning).toStartWith(`Skill set '${MAIN}' skill finding:`);
      });

      it("moves definitionSha and the content sha when an asset changes", async () => {
         write(`${ASSETS}/index.html`, "one");
         const pkg = await load();
         const definition = pkg.getWorkflow("analyst")!.source.definitionSha;
         const content = pkg.getSourceContentSha();

         write(`${ASSETS}/index.html`, "two");
         await pkg.reloadAllModels({});
         expect(pkg.getWorkflow("analyst")!.source.definitionSha).not.toBe(
            definition,
         );
         expect(pkg.getSourceContentSha()).not.toBe(content);
         expect(
            pkg.getWorkflow("analyst")!.skill_sets[0]!.assets![0]!.sha256,
         ).toBe(sha("two"));
      });

      it("hashes the sha the load took of an asset, not the file as it is now", () => {
         write("a.txt", "one");
         const listed = new Map([["a.txt", sha("one")]]);
         const before = computeSourceContentSha(listed);
         write("a.txt", "two");
         expect(computeSourceContentSha(listed)).toBe(before);
         expect(
            computeSourceContentSha(new Map([["a.txt", sha("two")]])),
         ).not.toBe(before);
      });

      it("skips a manifest-shaped file in a skill's assets with a warning, and still serves the skill and the workflow", async () => {
         write(
            `${MAIN}/top/SKILL.md`,
            "---\nname: top\ndescription: d\n---\n\nbody\n",
         );
         write(`${MAIN}/top/templates/a.txt`, "x");
         write(`${MAIN}/top/templates/publisher.json`, "{}");
         const pkg = await load();
         expect(
            pkg
               .getWorkflow("analyst")!
               .skill_sets[0]!.skills.map((f) => f.relative_filepath),
         ).toContain("top/SKILL.md");
         expect(
            pkg
               .getPackageMetadata()
               .warnings?.some(
                  (w) =>
                     w.severity === "warn" &&
                     w.message?.includes(
                        `${MAIN}/top/templates/publisher.json`,
                     ),
               ),
         ).toBe(true);
      });

      it("moves sourceContentSha when a skill's script changes, and when its .skillignore does", async () => {
         write(
            `${MAIN}/top/SKILL.md`,
            "---\nname: top\ndescription: d\n---\n\nbody\n",
         );
         write(`${MAIN}/top/scripts/run.py`, "print(1)");
         write(`${MAIN}/top/scripts/run_test.py`, "assert True");
         const pkg = await load();
         const base = pkg.getSourceContentSha();

         write(`${MAIN}/top/scripts/run.py`, "print(2)");
         await pkg.reloadAllModels({});
         const edited = pkg.getSourceContentSha();
         expect(edited).not.toBe(base);

         // Ignored by nothing yet, so editing the test file moves the hash;
         // once ignored, it no longer does.
         write(`${MAIN}/top/.skillignore`, "*_test.py\n");
         await pkg.reloadAllModels({});
         const ignored = pkg.getSourceContentSha();
         expect(ignored).not.toBe(edited);
         write(`${MAIN}/top/scripts/run_test.py`, "assert False");
         await pkg.reloadAllModels({});
         expect(pkg.getSourceContentSha()).toBe(ignored);
      });

      it("lists a PNG under assets/ and answers a read of it with a message naming its size, never its bytes", async () => {
         fs.mkdirSync(path.join(dir, ASSETS), { recursive: true });
         const png = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0xff, 0xfe, 0x00]);
         fs.writeFileSync(path.join(dir, ASSETS, "logo.png"), png);
         write(`${ASSETS}/ok.txt`, "fine");
         const pkg = await load();
         expect(
            pkg
               .getWorkflow("analyst")!
               .skill_sets[0]!.assets!.map((a) => a.relative_filepath),
         ).toEqual([`${LISTED}/logo.png`, `${LISTED}/ok.txt`]);
         const read = (await fileOf(pkg, "analyst", `${LISTED}/logo.png`))!;
         expect(read).not.toHaveProperty("file_contents");
         expect(read.message).toContain(`${png.length} bytes`);
         expect(read.message).toContain("binary");
      });

      it("keeps a workflow served when assets/ holds more than 1,000 entries", async () => {
         write(`${ASSETS}/a.txt`, "x");
         for (let i = 0; i < 1000; i++) {
            fs.mkdirSync(
               path.join(dir, ASSETS, `d${String(i).padStart(4, "0")}`),
               { recursive: true },
            );
         }
         write(`${ASSETS}/z.txt`, "x");
         const pkg = await load();
         expect(
            pkg
               .getWorkflow("analyst")!
               .skill_sets[0]!.assets!.map((a) => a.relative_filepath),
         ).toEqual([`${LISTED}/a.txt`]);
         expect(warningsOf(pkg).some((m) => m.includes("1000 entries"))).toBe(
            true,
         );
      });

      it("refuses a listed asset once its directory is swapped for a link", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         fs.mkdirSync(path.join(dir, "elsewhere"));
         fs.writeFileSync(path.join(dir, "elsewhere/index.html"), "SECRET");
         const pkg = await load();
         fs.rmSync(path.join(dir, ASSETS), { recursive: true });
         fs.symlinkSync(path.join(dir, "elsewhere"), path.join(dir, ASSETS));
         expect(
            await pkg.getWorkflowSkillFile("analyst", `${LISTED}/index.html`),
         ).toEqual({ changed: true });
      });

      it("refuses a listed asset edited after the load, since it no longer matches its sha", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         write(`${ASSETS}/index.html`, "<h1>changed</h1>");
         expect(
            await pkg.getWorkflowSkillFile("analyst", `${LISTED}/index.html`),
         ).toEqual({ changed: true });
      });

      it("keeps a skill named __proto__ and serves its asset", async () => {
         write(
            `${MAIN}/__proto__/SKILL.md`,
            "---\nname: __proto__\ndescription: d\n---\n\nbody\n",
         );
         write(`${MAIN}/__proto__/assets/a.txt`, "x");
         const pkg = await load();
         expect(
            (await fileOf(pkg, "analyst", "__proto__/assets/a.txt"))
               ?.file_contents,
         ).toBe("x");
      });

      it("pins the hashes of a package whose workflows ship no assets", async () => {
         const pkg = await load();
         expect(pkg.getWorkflow("analyst")!.source.definitionSha).toBe(
            "82232f8282d7b2d5b82fe7eb6e9f0e9b8b3531dac6eab27b8d70221a3373a4a6",
         );
         expect(pkg.getSourceContentSha()).toBe(
            "4a2a7b92fcec742e57427a5b1ecfb5d7c71653e3f12088a9712b6450ce462a47",
         );
      });
   });

   describe("REST and MCP surfaces", () => {
      it("serves the listing and the resolved definition from the controller", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         const controller = new SkillController(storeFor(pkg));

         expect(await controller.listWorkflows("env", "pkg")).toEqual(
            pkg.listWorkflows(),
         );
         const workflow = await controller.getWorkflow("env", "pkg", "analyst");
         expect(Object.keys(workflow).sort()).toEqual([
            "description",
            "has_instructions",
            "instructions",
            "name",
            "skill_sets",
            "source",
            "warnings",
         ]);
         expect(Object.keys(workflow.source).sort()).toEqual([
            "definitionSha",
            "environment",
            "package",
            "servedRevision",
            "sourceContentSha",
         ]);
         await expect(
            controller.getWorkflow("env", "pkg", "nope"),
         ).rejects.toBeInstanceOf(WorkflowNotFoundError);
         await expect(
            controller.getWorkflow("env", "pkg", "analyst", "full", "nope"),
         ).rejects.toBeInstanceOf(WorkflowNotFoundError);
      });

      const body = (r: { content: Array<{ resource?: { text: string } }> }) =>
         JSON.parse(r.content[0]!.resource!.text);
      const scopes = [{ environment: "env", package: "pkg" }];

      it("get_workflow lists, resolves and reports a missing name", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         const tool = toolFor(pkg);

         const listed = body(await tool({ scopes }));
         expect(listed.workflow).toBeNull();
         expect(
            listed.availableWorkflows.map((w: { name: string }) => w.name),
         ).toEqual(["analyst"]);

         const resolved = body(
            await tool({ scopes, workflow_name: "analyst" }),
         );
         expect(resolved.workflow).toEqual(pkg.getWorkflow("analyst"));

         const missing = body(await tool({ scopes, workflow_name: "nope" }));
         expect(missing.workflow).toBeNull();
         expect(missing.message).toContain("Fix:");
         expect(missing.availableWorkflows).toHaveLength(1);
      });

      describe("skills summary, one set and single-file read", () => {
         const RAW_REF = "---\nnot: frontmatter\n---\nraw glossary\n";
         const loadWithReference = async () => {
            write(`${MAIN}/conventions/reference/glossary.md`, RAW_REF);
            write("secret.txt", "outside the workflow");
            manifest({ workflows: { analyst: analyst() } });
            return load();
         };

         it("summary drops file_contents, keeps name, description and bytes, and the same pin", async () => {
            const pkg = await loadWithReference();
            const full = pkg.getWorkflow("analyst")!;
            const summary = pkg.getWorkflow("analyst", "summary")!;

            expect(summary.skill_sets[0]!.skills).toEqual(
               full.skill_sets[0]!.skills!.map((f) => ({
                  relative_filepath: f.relative_filepath,
                  ...(f.relative_filepath!.endsWith("/SKILL.md")
                     ? { name: "conventions", description: "House rules." }
                     : {}),
                  bytes: Buffer.byteLength(f.file_contents!),
               })),
            );
            expect(JSON.stringify(summary)).not.toContain("file_contents");
            expect(summary.source).toEqual(full.source);
            // The default is the full shape, as the sandbox renderer consumes it.
            expect(pkg.getWorkflow("analyst", "full")).toEqual(full);
            expect(
               full.skill_sets[0]!.skills!.every((f) => "file_contents" in f),
            ).toBe(true);
         });

         it("returns exactly one file with the definition's pin, and refuses anything outside the resolved set", async () => {
            const pkg = await loadWithReference();
            const full = pkg.getWorkflow("analyst")!;
            const controller = new SkillController(storeFor(pkg));
            const tool = toolFor(pkg);
            const files = full.skill_sets[0]!.skills!;
            const paths = files.map((f) => f.relative_filepath);
            expect(paths).toContain("conventions/reference/glossary.md");

            for (const file of files) {
               const read = await controller.getWorkflowSkillFile(
                  "env",
                  "pkg",
                  "analyst",
                  file.relative_filepath!,
               );
               expect(read).toEqual({ ...file, source: full.source });
               const viaMcp = body(
                  await tool({
                     scopes,
                     workflow_name: "analyst",
                     skill_path: file.relative_filepath,
                  }),
               );
               expect(viaMcp.skillFile).toEqual(read);
               expect(viaMcp).not.toHaveProperty("workflow");
            }

            for (const bad of [
               "../../../secret.txt",
               "conventions/../../../secret.txt",
               "workflows/other/instructions.md",
               "/secret.txt",
               "conventions",
               "",
            ]) {
               await expect(
                  controller.getWorkflowSkillFile("env", "pkg", "analyst", bad),
               ).rejects.toBeInstanceOf(WorkflowNotFoundError);
               if (bad === "") continue;
               const miss = body(
                  await tool({
                     scopes,
                     workflow_name: "analyst",
                     skill_path: bad,
                  }),
               );
               expect("workflow" in miss).toBe(false);
               expect(miss.skillFile.file_contents).toBeUndefined();
               expect(miss.skillFile.message).toContain("Fix:");
            }
            const noWorkflow = body(
               await tool({ scopes, skill_path: paths[0]! }),
            );
            expect(noWorkflow.skillFile).toBeUndefined();
            expect(noWorkflow.message).toContain("need workflow_name");
            await expect(
               controller.getWorkflowSkillFile("env", "pkg", "nope", paths[0]!),
            ).rejects.toBeInstanceOf(WorkflowNotFoundError);
         });

         it("reads a skill's SKILL.md for its own folder, as get_skill does for a blank path", async () => {
            const pkg = await loadWithReference();
            const skillMd = (await fileOf(
               pkg,
               "analyst",
               "conventions/SKILL.md",
            ))!;
            expect(skillMd.file_contents).toContain("name: ");
            for (const alias of [
               "conventions/",
               "./conventions/",
               "conventions/./",
            ]) {
               expect(await fileOf(pkg, "analyst", alias)).toEqual(skillMd);
            }
            // Still no file named for the skill alone, and none for no skill.
            expect(
               await pkg.getWorkflowSkillFile("analyst", "conventions"),
            ).toBe(undefined);
            expect(await pkg.getWorkflowSkillFile("analyst", "")).toBe(
               undefined,
            );
         });

         it("reads a file inside the picked set through the controller and get_workflow, where the whole workflow serves the first-listed copy", async () => {
            write(
               `${SECOND}/conventions/SKILL.md`,
               "---\nname: conventions\ndescription: Second copy.\n---\n",
            );
            manifest({
               workflows: {
                  analyst: { ...analyst(), skill_sets: [MAIN, SECOND] },
               },
            });
            const pkg = await load();
            const controller = new SkillController(storeFor(pkg));
            const tool = toolFor(pkg);
            const file = "conventions/SKILL.md";

            const viaRest = await controller.getWorkflowSkillFile(
               "env",
               "pkg",
               "analyst",
               file,
               "second",
            );
            expect(viaRest.file_contents).toContain("Second copy.");
            const viaMcp = body(
               await tool({
                  scopes,
                  workflow_name: "analyst",
                  skill_path: file,
                  skill_set: "second",
               }),
            );
            expect(viaMcp.skillFile).toEqual(viaRest);
            const whole = body(
               await tool({
                  scopes,
                  workflow_name: "analyst",
                  skill_path: file,
               }),
            );
            expect(whole.skillFile.file_contents).toContain("Use net revenue.");

            await expect(
               controller.getWorkflowSkillFile(
                  "env",
                  "pkg",
                  "analyst",
                  "extra/SKILL.md",
                  "main",
               ),
            ).rejects.toBeInstanceOf(WorkflowNotFoundError);
            const miss = body(
               await tool({
                  scopes,
                  workflow_name: "analyst",
                  skill_path: "extra/SKILL.md",
                  skill_set: "nope",
               }),
            );
            expect("workflow" in miss).toBe(false);
            expect(miss.skillFile.file_contents).toBeUndefined();
            expect(miss.skillFile.message).toContain("Fix:");
         });

         it("get_workflow summary and skill_set match REST and leave the default alone", async () => {
            const pkg = await loadWithReference();
            const controller = new SkillController(storeFor(pkg));
            const tool = toolFor(pkg);

            const viaMcp = body(
               await tool({
                  scopes,
                  workflow_name: "analyst",
                  skills: "summary",
               }),
            );
            expect(viaMcp.workflow).toEqual(
               await controller.getWorkflow("env", "pkg", "analyst", "summary"),
            );
            const dflt = body(await tool({ scopes, workflow_name: "analyst" }));
            expect(dflt.workflow).toEqual(pkg.getWorkflow("analyst"));
            expect(dflt.workflow.source.definitionSha).toBe(
               viaMcp.workflow.source.definitionSha,
            );

            const oneSet = body(
               await tool({
                  scopes,
                  workflow_name: "analyst",
                  skill_set: "main",
               }),
            );
            expect(oneSet.workflow).toEqual(
               await controller.getWorkflow(
                  "env",
                  "pkg",
                  "analyst",
                  "full",
                  "main",
               ),
            );
            expect(oneSet.workflow.instructions).toBeUndefined();
            const noSet = body(
               await tool({
                  scopes,
                  workflow_name: "analyst",
                  skill_set: "nope",
               }),
            );
            expect(noSet.workflow).toBeNull();
            expect(noSet.message).toContain("no skill set named");
         });
      });
   });

   describe("walked in the load worker", () => {
      const isRoot = process.getuid?.() === 0;
      const FS_READS = [
         "readFileSync",
         "openSync",
         "readdirSync",
         "opendirSync",
         "statSync",
         "lstatSync",
         "realpathSync",
      ] as const;
      const isWindows = process.platform === "win32";

      /** Records what each job asks of the worker, then runs it. */
      class RecordingPool extends PackageLoadPool {
         readonly requests: LoadPackageJob[] = [];
         override loadPackage(request: LoadPackageJob) {
            this.requests.push(request);
            return super.loadPackage(request);
         }
      }

      /** Lets a test change what the worker answered, as a worker that left something out would. */
      class TamperingPool extends PackageLoadPool {
         tamper?: (outcome: LoadPackageOutcome) => void;
         override async loadPackage(request: LoadPackageJob) {
            const outcome = await super.loadPackage(request);
            this.tamper?.(outcome);
            return outcome;
         }
      }

      const usePool = (pool: PackageLoadPool) =>
         __setPackageLoadPoolForTests(pool);

      afterAll(async () => {
         await usePool(new PackageLoadPool(1));
      });

      describe("which jobs ask for the walk", () => {
         let pool: RecordingPool;
         beforeEach(async () => {
            pool = new RecordingPool(1);
            await usePool(pool);
            manifest({ workflows: { analyst: analyst() } });
         });

         const readsWorkflows = () => pool.requests.map((j) => j.readWorkflows);

         it("asks on a create and on a reload, and not for a build plan refresh", async () => {
            const pkg = await load();
            expect(readsWorkflows()).toEqual([true]);
            await pkg.reloadAllModels({});
            expect(readsWorkflows()).toEqual([true, true]);
            pool.requests.length = 0;
            pkg.markBuildPlanStale();
            await pkg.refreshBuildPlan();
            expect(pool.requests).toHaveLength(1);
            expect(pool.requests[0]!.computeBuildPlan).toBe(true);
            expect(readsWorkflows()).toEqual([undefined]);
         });

         it("does not ask for a compile, which serves nothing", async () => {
            const pkg = await load();
            pool.requests.length = 0;
            const host: CompileSourceHost = {
               environmentName: "env",
               environmentPath: path.dirname(dir),
               withPackageLock: (_name, fn) => fn(),
               loadPackageLocked: async () => pkg,
               versionedPackage: async () => pkg,
            };
            await compileSource(
               host,
               "pkg",
               "model.malloy",
               undefined,
               false,
               undefined,
               "package",
            );
            expect(pool.requests).toHaveLength(1);
            expect(readsWorkflows()).toEqual([undefined]);
         });
      });

      describe("what the main thread adopts", () => {
         const HOUSE = "sets/a/house";
         const declared = () => ({
            default: { description: "Default", skill_sets: ["sets/a"] },
            one: {
               description: "One",
               instructions: "workflows/one.md",
               skill_sets: ["sets/a", "sets/b"],
            },
            two: { description: "Two", skill_sets: ["sets/a"] },
         });

         function richPackage() {
            write(
               `${HOUSE}/SKILL.md`,
               "---\nname: house\ndescription: House rules.\n---\n\nUse net revenue.\n",
            );
            // Root files named like Object.prototype members, which a catalog keyed by plain assignment would lose.
            for (const name of ["__proto__", "constructor", "toString"]) {
               write(`${HOUSE}/${name}`, `root file ${name}\n`);
            }
            write(`${HOUSE}/reference/x.md`, "# X\n");
            write(`${HOUSE}/ignored.txt`, "left out");
            write(`${HOUSE}/.skillignore`, "ignored.txt\n");
            fs.mkdirSync(path.join(dir, HOUSE, "assets"), { recursive: true });
            fs.writeFileSync(
               path.join(dir, HOUSE, "assets/logo.bin"),
               Buffer.from([0, 1, 2, 255, 254]),
            );
            write(
               "sets/b/extra/SKILL.md",
               "---\nname: extra\ndescription: Extra.\n---\n\nbody\n",
            );
            // A skill the strict assets/ rules set aside: its assets/ is a link out of the package.
            write(
               "sets/a/bad/SKILL.md",
               "---\nname: bad\ndescription: Bad.\n---\n\nbody\n",
            );
            fs.symlinkSync(
               os.tmpdir(),
               path.join(dir, "sets/a/bad/assets"),
               "dir",
            );
            write("workflows/one.md", "Be one.");
            manifest({ workflows: declared() });
         }

         /** The package an in-process walk of the same files gives, adopted as a worker's result is. */
         function inProcess(): Package {
            const pkg = new Package(
               "env",
               "pkg",
               dir,
               { name: "pkg" } as never,
               [],
               new Map(),
            );
            (
               pkg as unknown as {
                  adoptOutcomeMetadata: (o: LoadPackageOutcome) => void;
               }
            ).adoptOutcomeMetadata(
               servingIdentityOutcome(dir, declared(), ["model.malloy"]),
            );
            return pkg;
         }

         const sha256 = (bytes: Buffer | string) =>
            createHash("sha256").update(bytes).digest("hex");

         /** The pin as the files hash on disk: every file the walk read and the model, and the declaration. */
         function expectedSourceContentSha(): string {
            const { paths } = readPackageWorkflows(dir, declared());
            const shas = new Map(
               [...paths, "model.malloy"].map((p) => [
                  p,
                  sha256(fs.readFileSync(path.join(dir, p))),
               ]),
            );
            return computeSourceContentSha(shas, canonicalSha(declared()));
         }

         const withoutRevision = <T extends { source?: object }>(w: T) => ({
            ...w,
            ...(w.source && {
               source: { ...w.source, servedRevision: undefined },
            }),
         });

         it("is what the same walk reads in the main thread: workflows, sets, files and every hash", async () => {
            richPackage();
            const viaWorker = await load();
            const here = inProcess();

            expect(viaWorker.listWorkflows()).toEqual(here.listWorkflows());
            expect(viaWorker.listWorkflows().map((w) => w.name)).toEqual([
               "default",
               "one",
               "two",
            ]);
            for (const name of ["default", "one", "two"]) {
               const full = viaWorker.getWorkflow(name)!;
               expect(withoutRevision(full)).toEqual(
                  withoutRevision(here.getWorkflow(name)!),
               );
               for (const set of full.skill_sets) {
                  expect(
                     withoutRevision(
                        viaWorker.getWorkflow(name, "full", set.name)!,
                     ),
                  ).toEqual(
                     withoutRevision(here.getWorkflow(name, "full", set.name)!),
                  );
                  for (const file of set.skills ?? []) {
                     const read = await viaWorker.getWorkflowSkillFile(
                        name,
                        file.relative_filepath!,
                        set.name,
                     );
                     expect(read).toBeDefined();
                     expect(
                        withoutRevision(read as { source: object }),
                     ).toEqual(
                        withoutRevision(
                           (await here.getWorkflowSkillFile(
                              name,
                              file.relative_filepath!,
                              set.name,
                           )) as { source: object },
                        ),
                     );
                  }
               }
            }
            expect(viaWorker.listSkills().map((s) => s.name)).toEqual(
               here.listSkills().map((s) => s.name),
            );
            expect(viaWorker.getSourceContentSha()).toBe(
               here.getSourceContentSha(),
            );
            for (const name of ["default", "one", "two"]) {
               expect(viaWorker.getWorkflow(name)!.source.definitionSha).toBe(
                  here.getWorkflow(name)!.source.definitionSha,
               );
            }
         });

         it("keeps a skill's root files named like Object.prototype members, and the ones .skillignore and the assets/ rules leave out", async () => {
            richPackage();
            const pkg = await load();
            const files = pkg
               .getWorkflow("default")!
               .skill_sets[0]!.skills!.map((f) => f.relative_filepath);
            expect(files).toContain("house/reference/x.md");
            expect(files).not.toContain("house/ignored.txt");
            const skill = pkg.listSkills().find((s) => s.name === "house")!;
            for (const name of ["__proto__", "constructor", "toString"]) {
               expect(Object.hasOwn(skill.catalog.files, name)).toBe(true);
               expect(skill.catalog.files[name]!.size).toBe(
                  `root file ${name}\n`.length,
               );
            }
            expect(Object.hasOwn(skill.catalog.files, "ignored.txt")).toBe(
               false,
            );
            expect(pkg.listSkills().map((s) => s.name)).not.toContain("bad");
         });

         it("pins the package as the files hash on disk, the declaration and an instructions-only edit included", async () => {
            richPackage();
            const before = (await load()).getSourceContentSha();
            expect(before).toBe(expectedSourceContentSha());
            write("workflows/one.md", "Be one, differently.");
            const after = (await load()).getSourceContentSha();
            expect(after).not.toBe(before);
            expect(after).toBe(expectedSourceContentSha());
            write(`${HOUSE}/.skillignore`, "ignored.txt\n# edited\n");
            const afterIgnore = (await load()).getSourceContentSha();
            expect(afterIgnore).not.toBe(after);
            expect(afterIgnore).toBe(expectedSourceContentSha());
         });

         it("hands nothing to the main thread's disk: adopting reads no file", async () => {
            richPackage();
            const outcome = servingIdentityOutcome(dir, declared(), [
               "model.malloy",
            ]);
            const pkg = new Package(
               "env",
               "pkg",
               dir,
               { name: "pkg" } as never,
               [],
               new Map(),
            );
            const reads = FS_READS.map((fn) => spyOn(fsSync, fn));
            try {
               (
                  pkg as unknown as {
                     adoptOutcomeMetadata: (o: LoadPackageOutcome) => void;
                  }
               ).adoptOutcomeMetadata(outcome);
               expect(reads.map((spy) => spy.mock.calls.length)).toEqual(
                  FS_READS.map(() => 0),
               );
            } finally {
               for (const spy of reads) spy.mockRestore();
            }
            expect(pkg.getSourceContentSha()).toMatch(/^[0-9a-f]{64}$/);
         });

         it("reports a dry run's workflow warnings and unread skills/ without counting them", async () => {
            const harness = await startMetricsHarness();
            resetDeprecatedUsageMetricsForTest();
            try {
               write(
                  "skills/old/SKILL.md",
                  "---\nname: old\ndescription: d\n---\n\nbody\n",
               );
               manifest({
                  workflows: {
                     analyst: analyst(),
                     refused: { surprise: 1 },
                  },
               });
               const pkg = await Package.create(
                  "env",
                  "pkg",
                  dir,
                  malloyConfig,
                  false,
                  { dryRun: true },
               );
               const warnings = pkg.getPackageMetadata().warnings ?? [];
               expect(warnings.map((w) => w.code)).toContain(
                  "PACKAGE_SKILLS_DIR_NOT_READ",
               );
               expect(warnings.map((w) => w.message).join("\n")).toContain(
                  "Workflow 'refused' is not served",
               );
               expect(
                  await harness.collectCounter(
                     "publisher_deprecated_usage_total",
                     { feature: "skills_dir" },
                  ),
               ).toBe(0);
            } finally {
               resetDeprecatedUsageMetricsForTest();
               await harness.shutdown();
            }
         });
      });

      describe("a package whose skill sets cannot be read", () => {
         // A dot directory, which the package's own file listing skips, so only the walk reads it.
         const HIDDEN = ".sets/main";
         const unreadable =
            (fn: () => Promise<void>, others: Record<string, unknown> = {}) =>
            async () => {
               write(
                  `${HIDDEN}/conventions/SKILL.md`,
                  "---\nname: conventions\ndescription: d\n---\n\nbody\n",
               );
               manifest({
                  workflows: {
                     analyst: { ...analyst(), skill_sets: [HIDDEN] },
                     ...others,
                  },
               });
               fs.chmodSync(path.join(dir, HIDDEN), 0o000);
               try {
                  await fn();
               } finally {
                  fs.chmodSync(path.join(dir, HIDDEN), 0o755);
               }
            };

         it.skipIf(isRoot || isWindows)(
            "still loads its models, and answers the workflow routes 409 with the reason, not 404",
            unreadable(async () => {
               const pkg = await load();
               expect(pkg.getModelPaths()).toEqual(["model.malloy"]);
               const controller = new SkillController(storeFor(pkg));

               await expect(
                  controller.listWorkflows("env", "pkg"),
               ).rejects.toThrow(WorkflowsUnavailableError);
               await expect(
                  controller.getWorkflow("env", "pkg", "analyst"),
               ).rejects.toThrow(
                  `EACCES: permission denied, scandir '${HIDDEN}'`,
               );
               await expect(
                  controller.getWorkflowSkillFile(
                     "env",
                     "pkg",
                     "analyst",
                     "conventions/SKILL.md",
                  ),
               ).rejects.toBeInstanceOf(WorkflowsUnavailableError);
               // `analyst` is no `default`, so the package has no skills of its own to be unavailable.
               expect(pkg.listSkills()).toEqual([]);
               // A name the manifest never declared is still not found.
               await expect(
                  controller.getWorkflow("env", "pkg", "nope"),
               ).rejects.toBeInstanceOf(WorkflowNotFoundError);
            }),
         );

         it.skipIf(isRoot || isWindows)(
            "answers get_workflow with the reason as a tool error, and a name never declared as not found",
            unreadable(async () => {
               const pkg = await load();
               const tool = toolFor(pkg);
               const scopes = [{ environment: "env", package: "pkg" }];

               const listing = await tool({ scopes });
               expect(listing.isError).toBe(true);
               expect(listing.content[0]!.resource?.text).toContain(
                  `scandir '${HIDDEN}'`,
               );
               const named = await tool({ scopes, workflow_name: "analyst" });
               expect(named.isError).toBe(true);
               expect(named.content[0]!.resource?.text).toContain("EACCES");
               const undeclared = await tool({ scopes, workflow_name: "nope" });
               expect(undeclared.isError).toBeFalsy();
               const answer = JSON.parse(undeclared.content[0]!.resource!.text);
               expect(answer.workflow).toBeNull();
               expect(answer.message).toContain('No workflow named "nope"');
               // An empty list would read as a package with no workflows.
               expect(answer).not.toHaveProperty("availableWorkflows");
               expect(answer.message).toContain("EACCES");
            }),
         );

         it.skipIf(isRoot || isWindows)(
            "keeps a workflow refused at declaration not found, while another one's set cannot be read",
            unreadable(
               async () => {
                  const pkg = await load();
                  const controller = new SkillController(storeFor(pkg));
                  await expect(
                     controller.getWorkflow("env", "pkg", "refused"),
                  ).rejects.toBeInstanceOf(WorkflowNotFoundError);
                  await expect(
                     controller.getWorkflow("env", "pkg", "analyst"),
                  ).rejects.toBeInstanceOf(WorkflowsUnavailableError);
               },
               { refused: { surprise: 1 } },
            ),
         );

         it.skipIf(isRoot || isWindows)(
            "serves the bundled skills, and says nothing in get_context, when the package declares no default",
            unreadable(async () => {
               const pkg = await load();
               const controller = new SkillController(storeFor(pkg));
               const listed = await controller.listSkills("env", "pkg");
               expect(listed.length).toBeGreaterThan(0);
               expect(listed.every((skill) => skill.origin === "bundled")).toBe(
                  true,
               );
               const first = listed[0]!.name!;
               expect(
                  (await controller.getSkill("env", "pkg", first)).name,
               ).toBe(first);
               let handler: ((p: unknown) => Promise<ToolReply>) | undefined;
               registerGetSkillTool(
                  {
                     registerTool: (
                        _n: string,
                        _c: unknown,
                        h: (p: unknown) => Promise<ToolReply>,
                     ) => {
                        handler = h;
                     },
                  } as never,
                  storeFor(pkg),
                  BUNDLED_CATALOG,
               );
               const viaMcp = await handler!({
                  scopes: [{ environment: "env", package: "pkg" }],
                  skill_name: first,
               });
               expect(viaMcp.isError).toBeFalsy();
               expect(skillsFor({ pkgIndex: { pkg } } as never)).toEqual({});
            }),
         );

         it.skipIf(isRoot || isWindows)(
            "answers the reason, not an empty list, for the package's own skills when it declares a default",
            unreadable(
               async () => {
                  const pkg = await load();
                  const controller = new SkillController(storeFor(pkg));
                  await expect(
                     controller.listSkills("env", "pkg"),
                  ).rejects.toBeInstanceOf(WorkflowsUnavailableError);
                  await expect(
                     controller.getSkill("env", "pkg", "conventions"),
                  ).rejects.toBeInstanceOf(WorkflowsUnavailableError);
                  const reply = skillsFor({ pkgIndex: { pkg } } as never);
                  expect(reply).toEqual({
                     skills_note: expect.stringContaining(
                        `scandir '${HIDDEN}'`,
                     ),
                  });
               },
               { default: { skill_sets: [HIDDEN] } },
            ),
         );

         it.skipIf(isRoot || isWindows)(
            "serves the workflow, with a finding and no 409, when one skill's SKILL.md cannot be read",
            async () => {
               write(
                  `${MAIN}/readable/SKILL.md`,
                  "---\nname: readable\ndescription: d\n---\n\nbody\n",
               );
               manifest({ workflows: { analyst: analyst() } });
               const file = path.join(dir, MAIN, "conventions/SKILL.md");
               fs.chmodSync(file, 0o000);
               try {
                  const pkg = await load();
                  const workflow = pkg.getWorkflow("analyst")!;
                  expect(
                     workflow.skill_sets[0]!.skills!.map(
                        (f) => f.relative_filepath,
                     ),
                  ).toEqual(["readable/SKILL.md"]);
                  expect(workflow.warnings.join("\n")).toContain(
                     "cannot be read (EACCES",
                  );
                  expect(pkg.listWorkflows().map((w) => w.name)).toEqual([
                     "analyst",
                  ]);
               } finally {
                  fs.chmodSync(file, 0o644);
               }
            },
         );

         it.skipIf(isRoot || isWindows)(
            "makes the walk unavailable, and never refuses the workflow, for an instructions file that cannot be read",
            async () => {
               manifest({ workflows: { analyst: analyst() } });
               const file = path.join(dir, "workflows/analyst/instructions.md");
               fs.chmodSync(file, 0o000);
               try {
                  const pkg = await load();
                  const controller = new SkillController(storeFor(pkg));
                  await expect(
                     controller.getWorkflow("env", "pkg", "analyst"),
                  ).rejects.toBeInstanceOf(WorkflowsUnavailableError);
                  await expect(
                     controller.listWorkflows("env", "pkg"),
                  ).rejects.toBeInstanceOf(WorkflowsUnavailableError);
                  expect(
                     (pkg.getPackageMetadata().warnings ?? [])
                        .map((w) => w.message ?? "")
                        .join("\n"),
                  ).not.toContain("is not served");
               } finally {
                  fs.chmodSync(file, 0o644);
               }
            },
         );

         it.skipIf(isRoot || isWindows)(
            "serves its workflows again once the folder can be read and the package reloads",
            unreadable(async () => {
               const pkg = await load();
               expect(() => pkg.listWorkflows()).toThrow(
                  WorkflowsUnavailableError,
               );
               fs.chmodSync(path.join(dir, HIDDEN), 0o755);
               await pkg.reloadAllModels({});
               expect(pkg.listWorkflows().map((w) => w.name)).toEqual([
                  "analyst",
               ]);
            }),
         );
      });

      describe("a notebook in the package", () => {
         const NOTEBOOK =
            '>>>markdown\n# Notes\n>>>malloy\nrun: duckdb.sql("select 1 as id")\n';
         let pool: TamperingPool;
         let outcome: LoadPackageOutcome | undefined;
         beforeEach(async () => {
            pool = new TamperingPool(1);
            pool.tamper = (o) => {
               outcome = o;
            };
            outcome = undefined;
            await usePool(pool);
            manifest({});
            write("nb.malloynb", NOTEBOOK);
         });

         it("hashes into the source pin without joining the digests that keep a last good model serving", async () => {
            const pkg = await load();
            expect(
               Object.keys(outcome!.fileDigests!).some((url) =>
                  url.endsWith("nb.malloynb"),
               ),
            ).toBe(false);
            expect(outcome!.modelShas!.get("nb.malloynb")).toBe(sha(NOTEBOOK));
            const compiledFrom = (
               pkg as unknown as { compiledFrom: Map<string, unknown> }
            ).compiledFrom;
            expect(compiledFrom.has("nb.malloynb")).toBe(false);
            expect(compiledFrom.has("model.malloy")).toBe(true);
            const before = pkg.getSourceContentSha();
            write("nb.malloynb", `${NOTEBOOK}\n`);
            expect((await load()).getSourceContentSha()).not.toBe(before);
         });
      });

      describe("a skill set past the skill cap", () => {
         it("serves the first 64 skills by name and says so once", async () => {
            for (let i = 0; i < SKILLS_PER_SET_MAX + 6; i++) {
               const name = `s${String(i).padStart(3, "0")}`;
               write(
                  `${MAIN}/${name}/SKILL.md`,
                  `---\nname: ${name}\ndescription: d\n---\n\nbody\n`,
               );
            }
            manifest({ workflows: { analyst: analyst() } });
            const pkg = await load();
            const workflow = pkg.getWorkflow("analyst")!;
            const served = new Set(
               workflow.skill_sets[0]!.skills!.map(
                  (f) => f.relative_filepath!.split("/")[0],
               ),
            );
            // The fixture's own `conventions` skill sorts first and takes one of the 64.
            expect(served.size).toBe(SKILLS_PER_SET_MAX);
            expect(served.has("conventions")).toBe(true);
            expect(served.has("s062")).toBe(true);
            expect(served.has("s063")).toBe(false);
            expect(workflow.warnings).toHaveLength(1);
            expect(workflow.warnings[0]).toContain(
               `more than ${SKILLS_PER_SET_MAX} skill directories`,
            );
         });
      });

      describe("a worker that leaves out what the serving identity hashes", () => {
         let pool: TamperingPool;
         beforeEach(async () => {
            pool = new TamperingPool(1);
            await usePool(pool);
            manifest({ workflows: { analyst: analyst() } });
         });

         it("fails a create", async () => {
            pool.tamper = (o) => {
               (o.modelShas as Map<string, string>).delete("model.malloy");
            };
            await expect(load()).rejects.toBeInstanceOf(
               PackageLoadProtocolError,
            );
         });

         for (const [label, tamper] of [
            [
               "a model's sha",
               (o: LoadPackageOutcome) => {
                  (o.modelShas as Map<string, string>).delete("model.malloy");
               },
            ],
            [
               "a skill file's sha",
               (o: LoadPackageOutcome) => {
                  const shas = o.packageMetadata.workflowsRead!.walk
                     .contentShas as Map<string, string>;
                  shas.delete([...shas.keys()][0]!);
               },
            ],
            [
               "the whole walk",
               (o: LoadPackageOutcome) => {
                  delete o.packageMetadata.workflowsRead;
               },
            ],
         ] as const) {
            it(`fails a reload that lacks ${label}, and the package keeps serving what it had`, async () => {
               const pkg = await load();
               const served = {
                  revision: pkg.getServedRevision(),
                  sha: pkg.getSourceContentSha(),
                  workflows: pkg.listWorkflows(),
               };
               pool.tamper = tamper;
               await expect(pkg.reloadAllModels({})).rejects.toBeInstanceOf(
                  PackageLoadProtocolError,
               );
               expect(pkg.getServedRevision()).toBe(served.revision);
               expect(pkg.getSourceContentSha()).toBe(served.sha);
               expect(pkg.listWorkflows()).toEqual(served.workflows);
            });
         }
      });
   });
});
