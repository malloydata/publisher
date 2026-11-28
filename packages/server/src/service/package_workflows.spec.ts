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
import { logger } from "../logger";
import { skillsFor } from "../retrieval/search/response";
import { resetDeprecatedUsageMetricsForTest } from "../deprecated_usage_metrics";
import {
   PackageManifestError,
   WorkflowNotFoundError,
   SkillSetNotFoundError,
   SkillSetsUnavailableError,
} from "../errors";
import { registerGetSkillTool } from "../mcp/tools/get_skill_tool";
import { registerGetSkillSetTool } from "../mcp/tools/get_skill_set_tool";
import { registerGetWorkflowTool } from "../mcp/tools/get_workflow_tool";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
   type LoadPackageJob,
   type LoadPackageOutcome,
} from "../package_load/package_load_pool";
import { PackageLoadProtocolError } from "../package_load/protocol_decode";
import { expectWarning } from "../test_helpers/expect_warning";
import { servingIdentityOutcome } from "../test_helpers/serving_identity";
import { fakeOf } from "../test_helpers/store_fakes";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { compileSource, type CompileSourceHost } from "./compile_source";
import type { PackageLookup } from "./environment_ports";
import { Package } from "./package";
import {
   boundedDeclaration,
   computeSourceContentSha,
   MANIFEST_DECLARATION_MAX_BYTES,
} from "./package_revision";
import { BUNDLED_CATALOG, BUNDLED_SKILL_NAMES } from "./skills/package_skills";
import {
   SKILL_MD as HOSTILE_SKILL_MD,
   writeSkill as writeHostileSkill,
} from "./skills/__fixtures__/skill-directory/fixture";
import {
   readPackageWorkflows,
   SKILL_FILE_SKIPPED,
   SKILL_FINDING,
   SKILL_SHADOWED,
   SKILL_SETS_UNAVAILABLE,
   SKILLS_PER_SET_MAX,
   workflowFileServed,
   workflowsToWire,
} from "./skills/package_workflows";

const MAIN = "workflows/analyst/skill_sets/main";
const SECOND = "workflows/analyst/skill_sets/second";
/** The names the fixtures give the sets at MAIN and SECOND, unlike their folders' own. */
const HOUSE = "house";
const EXTRA = "extra";
const HOUSE_SET = { [HOUSE]: { path: MAIN } };
const TWO_SETS = { ...HOUSE_SET, [EXTRA]: { path: SECOND } };
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
      skillSets: [HOUSE],
   });
   const other = () => ({
      description: "Other",
      instructions: "workflows/other/instructions.md",
   });

   /** Declares the house set, unless `extra` declares its own `skillSets` or sets it to undefined for none. */
   function manifest(extra: Record<string, unknown>) {
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({
            name: "pkg",
            version: "1",
            skillSets: HOUSE_SET,
            ...extra,
         }),
      );
   }

   const load = () => Package.create("env", "pkg", dir, malloyConfig);

   /** A skill set's file as the tests that are not about a changed file expect it: served, or not served at all. */
   const fileOf = async (pkg: Package, skillSet: string, file: string) => {
      const read = await pkg.getSkillSetFile(skillSet, file);
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

   const setToolFor = (pkg: Package): Handler => {
      let handler: Handler | undefined;
      registerGetSkillSetTool(
         {
            registerTool: (_n: string, _config: unknown, h: Handler) => {
               handler = h;
            },
         } as never,
         storeFor(pkg),
      );
      return handler!;
   };

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
            skill_set_names: [HOUSE],
         },
         { name: "other", description: "Other", skill_set_names: [] },
      ]);
      expect(pkg.listSkillSets()).toEqual([
         { name: HOUSE, digest: expect.stringMatching(/^[0-9a-f]{64}$/) },
      ]);
      const metadata = pkg.getPackageMetadata();
      expect(metadata).not.toHaveProperty("workflows");
      expect(metadata).not.toHaveProperty("agents");
      expect(metadata.warnings ?? []).toEqual([]);

      expect(pkg.getWorkflow("analyst")).toEqual({
         name: "analyst",
         description: "Answers revenue questions",
         instructions: "Be careful with revenue.",
         skill_sets: [
            {
               name: HOUSE,
               path: MAIN,
               digest: expect.stringMatching(/^[0-9a-f]{64}$/),
               skills: [
                  {
                     relative_filepath: "conventions/SKILL.md",
                     file_contents:
                        "---\nname: conventions\ndescription: House rules.\n---\n\nUse net revenue.\n",
                     name: "conventions",
                     description: "House rules.",
                  },
               ],
               warnings: [],
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

   it("leaves a package that declares neither skillSets nor workflows untouched: nothing listed, no warnings", async () => {
      manifest({ skillSets: undefined });
      const pkg = await load();
      expect(pkg.getPackageMetadata().warnings ?? []).toEqual([]);
      expect(pkg.listWorkflows()).toEqual([]);
      expect(pkg.defaultSkills().skills).toEqual([]);
      expect(pkg.listSkillSets()).toEqual([]);
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

   it.each(["workflows", "skillSets"])(
      "lets a hostile %s value reach the main thread without failing the worker",
      async (key) => {
         for (const value of [
            "text",
            7,
            null,
            [1, 2],
            { a: { description: { x: 1 }, instructions: [], path: {} } },
            JSON.parse(
               '{"__proto__":{"description":"d","instructions":"x.md","path":"x"}}',
            ),
         ]) {
            manifest({ [key]: value });
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
      },
   );

   // Raw text: JSON.stringify of this value overflows the stack, which is the point.
   const writeDeepDeclaration = (key: "workflows" | "skillSets") => {
      const n = 50_000;
      const entry =
         key === "workflows"
            ? `{"description":"d","instructions":"x.md","x-deep":${"[".repeat(n)}${"]".repeat(n)}}`
            : `{"path":"x","x-deep":${"[".repeat(n)}${"]".repeat(n)}}`;
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         `{"name":"pkg","${key}":{"a":${entry}}}`,
      );
   };

   it.each(["workflows", "skillSets"] as const)(
      "serves a package whose %s value is nested past the stack",
      async (key) => {
         writeDeepDeclaration(key);
         const pkg = await load();
         expect(pkg.listWorkflows()).toEqual([]);
         expect(pkg.getSourceContentSha()).toMatch(/^[0-9a-f]{64}$/);
      },
   );

   it.each(["workflows", "skillSets"] as const)(
      "finishes a reload whose %s value is nested past the stack",
      async (key) => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         writeDeepDeclaration(key);
         await pkg.reloadAllModels({});
         expect(pkg.listWorkflows()).toEqual([]);
         expect(pkg.getSourceContentSha()).toMatch(/^[0-9a-f]{64}$/);
      },
   );

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
            workflows: { analyst: { ...analyst(), skillSets: [] } },
         });
         const afterSets = await shaOf();
         expect(afterSets).not.toBe(afterDescription);
         manifest({
            workflows: {
               analyst: { ...analyst(), skillSets: [] },
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
               skillSets: HOUSE_SET,
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
            skillSets: TWO_SETS,
            workflows: {
               analyst: { ...analyst(), skillSets: [HOUSE, EXTRA] },
            },
         });
         const digests = async () =>
            Object.fromEntries(
               (await load()).listSkillSets().map((s) => [s.name, s.digest]),
            );
         const before = await digests();
         write("workflows/analyst/instructions.md", "edited");
         write(`${SECOND}/extra/notes.md`, "# Notes\n");
         const after = await digests();
         expect(after[HOUSE]).toBe(before[HOUSE]);
         expect(after[EXTRA]).not.toBe(before[EXTRA]);
      });

      it("tells a package that declares neither key from one that declares an empty one", async () => {
         manifest({ skillSets: undefined });
         const without = await shaOf();
         manifest({ skillSets: undefined, workflows: {} });
         expect(await shaOf()).not.toBe(without);
         manifest({ skillSets: {} });
         expect(await shaOf()).not.toBe(without);
         manifest({ skillSets: undefined, description: "x" });
         expect(await shaOf()).toBe(without);
      });

      it("moves on an edit to the skillSets map alone, the files it names staying as they were", async () => {
         const base = await shaOf();
         manifest({
            skillSets: {
               [HOUSE]: { path: MAIN, description: "Now described" },
            },
            workflows: { analyst: analyst() },
         });
         expect(await shaOf()).not.toBe(base);
      });

      it("moves on an edit under a set no workflow lists, and watches it", async () => {
         write(
            `${SECOND}/lone/SKILL.md`,
            "---\nname: lone\ndescription: d\n---\n\nbody\n",
         );
         manifest({
            skillSets: TWO_SETS,
            workflows: { analyst: analyst() },
         });
         const pkg = await load();
         expect(pkg.workflowPaths()).toContain(SECOND);
         // What watch mode asks, by name, before it reloads on an edit.
         for (const edited of ["lone/SKILL.md", "lone/notes.md"]) {
            expect(
               workflowFileServed(
                  dir,
                  pkg.workflowPaths(),
                  `${SECOND}/${edited}`,
               ),
            ).toBe(true);
         }
         const base = pkg.getSourceContentSha();
         write(`${SECOND}/lone/notes.md`, "# Notes\n");
         expect((await load()).getSourceContentSha()).not.toBe(base);
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
                  `Skill set '${HOUSE}' skill finding:`,
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
         expectWarning(
            pkg.getSkillSet(HOUSE)!.warnings,
            SKILL_FINDING,
            HOUSE,
            "no description",
         );
         expect(pkg.getWorkflow("analyst")!.warnings).toEqual([]);
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
         expect(pkg.listSkillSets().map((s) => s.name)).toEqual([HOUSE]);
         expect(pkg.getPackageMetadata().description).toBe("patched");
         expect(pkg.listWorkflows().map((w) => w.name)).toEqual(["analyst"]);
      });
   });

   describe("the default set's skills", () => {
      const skillsOf = (pkg: Package) =>
         pkg.defaultSkills().skills.map((s) => s.name);

      it("are the package's advertised skills, and no other set's", async () => {
         write(
            `${SECOND}/extra/SKILL.md`,
            "---\nname: extra\ndescription: d\n---\n\nbody\n",
         );
         manifest({
            skillSets: { default: { path: MAIN }, [EXTRA]: { path: SECOND } },
         });
         const pkg = await load();
         expect(skillsOf(pkg)).toEqual(["conventions"]);
         expect(pkg.defaultSkills().skills[0]!.description).toBe(
            "House rules.",
         );
         expect(pkg.getPackageMetadata().warnings ?? []).toEqual([]);
      });

      it("are none for a package with no set named default", async () => {
         manifest({ workflows: { analyst: analyst() } });
         expect(skillsOf(await load())).toEqual([]);
      });

      it("are not those of a workflow named default, which is an ordinary workflow", async () => {
         manifest({
            workflows: {
               default: { ...analyst(), skillSets: [HOUSE] },
            },
         });
         const pkg = await load();
         expect(pkg.listWorkflows().map((w) => w.name)).toEqual(["default"]);
         expect(skillsOf(pkg)).toEqual([]);
      });

      it("follow a reload", async () => {
         manifest({ skillSets: { default: { path: MAIN } } });
         const pkg = await load();
         expect(skillsOf(pkg)).toEqual(["conventions"]);
         manifest({ workflows: { other: other() } });
         await pkg.reloadAllModels({});
         expect(skillsOf(pkg)).toEqual([]);
      });
   });

   describe("a manifest whose whole declaration is refused", () => {
      it.each([
         [
            "workflows is not an object",
            { workflows: "nope" },
            "'workflows' must be an object",
         ],
         [
            "skillSets is not an object",
            { skillSets: "nope", workflows: { analyst: analyst() } },
            "'skillSets' must be an object",
         ],
         [
            "workflows is over the size bound",
            { workflows: { big: "x".repeat(MANIFEST_DECLARATION_MAX_BYTES) } },
            "too deeply nested or over 1 MiB",
         ],
         [
            "the two together are over the size bound",
            {
               skillSets: {
                  a: "x".repeat(MANIFEST_DECLARATION_MAX_BYTES * 0.6),
               },
               workflows: {
                  b: "x".repeat(MANIFEST_DECLARATION_MAX_BYTES * 0.6),
               },
            },
            "too deeply nested or over 1 MiB together",
         ],
      ])(
         "answers any name WORKFLOW_NOT_SERVED or SKILL_SET_NOT_SERVED with the manifest warning's text when %s",
         async (_label, declared, text) => {
            manifest(declared);
            const pkg = await load();
            const controller = new SkillController(storeFor(pkg));
            const refused = (kind: "WORKFLOW" | "SKILL_SET") => ({
               reason: `${kind}_NOT_SERVED`,
               message: expect.stringContaining(text),
            });
            for (const name of ["analyst", "default"]) {
               await expect(
                  controller.getWorkflow("env", "pkg", name),
               ).rejects.toMatchObject(refused("WORKFLOW"));
               await expect(
                  controller.getSkillSet("env", "pkg", name),
               ).rejects.toMatchObject(refused("SKILL_SET"));
               await expect(
                  controller.getSkillSetFile(
                     "env",
                     "pkg",
                     name,
                     "conventions/SKILL.md",
                  ),
               ).rejects.toMatchObject(refused("SKILL_SET"));
            }
            const answer = JSON.parse(
               (
                  await toolFor(pkg)({
                     scopes: [{ environment: "env", package: "pkg" }],
                     workflow: "analyst",
                  })
               ).content[0]!.resource!.text,
            );
            expect(answer.workflow).toBeNull();
            expect(answer.reason).toBe("WORKFLOW_NOT_SERVED");
            expect(answer.message).toContain(text);
            // The warning is about no one workflow, so it has no code to read.
            const warning = (pkg.getPackageMetadata().warnings ?? []).find(
               (w) => w.message.includes(text),
            );
            expect(warning?.code).toBeUndefined();
         },
      );

      it("is read from the walk's own field, never inferred from a warning that has no code", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         (
            pkg as unknown as { workflows: { warnings: object[] } }
         ).workflows.warnings.push({ severity: "warn", message: "a note" });
         expect(pkg.workflowNotServed("nope")?.reason).toBe(
            "WORKFLOW_NOT_DECLARED",
         );
      });

      it("leaves an undeclared name's Fix clause off when asked, and keeps a refusal's own", async () => {
         manifest({ workflows: { analyst: analyst(), bad: { nope: 1 } } });
         const pkg = await load();
         const undeclared = pkg.workflowNotServed("nope", false)!;
         expect(undeclared.message).toBe(
            "No workflow named 'nope' is declared in package 'pkg'.",
         );
         expect(pkg.workflowNotServed("nope")!.message).toBe(
            `${undeclared.message} Fix: use a name the package lists.`,
         );
         expect(pkg.workflowNotServed("bad", false)!.message).toContain("Fix:");
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
               message:
                  '"agents" is no longer read. Fix: declare "workflows" instead; the "Migrating from agents and a root skills/" section of the packages guide shows how each agent maps onto one',
               severity: "warn",
            },
         ]);
         expect(await used("agents")).toBe(1);
         await pkg.reloadAllModels({});
         expect(await used("agents")).toBe(2);
      });

      it("warns of a root skills/ directory no set is declared at, with its code, and counts it once per load", async () => {
         write(
            "skills/old/SKILL.md",
            "---\nname: old\ndescription: d\n---\n\nbody\n",
         );
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         expect(pkg.defaultSkills().skills).toEqual([]);
         expect(
            (pkg.getPackageMetadata().warnings ?? []).filter(
               (w) => w.code === "PACKAGE_SKILLS_DIR_NOT_READ",
            ),
         ).toEqual([
            {
               code: "PACKAGE_SKILLS_DIR_NOT_READ",
               message:
                  'skills/ is no longer read, so none of its skills is served. Fix: add "skillSets": { "default": { "path": "skills" } } to publisher.json, or move each skill into a skill set folder and declare that folder as the "default" set',
               severity: "warn",
            },
         ]);
         expect(await used("skills_dir")).toBe(1);
         await pkg.reloadAllModels({});
         expect(await used("skills_dir")).toBe(2);
      });

      it("says nothing of skills/ when the set declared at it is refused, since the refusal says so, and counts nothing", async () => {
         write(
            "skills/old/SKILL.md",
            "---\nname: old\ndescription: d\n---\n\nbody\n",
         );
         manifest({
            skillSets: { default: { path: "skills", surprise: 1 } },
         });
         const pkg = await load();
         expect(pkg.defaultSkills().skills).toEqual([]);
         expect(codes(pkg)).toContain("skill-set-not-served");
         expect(codes(pkg)).not.toContain("PACKAGE_SKILLS_DIR_NOT_READ");
         expect(await used("skills_dir")).toBe(0);
      });

      it("says nothing of skills/ when the default set is declared at it", async () => {
         write(
            "skills/old/SKILL.md",
            "---\nname: old\ndescription: d\n---\n\nbody\n",
         );
         manifest({ skillSets: { default: { path: "skills" } } });
         const pkg = await load();
         expect(pkg.defaultSkills().skills.map((s) => s.name)).toEqual(["old"]);
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

   describe("a skill set served alone", () => {
      const DUPLICATE = `Workflow 'analyst' lists skill 'conventions' in both '${HOUSE}' and '${EXTRA}'; both sets are served whole and an installer keeps the copy in '${HOUSE}', listed first. Fix: rename one of the two skills, or list only one of the sets.`;

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
            skillSets: TWO_SETS,
            workflows: {
               analyst: { ...analyst(), skillSets: [HOUSE, EXTRA] },
            },
         });
      });

      it("returns the set the workflow embeds, with the package's pin and no definitionSha", async () => {
         const pkg = await load();
         const full = pkg.getWorkflow("analyst")!;
         const { source, ...bare } = pkg.getSkillSet(HOUSE)!;
         expect(bare).toEqual(full.skill_sets[0]);
         expect(source).toEqual({
            environment: "env",
            package: "pkg",
            sourceContentSha: full.source.sourceContentSha,
            servedRevision: full.source.servedRevision,
         });
      });

      it("carries that set's own findings, and the shadowing ones stay on the workflow", async () => {
         const pkg = await load();
         const second = pkg.getSkillSet(EXTRA)!;
         expectWarning(second.warnings, SKILL_FILE_SKIPPED, EXTRA, "a..b.py");
         expect(second.warnings.map((w) => w.message)).not.toContain(DUPLICATE);
         expect(pkg.getSkillSet(HOUSE)!.warnings).toEqual([]);
         const whole = pkg.getWorkflow("analyst")!.warnings;
         expectWarning(whole, SKILL_SHADOWED, "analyst", "conventions");
         expect(whole.map((w) => w.message)).toContain(DUPLICATE);
         // The set's own finding is on the set, never repeated on the workflow.
         expect(whole.map((w) => w.message).join("\n")).not.toContain(
            "a..b.py",
         );
      });

      it("is not served for a name no set declares, a workflow's name included", async () => {
         const pkg = await load();
         expect(pkg.getSkillSet("nope")).toBeUndefined();
         expect(pkg.getSkillSet("analyst")).toBeUndefined();
         expect(pkg.getWorkflow(HOUSE)).toBeUndefined();
      });

      const paths = (set: { skills: Array<{ relative_filepath: string }> }) =>
         set.skills.map((f) => f.relative_filepath);

      it("serves every set whole, a skill name two sets hold in both", async () => {
         const pkg = await load();
         for (const view of ["full", "summary"] as const) {
            expect(
               paths(pkg.getWorkflow("analyst", view)!.skill_sets[0]!),
            ).toEqual(["conventions/SKILL.md"]);
            expect(
               paths(pkg.getWorkflow("analyst", view)!.skill_sets[1]!),
            ).toEqual(["conventions/SKILL.md", "extra/SKILL.md"]);
            expect(paths(pkg.getSkillSet(EXTRA, view)!)).toEqual([
               "conventions/SKILL.md",
               "extra/SKILL.md",
            ]);
         }
         const [conventions] = pkg.getSkillSet(EXTRA)!.skills as Array<{
            file_contents: string;
         }>;
         expect(conventions!.file_contents).toContain("Second copy.");
      });

      it("keeps a set's skills when an earlier set gains a skill it holds, as its digest does", async () => {
         fs.rmSync(path.join(dir, MAIN, "conventions"), { recursive: true });
         write(
            `${MAIN}/other/SKILL.md`,
            "---\nname: other\ndescription: d\n---\n",
         );
         const before = (await load()).getSkillSet(EXTRA)!;
         expect(paths(before)).toEqual([
            "conventions/SKILL.md",
            "extra/SKILL.md",
         ]);

         write(
            `${MAIN}/conventions/SKILL.md`,
            "---\nname: conventions\ndescription: Now in main.\n---\n",
         );
         const after = (await load()).getSkillSet(EXTRA)!;
         expect(after.skills).toEqual(before.skills);
         expect(after.digest).toBe(before.digest);
      });

      it("reads a file inside the set it names, where two sets hold the name", async () => {
         const pkg = await load();
         const file = "conventions/SKILL.md";
         expect((await fileOf(pkg, HOUSE, file))!.file_contents).toContain(
            "Use net revenue.",
         );
         const picked = (await fileOf(pkg, EXTRA, file))!;
         expect(picked.file_contents).toContain("Second copy.");
         expect(picked.source).toEqual(pkg.getSkillSet(EXTRA)!.source!);
         expect(picked.digest).toBe(pkg.getSkillSet(EXTRA)!.digest);
         expect(await fileOf(pkg, EXTRA, "extra/SKILL.md")).toBeDefined();
         // A name the set does not hold is not served, though another set does.
         expect(await fileOf(pkg, HOUSE, "extra/SKILL.md")).toBeUndefined();
         expect(await fileOf(pkg, "nope", file)).toBeUndefined();
         expect(await pkg.getSkillSetFile("analyst", file)).toBeUndefined();
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

      it("serves a listed asset through the file lookup, REST and MCP, and nothing unlisted", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         write(
            `${ASSETS}/unlisted.malloy`,
            `source: x is duckdb.sql("select 1 as id")\n`,
         );
         write("secret.txt", "outside");
         const pkg = await load();
         const listed = `${LISTED}/index.html`;
         const set = pkg.getSkillSet(HOUSE)!;
         const expected = {
            relative_filepath: listed,
            file_contents: "<h1>hi</h1>",
            digest: set.digest,
            source: set.source!,
         };

         expect(await fileOf(pkg, HOUSE, listed)).toEqual(expected);
         expect(
            await new SkillController(storeFor(pkg)).getSkillSetFile(
               "env",
               "pkg",
               HOUSE,
               listed,
            ),
         ).toEqual(expected);
         const viaMcp = JSON.parse(
            (
               await setToolFor(pkg)({
                  scopes: [{ environment: "env", package: "pkg" }],
                  skill_set: HOUSE,
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
            expect(await fileOf(pkg, HOUSE, bad)).toBeUndefined();
         }
      });

      it("serves a file by its set, skill and path", async () => {
         write(
            `${SECOND}/extra/SKILL.md`,
            "---\nname: extra\ndescription: d\n---\n\nbody\n",
         );
         write(`${SECOND}/extra/scripts/run.py`, "print('hi')\n");
         manifest({
            skillSets: TWO_SETS,
            workflows: {
               analyst: { ...analyst(), skillSets: [HOUSE, EXTRA] },
            },
         });
         const pkg = await load();
         expect(
            (await fileOf(pkg, EXTRA, "extra/scripts/run.py"))?.file_contents,
         ).toBe("print('hi')\n");
         expect(
            (await fileOf(pkg, HOUSE, "conventions/SKILL.md"))?.file_contents,
         ).toContain("Use net revenue.");
         // A set serves its own files only, though a workflow lists both.
         expect(await fileOf(pkg, HOUSE, "extra/scripts/run.py")).toBe(
            undefined,
         );
         expect(await fileOf(pkg, "nope", "extra/SKILL.md")).toBe(undefined);
      });

      it("serves a skill name two sets hold from each set, its files only through the set that holds them", async () => {
         write(
            `${SECOND}/conventions/SKILL.md`,
            "---\nname: conventions\ndescription: d\n---\n\nSecond copy.\n",
         );
         write(
            `${SECOND}/conventions/reference/only-second.md`,
            "# Only here\n",
         );
         manifest({
            skillSets: TWO_SETS,
            workflows: {
               analyst: { ...analyst(), skillSets: [HOUSE, EXTRA] },
            },
         });
         const pkg = await load();
         const skill = async (set: string) =>
            (await fileOf(pkg, set, "conventions/SKILL.md"))?.file_contents ??
            "";
         expect(await skill(HOUSE)).toContain("Use net revenue.");
         expect(await skill(EXTRA)).toContain("Second copy.");
         expect(
            await fileOf(pkg, HOUSE, "conventions/reference/only-second.md"),
         ).toBeUndefined();
         expect(
            (await fileOf(pkg, EXTRA, "conventions/reference/only-second.md"))
               ?.file_contents,
         ).toBe("# Only here\n");
      });

      it("answers a listed asset that changed since the load as changed, on every surface, though it keeps its size", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         const listed = `${LISTED}/index.html`;
         write(`${ASSETS}/index.html`, "<h1>ho</h1>");
         expect(await pkg.getSkillSetFile(HOUSE, listed)).toEqual({
            changed: true,
         });
         await expect(
            new SkillController(storeFor(pkg)).getSkillSetFile(
               "env",
               "pkg",
               HOUSE,
               listed,
            ),
         ).rejects.toThrow("changed on disk after the package loaded");
         const viaMcp = JSON.parse(
            (
               await setToolFor(pkg)({
                  scopes: [{ environment: "env", package: "pkg" }],
                  skill_set: HOUSE,
                  skill_path: listed,
               })
            ).content[0]!.resource!.text,
         );
         // Not `skillSet: null`, which a caller reads as "the name did not match".
         expect("skillSet" in viaMcp).toBe(false);
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
         const plain = await fileOf(pkg, HOUSE, `${LISTED}/index.html`);
         expect(plain).toBeDefined();
         expect(await fileOf(pkg, HOUSE, `./${LISTED}/index.html`)).toEqual(
            plain,
         );
         expect(
            (await fileOf(pkg, HOUSE, "./conventions/SKILL.md"))
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
            await pkg.getSkillSetFile(HOUSE, `${LISTED}/index.html`),
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
         expect(warning).toStartWith(`Skill set '${HOUSE}' skill finding:`);
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
         const read = (await fileOf(pkg, HOUSE, `${LISTED}/logo.png`))!;
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
            await pkg.getSkillSetFile(HOUSE, `${LISTED}/index.html`),
         ).toEqual({ changed: true });
      });

      it("refuses a listed asset edited after the load, since it no longer matches its sha", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         write(`${ASSETS}/index.html`, "<h1>changed</h1>");
         expect(
            await pkg.getSkillSetFile(HOUSE, `${LISTED}/index.html`),
         ).toEqual({ changed: true });
      });

      /** `fs.promises.open` held at the first open of a file ending `suffix`, which `reached` announces; `release` lets it through. */
      const holdOpen = (suffix: string) => {
         const real = fsSync.promises.open;
         let release!: () => void;
         const held = new Promise<void>((resolve) => (release = resolve));
         let reach!: () => void;
         const reached = new Promise<void>((resolve) => (reach = resolve));
         const spy = spyOn(fsSync.promises, "open").mockImplementation(((
            file: Parameters<typeof real>[0],
            ...rest: unknown[]
         ) => {
            const open = () =>
               (real as (...args: unknown[]) => Promise<unknown>)(
                  file,
                  ...rest,
               );
            if (!String(file).endsWith(suffix)) return open();
            reach();
            return held.then(open);
         }) as never);
         return { reached, release, restore: () => spy.mockRestore() };
      };

      it("answers a read with the identity it began under when a reload lands while it is pending", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         const began = pkg.getSkillSet(HOUSE)!.source!;
         const hold = holdOpen("index.html");
         try {
            const pending = pkg.getSkillSetFile(HOUSE, `${LISTED}/index.html`);
            await hold.reached;
            write(
               "model.malloy",
               `source: t is duckdb.sql("select 2 as id")\n`,
            );
            await pkg.reloadAllModels({});
            expect(pkg.getSourceContentSha()).not.toBe(began.sourceContentSha);
            hold.release();
            expect(await pending).toMatchObject({
               file_contents: "<h1>hi</h1>",
               source: began,
            });
         } finally {
            hold.release();
            hold.restore();
         }
      });

      it.skipIf(process.platform === "win32")(
         "opens a listed file without following a link, waiting on it or letting it become the controlling terminal",
         async () => {
            write(`${ASSETS}/index.html`, "<h1>hi</h1>");
            const pkg = await load();
            const real = fsSync.promises.open;
            const flags: number[] = [];
            const spy = spyOn(fsSync.promises, "open").mockImplementation(((
               file: Parameters<typeof real>[0],
               flag: number,
               ...rest: unknown[]
            ) => {
               if (String(file).endsWith("index.html")) flags.push(flag);
               return (real as (...args: unknown[]) => Promise<unknown>)(
                  file,
                  flag,
                  ...rest,
               );
            }) as never);
            try {
               await fileOf(pkg, HOUSE, `${LISTED}/index.html`);
            } finally {
               spy.mockRestore();
            }
            expect(flags).toHaveLength(1);
            for (const flag of [
               fsSync.constants.O_NOFOLLOW,
               fsSync.constants.O_NONBLOCK,
               fsSync.constants.O_NOCTTY,
            ]) {
               expect(flags[0]! & flag).toBe(flag);
            }
         },
      );

      it("keeps a skill named __proto__ and serves its asset", async () => {
         write(
            `${MAIN}/__proto__/SKILL.md`,
            "---\nname: __proto__\ndescription: d\n---\n\nbody\n",
         );
         write(`${MAIN}/__proto__/assets/a.txt`, "x");
         const pkg = await load();
         expect(
            (await fileOf(pkg, HOUSE, "__proto__/assets/a.txt"))?.file_contents,
         ).toBe("x");
      });

      it("pins the hashes of a package whose workflows ship no assets", async () => {
         const pkg = await load();
         expect(pkg.getWorkflow("analyst")!.source.definitionSha).toBe(
            "a482a92c1b78d1d45b134156d952b415cda1dad9162129fc5cbc29923c564e22",
         );
         expect(pkg.getSourceContentSha()).toBe(
            "aced6284c25d47b331c136f51e89822ca2828cba56f67205ac44c59777cf40c5",
         );
      });
   });

   describe("REST and MCP surfaces", () => {
      it("serves the listings and the resolved definitions from the controller", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         const controller = new SkillController(storeFor(pkg));

         expect(await controller.listWorkflows("env", "pkg")).toEqual(
            pkg.listWorkflows(),
         );
         expect(await controller.listSkillSets("env", "pkg")).toEqual(
            pkg.listSkillSets(),
         );
         const workflow = await controller.getWorkflow("env", "pkg", "analyst");
         expect(Object.keys(workflow).sort()).toEqual([
            "description",
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
         const set = await controller.getSkillSet("env", "pkg", HOUSE);
         expect(Object.keys(set).sort()).toEqual([
            "digest",
            "name",
            "path",
            "skills",
            "source",
            "warnings",
         ]);
         expect(Object.keys(set.source!).sort()).toEqual([
            "environment",
            "package",
            "servedRevision",
            "sourceContentSha",
         ]);
         await expect(
            controller.getWorkflow("env", "pkg", "nope"),
         ).rejects.toBeInstanceOf(WorkflowNotFoundError);
         await expect(
            controller.getSkillSet("env", "pkg", "nope"),
         ).rejects.toMatchObject({ reason: "SKILL_SET_NOT_DECLARED" });
         await expect(
            controller.getSkillSet("env", "pkg", "analyst"),
         ).rejects.toBeInstanceOf(SkillSetNotFoundError);
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

         const resolved = body(await tool({ scopes, workflow: "analyst" }));
         expect(resolved.workflow).toEqual(pkg.getWorkflow("analyst"));

         const missing = body(await tool({ scopes, workflow: "nope" }));
         expect(missing.workflow).toBeNull();
         expect(missing.reason).toBe("WORKFLOW_NOT_DECLARED");
         expect(missing.message).toContain("Fix:");
         expect(missing.availableWorkflows).toHaveLength(1);
      });

      it("get_skill_set lists, resolves and reports a missing name", async () => {
         manifest({ workflows: { analyst: analyst() } });
         const pkg = await load();
         const tool = setToolFor(pkg);

         const listed = body(await tool({ scopes }));
         expect(listed.skillSet).toBeNull();
         expect(listed.availableSkillSets).toEqual(pkg.listSkillSets());

         const resolved = body(await tool({ scopes, skill_set: HOUSE }));
         expect(resolved.skillSet).toEqual(pkg.getSkillSet(HOUSE));

         const missing = body(await tool({ scopes, skill_set: "analyst" }));
         expect(missing.skillSet).toBeNull();
         expect(missing.reason).toBe("SKILL_SET_NOT_DECLARED");
         expect(missing.message).toContain("Fix:");
         expect(missing.availableSkillSets).toHaveLength(1);
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
            // The set route's summary is the workflow's embedded one, less the pin.
            const { source, ...bare } = pkg.getSkillSet(HOUSE, "summary")!;
            expect(bare).toEqual(summary.skill_sets[0]);
            expect(source!.sourceContentSha).toBe(
               summary.source.sourceContentSha,
            );
         });

         it("returns exactly one file with the set's pin, and refuses anything outside the set", async () => {
            const pkg = await loadWithReference();
            const set = pkg.getSkillSet(HOUSE)!;
            const controller = new SkillController(storeFor(pkg));
            const tool = setToolFor(pkg);
            const files = set.skills!;
            const paths = files.map((f) => f.relative_filepath);
            expect(paths).toContain("conventions/reference/glossary.md");

            for (const file of files) {
               const read = await controller.getSkillSetFile(
                  "env",
                  "pkg",
                  HOUSE,
                  file.relative_filepath!,
               );
               expect(read).toEqual({
                  relative_filepath: file.relative_filepath,
                  file_contents: file.file_contents,
                  digest: set.digest,
                  source: set.source!,
               });
               const viaMcp = body(
                  await tool({
                     scopes,
                     skill_set: HOUSE,
                     skill_path: file.relative_filepath,
                  }),
               );
               expect(viaMcp.skillFile).toEqual(read);
               expect(viaMcp).not.toHaveProperty("skillSet");
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
                  controller.getSkillSetFile("env", "pkg", HOUSE, bad),
               ).rejects.toBeInstanceOf(SkillSetNotFoundError);
               if (bad === "") continue;
               const miss = body(
                  await tool({ scopes, skill_set: HOUSE, skill_path: bad }),
               );
               expect("skillSet" in miss).toBe(false);
               expect(miss.skillFile.file_contents).toBeUndefined();
               expect(miss.skillFile.message).toContain("Fix:");
            }
            const noSet = body(await tool({ scopes, skill_path: paths[0]! }));
            expect(noSet.skillFile).toBeUndefined();
            expect(noSet.message).toContain("needs skill_set");
            await expect(
               controller.getSkillSetFile("env", "pkg", "nope", paths[0]!),
            ).rejects.toBeInstanceOf(SkillSetNotFoundError);
         });

         it("reads a skill's SKILL.md for its own folder, as get_skill does for a blank path", async () => {
            const pkg = await loadWithReference();
            const skillMd = (await fileOf(pkg, HOUSE, "conventions/SKILL.md"))!;
            expect(skillMd.file_contents).toContain("name: ");
            for (const alias of [
               "conventions/",
               "./conventions/",
               "conventions/./",
            ]) {
               expect(await fileOf(pkg, HOUSE, alias)).toEqual(skillMd);
            }
            // Still no file named for the skill alone, and none for no skill.
            expect(await pkg.getSkillSetFile(HOUSE, "conventions")).toBe(
               undefined,
            );
            expect(await pkg.getSkillSetFile(HOUSE, "")).toBe(undefined);
         });

         it("reads a file inside each set through the controller and get_skill_set, where two sets hold the name", async () => {
            write(
               `${SECOND}/conventions/SKILL.md`,
               "---\nname: conventions\ndescription: Second copy.\n---\n",
            );
            manifest({
               skillSets: TWO_SETS,
               workflows: {
                  analyst: { ...analyst(), skillSets: [HOUSE, EXTRA] },
               },
            });
            const pkg = await load();
            const controller = new SkillController(storeFor(pkg));
            const tool = setToolFor(pkg);
            const file = "conventions/SKILL.md";

            const viaRest = await controller.getSkillSetFile(
               "env",
               "pkg",
               EXTRA,
               file,
            );
            expect(viaRest.file_contents).toContain("Second copy.");
            const viaMcp = body(
               await tool({ scopes, skill_path: file, skill_set: EXTRA }),
            );
            expect(viaMcp.skillFile).toEqual(viaRest);
            const first = body(
               await tool({ scopes, skill_path: file, skill_set: HOUSE }),
            );
            expect(first.skillFile.file_contents).toContain("Use net revenue.");

            await expect(
               controller.getSkillSetFile(
                  "env",
                  "pkg",
                  HOUSE,
                  "extra/SKILL.md",
               ),
            ).rejects.toBeInstanceOf(SkillSetNotFoundError);
            const miss = body(
               await tool({
                  scopes,
                  skill_path: "extra/SKILL.md",
                  skill_set: "nope",
               }),
            );
            expect("skillFile" in miss).toBe(false);
            expect(miss.skillSet).toBeNull();
            expect(miss.reason).toBe("SKILL_SET_NOT_DECLARED");
            expect(miss.message).toContain("Fix:");
         });

         it("get_workflow and get_skill_set summaries match REST", async () => {
            const pkg = await loadWithReference();
            const controller = new SkillController(storeFor(pkg));

            const viaMcp = body(
               await toolFor(pkg)({
                  scopes,
                  workflow: "analyst",
                  skills: "summary",
               }),
            );
            expect(viaMcp.workflow).toEqual(
               await controller.getWorkflow("env", "pkg", "analyst", "summary"),
            );
            const dflt = body(
               await toolFor(pkg)({ scopes, workflow: "analyst" }),
            );
            expect(dflt.workflow).toEqual(pkg.getWorkflow("analyst"));
            expect(dflt.workflow.source.definitionSha).toBe(
               viaMcp.workflow.source.definitionSha,
            );

            const setViaMcp = body(
               await setToolFor(pkg)({
                  scopes,
                  skill_set: HOUSE,
                  skills: "summary",
               }),
            );
            expect(setViaMcp.skillSet).toEqual(
               await controller.getSkillSet("env", "pkg", HOUSE, "summary"),
            );
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

      /** Sends the worker the bundled skill names a test gives it, in place of the real ones. */
      class NamingPool extends PackageLoadPool {
         names: readonly string[] = [];
         override loadPackage(request: LoadPackageJob) {
            return super.loadPackage(
               request.readWorkflows
                  ? {
                       ...request,
                       readWorkflows: { bundledSkillNames: this.names },
                    }
                  : request,
            );
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
         const asked = { bundledSkillNames: BUNDLED_SKILL_NAMES };

         it("asks on a create and on a reload, with the bundled skill names, and not for a build plan refresh", async () => {
            const pkg = await load();
            expect(readsWorkflows()).toEqual([asked]);
            await pkg.reloadAllModels({});
            expect(readsWorkflows()).toEqual([asked, asked]);
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
         const HOUSE_DIR = "sets/a/house";
         const declared = () => ({
            skillSets: {
               default: { path: "sets/a" },
               beta: { path: "sets/b" },
            },
            workflows: {
               default: {
                  description: "Default",
                  instructions: "workflows/one.md",
                  skillSets: ["default"],
               },
               one: {
                  description: "One",
                  instructions: "workflows/one.md",
                  skillSets: ["default", "beta"],
               },
               two: {
                  description: "Two",
                  instructions: "workflows/one.md",
                  skillSets: ["default"],
               },
            },
         });

         function richPackage() {
            write(
               `${HOUSE_DIR}/SKILL.md`,
               "---\nname: house\ndescription: House rules.\n---\n\nUse net revenue.\n",
            );
            // Root files named like Object.prototype members, which a catalog keyed by plain assignment would lose.
            for (const name of ["__proto__", "constructor", "toString"]) {
               write(`${HOUSE_DIR}/${name}`, `root file ${name}\n`);
            }
            write(`${HOUSE_DIR}/reference/x.md`, "# X\n");
            write(`${HOUSE_DIR}/ignored.txt`, "left out");
            write(`${HOUSE_DIR}/.skillignore`, "ignored.txt\n");
            fs.mkdirSync(path.join(dir, HOUSE_DIR, "assets"), {
               recursive: true,
            });
            fs.writeFileSync(
               path.join(dir, HOUSE_DIR, "assets/logo.bin"),
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
            manifest(declared());
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
            const declaration = boundedDeclaration(declared());
            const { contentShas } = readPackageWorkflows(
               dir,
               declaration.value,
               [],
            );
            const shas = new Map(
               [...contentShas.keys(), "model.malloy"].map((p) => [
                  p,
                  sha256(fs.readFileSync(path.join(dir, p))),
               ]),
            );
            return computeSourceContentSha(shas, declaration.sha);
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
            }
            expect(viaWorker.listSkillSets()).toEqual(here.listSkillSets());
            for (const { name } of viaWorker.listSkillSets()) {
               const set = viaWorker.getSkillSet(name)!;
               expect(withoutRevision(set)).toEqual(
                  withoutRevision(here.getSkillSet(name)!),
               );
               for (const file of set.skills ?? []) {
                  const read = await viaWorker.getSkillSetFile(
                     name,
                     file.relative_filepath!,
                  );
                  expect(read).toBeDefined();
                  expect(withoutRevision(read as { source: object })).toEqual(
                     withoutRevision(
                        (await here.getSkillSetFile(
                           name,
                           file.relative_filepath!,
                        )) as { source: object },
                     ),
                  );
               }
            }
            expect(viaWorker.defaultSkills().skills.map((s) => s.name)).toEqual(
               here.defaultSkills().skills.map((s) => s.name),
            );
            expect(viaWorker.defaultSkills().overrides).toEqual(
               here.defaultSkills().overrides,
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
            const skill = pkg
               .defaultSkills()
               .skills.find((s) => s.name === "house")!;
            for (const name of ["__proto__", "constructor", "toString"]) {
               expect(Object.hasOwn(skill.catalog.files, name)).toBe(true);
               expect(skill.catalog.files[name]!.size).toBe(
                  `root file ${name}\n`.length,
               );
            }
            expect(Object.hasOwn(skill.catalog.files, "ignored.txt")).toBe(
               false,
            );
            expect(pkg.defaultSkills().skills.map((s) => s.name)).not.toContain(
               "bad",
            );
         });

         it("pins the package as the files hash on disk, the declaration and an instructions-only edit included", async () => {
            richPackage();
            const before = (await load()).getSourceContentSha();
            expect(before).toBe(expectedSourceContentSha());
            write("workflows/one.md", "Be one, differently.");
            const after = (await load()).getSourceContentSha();
            expect(after).not.toBe(before);
            expect(after).toBe(expectedSourceContentSha());
            write(`${HOUSE_DIR}/.skillignore`, "ignored.txt\n# edited\n");
            const afterIgnore = (await load()).getSourceContentSha();
            expect(afterIgnore).not.toBe(after);
            expect(afterIgnore).toBe(expectedSourceContentSha());
         });

         it("tells the worker the bundled skill names, so a clash is a finding the main thread adopts, and an override silences it", async () => {
            const bundled = BUNDLED_SKILL_NAMES[0]!;
            write(
               `${MAIN}/${bundled}/SKILL.md`,
               `---\nname: ${bundled}\ndescription: d\n---\n\nbody\n`,
            );
            manifest({ workflows: { analyst: analyst() } });
            const clash = (pkg: Package) =>
               (pkg.getPackageMetadata().warnings ?? []).filter((w) =>
                  w.message?.includes("is named like a bundled skill"),
               );
            expect(clash(await load())).toEqual([
               expect.objectContaining({
                  code: SKILL_FINDING,
                  subject: HOUSE,
                  severity: "warn",
               }),
            ]);
            manifest({
               skillSets: {
                  [HOUSE]: { path: MAIN, overrides: [bundled] },
               },
               workflows: { analyst: analyst() },
            });
            expect(clash(await load())).toEqual([]);
         });

         it("reads a clash against the names the worker is sent, never a catalog of its own", async () => {
            const bundled = BUNDLED_SKILL_NAMES[0]!;
            write(
               `${MAIN}/${bundled}/SKILL.md`,
               `---\nname: ${bundled}\ndescription: d\n---\n\nbody\n`,
            );
            manifest({ workflows: { analyst: analyst() } });
            const pool = new NamingPool(1);
            await usePool(pool);
            const clashes = async () =>
               (await load())
                  .getPackageMetadata()
                  .warnings?.filter((w) =>
                     w.message?.includes("is named like a bundled skill"),
                  ) ?? [];
            expect(await clashes()).toEqual([]);
            pool.names = [bundled];
            expect(await clashes()).toHaveLength(1);
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

      describe("a refused workflow's or skill set's log line", () => {
         const refusalLogs = (warn: { mock: { calls: unknown[][] } }) =>
            warn.mock.calls.filter(
               (call) => call[0] === "Workflow is not served",
            );

         it("is one warn per refused workflow per load, with the package, the workflow and the code", async () => {
            manifest({
               workflows: {
                  analyst: analyst(),
                  refused: { surprise: 1 },
                  "also-refused": { description: 1 },
               },
            });
            const warn = spyOn(logger, "warn").mockImplementation(() => logger);
            try {
               await load();
               const logs = refusalLogs(warn);
               expect(logs).toHaveLength(2);
               expect(
                  logs.map(([, fields]) => {
                     const { packageName, workflow, code } = fields as Record<
                        string,
                        string
                     >;
                     return { packageName, workflow, code };
                  }),
               ).toEqual([
                  {
                     packageName: "pkg",
                     workflow: "refused",
                     code: "workflow-not-served",
                  },
                  {
                     packageName: "pkg",
                     workflow: "also-refused",
                     code: "workflow-not-served",
                  },
               ]);
            } finally {
               warn.mockRestore();
            }
         });

         it("is one warn per refused skill set per load, with the package, the set and the code", async () => {
            manifest({
               skillSets: {
                  ...HOUSE_SET,
                  broken: { path: SECOND, surprise: 1 },
               },
               workflows: { analyst: analyst() },
            });
            const warn = spyOn(logger, "warn").mockImplementation(() => logger);
            try {
               await load();
               const logs = (warn.mock.calls as unknown[][]).filter(
                  (call) => call[0] === "Skill set is not served",
               );
               expect(logs).toHaveLength(1);
               const { packageName, skillSet, code } = logs[0]![1] as Record<
                  string,
                  string
               >;
               expect({ packageName, skillSet, code }).toEqual({
                  packageName: "pkg",
                  skillSet: "broken",
                  code: "skill-set-not-served",
               });
            } finally {
               warn.mockRestore();
            }
         });

         it("is not written for a dry run, which adopts the same walk", async () => {
            manifest({
               workflows: { analyst: analyst(), refused: { surprise: 1 } },
            });
            const warn = spyOn(logger, "warn").mockImplementation(() => logger);
            try {
               await Package.create("env", "pkg", dir, malloyConfig, false, {
                  dryRun: true,
               });
               expect(refusalLogs(warn)).toEqual([]);
            } finally {
               warn.mockRestore();
            }
         });

         it("is not written for a package whose workflows are all served", async () => {
            manifest({ workflows: { analyst: analyst() } });
            const warn = spyOn(logger, "warn").mockImplementation(() => logger);
            try {
               await load();
               expect(refusalLogs(warn)).toEqual([]);
            } finally {
               warn.mockRestore();
            }
         });
      });

      describe("a package whose skill sets cannot be read", () => {
         // A dot directory, which the package's own file listing skips, so only the walk reads it.
         const HIDDEN = ".sets/main";
         const unreadable =
            (
               fn: () => Promise<void>,
               {
                  set = "hidden",
                  workflows = {},
               }: { set?: string; workflows?: Record<string, unknown> } = {},
            ) =>
            async () => {
               write(
                  `${HIDDEN}/conventions/SKILL.md`,
                  "---\nname: conventions\ndescription: d\n---\n\nbody\n",
               );
               manifest({
                  skillSets: { [set]: { path: HIDDEN } },
                  workflows: {
                     analyst: { ...analyst(), skillSets: [set] },
                     ...workflows,
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
            "still loads its models, and answers the workflow and skill set routes 409 with the reason, not 404",
            unreadable(async () => {
               const pkg = await load();
               expect(pkg.getModelPaths()).toEqual(["model.malloy"]);
               const controller = new SkillController(storeFor(pkg));
               const eacces = `EACCES: permission denied, scandir '${HIDDEN}'`;

               await expect(
                  controller.listWorkflows("env", "pkg"),
               ).rejects.toThrow(SkillSetsUnavailableError);
               await expect(
                  controller.listSkillSets("env", "pkg"),
               ).rejects.toThrow(SkillSetsUnavailableError);
               await expect(
                  controller.getWorkflow("env", "pkg", "analyst"),
               ).rejects.toThrow(eacces);
               await expect(
                  controller.getSkillSet("env", "pkg", "hidden"),
               ).rejects.toThrow(eacces);
               await expect(
                  controller.getSkillSetFile(
                     "env",
                     "pkg",
                     "hidden",
                     "conventions/SKILL.md",
                  ),
               ).rejects.toBeInstanceOf(SkillSetsUnavailableError);
               // The unreadable set is no `default`, so the package has no skills of its own to be unavailable.
               expect(pkg.defaultSkills().skills).toEqual([]);
               // A name the manifest never declared is still not found.
               await expect(
                  controller.getWorkflow("env", "pkg", "nope"),
               ).rejects.toBeInstanceOf(WorkflowNotFoundError);
               await expect(
                  controller.getSkillSet("env", "pkg", "nope"),
               ).rejects.toMatchObject({ reason: "SKILL_SET_NOT_DECLARED" });
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
               const named = await tool({ scopes, workflow: "analyst" });
               expect(named.isError).toBe(true);
               expect(named.content[0]!.resource?.text).toContain("EACCES");
               const undeclared = await tool({ scopes, workflow: "nope" });
               expect(undeclared.isError).toBeFalsy();
               const answer = JSON.parse(undeclared.content[0]!.resource!.text);
               expect(answer.workflow).toBeNull();
               expect(answer.reason).toBe("WORKFLOW_NOT_DECLARED");
               expect(answer.message).toContain(
                  "No workflow named 'nope' is declared in package 'pkg'",
               );
               // An empty list would read as a package with no workflows.
               expect(answer).not.toHaveProperty("availableWorkflows");
               expect(answer.message).toContain("EACCES");
               // Its Fix clause sends the reader to a list this reply says cannot be made.
               expect(answer.message).not.toContain("Fix:");
            }),
         );

         it.skipIf(isRoot || isWindows)(
            "answers get_skill_set with the reason as a tool error, and a name never declared as not found",
            unreadable(async () => {
               const pkg = await load();
               const tool = setToolFor(pkg);
               const scopes = [{ environment: "env", package: "pkg" }];

               const listing = await tool({ scopes });
               expect(listing.isError).toBe(true);
               expect(listing.content[0]!.resource?.text).toContain(
                  `scandir '${HIDDEN}'`,
               );
               const named = await tool({ scopes, skill_set: "hidden" });
               expect(named.isError).toBe(true);
               expect(named.content[0]!.resource?.text).toContain("EACCES");
               const undeclared = await tool({ scopes, skill_set: "nope" });
               expect(undeclared.isError).toBeFalsy();
               const answer = JSON.parse(undeclared.content[0]!.resource!.text);
               expect(answer.skillSet).toBeNull();
               expect(answer.reason).toBe("SKILL_SET_NOT_DECLARED");
               expect(answer.message).toContain("EACCES");
               // An empty list would read as a package with no skill sets, and the Fix clause points at that list.
               expect(answer).not.toHaveProperty("availableSkillSets");
               expect(answer.message).not.toContain("Fix:");
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
                  ).rejects.toMatchObject({
                     reason: "WORKFLOW_NOT_SERVED",
                     message: expect.stringContaining(
                        "Workflow 'refused' is not served: ",
                     ),
                  });
                  await expect(
                     controller.getWorkflow("env", "pkg", "analyst"),
                  ).rejects.toBeInstanceOf(SkillSetsUnavailableError);
               },
               { workflows: { refused: { surprise: 1 } } },
            ),
         );

         it.skipIf(isRoot || isWindows)(
            "serves the bundled skills, and says nothing in get_context, when the unreadable set is not the default",
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
            "answers the reason, not an empty list, for the package's own skills when the unreadable set is the default",
            unreadable(
               async () => {
                  const pkg = await load();
                  const controller = new SkillController(storeFor(pkg));
                  await expect(
                     controller.listSkills("env", "pkg"),
                  ).rejects.toBeInstanceOf(SkillSetsUnavailableError);
                  await expect(
                     controller.getSkill("env", "pkg", "conventions"),
                  ).rejects.toBeInstanceOf(SkillSetsUnavailableError);
                  const reply = skillsFor({ pkgIndex: { pkg } } as never);
                  expect(reply).toEqual({
                     skills_note: expect.stringContaining(
                        `scandir '${HIDDEN}'`,
                     ),
                  });
               },
               { set: "default" },
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
                  expect(
                     workflow.skill_sets[0]!.warnings.map(
                        (w) => w.message,
                     ).join("\n"),
                  ).toContain("cannot be read (EACCES");
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
                  ).rejects.toBeInstanceOf(SkillSetsUnavailableError);
                  await expect(
                     controller.listWorkflows("env", "pkg"),
                  ).rejects.toBeInstanceOf(SkillSetsUnavailableError);
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
                  SkillSetsUnavailableError,
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
            manifest({ skillSets: undefined });
            write("nb.malloynb", NOTEBOOK);
         });

         it("hashes into the source pin without joining the digests that keep a last good model serving", async () => {
            const pkg = await load();
            expect(
               Object.keys(outcome!.fileDigests!).some((url) =>
                  url.endsWith("nb.malloynb"),
               ),
            ).toBe(false);
            // The worker ships the one hash, over the model and the notebook, which declare nothing else.
            expect(outcome!.sourceContentSha).toBe(
               computeSourceContentSha(
                  new Map([
                     [
                        "model.malloy",
                        sha(
                           fs.readFileSync(
                              path.join(dir, "model.malloy"),
                              "utf8",
                           ),
                        ),
                     ],
                     ["nb.malloynb", sha(NOTEBOOK)],
                  ]),
               ),
            );
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

      describe("a SKILL.md with a byte-order mark, CRLF and keys a host trusts", () => {
         const HOSTILE = "sets/hostile";
         const SET_NAME = "hostile";
         const FILE = "pricing/SKILL.md";
         const contentsIn = (set: {
            skills: Array<{
               relative_filepath: string;
               file_contents?: string;
            }>;
         }) =>
            set.skills.find((f) => f.relative_filepath === FILE)?.file_contents;

         it("comes back byte for byte from every REST read that carries it, and from get_skill_set and get_workflow", async () => {
            writeHostileSkill(dir, HOSTILE);
            manifest({
               skillSets: { [SET_NAME]: { path: HOSTILE } },
               workflows: {
                  analyst: { ...analyst(), skillSets: [SET_NAME] },
               },
            });
            const pkg = await load();
            const controller = new SkillController(storeFor(pkg));
            const scopes = [{ environment: "env", package: "pkg" }];
            const text = (reply: ToolReply) =>
               JSON.parse(reply.content[0]!.resource!.text);

            const set = await controller.getSkillSet("env", "pkg", SET_NAME);
            expect(contentsIn(set)).toBe(HOSTILE_SKILL_MD);
            const file = await controller.getSkillSetFile(
               "env",
               "pkg",
               SET_NAME,
               FILE,
            );
            expect(file.file_contents).toBe(HOSTILE_SKILL_MD);
            const workflow = await controller.getWorkflow(
               "env",
               "pkg",
               "analyst",
            );
            expect(contentsIn(workflow.skill_sets[0]!)).toBe(HOSTILE_SKILL_MD);

            const viaSet = text(
               await setToolFor(pkg)({ scopes, skill_set: SET_NAME }),
            );
            expect(contentsIn(viaSet.skillSet)).toBe(HOSTILE_SKILL_MD);
            const viaFile = text(
               await setToolFor(pkg)({
                  scopes,
                  skill_set: SET_NAME,
                  skill_path: FILE,
               }),
            );
            expect(viaFile.skillFile.file_contents).toBe(HOSTILE_SKILL_MD);
            const viaWorkflow = text(
               await toolFor(pkg)({ scopes, workflow: "analyst" }),
            );
            expect(contentsIn(viaWorkflow.workflow.skill_sets[0])).toBe(
               HOSTILE_SKILL_MD,
            );
         });
      });

      describe("a default skill named like a bundled skill", () => {
         it("is served in its place only when the default set overrides the name, and the set route serves it either way", async () => {
            const bundled = BUNDLED_SKILL_NAMES[0]!;
            write(
               `${MAIN}/${bundled}/SKILL.md`,
               `---\nname: ${bundled}\ndescription: ours\n---\n\nOUR BODY\n`,
            );
            for (const [overrides, ours] of [
               [[], false],
               [[bundled], true],
            ] as const) {
               manifest({
                  skillSets: {
                     default: { path: MAIN, overrides: [...overrides] },
                  },
               });
               const pkg = await load();
               const controller = new SkillController(storeFor(pkg));
               const listed = (await controller.listSkills("env", "pkg")).find(
                  (skill) => skill.name === bundled,
               );
               expect(listed?.origin).toBe(ours ? "package" : "bundled");
               const read = await controller.getSkill("env", "pkg", bundled);
               expect((read.content ?? "").includes("OUR BODY")).toBe(ours);
               const set = await controller.getSkillSet(
                  "env",
                  "pkg",
                  "default",
               );
               expect(
                  set.skills.find(
                     (f) => f.relative_filepath === `${bundled}/SKILL.md`,
                  )?.file_contents,
               ).toContain("OUR BODY");
            }
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
            const { warnings } = pkg.getSkillSet(HOUSE)!;
            expect(warnings).toHaveLength(1);
            expectWarning(
               warnings,
               SKILL_FINDING,
               HOUSE,
               `more than ${SKILLS_PER_SET_MAX} skill directories`,
            );
            expect(workflow.warnings).toEqual([]);
         });
      });

      describe("a walk over a package path that no longer resolves", () => {
         let pool: TamperingPool;
         beforeEach(async () => {
            pool = new TamperingPool(1);
            await usePool(pool);
            manifest({ workflows: { analyst: analyst() } });
            // What the worker ships when `realpath` of the package fails, made by the same reader.
            pool.tamper = (o) => {
               o.packageMetadata.workflowsRead = {
                  walk: workflowsToWire(
                     readPackageWorkflows(
                        path.join(dir, "gone"),
                        {
                           skillSets: HOUSE_SET,
                           workflows: { analyst: analyst() },
                        },
                        [],
                     ),
                  ),
               };
            };
         });

         it("answers every set and workflow read 409, never 404, though the set it names is not on disk, and a name never declared 404", async () => {
            const pkg = await load();
            expect(pkg.getModelPaths()).toEqual(["model.malloy"]);
            const controller = new SkillController(storeFor(pkg));
            for (const call of [
               () => controller.listWorkflows("env", "pkg"),
               () => controller.getWorkflow("env", "pkg", "analyst"),
               () => controller.listSkillSets("env", "pkg"),
               () => controller.getSkillSet("env", "pkg", HOUSE),
               () =>
                  controller.getSkillSetFile(
                     "env",
                     "pkg",
                     HOUSE,
                     "conventions/SKILL.md",
                  ),
            ]) {
               await expect(call()).rejects.toBeInstanceOf(
                  SkillSetsUnavailableError,
               );
            }
            await expect(
               controller.getWorkflow("env", "pkg", "nope"),
            ).rejects.toBeInstanceOf(WorkflowNotFoundError);
            await expect(
               controller.getSkillSet("env", "pkg", "nope"),
            ).rejects.toMatchObject({ reason: "SKILL_SET_NOT_DECLARED" });
            const unavailable = (
               pkg.getPackageMetadata().warnings ?? []
            ).filter((w) => w.code === SKILL_SETS_UNAVAILABLE);
            expect(unavailable).toHaveLength(1);
            expect(unavailable[0]!.subject).toBeUndefined();
         });

         it("answers get_workflow and get_skill_set with the reason as a tool error", async () => {
            const pkg = await load();
            const scopes = [{ environment: "env", package: "pkg" }];
            expect((await toolFor(pkg)({ scopes })).isError).toBe(true);
            expect(
               (await toolFor(pkg)({ scopes, workflow: "analyst" })).isError,
            ).toBe(true);
            expect((await setToolFor(pkg)({ scopes })).isError).toBe(true);
            expect(
               (await setToolFor(pkg)({ scopes, skill_set: HOUSE })).isError,
            ).toBe(true);
            const undeclared = await toolFor(pkg)({ scopes, workflow: "nope" });
            expect(undeclared.isError).toBeFalsy();
            expect(
               JSON.parse(undeclared.content[0]!.resource!.text).reason,
            ).toBe("WORKFLOW_NOT_DECLARED");
         });
      });

      describe("a worker that leaves out what the serving identity needs", () => {
         let pool: TamperingPool;
         beforeEach(async () => {
            pool = new TamperingPool(1);
            await usePool(pool);
            manifest({ workflows: { analyst: analyst() } });
         });

         it("fails a create", async () => {
            pool.tamper = (o) => {
               delete o.sourceContentSha;
            };
            await expect(load()).rejects.toBeInstanceOf(
               PackageLoadProtocolError,
            );
         });

         for (const [label, tamper] of [
            [
               "the source content sha",
               (o: LoadPackageOutcome) => {
                  delete o.sourceContentSha;
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
