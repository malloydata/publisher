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
} from "bun:test";
import { createHash } from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { SkillController } from "../controller/skill.controller";
import { resetDeprecatedUsageMetricsForTest } from "../deprecated_usage_metrics";
import { PackageManifestError, WorkflowNotFoundError } from "../errors";
import { registerGetWorkflowTool } from "../mcp/tools/get_workflow_tool";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { fakeOf } from "../test_helpers/store_fakes";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import type { PackageLookup } from "./environment_ports";
import { Package } from "./package";
import { computeSourceContentSha } from "./package_revision";

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
         // A name both sets hold gives the workflow a warning of its own.
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

      it("carries the workflow's own warnings and that set's findings, not another set's", async () => {
         const pkg = await load();
         const second = pkg.getWorkflow("analyst", "full", "second")!;
         expect(second.warnings).toContain(DUPLICATE);
         expect(second.warnings.join("\n")).toContain("a..b.py");
         const main = pkg.getWorkflow("analyst", "full", "main")!;
         expect(main.warnings).toContain(DUPLICATE);
         expect(main.warnings.join("\n")).not.toContain("a..b.py");
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

         expect(pkg.getWorkflowSkillFile("analyst", listed)).toEqual(expected);
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
            expect(pkg.getWorkflowSkillFile("analyst", bad)).toBeUndefined();
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
            pkg.getWorkflowSkillFile("analyst", "extra/scripts/run.py")
               ?.file_contents,
         ).toBe("print('hi')\n");
         expect(
            pkg.getWorkflowSkillFile("analyst", "conventions/SKILL.md")
               ?.file_contents,
         ).toContain("Use net revenue.");
         expect(pkg.getWorkflowSkillFile("nope", "extra/SKILL.md")).toBe(
            undefined,
         );
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
         const skill = (pkg: Package) =>
            pkg.getWorkflowSkillFile("analyst", "conventions/SKILL.md")
               ?.file_contents ?? "";
         expect(skill(first)).toContain("Use net revenue.");
         expect(skill(first)).not.toContain("Second copy.");
         expect(
            first.getWorkflowSkillFile(
               "analyst",
               "conventions/reference/only-second.md",
            ),
         ).toBeUndefined();

         const reversed = await served([SECOND, MAIN]);
         expect(skill(reversed)).toContain("Second copy.");
         expect(
            reversed.getWorkflowSkillFile(
               "analyst",
               "conventions/reference/only-second.md",
            )?.file_contents,
         ).toBe("# Only here\n");
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
            pkg.getWorkflowSkillFile("analyst", `${LISTED}/index.html`),
         ).toBeUndefined();
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

      it("hashes a precomputed asset sha instead of reading the file again", () => {
         write("a.txt", "one");
         const shas = new Map([["a.txt", sha("listed")]]);
         const before = computeSourceContentSha(
            dir,
            ["a.txt"],
            undefined,
            shas,
         );
         write("a.txt", "two");
         expect(computeSourceContentSha(dir, ["a.txt"], undefined, shas)).toBe(
            before,
         );
         expect(computeSourceContentSha(dir, ["a.txt"])).not.toBe(before);
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
         const read = pkg.getWorkflowSkillFile(
            "analyst",
            `${LISTED}/logo.png`,
         )!;
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
            pkg.getWorkflowSkillFile("analyst", `${LISTED}/index.html`),
         ).toBeUndefined();
      });

      it("refuses a listed asset edited after the load, since it no longer matches its sha", async () => {
         write(`${ASSETS}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         write(`${ASSETS}/index.html`, "<h1>changed</h1>");
         expect(
            pkg.getWorkflowSkillFile("analyst", `${LISTED}/index.html`),
         ).toBeUndefined();
      });

      it("keeps a skill named __proto__ and serves its asset", async () => {
         write(
            `${MAIN}/__proto__/SKILL.md`,
            "---\nname: __proto__\ndescription: d\n---\n\nbody\n",
         );
         write(`${MAIN}/__proto__/assets/a.txt`, "x");
         const pkg = await load();
         expect(
            pkg.getWorkflowSkillFile("analyst", "__proto__/assets/a.txt")
               ?.file_contents,
         ).toBe("x");
      });

      it("pins the hashes of a package whose workflows ship no assets", async () => {
         const pkg = await load();
         expect(pkg.getWorkflow("analyst")!.source.definitionSha).toBe(
            "1c4c1c2eee109b85a7da409d3459cc53feb60857e630af09dc52e067e6d0f48d",
         );
         expect(pkg.getSourceContentSha()).toBe(
            "8f00adde5bd6cee01894a3d90776a20624db003cdf21790c115603efc842e073",
         );
      });
   });

   describe("REST and MCP surfaces", () => {
      const storeFor = (pkg: Package): PackageLookup =>
         fakeOf<PackageLookup>({
            getEnvironment: async () => ({
               getPackage: async () => pkg,
            }),
         });

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

      type Handler = (params: unknown) => Promise<{
         isError?: boolean;
         content: Array<{ resource?: { text: string } }>;
      }>;

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
               expect(miss.skillFile).toBeUndefined();
               expect(miss.message).toContain("Fix:");
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
});
