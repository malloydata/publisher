// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Package agents end to end through `Package.create`, which sends publisher.json
 * through the package-load worker pool, the path production takes. A mock of
 * the worker would hide a handoff that forgets `agents`.
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
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { SkillController } from "../controller/skill.controller";
import { AgentNotFoundError, PackageManifestError } from "../errors";
import { registerGetAgentTool } from "../mcp/tools/get_agent_tool";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Package } from "./package";
import type { EnvironmentStore } from "./environment_store";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

describe("package agents", () => {
   let dir: string;
   let duckdb: { close: () => Promise<void> };
   let malloyConfig: import("@malloydata/malloy").MalloyConfig;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
   });

   beforeEach(async () => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-agents-"));
      fs.writeFileSync(
         path.join(dir, "model.malloy"),
         `source: t is duckdb.sql("select 1 as id")\n`,
      );
      write("agents/analyst/instructions.md", "Be careful with revenue.");
      write(
         "agents/analyst/skills/conventions/SKILL.md",
         "---\nname: conventions\ndescription: House rules.\n---\n\nUse net revenue.\n",
      );
      write("agents/analyst/tasks/weekly.md", "Summarize last week.");
      write("agents/other/instructions.md", "Other brief.");
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

   const analyst = () => ({
      description: "Answers revenue questions",
      instructions: "agents/analyst/instructions.md",
      skills: ["agents/analyst/skills"],
      schedules: [
         { cron: "0 13 * * MON", task: "agents/analyst/tasks/weekly.md" },
      ],
   });
   const other = () => ({
      description: "Other",
      instructions: "agents/other/instructions.md",
   });

   function manifest(extra: Record<string, unknown>) {
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", version: "1", ...extra }),
      );
   }

   const load = () => Package.create("env", "pkg", dir, malloyConfig);

   it("carries agents through the worker, the metadata and the lookup", async () => {
      manifest({ agents: { analyst: analyst(), other: other() } });
      const pkg = await load();

      const metadata = pkg.getPackageMetadata();
      expect(metadata.agents).toEqual([
         {
            name: "analyst",
            description: "Answers revenue questions",
            model: "inherit",
            schedules: [
               { cron: "0 13 * * MON", task: "agents/analyst/tasks/weekly.md" },
            ],
         },
         {
            name: "other",
            description: "Other",
            model: "inherit",
            schedules: [],
         },
      ]);
      expect(metadata.warnings ?? []).toEqual([]);

      const agent = pkg.getAgent("analyst")!;
      expect(agent).toEqual({
         name: "analyst",
         description: "Answers revenue questions",
         model: "inherit",
         instructions: "Be careful with revenue.",
         skills: [
            {
               relative_filepath: "conventions/SKILL.md",
               file_contents:
                  '---\nname: "conventions"\ndescription: "House rules."\n---\n\nUse net revenue.\n',
            },
         ],
         schedules: [
            {
               cron: "0 13 * * MON",
               task: "agents/analyst/tasks/weekly.md",
               taskContent: "Summarize last week.",
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
      expect(pkg.getAgent("missing")).toBeUndefined();
   });

   it("leaves a package without agents untouched: no key, no warnings", async () => {
      manifest({});
      const pkg = await load();
      const metadata = pkg.getPackageMetadata();
      expect("agents" in metadata && metadata.agents !== undefined).toBe(false);
      expect(metadata.warnings ?? []).toEqual([]);
      expect(pkg.listAgents()).toEqual([]);
   });

   it("serves the package and the good agents when one agent is bad", async () => {
      manifest({
         agents: {
            analyst: analyst(),
            bad: { ...other(), tools: ["Bash"] },
            "Also Bad": other(),
         },
      });
      const pkg = await load();
      expect(pkg.listAgents().map((a) => a.name)).toEqual(["analyst"]);
      const warnings = (pkg.getPackageMetadata().warnings ?? []).map(
         (w) => w.message ?? "",
      );
      expect(warnings).toHaveLength(2);
      expect(warnings.join("\n")).toContain("Package agent 'bad'");
      expect(warnings.join("\n")).toContain("Fix:");
   });

   it("lets a hostile agents value reach the main thread without failing the worker", async () => {
      for (const agents of [
         "text",
         7,
         null,
         [1, 2],
         { a: { description: { x: 1 }, instructions: [] } },
         JSON.parse('{"__proto__":{"description":"d","instructions":"x.md"}}'),
      ]) {
         manifest({ agents });
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
         expect(pkg?.listAgents() ?? []).toEqual([]);
      }
   });

   describe("serving identity", () => {
      const shaOf = async () => (await load()).getSourceContentSha();

      beforeEach(() => manifest({ agents: { analyst: analyst() } }));

      it("moves on an instructions, skill or task edit", async () => {
         const base = await shaOf();
         write("agents/analyst/instructions.md", "edited");
         const afterInstructions = await shaOf();
         expect(afterInstructions).not.toBe(base);
         write(
            "agents/analyst/skills/conventions/SKILL.md",
            "---\nname: conventions\ndescription: House rules.\n---\n\nEdited.\n",
         );
         const afterSkill = await shaOf();
         expect(afterSkill).not.toBe(afterInstructions);
         write("agents/analyst/tasks/weekly.md", "edited");
         expect(await shaOf()).not.toBe(afterSkill);
      });

      it("moves on a model, cron or skills-list edit", async () => {
         const base = await shaOf();
         manifest({ agents: { analyst: { ...analyst(), model: "sonnet" } } });
         const afterModel = await shaOf();
         expect(afterModel).not.toBe(base);
         manifest({
            agents: {
               analyst: {
                  ...analyst(),
                  model: "sonnet",
                  schedules: [
                     {
                        cron: "0 14 * * MON",
                        task: "agents/analyst/tasks/weekly.md",
                     },
                  ],
               },
            },
         });
         const afterCron = await shaOf();
         expect(afterCron).not.toBe(afterModel);
         manifest({
            agents: {
               analyst: { ...analyst(), model: "sonnet", skills: [] },
            },
         });
         expect(await shaOf()).not.toBe(afterCron);
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
               agents: { analyst: analyst() },
            }),
         );
         expect(await shaOf()).toBe(base);
      });

      it("keeps one agent's definitionSha when another agent is edited", async () => {
         manifest({ agents: { analyst: analyst(), other: other() } });
         const before = (await load()).getAgent("other")!.source!.definitionSha;
         write("agents/analyst/instructions.md", "edited");
         expect((await load()).getAgent("other")!.source!.definitionSha).toBe(
            before,
         );
      });

      it("matches a package with no agents key to its pre-agents sha", async () => {
         manifest({});
         const withoutKey = await shaOf();
         manifest({ agents: {} });
         expect(await shaOf()).not.toBe(withoutKey);
         manifest({ description: "x" });
         expect(await shaOf()).toBe(withoutKey);
      });
   });

   describe("reload and metadata PATCH", () => {
      it("picks up an agent edit on reload and re-pins the sha", async () => {
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         const before = pkg.getSourceContentSha();
         const beforeDefinition =
            pkg.getAgent("analyst")!.source!.definitionSha;

         write("agents/analyst/instructions.md", "after reload");
         manifest({
            agents: { analyst: analyst(), other: other() },
         });
         await pkg.reloadAllModels({});

         expect(pkg.getAgent("analyst")!.instructions).toBe("after reload");
         expect(pkg.getAgent("analyst")!.source!.definitionSha).not.toBe(
            beforeDefinition,
         );
         expect(pkg.getAgent("other")).toBeDefined();
         expect(pkg.getSourceContentSha()).not.toBe(before);
         expect(pkg.getAgent("analyst")!.source!.sourceContentSha).toBe(
            pkg.getSourceContentSha(),
         );
      });

      it("drops an agent on reload when the manifest removes it", async () => {
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         manifest({});
         await pkg.reloadAllModels({});
         expect(pkg.listAgents()).toEqual([]);
         expect(pkg.getPackageMetadata().agents).toBeUndefined();
      });

      it("keeps agents across a metadata PATCH and ignores agents in the body", async () => {
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         // Environment.updatePackage replaces the stored metadata exactly like this.
         pkg.setPackageMetadata({
            name: "pkg",
            description: "patched",
            agents: [
               {
                  name: "injected",
                  description: "x",
                  model: "x",
                  schedules: [],
               },
            ],
         });
         const metadata = pkg.getPackageMetadata();
         expect(metadata.description).toBe("patched");
         expect(metadata.agents?.map((a) => a.name)).toEqual(["analyst"]);
      });
   });

   describe("REST and MCP surfaces", () => {
      const storeFor = (pkg: Package): EnvironmentStore =>
         ({
            getEnvironment: async () => ({
               getPackage: async () => pkg,
            }),
         }) as unknown as EnvironmentStore;

      it("serves the listing and the resolved definition from the controller", async () => {
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         const controller = new SkillController(storeFor(pkg));

         expect(await controller.listAgents("env", "pkg")).toEqual(
            pkg.listAgents(),
         );
         const agent = await controller.getAgent("env", "pkg", "analyst");
         expect(Object.keys(agent).sort()).toEqual([
            "description",
            "instructions",
            "model",
            "name",
            "schedules",
            "skills",
            "source",
            "warnings",
         ]);
         expect(Object.keys(agent.source!).sort()).toEqual([
            "definitionSha",
            "environment",
            "package",
            "servedRevision",
            "sourceContentSha",
         ]);
         await expect(
            controller.getAgent("env", "pkg", "nope"),
         ).rejects.toBeInstanceOf(AgentNotFoundError);
      });

      type Handler = (params: unknown) => Promise<{
         isError?: boolean;
         content: Array<{ resource?: { text: string } }>;
      }>;

      const toolFor = (pkg: Package): Handler => {
         let handler: Handler | undefined;
         let description = "";
         registerGetAgentTool(
            {
               tool: (_n: string, d: string, _s: unknown, h: Handler) => {
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

      it("get_agent lists, resolves and reports a missing name", async () => {
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         const tool = toolFor(pkg);

         const listed = body(await tool({ scopes }));
         expect(listed.agent).toBeNull();
         expect(
            listed.availableAgents.map((a: { name: string }) => a.name),
         ).toEqual(["analyst"]);

         const resolved = body(await tool({ scopes, agent_name: "analyst" }));
         expect(resolved.agent).toEqual(pkg.getAgent("analyst"));

         const missing = body(await tool({ scopes, agent_name: "nope" }));
         expect(missing.agent).toBeNull();
         expect(missing.message).toContain("Fix:");
         expect(missing.availableAgents).toHaveLength(1);
      });
   });
});
