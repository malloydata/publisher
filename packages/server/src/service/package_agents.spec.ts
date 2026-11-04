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
import { createHash } from "crypto";
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
import { computeSourceContentSha } from "./package_revision";
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
      budget: { turns: 30, job_minutes: 15 },
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
            budget: { turns: 30, job_minutes: 15 },
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
         budget: { turns: 30, job_minutes: 15 },
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

   // Raw text: JSON.stringify of this value overflows the stack, which is the point.
   const writeDeepAgents = () => {
      const n = 50_000;
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         `{"name":"pkg","agents":{"a":{"description":"d","instructions":"x.md","x-deep":${"[".repeat(n)}${"]".repeat(n)}}}}`,
      );
   };

   it("serves a package whose agents value is nested past the stack", async () => {
      writeDeepAgents();
      const pkg = await load();
      expect(pkg.listAgents()).toEqual([]);
      expect(pkg.getSourceContentSha()).toMatch(/^[0-9a-f]{64}$/);
   });

   it("finishes a reload whose agents value is nested past the stack", async () => {
      manifest({ agents: { analyst: analyst() } });
      const pkg = await load();
      writeDeepAgents();
      await pkg.reloadAllModels({});
      expect(pkg.listAgents()).toEqual([]);
      expect(pkg.getSourceContentSha()).toMatch(/^[0-9a-f]{64}$/);
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

      it("finishes a reload, with a warn finding, when a package skill's reference is a regular file", async () => {
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         const before = pkg.getSourceContentSha();

         write(
            "skills/odd/SKILL.md",
            "---\nname: odd\ndescription: d\n---\n\nbody\n",
         );
         write("skills/odd/reference", "not a directory");
         await pkg.reloadAllModels({});

         expect(pkg.listSkills().map((s) => s.name)).toEqual(["odd"]);
         expect(pkg.getSourceContentSha()).not.toBe(before);
         expect(pkg.getAgent("analyst")).toBeDefined();
         expect(
            pkg
               .getPackageMetadata()
               .warnings?.filter((w) =>
                  w.message?.includes("cannot be listed"),
               ),
         ).toEqual([
            expect.objectContaining({
               severity: "warn",
               message: expect.any(String),
            }),
         ]);
      });

      it("serves an agent whose skill has no description and reports it as a warn finding", async () => {
         write(
            "agents/analyst/skills/quiet/SKILL.md",
            "---\nname: quiet\n---\n\nbody\n",
         );
         manifest({ agents: { analyst: analyst() } });
         const pkg = await load();
         expect(pkg.getAgent("analyst")).toBeDefined();
         const finding = pkg
            .getPackageMetadata()
            .warnings?.find((w) => w.message?.includes("no description"));
         expect(finding?.severity).toBe("warn");
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

   describe("skill assets", () => {
      const A = "agents/analyst/skills/conventions/assets";
      const LISTED = "conventions/assets";
      const sha = (text: string) =>
         createHash("sha256").update(text).digest("hex");

      beforeEach(() => manifest({ agents: { analyst: analyst() } }));

      it("keeps an agent without assets at the definitionSha it had before assets existed", async () => {
         const pkg = await load();
         expect(pkg.getAgent("analyst")!.source!.definitionSha).toBe(
            "521f46badf332e853cb97d2bbdaf3e32fcc3f9a9c1478bba2fd3c64eb1c3e47d",
         );
         const agent = pkg.getAgent("analyst")!;
         expect("assets" in agent).toBe(false);
         expect("asset_roots" in agent).toBe(false);
      });

      it("lists assets in the full view and counts them in the summary", async () => {
         write(`${A}/index.html`, "<h1>hi</h1>");
         write(`${A}/css/app.css`, "body{}");
         const pkg = await load();

         const full = pkg.getAgent("analyst")!;
         expect(full.asset_roots).toEqual({
            conventions: "agents/analyst/skills/conventions",
         });
         expect(full.assets).toEqual([
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

         const summary = pkg.getAgent("analyst", "summary")!;
         expect(summary.asset_counts).toEqual({ conventions: 2 });
         expect(summary).not.toHaveProperty("assets");
         expect(summary).not.toHaveProperty("asset_roots");
         expect(summary.source).toEqual(full.source);
      });

      it("serves a listed asset through the skill-file lookup, REST and MCP, and nothing unlisted", async () => {
         write(`${A}/index.html`, "<h1>hi</h1>");
         write(
            `${A}/unlisted.malloy`,
            `source: x is duckdb.sql("select 1 as id")\n`,
         );
         write("secret.txt", "outside");
         const pkg = await load();
         const listed = `${LISTED}/index.html`;
         const expected = {
            relative_filepath: listed,
            file_contents: "<h1>hi</h1>",
            source: pkg.getAgent("analyst")!.source,
         };

         expect(pkg.getAgentSkillFile("analyst", listed)).toEqual(expected);
         const store = {
            getEnvironment: async () => ({ getPackage: async () => pkg }),
         } as unknown as EnvironmentStore;
         expect(
            await new SkillController(store).getAgentSkillFile(
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
         registerGetAgentTool(
            {
               tool: (_n: string, _d: string, _s: unknown, h: Handler) => {
                  handler = h;
               },
            } as never,
            store,
         );
         const viaMcp = JSON.parse(
            (
               await handler!({
                  scopes: [{ environment: "env", package: "pkg" }],
                  agent_name: "analyst",
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
            expect(pkg.getAgentSkillFile("analyst", bad)).toBeUndefined();
         }
      });

      it("refuses to serve a listed asset that became a link after the load", async () => {
         write(`${A}/index.html`, "<h1>hi</h1>");
         write("secret.txt", "outside");
         const pkg = await load();
         fs.rmSync(path.join(dir, A, "index.html"));
         fs.symlinkSync(
            path.join(dir, "secret.txt"),
            path.join(dir, A, "index.html"),
         );
         expect(
            pkg.getAgentSkillFile("analyst", `${LISTED}/index.html`),
         ).toBeUndefined();
      });

      it("drops the agent and says why when an asset is a link", async () => {
         write("secret.txt", "outside");
         fs.mkdirSync(path.join(dir, A), { recursive: true });
         fs.symlinkSync(
            path.join(dir, "secret.txt"),
            path.join(dir, A, "link.txt"),
         );
         const pkg = await load();
         expect(pkg.getAgent("analyst")).toBeUndefined();
         const warning = pkg
            .getPackageMetadata()
            .warnings?.find((w) => w.message?.includes("link.txt"));
         expect(warning?.message).toStartWith(
            "Package agent 'analyst' is not served:",
         );
      });

      it("moves definitionSha and the content sha when an asset changes", async () => {
         write(`${A}/index.html`, "one");
         const pkg = await load();
         const definition = pkg.getAgent("analyst")!.source!.definitionSha;
         const content = pkg.getSourceContentSha();

         write(`${A}/index.html`, "two");
         await pkg.reloadAllModels({});
         expect(pkg.getAgent("analyst")!.source!.definitionSha).not.toBe(
            definition,
         );
         expect(pkg.getSourceContentSha()).not.toBe(content);
         expect(pkg.getAgent("analyst")!.assets![0]!.sha256).toBe(sha("two"));
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

      it("ignores a package's own skills/<x>/assets", async () => {
         write(
            "skills/top/SKILL.md",
            "---\nname: top\ndescription: d\n---\n\nbody\n",
         );
         write("skills/top/assets/a.txt", "x");
         write("skills/top/assets/publisher.json", "{}");
         const pkg = await load();
         expect(pkg.listSkills().map((s) => s.name)).toEqual(["top"]);
         expect(
            pkg
               .getPackageMetadata()
               .warnings?.filter((w) => w.message?.includes("assets")) ?? [],
         ).toEqual([]);
         expect(pkg.getAgent("analyst")).toBeDefined();
      });

      it("keeps an agent served, with a warning, when assets/ holds a PNG", async () => {
         fs.mkdirSync(path.join(dir, A), { recursive: true });
         fs.writeFileSync(
            path.join(dir, A, "logo.png"),
            Buffer.from([0x89, 0x50, 0x4e, 0x47, 0xff, 0xfe, 0x00]),
         );
         write(`${A}/ok.txt`, "fine");
         const pkg = await load();
         expect(
            pkg.getAgent("analyst")!.assets!.map((a) => a.relative_filepath),
         ).toEqual([`${LISTED}/ok.txt`]);
         expect(
            pkg
               .getPackageMetadata()
               .warnings?.some(
                  (w) =>
                     w.severity === "warn" &&
                     w.message?.includes("logo.png") &&
                     w.message.startsWith("Package agent 'analyst'"),
               ),
         ).toBe(true);
         expect(
            pkg.getAgentSkillFile("analyst", `${LISTED}/logo.png`),
         ).toBeUndefined();
      });

      it("keeps an agent served when assets/ holds more than 1,000 entries", async () => {
         write(`${A}/a.txt`, "x");
         for (let i = 0; i < 1000; i++) {
            fs.mkdirSync(path.join(dir, A, `d${String(i).padStart(4, "0")}`), {
               recursive: true,
            });
         }
         write(`${A}/z.txt`, "x");
         const pkg = await load();
         expect(
            pkg.getAgent("analyst")!.assets!.map((a) => a.relative_filepath),
         ).toEqual([`${LISTED}/a.txt`]);
         expect(
            pkg
               .getPackageMetadata()
               .warnings?.some((w) => w.message?.includes("1000 entries")),
         ).toBe(true);
      });

      it("refuses a listed asset once its directory is swapped for a link", async () => {
         write(`${A}/index.html`, "<h1>hi</h1>");
         fs.mkdirSync(path.join(dir, "elsewhere"));
         fs.writeFileSync(path.join(dir, "elsewhere/index.html"), "SECRET");
         const pkg = await load();
         fs.rmSync(path.join(dir, A), { recursive: true });
         fs.symlinkSync(path.join(dir, "elsewhere"), path.join(dir, A));
         expect(
            pkg.getAgentSkillFile("analyst", `${LISTED}/index.html`),
         ).toBeUndefined();
      });

      it("refuses a listed asset edited after the load, since it no longer matches its sha", async () => {
         write(`${A}/index.html`, "<h1>hi</h1>");
         const pkg = await load();
         write(`${A}/index.html`, "<h1>changed</h1>");
         expect(
            pkg.getAgentSkillFile("analyst", `${LISTED}/index.html`),
         ).toBeUndefined();
      });

      it("keeps a skill named __proto__ in the roots and the summary counts", async () => {
         write(
            "agents/analyst/skills/__proto__/SKILL.md",
            "---\nname: __proto__\ndescription: d\n---\n\nbody\n",
         );
         write("agents/analyst/skills/__proto__/assets/a.txt", "x");
         const pkg = await load();
         const full = pkg.getAgent("analyst")!;
         expect(Object.keys(full.asset_roots!)).toEqual(["__proto__"]);
         expect(
            Object.keys(pkg.getAgent("analyst", "summary")!.asset_counts!),
         ).toEqual(["__proto__"]);
         expect(
            pkg.getAgentSkillFile("analyst", "__proto__/assets/a.txt")
               ?.file_contents,
         ).toBe("x");
      });

      it("pins sourceContentSha for a package whose agents ship no assets", async () => {
         const pkg = await load();
         expect(pkg.getSourceContentSha()).toBe(
            "00e29ff908aab56d8b8810270241bf96d0e3d11506ba341d6fba153377728954",
         );
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
            "budget",
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

      describe("skills summary and single-file read", () => {
         const RAW_REF = "---\nnot: frontmatter\n---\nraw glossary\n";
         const loadWithReference = async () => {
            write(
               "agents/analyst/skills/conventions/reference/glossary.md",
               RAW_REF,
            );
            write("secret.txt", "outside the agent");
            manifest({ agents: { analyst: analyst() } });
            return load();
         };

         it("summary drops file_contents, keeps name, description and bytes, and the same pin", async () => {
            const pkg = await loadWithReference();
            const full = pkg.getAgent("analyst")!;
            const summary = pkg.getAgent("analyst", "summary")!;

            expect(summary.skills).toEqual(
               full.skills!.map((f) => ({
                  relative_filepath: f.relative_filepath,
                  ...(f.relative_filepath!.endsWith("/SKILL.md")
                     ? { name: "conventions", description: "House rules." }
                     : {}),
                  bytes: Buffer.byteLength(f.file_contents!),
               })),
            );
            expect(JSON.stringify(summary)).not.toContain("file_contents");
            expect(summary.source).toEqual(full.source);
            expect({ ...summary, skills: full.skills } as typeof full).toEqual(
               full,
            );
            // The default is the full shape, as the sandbox renderer consumes it.
            expect(pkg.getAgent("analyst", "full")).toEqual(full);
            expect(full.skills!.every((f) => "file_contents" in f)).toBe(true);
         });

         it("returns exactly one file with the definition's pin, and refuses anything outside the resolved set", async () => {
            const pkg = await loadWithReference();
            const full = pkg.getAgent("analyst")!;
            const controller = new SkillController(storeFor(pkg));
            const tool = toolFor(pkg);
            const paths = full.skills!.map((f) => f.relative_filepath);
            expect(paths).toContain("conventions/reference/glossary.md");

            for (const file of full.skills!) {
               const read = await controller.getAgentSkillFile(
                  "env",
                  "pkg",
                  "analyst",
                  file.relative_filepath!,
               );
               expect(read).toEqual({ ...file, source: full.source });
               const viaMcp = body(
                  await tool({
                     scopes,
                     agent_name: "analyst",
                     skill_path: file.relative_filepath,
                  }),
               );
               expect(viaMcp.skillFile).toEqual(read);
               expect(viaMcp).not.toHaveProperty("agent");
            }

            for (const bad of [
               "../../../secret.txt",
               "conventions/../../../secret.txt",
               "agents/other/instructions.md",
               "/secret.txt",
               "conventions",
               "",
            ]) {
               await expect(
                  controller.getAgentSkillFile("env", "pkg", "analyst", bad),
               ).rejects.toBeInstanceOf(AgentNotFoundError);
               if (bad === "") continue;
               const miss = body(
                  await tool({
                     scopes,
                     agent_name: "analyst",
                     skill_path: bad,
                  }),
               );
               expect(miss.skillFile).toBeUndefined();
               expect(miss.message).toContain("Fix:");
            }
            const noAgent = body(await tool({ scopes, skill_path: paths[0]! }));
            expect(noAgent.skillFile).toBeUndefined();
            expect(noAgent.message).toContain("needs agent_name");
            await expect(
               controller.getAgentSkillFile("env", "pkg", "nope", paths[0]!),
            ).rejects.toBeInstanceOf(AgentNotFoundError);
         });

         it("get_agent summary matches REST and leaves the default alone", async () => {
            const pkg = await loadWithReference();
            const controller = new SkillController(storeFor(pkg));
            const tool = toolFor(pkg);

            const viaMcp = body(
               await tool({ scopes, agent_name: "analyst", skills: "summary" }),
            );
            expect(viaMcp.agent).toEqual(
               await controller.getAgent("env", "pkg", "analyst", "summary"),
            );
            const dflt = body(await tool({ scopes, agent_name: "analyst" }));
            expect(dflt.agent).toEqual(pkg.getAgent("analyst"));
            expect(dflt.agent.source.definitionSha).toBe(
               viaMcp.agent.source.definitionSha,
            );
         });
      });
   });
});
