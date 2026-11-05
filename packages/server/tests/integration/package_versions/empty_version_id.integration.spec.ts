// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * An empty `versionId` (`?versionId=`, or `"versionId": ""` in a body) names
 * no version: a versioned package answers from its `latest`, and a package
 * with no versions answers as itself. Credible's router sends "" on API
 * calls, so on every route that reads a version, "" must answer exactly as
 * leaving it out does.
 *
 * modes.integration.spec.ts covers 17 routes this way. This file covers the
 * rest: reports (list and one), one dashboard, a notebook cell, skills,
 * agents and an agent's skill file, the package duckdb connection's tables
 * (and one table), the report write, a materialization's create (body),
 * get, stop and delete, and the model query with the version in its URL.
 *
 * Two packages are probed: one with two published versions (`latest` is the
 * higher, 2.0.0), and one published with versioning off, so it has none. Each
 * package tree answers its own number from its model, report titles, notebook,
 * skills and agent, so an answer proves which tree served it. A route's
 * answer with "" is compared to its answer with nothing, status and body.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "empty-version-id-env";
const FLAG = "PUBLISHER_PACKAGE_VERSIONING";
const VERSIONED = "versioned";
const PLAIN = "plain";
const TERMINAL = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];
const JSON_HEADERS = { "content-type": "application/json" };

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
const savedFlag = process.env[FLAG];

function setFlag(value: string | undefined): void {
   if (value === undefined) delete process.env[FLAG];
   else process.env[FLAG] = value;
}

/**
 * One package tree whose every surface names `answer`, with a persisted
 * source so the package can be materialized.
 */
function writePackage(
   dir: string,
   manifest: Record<string, unknown>,
   answer: number,
): string {
   fs.mkdirSync(path.join(dir, "dashboards"), { recursive: true });
   fs.mkdirSync(path.join(dir, "skills", "house-rules"), { recursive: true });
   fs.mkdirSync(path.join(dir, "agents", "analyst", "skills", "conventions"), {
      recursive: true,
   });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({
         ...manifest,
         agents: {
            analyst: {
               description: `Analyst v${answer}`,
               instructions: "agents/analyst/instructions.md",
               skills: ["agents/analyst/skills"],
            },
         },
      }),
   );
   fs.writeFileSync(
      path.join(dir, "skills", "house-rules", "SKILL.md"),
      `---\nname: house-rules\ndescription: House rules v${answer}\n---\n\nrules v${answer}\n`,
   );
   fs.writeFileSync(
      path.join(dir, "agents", "analyst", "instructions.md"),
      `instructions v${answer}\n`,
   );
   fs.writeFileSync(
      path.join(dir, "agents", "analyst", "skills", "conventions", "SKILL.md"),
      `---\nname: conventions\ndescription: Conventions v${answer}\n---\n\nconventions v${answer}\n`,
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      [
         "##! experimental.persistence",
         `source: numbers is duckdb.sql("SELECT ${answer} AS answer") extend {`,
         "  view: which_version is { select: answer }",
         "}",
         '#@ persist name="summary"',
         "source: summary is numbers -> { group_by: answer }",
         "",
      ].join("\n"),
   );
   fs.writeFileSync(
      path.join(dir, "dashboards", "overview.malloy"),
      `## artifact { layout=grid title="Overview v${answer}" tiles=["numbers -> which_version"] }\n` +
         `import { numbers } from '../model.malloy'\n`,
   );
   fs.writeFileSync(
      path.join(dir, "notes.malloynb"),
      `>>>markdown\n# Notes v${answer}\n>>>malloy\nimport "model.malloy"\n` +
         `>>>malloy\nrun: numbers -> which_version\n`,
   );
   return dir;
}

const pkgApi = (name: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}`;

async function publish(name: string, location: string): Promise<Response> {
   return fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`, {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify({ name, location }),
   });
}

/** A response's status and body, the body parsed when it is JSON. */
interface Answer {
   status: number;
   body: unknown;
}

async function answerOf(
   res: Response,
   normalize: (text: string) => string = (t) => t,
): Promise<Answer> {
   const text = normalize(await res.text());
   try {
      return { status: res.status, body: JSON.parse(text) };
   } catch {
      return { status: res.status, body: text };
   }
}

/** Running a query stamps each field with a fresh id; the rest is stable. */
const withoutRunIds = (text: string) =>
   text.replace(
      /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g,
      "<id>",
   );

/** A read route, and a text its answer carries only when tree `n` served it. */
interface ReadRoute {
   name: string;
   path: string;
   marker: (n: number) => string;
   normalize?: (text: string) => string;
}

const READ_ROUTES: ReadRoute[] = [
   { name: "GET reports", path: "/reports", marker: (n) => `Overview v${n}` },
   {
      name: "GET report",
      path: "/reports/dashboards/overview.malloy",
      marker: (n) => `Overview v${n}`,
   },
   {
      name: "GET dashboard",
      path: "/dashboards/overview",
      marker: (n) => `Overview v${n}`,
   },
   {
      name: "GET notebook cell",
      path: "/notebooks/notes.malloynb/cells/2",
      marker: (n) => `SELECT ${n} AS answer`,
      normalize: withoutRunIds,
   },
   { name: "GET skills", path: "/skills", marker: (n) => `House rules v${n}` },
   {
      name: "GET skill",
      path: "/skills/house-rules",
      marker: (n) => `rules v${n}`,
   },
   { name: "GET agents", path: "/agents", marker: (n) => `Analyst v${n}` },
   {
      name: "GET agent",
      path: "/agents/analyst",
      marker: (n) => `instructions v${n}`,
   },
   {
      name: "GET agent skill file",
      path: "/agents/analyst/skills/conventions/SKILL.md",
      marker: (n) => `conventions v${n}`,
   },
   // The package's duckdb sandbox is in memory, one per loaded tree, and the
   // suite's build of `latest` puts `summary` in that tree's alone. So the
   // table is the marker: it says the build's tree answered.
   {
      name: "GET connection tables",
      path: "/connections/duckdb/schemas/memory.main/tables",
      marker: () => "memory.main.summary",
   },
   {
      name: "GET connection table",
      path: "/connections/duckdb/schemas/memory.main/tables/memory.main.summary",
      marker: () => "memory.main.summary",
   },
];

/**
 * Every read route's answer with `?versionId=` beside its answer with none,
 * keyed by route, so a failure names its route.
 */
async function expectEmptyReadsAsNone(pkg: string, n: number): Promise<void> {
   const actual: Record<string, unknown> = {};
   const wanted: Record<string, unknown> = {};
   for (const route of READ_ROUTES) {
      const url = `${pkgApi(pkg)}${route.path}`;
      const none = await answerOf(await fetch(url), route.normalize);
      const empty = await answerOf(
         await fetch(`${url}?versionId=`),
         route.normalize,
      );
      actual[route.name] = {
         statuses: [none.status, empty.status],
         body: empty.body,
         marked: JSON.stringify(empty.body).includes(route.marker(n)),
      };
      wanted[route.name] = {
         statuses: [200, 200],
         body: none.body,
         marked: true,
      };
   }
   expect(actual).toEqual(wanted);
}

/** POST a query; its status and, when it answers, its rows. */
async function query(
   pkg: string,
   urlVersion: string | undefined,
   bodyVersion: string | undefined,
): Promise<{ status: number; rows?: unknown; reason?: unknown }> {
   const res = await fetch(
      `${pkgApi(pkg)}/models/model.malloy/query${
         urlVersion === undefined
            ? ""
            : `?versionId=${encodeURIComponent(urlVersion)}`
      }`,
      {
         method: "POST",
         headers: JSON_HEADERS,
         body: JSON.stringify({
            query: "run: numbers -> which_version",
            compactJson: true,
            ...(bodyVersion !== undefined ? { versionId: bodyVersion } : {}),
         }),
      },
   );
   const body = (await res.json()) as { result?: string; reason?: string };
   return res.status === 200
      ? { status: 200, rows: JSON.parse(String(body.result)) }
      : { status: res.status, reason: body.reason };
}

type Run = Record<string, unknown> & {
   id: string;
   status: string;
   metadata: Record<string, unknown>;
   manifest?: { entries: Record<string, Record<string, unknown>> } | null;
};

async function createRun(
   pkg: string,
   body: Record<string, unknown>,
): Promise<{ status: number; run: Run }> {
   const res = await fetch(`${pkgApi(pkg)}/materializations`, {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify(body),
   });
   return { status: res.status, run: (await res.json()) as Run };
}

/** Poll a run to a terminal status, with no versionId. */
async function settle(pkg: string, id: string): Promise<Run> {
   const deadline = Date.now() + 120_000;
   for (;;) {
      const res = await fetch(`${pkgApi(pkg)}/materializations/${id}`);
      expect(res.status).toBe(200);
      const run = (await res.json()) as Run;
      if (TERMINAL.includes(run.status)) return run;
      if (Date.now() > deadline) throw new Error(`run ${id} never settled`);
      await new Promise((r) => setTimeout(r, 100));
   }
}

/** What a settled run built: each source, the table it went to, its entity. */
function builtSources(run: Run): unknown[] {
   return Object.values(run.manifest?.entries ?? {}).map((e) => [
      e.sourceName,
      e.physicalTableName,
      e.sourceEntityId,
   ]);
}

/** A package kind this file probes, and what its trees answer. */
interface Kind {
   title: string;
   pkg: string;
   flag: string | undefined;
   /** The number the tree that "" (and nothing) should reach answers. */
   n: number;
   /** The version a run of that tree records in its metadata, if any. */
   runVersion: Record<string, unknown>;
}

const KINDS: Kind[] = [
   {
      title: "a versioned package (latest is 2.0.0 of 1.0.0 and 2.0.0)",
      pkg: VERSIONED,
      flag: "on",
      n: 2,
      runVersion: { versionId: "2.0.0", scope: "package" },
   },
   {
      title: "a package with no versions, versioning off",
      pkg: PLAIN,
      flag: undefined,
      n: 0,
      runVersion: {},
   },
];

describe("an empty versionId answers as no versionId", () => {
   beforeAll(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "empty-version-id-")),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: JSON_HEADERS,
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);

      // Off: installed in place, with no versions.
      setFlag(undefined);
      const plain = await publish(
         PLAIN,
         writePackage(path.join(root, "plain"), { name: PLAIN }, 0),
      );
      expect(plain.status).toBe(200);
      expect(((await plain.json()) as { versionId?: unknown }).versionId).toBe(
         undefined,
      );

      // On: two published versions, the later one latest.
      setFlag("on");
      for (const [version, answer] of [
         ["1.0.0", 1],
         ["2.0.0", 2],
      ] as const) {
         const res = await publish(
            VERSIONED,
            writePackage(
               path.join(root, `v${answer}`),
               { name: VERSIONED, version },
               answer,
            ),
         );
         expect(res.status).toBe(200);
         expect(((await res.json()) as { versionId?: unknown }).versionId).toBe(
            version,
         );
      }
      setFlag(undefined);
   }, 180_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
      setFlag(savedFlag);
   });

   for (const kind of KINDS) {
      describe(kind.title, () => {
         /** A settled run of the tree "" should reach, built with no versionId. */
         let built: Run;

         beforeAll(async () => {
            setFlag(kind.flag);
            const created = await createRun(kind.pkg, {});
            expect(created.status).toBe(201);
            built = await settle(kind.pkg, created.run.id);
            expect(built.status).toBe("MANIFEST_FILE_READY");
         }, 180_000);

         afterAll(() => setFlag(undefined));

         it("answers each report, dashboard, notebook cell, skill, agent and connection-table read the same with versionId= as with none", async () => {
            await expectEmptyReadsAsNone(kind.pkg, kind.n);
         });

         it("runs a model query the same with ?versionId= in the URL as with none", async () => {
            const none = await query(kind.pkg, undefined, undefined);
            expect(none).toEqual({ status: 200, rows: [{ answer: kind.n }] });
            expect(await query(kind.pkg, "", undefined)).toEqual(none);
            expect(await query(kind.pkg, "", "")).toEqual(none);
         });

         it("creates a materialization with body versionId '' as with none, building the same tree", async () => {
            const runs: Record<string, unknown> = {};
            for (const [label, body] of [
               ["none", {}],
               ["empty", { versionId: "" }],
            ] as const) {
               const created = await createRun(kind.pkg, body);
               const settled =
                  created.status === 201
                     ? await settle(kind.pkg, created.run.id)
                     : undefined;
               runs[label] = {
                  status: created.status,
                  metadata: created.run.metadata,
                  settled: settled?.status,
                  built: settled && builtSources(settled),
               };
            }
            expect(runs.empty).toEqual(runs.none);
            expect(runs.none).toEqual({
               status: 201,
               metadata: {
                  forceRefresh: false,
                  reseed: false,
                  sourceNames: null,
                  mode: "auto",
                  trigger: "ON_DEMAND",
                  ...kind.runVersion,
               },
               settled: "MANIFEST_FILE_READY",
               built: builtSources(built),
            });
         }, 300_000);

         it("gets, and refuses to stop, a settled run the same with ?versionId= as with none", async () => {
            const run = `${pkgApi(kind.pkg)}/materializations/${built.id}`;
            const got = await answerOf(await fetch(run));
            expect(got.status).toBe(200);
            expect((got.body as Run).metadata).toMatchObject(kind.runVersion);
            expect(await answerOf(await fetch(`${run}?versionId=`))).toEqual(
               got,
            );

            const stop = async (q: string) =>
               answerOf(
                  await fetch(`${run}?action=stop${q}`, { method: "POST" }),
               );
            const stopNone = await stop("");
            expect(stopNone).toEqual({
               status: 409,
               body: {
                  code: 409,
                  message: `Materialization ${built.id} is MANIFEST_FILE_READY, cannot stop`,
               },
            });
            expect(await stop("&versionId=")).toEqual(stopNone);
         });

         it("stops an active run with ?versionId=", async () => {
            // A build can finish before the stop lands; try again on a fresh
            // run, as api_contract does.
            let stopped: Answer | undefined;
            for (let attempt = 0; attempt < 10 && !stopped; attempt++) {
               const created = await createRun(kind.pkg, {});
               expect(created.status).toBe(201);
               const res = await answerOf(
                  await fetch(
                     `${pkgApi(kind.pkg)}/materializations/${created.run.id}?action=stop&versionId=`,
                     { method: "POST" },
                  ),
               );
               if (res.status === 200) stopped = res;
               else expect(res.status).toBe(409);
               await settle(kind.pkg, created.run.id);
               if (stopped) {
                  expect((stopped.body as Run).id).toBe(created.run.id);
               }
            }
            expect(stopped?.status).toBe(200);
         }, 300_000);

         it("deletes a settled run the same with ?versionId= as with none", async () => {
            const ids: string[] = [];
            for (let i = 0; i < 2; i++) {
               const created = await createRun(kind.pkg, {});
               expect(created.status).toBe(201);
               await settle(kind.pkg, created.run.id);
               ids.push(created.run.id);
            }
            const runUrl = (id: string) =>
               `${pkgApi(kind.pkg)}/materializations/${id}`;
            const deleted = [
               await fetch(runUrl(ids[0]), { method: "DELETE" }),
               await fetch(`${runUrl(ids[1])}?versionId=`, {
                  method: "DELETE",
               }),
            ];
            expect(
               await Promise.all(
                  deleted.map(async (r) => [r.status, await r.text()]),
               ),
            ).toEqual([
               [204, ""],
               [204, ""],
            ]);
            for (const id of ids) {
               expect([id, (await fetch(runUrl(id))).status]).toEqual([
                  id,
                  404,
               ]);
            }
         }, 300_000);
      });
   }

   describe("the report write", () => {
      const report = (title: string) =>
         `## artifact { layout=grid title="${title}" tiles=["numbers -> which_version"] }\n` +
         "import { numbers } from '../model.malloy'\n";

      const put = async (
         pkg: string,
         file: string,
         q: string,
         body: Record<string, unknown>,
      ) =>
         answerOf(
            await fetch(`${pkgApi(pkg)}/models/written/${file}${q}`, {
               method: "PUT",
               headers: JSON_HEADERS,
               body: JSON.stringify(body),
            }),
         );

      it("refuses a write to a versioned package the same with ?versionId= as with none, and writes nothing", async () => {
         setFlag("on");
         try {
            const source = report("Written");
            const none = await put(VERSIONED, "none.malloy", "", { source });
            expect(none.status).toBe(409);
            expect((none.body as { reason?: string }).reason).toBe(
               "PACKAGE_IS_VERSIONED",
            );
            expect(
               await put(VERSIONED, "empty.malloy", "?versionId=", { source }),
            ).toEqual(none);

            const paths = (
               (await (
                  await fetch(`${pkgApi(VERSIONED)}/reports?versionId=`)
               ).json()) as { path: string }[]
            ).map((r) => r.path);
            expect(paths).toEqual([
               "dashboards/overview.malloy",
               "notes.malloynb",
            ]);
         } finally {
            setFlag(undefined);
         }
      });

      it("creates, then replaces, a report in a package with no versions the same with ?versionId= as with none", async () => {
         const results: Record<string, unknown> = {};
         for (const [label, stem, q] of [
            ["none", "written_with_none", ""],
            ["empty", "written_with_empty", "?versionId="],
         ] as const) {
            const file = `${stem}.malloy`;
            const created = await put(PLAIN, file, q, {
               source: report("First"),
            });
            const hash = (created.body as { contentHash?: string }).contentHash;
            const replaced = await put(PLAIN, file, q, {
               source: report("Second"),
               expectedHash: hash,
            });
            const served = await answerOf(
               await fetch(`${pkgApi(PLAIN)}/reports/written/${file}${q}`),
            );
            // The file's name is the one thing the two writes differ in.
            results[label] = JSON.parse(
               JSON.stringify({ created, replaced, served }).replaceAll(
                  stem,
                  "<stem>",
               ),
            );
         }
         expect(results.empty).toEqual(results.none);
         expect(results.none).toMatchObject({
            created: { status: 201, body: { created: true } },
            replaced: { status: 200, body: { created: false } },
            served: { status: 200, body: { title: "Second" } },
         });
      });
   });
});
