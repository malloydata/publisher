// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The parts of the versions contract a client generated from api-doc.yaml
 * sees, over HTTP, with package versioning off and on:
 *
 *  - a materialization response carries only properties the `Materialization`
 *    schema declares (no top-level `version`), and a versioned run's version
 *    goes out in `metadata.versionId`;
 *  - a malformed `versionId` answers 400 `VERSION_ID_INVALID` on the
 *    package-scoped routes, which api-doc.yaml declares (see
 *    src/version_id_contract.spec.ts);
 *  - a version with a leading zero publishes, and the same version written
 *    without it is then refused, so one package never holds both;
 *  - under explicit promotion a `scope: package` version is built before
 *    anything is `latest`, and serves the table it built once promoted.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "versions-api-contract-env";
const FLAG = "PUBLISHER_PACKAGE_VERSIONING";
const PROMOTION = "PUBLISHER_VERSION_PROMOTION";
const TERMINAL = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
const saved = { flag: process.env[FLAG], promotion: process.env[PROMOTION] };

/** The properties api-doc.yaml declares on a Materialization. */
const MATERIALIZATION_PROPERTIES = new Set(
   Object.keys(
      (
         Bun.YAML.parse(
            fs.readFileSync(
               path.resolve(import.meta.dir, "../../../../../api-doc.yaml"),
               "utf8",
            ),
         ) as {
            components: {
               schemas: { Materialization: { properties: object } };
            };
         }
      ).components.schemas.Materialization.properties,
   ),
);

function setEnv(name: string, value: string | undefined): void {
   if (value === undefined) delete process.env[name];
   else process.env[name] = value;
}

/**
 * A package whose persisted `summary` answers `answer`, with a report, so
 * every route below has something to read.
 */
function writePackage(
   folder: string,
   manifest: Record<string, unknown>,
   answer: number,
): string {
   const dir = path.join(root, folder);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(path.join(dir, "publisher.json"), JSON.stringify(manifest));
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      [
         "##! experimental.persistence",
         `source: base is duckdb.sql("SELECT ${answer} AS answer")`,
         '#@ persist name="summary"',
         "source: summary is base -> { group_by: answer }",
         "",
      ].join("\n"),
   );
   fs.writeFileSync(
      path.join(dir, "report.malloy"),
      '## artifact { layout=grid title="Report" tiles=["summary -> { select: answer }"] }\n' +
         "import { summary } from 'model.malloy'\n",
   );
   return dir;
}

const pkgApi = (name: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}`;

async function body(res: Response): Promise<Record<string, unknown>> {
   return (await res.json()) as Record<string, unknown>;
}

async function publish(
   name: string,
   location: string,
): Promise<{ status: number; json: Record<string, unknown> }> {
   const res = await fetch(
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
      {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({ name, location }),
      },
   );
   return { status: res.status, json: await body(res) };
}

/** Every top-level key of a run the schema does not declare. */
function undeclared(run: Record<string, unknown>): string[] {
   return Object.keys(run).filter((k) => !MATERIALIZATION_PROPERTIES.has(k));
}

/** Poll a run to a terminal status; the last GET's body. */
async function settle(name: string, id: string) {
   const deadline = Date.now() + 120_000;
   for (;;) {
      const res = await fetch(`${pkgApi(name)}/materializations/${id}`);
      expect(res.status).toBe(200);
      const run = await body(res);
      if (TERMINAL.includes(String(run.status))) return run;
      if (Date.now() > deadline) throw new Error(`run ${id} never settled`);
      await new Promise((r) => setTimeout(r, 200));
   }
}

/**
 * Create, get, list and stop answers for one package's runs. Stop needs a
 * run that is still active, so a second run is stopped as soon as it is
 * created (as materialization_lifecycle does); a build that wins that race
 * is stopped again on a fresh run.
 */
async function everyRunResponse(
   name: string,
   create: Record<string, unknown>,
): Promise<Record<string, Record<string, unknown>[]>> {
   const post = () =>
      fetch(`${pkgApi(name)}/materializations`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify(create),
      });
   const created = await post();
   expect(created.status).toBe(201);
   const createdRun = await body(created);
   const got = await settle(name, String(createdRun.id));
   expect(got.status).toBe("MANIFEST_FILE_READY");
   const listed = await fetch(`${pkgApi(name)}/materializations`);
   expect(listed.status).toBe(200);
   const list = (await listed.json()) as Record<string, unknown>[];

   let stopped: Record<string, unknown> | undefined;
   for (let attempt = 0; attempt < 5 && !stopped; attempt++) {
      const second = await post();
      expect(second.status).toBe(201);
      const { id } = await body(second);
      const stop = await fetch(
         `${pkgApi(name)}/materializations/${id}?action=stop`,
         { method: "POST" },
      );
      if (stop.status === 200) stopped = await body(stop);
      await settle(name, String(id));
   }
   expect(stopped).toBeDefined();
   return {
      create: [createdRun],
      get: [got],
      list,
      stop: [stopped!],
   };
}

describe("the versions contract a generated client sees", () => {
   beforeAll(async () => {
      setEnv(FLAG, undefined);
      setEnv(PROMOTION, undefined);
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "versions-api-contract-")),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);

      // Off: installed in place, no versions.
      expect(
         (await publish("plain", writePackage("plain", { name: "plain" }, 0)))
            .status,
      ).toBe(200);
      // On: a published version.
      setEnv(FLAG, "on");
      const versioned = await publish(
         "ver",
         writePackage("ver-1", { name: "ver", version: "1.0.0" }, 1),
      );
      expect([versioned.status, versioned.json.versionId]).toEqual([
         200,
         "1.0.0",
      ]);
      setEnv(FLAG, undefined);
   }, 180_000);

   afterEach(() => {
      setEnv(FLAG, undefined);
      setEnv(PROMOTION, undefined);
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
      setEnv(FLAG, saved.flag);
      setEnv(PROMOTION, saved.promotion);
   });

   it(
      "answers every materialization route with declared properties only, versioning off",
      async () => {
         const answers = await everyRunResponse("plain", {});
         for (const [route, runs] of Object.entries(answers)) {
            expect(runs.length).toBeGreaterThan(0);
            for (const run of runs) {
               expect([route, undeclared(run)]).toEqual([route, []]);
               expect([route, "version" in run]).toEqual([route, false]);
               expect(
                  (run.metadata as Record<string, unknown> | null)?.versionId,
               ).toBeUndefined();
            }
         }
      },
      { timeout: 300_000 },
   );

   it(
      "answers every materialization route with declared properties only, versioning on, the version in metadata",
      async () => {
         setEnv(FLAG, "on");
         const answers = await everyRunResponse("ver", { versionId: "1.0.0" });
         for (const [route, runs] of Object.entries(answers)) {
            expect(runs.length).toBeGreaterThan(0);
            for (const run of runs) {
               expect([route, undeclared(run)]).toEqual([route, []]);
               expect([route, "version" in run]).toEqual([route, false]);
               expect([
                  route,
                  (run.metadata as Record<string, unknown>).versionId,
               ]).toEqual([route, "1.0.0"]);
            }
         }
      },
      { timeout: 300_000 },
   );

   /** One malformed versionId on each sampled route; [route, status, reason]. */
   async function malformedOn(name: string) {
      const bad = "versionId=not-a-version";
      const json = { "content-type": "application/json" };
      const sql = JSON.stringify({ sqlStatement: "SELECT 1 AS n" });
      const calls: [string, () => Promise<Response>][] = [
         [
            "schemas",
            () => fetch(`${pkgApi(name)}/connections/duckdb/schemas?${bad}`),
         ],
         [
            "tables",
            () =>
               fetch(
                  `${pkgApi(name)}/connections/duckdb/schemas/main/tables?${bad}`,
               ),
         ],
         [
            "sqlQuery",
            () =>
               fetch(`${pkgApi(name)}/connections/duckdb/sqlQuery?${bad}`, {
                  method: "POST",
                  headers: json,
                  body: sql,
               }),
         ],
         [
            "sqlTemporaryTable",
            () =>
               fetch(
                  `${pkgApi(name)}/connections/duckdb/sqlTemporaryTable?${bad}`,
                  { method: "POST", headers: json, body: sql },
               ),
         ],
         ["models", () => fetch(`${pkgApi(name)}/models?${bad}`)],
         [
            "models/{path}",
            () => fetch(`${pkgApi(name)}/models/model.malloy?${bad}`),
         ],
         [
            "compile",
            () =>
               fetch(`${pkgApi(name)}/models/model.malloy/compile?${bad}`, {
                  method: "POST",
                  headers: json,
                  body: JSON.stringify({
                     source: "run: base -> { select: answer }",
                  }),
               }),
         ],
         [
            "query (body)",
            () =>
               fetch(`${pkgApi(name)}/models/model.malloy/query`, {
                  method: "POST",
                  headers: json,
                  body: JSON.stringify({
                     query: "run: base -> { select: answer }",
                     versionId: "not-a-version",
                  }),
               }),
         ],
         ["reports", () => fetch(`${pkgApi(name)}/reports?${bad}`)],
         [
            "reports/{path}",
            () => fetch(`${pkgApi(name)}/reports/report.malloy?${bad}`),
         ],
         ["dashboards", () => fetch(`${pkgApi(name)}/dashboards?${bad}`)],
         ["notebooks", () => fetch(`${pkgApi(name)}/notebooks?${bad}`)],
         ["databases", () => fetch(`${pkgApi(name)}/databases?${bad}`)],
         [
            "materializations",
            () => fetch(`${pkgApi(name)}/materializations?${bad}`),
         ],
         [
            "materializations/{id}",
            () => fetch(`${pkgApi(name)}/materializations/no-such-run?${bad}`),
         ],
         [
            "materializations (create body)",
            () =>
               fetch(`${pkgApi(name)}/materializations`, {
                  method: "POST",
                  headers: json,
                  body: JSON.stringify({ versionId: "not-a-version" }),
               }),
         ],
         ["skills", () => fetch(`${pkgApi(name)}/skills?${bad}`)],
         ["data-apps", () => fetch(`${pkgApi(name)}/data-apps?${bad}`)],
      ];
      const out: [string, number, unknown][] = [];
      for (const [route, call] of calls) {
         const res = await call();
         out.push([route, res.status, (await body(res)).reason]);
      }
      return out;
   }

   it("answers a malformed versionId 400 VERSION_ID_INVALID on the sampled routes, versioning off and on", async () => {
      for (const [flag, name] of [
         [undefined, "plain"],
         ["on", "ver"],
      ] as const) {
         setEnv(FLAG, flag);
         const answers = await malformedOn(name);
         expect(answers).toEqual(
            answers.map(([route]) => [route, 400, "VERSION_ID_INVALID"]),
         );
      }
   });

   it("publishes a version with a leading zero, refuses it written without, and keeps build metadata digits", async () => {
      setEnv(FLAG, "on");
      const padded = await publish(
         "zeros",
         writePackage("zeros-padded", { name: "zeros", version: "01.0.0" }, 9),
      );
      expect([padded.status, padded.json.versionId]).toEqual([200, "01.0.0"]);
      const unpadded = await publish(
         "zeros",
         writePackage("zeros-unpadded", { name: "zeros", version: "1.0.0" }, 8),
      );
      expect([unpadded.status, unpadded.json.reason]).toEqual([
         409,
         "VERSION_CONFLICT",
      ]);
      const read = await fetch(`${pkgApi("zeros")}?versionId=01.0.0`);
      expect([read.status, (await body(read)).versionId]).toEqual([
         200,
         "01.0.0",
      ]);
      const other = await fetch(`${pkgApi("zeros")}?versionId=1.0.0`);
      expect([other.status, (await body(other)).reason]).toEqual([
         404,
         "VERSION_NOT_FOUND",
      ]);

      const withBuild = await publish(
         "ver",
         writePackage("ver-build", { name: "ver", version: "1.0.0+001" }, 2),
      );
      expect([withBuild.status, withBuild.json.versionId]).toEqual([
         200,
         "1.0.0+001",
      ]);
      const pinned = await fetch(`${pkgApi("ver")}?versionId=1.0.0%2B001`);
      expect([pinned.status, (await body(pinned)).versionId]).toEqual([
         200,
         "1.0.0+001",
      ]);
      const listed = (await (
         await fetch(`${pkgApi("ver")}/versions`)
      ).json()) as { id: string }[];
      expect(listed.map((v) => v.id).sort()).toEqual(["1.0.0", "1.0.0+001"]);
   });

   it(
      "under explicit promotion, builds a default-scope version before anything is latest, and serves its table once promoted",
      async () => {
         setEnv(FLAG, "on");
         setEnv(PROMOTION, "explicit");
         const published = await publish(
            "held",
            writePackage("held-1", { name: "held", version: "1.0.0" }, 5),
         );
         expect(published.status).toBe(200);
         const pkg = await body(
            await fetch(`${pkgApi("held")}?versionId=1.0.0`),
         );
         expect(pkg.latestVersion ?? null).toBeNull();

         const created = await fetch(`${pkgApi("held")}/materializations`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ versionId: "1.0.0" }),
         });
         const createdBody = await body(created);
         expect([created.status, createdBody.reason]).toEqual([201, undefined]);
         const built = await settle("held", String(createdBody.id));
         expect(built.status).toBe("MANIFEST_FILE_READY");
         expect(built.metadata).toMatchObject({
            versionId: "1.0.0",
            scope: "package",
         });

         const promoted = await fetch(`${pkgApi("held")}/latest`, {
            method: "PUT",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ version: "1.0.0" }),
         });
         expect(promoted.status).toBe(200);

         // Served from the table: the SQL that ran reads `summary`, not the
         // source's own SQL.
         const query = await fetch(
            `${pkgApi("held")}/models/model.malloy/query`,
            {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({
                  query: "run: summary -> { select: answer }",
               }),
            },
         );
         expect(query.status).toBe(200);
         const result = JSON.parse(String((await body(query)).result)) as {
            sql: string;
         };
         expect(result.sql).toContain('"summary"');
         expect(result.sql).not.toContain("SELECT 5 AS answer");
      },
      { timeout: 300_000 },
   );
});
