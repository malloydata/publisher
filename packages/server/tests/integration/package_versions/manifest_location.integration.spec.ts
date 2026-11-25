// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A published version bound to a build manifest on this server's own disk:
 * an absolute path or a file:// URL, as a deployment that keeps manifests on
 * a mounted volume (or a local stack's stand-in for a bucket) names them. The
 * manifest loader has always read those; the version routes take them too,
 * through the version manifest route, the deprecated PATCH and the publish.
 * A relative path is refused on all three: it would resolve against the
 * server's working directory.
 *
 * The evidence is the served query: bound to a manifest that names a table,
 * the version's persisted source reads that table rather than computing from
 * its base data.
 */

import { withNativeSchema } from "../../../src/test_helpers/host_manifest";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";
import {
   deleteLeftoverEnvironment,
   RestE2EEnv,
   startRestE2E,
} from "../../harness/rest_e2e";

const ENV_NAME = "manifest-location-env";
const PKG = "persist-test";
const FIXTURE = path.resolve(__dirname, "../../fixtures/persist-test");
/**
 * The table every manifest here binds: a temporary table on the version's
 * sandbox, made from one SELECT (raw SQL on the sandbox runs only as a single
 * SELECT, duckdb_lockdown.ts), so its name is the one the route returns.
 */
let BOUND_TABLE = "";
/** The rows BOUND_TABLE holds: a count no live answer gives. */
const BOUND_ROWS = 13;

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";

const api = (suffix: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages${suffix}`;

async function call(
   method: string,
   suffix: string,
   body?: unknown,
): Promise<{ status: number; json: Record<string, unknown> }> {
   const res = await fetch(api(suffix), {
      method,
      headers: { "content-type": "application/json" },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
   });
   return {
      status: res.status,
      json: (await res.json()) as Record<string, unknown>,
   };
}

/** The persisted source's id, which a manifest entry must be keyed by. */
async function orderSummarySourceEntityId(): Promise<string> {
   const { json } = await call("GET", `/${PKG}?versionId=1.0.0`);
   const sources =
      (
         json.buildPlan as {
            sources?: Record<string, { sourceEntityId?: string }>;
         }
      )?.sources ?? {};
   const id = Object.values(sources)[0]?.sourceEntityId;
   expect(typeof id).toBe("string");
   return id as string;
}

/** A manifest file routing the persisted source to BOUND_TABLE. */
function writeManifest(name: string, sourceEntityId: string): string {
   const file = path.join(root, name);
   fs.writeFileSync(
      file,
      JSON.stringify({
         builtAt: new Date().toISOString(),
         strict: false,
         entries: {
            // With the column types its build would record, as a host
            // sends them (host_manifest.ts).
            [sourceEntityId]: withNativeSchema(
               {
                  sourceEntityId,
                  sourceName: "order_summary",
                  physicalTableName: BOUND_TABLE,
                  connectionName: "duckdb",
               },
               [
                  { name: "category", type: "VARCHAR" },
                  { name: "total_orders", type: "BIGINT" },
                  { name: "total_revenue", type: "BIGINT" },
               ],
            ),
         },
      }),
   );
   return file;
}

/**
 * Run the persisted source: live, from the package's CSV; bound, from
 * BOUND_TABLE's BOUND_ROWS rows.
 */
async function runOrderSummary(): Promise<{ status: number; text: string }> {
   const res = await fetch(
      api(`/${PKG}/models/persist_test.malloy/query?versionId=1.0.0`),
      {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            query: "run: order_summary -> { aggregate: c is count() }",
            compactJson: true,
         }),
      },
   );
   return { status: res.status, text: await res.text() };
}

async function expectBoundTo(location: string): Promise<void> {
   const version = await call("GET", `/${PKG}/versions/1.0.0`);
   expect(version.json.manifestLocation).toBe(location);
   // Served from the bound table: its BOUND_ROWS rows, not live's.
   const served = await runOrderSummary();
   expect(served.status).toBe(200);
   expect(countOf(served.text)).toBe(BOUND_ROWS);
}

/** The `c` a run of the persisted source answered. */
function countOf(text: string): number {
   const body = JSON.parse(text) as { result?: string };
   const rows = JSON.parse(body.result ?? "[]") as { c: number }[];
   return Number(rows[0]?.c);
}

async function expectLive(): Promise<void> {
   const live = await runOrderSummary();
   expect(live.status).toBe(200);
   expect(countOf(live.text)).not.toBe(BOUND_ROWS);
}

describe("a version bound to a manifest on this server's disk", () => {
   let sourceEntityId = "";

   beforeAll(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "manifest-location-")),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      await deleteLeftoverEnvironment(baseUrl, ENV_NAME);
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
      // The fixture's publisher.json declares 1.0.0.
      const published = await call("POST", "", {
         name: PKG,
         location: FIXTURE,
      });
      expect([published.status, published.json.versionId]).toEqual([
         200,
         "1.0.0",
      ]);
      sourceEntityId = await orderSummarySourceEntityId();
      // The table every manifest here binds, in the version's own DuckDB:
      // BOUND_ROWS rows, so an answer read from it is told from live's.
      const created_ = await fetch(
         `${api(`/${PKG}/connections/duckdb/sqlTemporaryTable`)}?versionId=1.0.0`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
               sqlStatement: `SELECT 'c' || i AS category, 1::BIGINT AS total_orders, 1::BIGINT AS total_revenue FROM range(${BOUND_ROWS}) t(i)`,
            }),
         },
      );
      expect(created_.status).toBeLessThan(300);
      BOUND_TABLE = ((await created_.json()) as { table?: string }).table ?? "";
      expect(BOUND_TABLE).not.toBe("");
   }, 180_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("serves live with no manifest", async () => {
      await expectLive();
   });

   it("binds an absolute path, then a file:// URL, through the version manifest route, and clears with null", async () => {
      const file = writeManifest("by-path.json", sourceEntityId);
      const byPath = await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestLocation: file,
      });
      expect(byPath.status).toBe(200);
      await expectBoundTo(file);

      const url = pathToFileURL(
         writeManifest("by-url.json", sourceEntityId),
      ).href;
      const byUrl = await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestLocation: url,
      });
      expect(byUrl.status).toBe(200);
      await expectBoundTo(url);

      const cleared = await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestLocation: null,
      });
      expect(cleared.status).toBe(200);
      await expectLive();
   });

   it("clears a binding with an empty body, as a client that drops null fields sends it", async () => {
      const file = writeManifest("then-empty.json", sourceEntityId);
      const bound = await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestLocation: file,
      });
      expect(bound.status).toBe(200);
      await expectBoundTo(file);

      // A typo is refused and leaves the binding as it was.
      const typo = await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestUri: file,
      });
      expect(typo.status).toBe(400);
      await expectBoundTo(file);

      const cleared = await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {});
      expect(cleared.status).toBe(200);
      await expectLive();
   });

   it("binds and clears through the legacy combined-name PATCH, whose body carries other fields", async () => {
      // `PATCH …/packages/<pkg>___<version>` is rewritten to this PUT with
      // the old package-update body, plus the `versionId` the legacy body
      // middleware adds; only `manifestLocation` is read from it.
      const file = writeManifest("by-legacy-patch.json", sourceEntityId);
      const bound = await call("PATCH", `/${PKG}___1.0.0`, {
         name: `${PKG}___1.0.0`,
         description: "an orchestrator's full package body",
         location: "ignored",
         manifestLocation: file,
      });
      expect(bound.status).toBe(200);
      await expectBoundTo(file);

      const cleared = await call("PATCH", `/${PKG}___1.0.0`, {
         name: `${PKG}___1.0.0`,
         manifestLocation: null,
      });
      expect(cleared.status).toBe(200);
      await expectLive();

      // As before: the field must be present on this path.
      const missing = await call("PATCH", `/${PKG}___1.0.0`, {
         name: `${PKG}___1.0.0`,
      });
      expect(missing.status).toBe(400);
   });

   it("refuses a PUT not sent as JSON, rather than reading it as a clear", async () => {
      const file = writeManifest("then-text.json", sourceEntityId);
      await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestLocation: file,
      });
      await expectBoundTo(file);
      const res = await fetch(api(`/${PKG}/versions/1.0.0/manifest`), {
         method: "PUT",
         body: JSON.stringify({ manifestLocation: file }),
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("application/json");
      await expectBoundTo(file);
   });

   it("binds an absolute path through the deprecated PATCH, which binds latest", async () => {
      const file = writeManifest("by-patch.json", sourceEntityId);
      const patched = await call("PATCH", `/${PKG}`, {
         name: PKG,
         manifestLocation: file,
      });
      expect(patched.status).toBe(200);
      await expectBoundTo(file);
      await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestLocation: null,
      });
      await expectLive();
   });

   it("binds an absolute path sent with a publish of the version it holds", async () => {
      const file = writeManifest("by-publish.json", sourceEntityId);
      const placed = await call("POST", "", {
         name: PKG,
         location: FIXTURE,
         manifestLocation: file,
      });
      expect([placed.status, placed.json.versionId]).toEqual([200, "1.0.0"]);
      await expectBoundTo(file);
      await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
         manifestLocation: null,
      });
      await expectLive();
   });

   it("refuses a relative path on all three, binding nothing", async () => {
      const relative = "manifests/by-path.json";
      const answers = [
         await call("PUT", `/${PKG}/versions/1.0.0/manifest`, {
            manifestLocation: relative,
         }),
         await call("PATCH", `/${PKG}`, {
            name: PKG,
            manifestLocation: relative,
         }),
         await call("POST", "", {
            name: PKG,
            location: FIXTURE,
            manifestLocation: relative,
         }),
      ];
      for (const answer of answers) {
         expect(answer.status).toBe(400);
         expect(String(answer.json.message)).toContain("an absolute path");
      }
      expect(
         (await call("GET", `/${PKG}/versions/1.0.0`)).json.manifestLocation,
      ).toBeNull();
      await expectLive();
   });
});
