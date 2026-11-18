// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * DELETE …/connections/{connectionName}/materializedTables/{tableName}
 * (drop-materialized-table) through the HTTP route, the flow a host that
 * owns a package's materialization lifecycle runs: build, find the run's
 * table, see the drop refused while the run's record names it and the
 * package serves it, delete the record (record-only), drop the table, and
 * find the package answering live. On the package's own `duckdb` sandbox,
 * with a table Publisher named, so its ownership mark is what proves it is
 * the package's once the record is gone. The guards on every warehouse are
 * tests/component/drop_materialized_table.component.spec.ts.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";
import { pollFor, TERMINAL_STATUSES, waitUntilOk } from "./polling";

const ENV = "drop-table-env";
const PKG = "drop-table-pkg";
const API = `/api/v0/environments/${ENV}/packages/${PKG}`;

const MODEL = [
   "##! experimental.persistence",
   `source: base is duckdb.sql("SELECT 'live' AS who, 1 AS k")`,
   "#@ persist",
   "source: summary is base -> { group_by: who }",
   "",
].join("\n");

describe("drop-materialized-table over HTTP", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let packageDir: string;

   const url = (p: string) => `${baseUrl}${API}${p}`;
   const dropUrl = (table: string, query = "", connection = "duckdb") =>
      url(
         `/connections/${connection}/materializedTables/${encodeURIComponent(table)}${query}`,
      );
   const drop = (table: string, query = "", connection = "duckdb") =>
      fetch(dropUrl(table, query, connection), { method: "DELETE" });

   async function bodyOf(res: Response) {
      return (await res.json()) as {
         code: number;
         message: string;
         reason?: string;
      };
   }

   async function who(): Promise<string> {
      const res = await fetch(url("/models/model.malloy/query"), {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            query: "run: summary -> { select: who }",
            compactJson: true,
         }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      return (JSON.parse(body.result) as { who: string }[])[0].who;
   }

   beforeAll(async () => {
      packageDir = fs.mkdtempSync(path.join(os.tmpdir(), "drop-table-pkg-"));
      fs.writeFileSync(path.join(packageDir, "model.malloy"), MODEL);
      fs.writeFileSync(
         path.join(packageDir, "publisher.json"),
         JSON.stringify({ name: PKG, version: "1.0.0", description: "x" }),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV,
            packages: [{ name: PKG, location: packageDir }],
            connections: [],
         }),
      });
      if (!created.ok) {
         throw new Error(
            `Failed to create the environment (${created.status}): ${await created.text()}`,
         );
      }
      await waitUntilOk(url(""), "The package");
   });

   afterAll(async () => {
      if (baseUrl) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV}`, {
            method: "DELETE",
         }).catch(() => undefined);
      }
      await env?.stop();
      env = null;
      if (packageDir) fs.rmSync(packageDir, { recursive: true, force: true });
   });

   it("refuses while a run names the table, drops it once the record is gone, and the package answers live", async () => {
      const created = (await (
         await fetch(url("/materializations"), {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "{}",
         })
      ).json()) as { id: string };
      const run = await pollFor(
         async () =>
            (await (
               await fetch(url(`/materializations/${created.id}`))
            ).json()) as {
               status: string;
               manifest?: {
                  entries?: Record<string, { physicalTableName: string }>;
               };
            },
         (m) => TERMINAL_STATUSES.includes(m.status),
         "the build did not settle",
         90_000,
      );
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const table = Object.values(run.manifest!.entries!)[0].physicalTableName;
      // Publisher named it: its mark, not an author's name=, proves it.
      expect(table).toMatch(/__p[0-9a-f]{8}$/);

      // The run names it and the package serves it.
      let res = await drop(table);
      expect(res.status).toBe(409);
      expect(await bodyOf(res)).toMatchObject({
         code: 409,
         reason: "TABLE_IN_USE",
      });
      res = await drop(table, `?materializationId=${created.id}`);
      expect(res.status).toBe(409);
      expect((await bodyOf(res)).reason).toBe("TABLE_IN_USE");

      // Malformed or unknown inputs.
      res = await drop(table, "?materializationId=");
      expect(res.status).toBe(400);
      res = await drop(table, "?materializationId=no-such-run");
      expect(res.status).toBe(404);
      res = await drop(table, "", "no_such_connection");
      expect(res.status).toBe(404);
      expect((await bodyOf(res)).reason).toBeUndefined();
      res = await drop("a..b");
      expect(res.status).toBe(400);
      res = await drop(table, "?versionId=not-a-version");
      expect(res.status).toBe(400);

      // Record-only delete: the package rebinds to live.
      res = await fetch(url(`/materializations/${created.id}`), {
         method: "DELETE",
      });
      expect(res.status).toBe(204);
      expect(await who()).toBe("live");

      res = await drop(table);
      expect(res.status).toBe(204);
      expect(await who()).toBe("live");

      // Gone, and nothing records it: nothing proves it was the package's.
      res = await drop(table);
      expect(res.status).toBe(404);
      expect((await bodyOf(res)).reason).toBe("TABLE_NOT_FOUND");
   });

   it("refuses a table the package did not build", async () => {
      // A table nothing built: no mark and no record.
      const res = await drop("never_built_t");
      expect(res.status).toBe(404);
      expect((await bodyOf(res)).reason).toBe("TABLE_NOT_FOUND");
   });
});
