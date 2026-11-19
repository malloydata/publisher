// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { MaterializationService } from "./materialization_service";
import { KEPT_TABLES_KEY } from "./reclaimer";

/**
 * Two author `name=` tables that differ only in case are two tables on
 * Postgres. Renamed away from, each is decided on its own: both are kept
 * (an author's name is never dropped) and both are listed as kept, so
 * neither is forgotten. Gated on PUBLISHER_TEST_POSTGRES_URL.
 */

const URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";
const SCHEMA = `casevar_${Math.random().toString(36).slice(2, 8)}`;

const model = (upper: string, lower: string) =>
   [
      "##! experimental.persistence",
      `#@ persist name="${SCHEMA}.${upper}"`,
      `source: a is warehouse.sql("SELECT 'upper' AS who")`,
      `#@ persist name="${SCHEMA}.${lower}"`,
      `source: b is warehouse.sql("SELECT 'lower' AS who")`,
      "",
   ].join("\n");

describe.skipIf(!URL)("case-variant author names on Postgres", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restore: () => void;

   async function sql(text: string): Promise<Record<string, unknown>[]> {
      const { Client } = await import("pg");
      const c = new Client({ connectionString: URL });
      await c.connect();
      try {
         return (await c.query(text)).rows;
      } finally {
         await c.end();
      }
   }

   async function tables(): Promise<string[]> {
      return (
         await sql(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = '${SCHEMA}' ORDER BY 1`,
         )
      ).map((r) => String(r["table_name"]));
   }

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
      const deadline = Date.now() + 60_000;
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
      ).runningAbortControllers;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            while (running.has(created.id)) {
               await new Promise((r) => setTimeout(r, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
   }

   beforeEach(async () => {
      restore = installSettings({ env: {} }).restore;
      ({ root, envPath, db, repo } = await openTempStorage("casevar-", {
         id: ENV_ID,
         name: ENV,
      }));
      writePackageFiles(
         path.join(envPath, PKG),
         { "model.malloy": model("Orders", "orders") },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, [
         {
            name: "warehouse",
            type: "postgres",
            postgresConnection: { connectionString: URL },
         } as never,
      ]);
      await sql(`CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   });

   afterEach(async () => {
      await sql(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`).catch(() => {});
      await env.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restore();
   });

   it("keeps and lists both tables a rename moved away from", async () => {
      const first = await build();
      expect(first.status).toBe("MANIFEST_FILE_READY");
      fs.writeFileSync(
         path.join(envPath, PKG, "model.malloy"),
         model("Orders_v2", "orders_v2"),
      );
      await env.getPackage(parsePackageName(PKG), true);
      const second = await build();
      expect(second.status).toBe("MANIFEST_FILE_READY");

      expect(await tables()).toEqual(
         expect.arrayContaining(["Orders", "orders", "Orders_v2", "orders_v2"]),
      );
      const kept = (second.metadata?.[KEPT_TABLES_KEY] ?? []) as {
         physicalTableName: string;
      }[];
      expect(kept.map((t) => t.physicalTableName).sort()).toEqual(
         [`${SCHEMA}.Orders`, `${SCHEMA}.orders`].sort(),
      );
   });
});
