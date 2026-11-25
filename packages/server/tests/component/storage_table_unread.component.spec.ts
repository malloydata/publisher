// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` table whose bind-time read fails (its destination session
 * could not read its columns) is left unread: its queries answer live, equal
 * to live, and it is read again with backoff, serving once a read succeeds.
 * Never judged incompatible for a failed read, and named while unread by a
 * `storage-table-unread` package warning. The read is made to fail by
 * standing in for the package's destination read
 * (`readStorageColumns`) on the bind a failed query read triggers.
 *
 * `#@ persist`, `#(materialize)` and `#(rollup)`, each in a DuckLake
 * destination. Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the warehouse
 * and the DuckLake catalog.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { ObservedColumns } from "../../src/service/materialization/domain/types";
import type { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";

const ENV = "e1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const RAW_ROWS = `SELECT * FROM (VALUES
    (1, CAST(9.0 AS DECIMAL(10,1))), (1, CAST(10.0 AS DECIMAL(10,1))),
    (2, CAST(2.4 AS DECIMAL(10,1)))
  ) AS t(k, amt)`;

const HEAD = `##! experimental { persistence }
source: raw is warehouse.table('public.su_raw')
`;

/** Grouped by a number, never a string, so every answer compares as live's. */
const AS_TEXT = "-> { group_by: k, total_text is total::string; order_by: k }";

const MECHANISMS = [
   {
      label: "#@ persist",
      cell: "s-unread ps",
      model: `${HEAD}
#@ persist storage="lake"
source: a is raw -> { group_by: k; aggregate: total is amt.sum() }
`,
      table: /^a$/,
      query: `run: a ${AS_TEXT}`,
   },
   {
      label: "#(materialize)",
      cell: "s-unread ms",
      model: `${HEAD}
#(materialize) storage="lake"
source: a is raw extend { dimension: k2 is k }
`,
      table: /^a$/,
      query: `run: a -> { group_by: k; aggregate: total is amt.sum() } ${AS_TEXT}`,
   },
   {
      label: "#(rollup)",
      cell: "s-unread rs",
      model: `${HEAD}
source: a is raw extend {
  #(rollup) grain=["k"] storage="lake"
  measure: total is amt.sum()
}
`,
      table: /^a__rollup__/,
      query: `run: a -> { group_by: k; aggregate: total } ${AS_TEXT}`,
   },
];

const EXPECTED = [
   { k: 1, total_text: "19.0" },
   { k: 2, total_text: "2.4" },
];

interface Entry {
   sourceName?: string;
   sourceEntityId: string;
}

for (const mechanism of POSTGRES_URL ? MECHANISMS : []) {
   describe(`a storage= table whose bind-time read fails answers live, then serves once read (${mechanism.label}, ${mechanism.cell}; Postgres, DuckLake)`, () => {
      let restore: () => void;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let pkg: Package;
      let service: MaterializationService;
      const dbs: string[] = [];
      let admin: PostgresConnection;
      let address: string;

      const ask = async () => {
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query: mechanism.query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: JSON.parse(JSON.stringify(result.compactResult)) as unknown,
            kind: result.path?.servedBy?.kind,
         };
      };
      const until = async (ok: () => Promise<boolean>) => {
         const deadline = Date.now() + 60_000;
         while (!(await ok())) {
            if (Date.now() > deadline) throw new Error("never settled");
            await new Promise((r) => setTimeout(r, 100));
         }
      };
      const mz = () =>
         pkg.materialization() as unknown as {
            readStorageColumns: (
               ...a: unknown[]
            ) => Promise<ReadonlyMap<string, ObservedColumns>>;
            onIncompatible(handles: readonly string[]): void;
         };

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         let envPath: string;
         ({ root, envPath, db, repo } = await openTempStorage("su-", {
            id: "env-1",
            name: ENV,
         }));
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": mechanism.model },
            { manifest: { name: PKG, description: "fixture" } },
         );
         const token = `${process.pid}_${Date.now()}`;
         dbs.push(`su_wh_${token}`, `su_lake_${token}`);
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         for (const name of dbs) await admin.runSQL(`CREATE DATABASE ${name}`);
         const parsed = new URL(POSTGRES_URL!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const bucket = path.join(root, "bucket_lake");
         fs.mkdirSync(bucket);
         env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "postgres",
                  postgresConnection: { ...pg, databaseName: dbs[0] },
               } as never,
            ],
            [
               {
                  name: "lake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: { ...pg, databaseName: dbs[1] },
                     },
                     storage: { bucketUrl: bucket },
                  },
               } as never,
            ],
         );
         const warehouse = await env.getMalloyConnection("warehouse");
         await warehouse.runSQL(`CREATE TABLE public.su_raw AS ${RAW_ROWS}`);
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         const created = await service.createMaterialization(ENV, PKG, {});
         let m: Materialization | null = null;
         await until(async () => {
            m = await repo.getMaterializationById(created.id);
            return !!m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status);
         });
         expect([m!.status, m!.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const entry = Object.values(
            (m!.manifest as { entries: Record<string, Entry> }).entries,
         ).find((e) => mechanism.table.test(e.sourceName ?? ""))!;
         address = entry.sourceEntityId;
         pkg = await env.getPackage(parsePackageName(PKG), false);
         await until(async () => (await ask()).kind !== "live");
      }, 300_000);
      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         for (const name of dbs) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close().catch(() => undefined);
         if (root) fs.rmSync(root, { recursive: true, force: true });
         restore?.();
      });

      it("serves its table, equal to live, before any read fails", async () => {
         const served = await ask();
         expect(served.kind).not.toBe("live");
         expect(served.rows).toEqual(EXPECTED);
      });

      it("answers live while its read fails, never judged incompatible, then serves once a read succeeds", async () => {
         const m = mz();
         const read = m.readStorageColumns.bind(m);
         let failing = true;
         let reads = 0;
         m.readStorageColumns = async (...a: unknown[]) => {
            const out = await read(...a);
            if (!failing || !out.has(address)) return out;
            reads += 1;
            const next = new Map(out);
            next.set(address, {
               unread: "catalog_query_failed",
            } as ObservedColumns);
            return next;
         };
         try {
            // A failed query read's trigger: the package reads and binds
            // every bound table again, through the stand-in.
            m.onIncompatible([address]);
            await until(async () => reads > 0);
            await until(async () => (await ask()).kind === "live");
            const live = await ask();
            expect(live.rows).toEqual(EXPECTED);
            // Named as a `storage=` table, in its destination, with its
            // reason and its pending retry.
            const unread = (pkg.getPackageMetadata().warnings ?? []).filter(
               (w) => /table-unread$/.test(w.code ?? ""),
            );
            expect(unread.map((w) => [w.code, w.reason])).toEqual([
               ["storage-table-unread", "catalog_query_failed"],
            ]);
            // The unread table's own source, a rollup's not its base's.
            expect(unread[0].sourceEntityId).toBe(address);
            expect(unread[0].sourceName).toMatch(mechanism.table);
            expect(unread[0].message).toContain(
               "could not be read in its storage= destination `lake` when it was bound (the catalog query failed), so its queries are answered live. A retry is pending.",
            );
            expect(pkg.incompatibleTables().has(address)).toBe(false);
            expect(
               pkg
                  .materialization()
                  .serveVerdicts()
                  .filter((v) => v.reason === "incompatible"),
            ).toEqual([]);
            // The read succeeds again: the retry (1 s, doubling) reads it.
            failing = false;
            await until(async () => (await ask()).kind !== "live");
            const served = await ask();
            expect(served.rows).toEqual(EXPECTED);
            expect(
               (pkg.getPackageMetadata().warnings ?? []).filter((w) =>
                  /table-unread$/.test(w.code ?? ""),
               ),
            ).toEqual([]);
         } finally {
            m.readStorageColumns = read;
         }
      }, 120_000);
   });
}
