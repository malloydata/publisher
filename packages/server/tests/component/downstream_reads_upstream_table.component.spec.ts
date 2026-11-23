// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A downstream is built from its upstream's stored table, never by
 * recomputing the upstream from raw: the drop-raw proof. The upstream is
 * built alone; its raw table is then dropped out of band; the downstream,
 * built next, still builds, reports `upstreamReuse: "reused"`, names the
 * upstream's table in `builtFrom` (with that build's `builtAt`), and its
 * stored rows equal what live answered before the drop, as text. Its build
 * SQL reads the upstream's table. DuckDB, and Postgres when
 * `PUBLISHER_TEST_POSTGRES_URL` names one.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/** The raw rows, created in the warehouse before the package loads. */
const RAW_ROWS = `SELECT * FROM (VALUES
    ('a', CAST(9.0 AS DECIMAL(10,1))), ('a', CAST(10.5 AS DECIMAL(10,1))),
    ('b', CAST(2.4 AS DECIMAL(10,1)))
  ) AS t(cat, amt)`;

const HEAD = `##! experimental { persistence }
source: raw is warehouse.table('__RAW__')
`;

/** Each total as the warehouse renders it, so rows compare as text. */
const AS_TEXT =
   "-> { group_by: cat, total_text is total::string; order_by: cat }";

interface Mechanism {
   label: string;
   /** The package's files; `m.malloy` declares the upstream `a`. */
   files: Record<string, string>;
   /** The upstream's source name, built alone first. */
   upstream: string;
   /** The downstream's entry's `sourceName`. */
   downstream: RegExp;
   query: string;
   servedBy: string;
   /** Its table holds `cat` and `total`, compared directly. */
   storesTotal?: boolean;
}

const PERSIST_A = `#@ persist name="dr_a"
source: a is raw -> { select: cat, amt }
`;

const MECHANISMS: Mechanism[] = [
   {
      label: "#@ persist over #@ persist",
      files: {
         "m.malloy": `${HEAD}${PERSIST_A}
#@ persist name="dr_b"
source: b is a -> { group_by: cat; aggregate: total is amt.sum() }
`,
      },
      upstream: "a",
      downstream: /^b$/,
      query: `run: b ${AS_TEXT}`,
      servedBy: "materialized",
      storesTotal: true,
   },
   {
      label: "persisted view",
      files: {
         "m.malloy": `${HEAD}${PERSIST_A}
source: s is a extend {
  #@ persist name="dr_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      },
      upstream: "a",
      downstream: /^s__by_cat__view$/,
      query: `run: s -> by_cat ${AS_TEXT}`,
      servedBy: "view",
      storesTotal: true,
   },
   {
      label: "persisted query",
      files: {
         "m.malloy": `${HEAD}${PERSIST_A}
#@ persist name="dr_q"
query: q is a -> { group_by: cat; aggregate: total is amt.sum() }
`,
      },
      upstream: "a",
      downstream: /^q__query$/,
      query: `run: q ${AS_TEXT}`,
      servedBy: "query",
      storesTotal: true,
   },
   {
      label: "persisted view over a #(materialize) source",
      files: {
         "m.malloy": `${HEAD}#(materialize)
source: a is raw extend {
  dimension: cat2 is cat
}

source: s is a extend {
  #@ persist name="dr_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      },
      upstream: "a",
      downstream: /^s__by_cat__view$/,
      query: `run: s -> by_cat ${AS_TEXT}`,
      servedBy: "view",
      storesTotal: true,
   },
   {
      label: "persisted query over a #(materialize) source",
      files: {
         "m.malloy": `${HEAD}#(materialize)
source: a is raw extend {
  dimension: cat2 is cat
}

#@ persist name="dr_q"
query: q is a -> { group_by: cat; aggregate: total is amt.sum() }
`,
      },
      upstream: "a",
      downstream: /^q__query$/,
      query: `run: q ${AS_TEXT}`,
      servedBy: "query",
      storesTotal: true,
   },
   {
      label: "persisted view over a #(materialize) source that inlines a join_one",
      files: {
         "m.malloy": `${HEAD}source: cats is warehouse.sql("""
  SELECT 'a' AS cat, 'Alpha' AS label UNION ALL SELECT 'b', 'Beta'
""") extend { primary_key: cat }

#(materialize) inline=["k"]
source: a is raw extend {
  join_one: k is cats on cat = k.cat
}

source: s is a extend {
  #@ persist name="dr_v"
  view: by_cat is {
    group_by: cat is k.label
    aggregate: total is amt.sum()
  }
}
`,
      },
      upstream: "a",
      downstream: /^s__by_cat__view$/,
      query: `run: s -> by_cat ${AS_TEXT}`,
      servedBy: "view",
      storesTotal: true,
   },
   {
      label: "rollup over an inherited #@ persist",
      files: {
         "m.malloy": `${HEAD}${PERSIST_A}
source: rolled is a extend {
  #(rollup) grain=["cat"]
  measure: total is amt.sum()
}
`,
      },
      upstream: "a",
      downstream: /rolled/,
      query: `run: rolled -> { group_by: cat; aggregate: total } ${AS_TEXT}`,
      servedBy: "rollup",
   },
   {
      label: "rollup over its own #@ persist",
      files: {
         "m.malloy": `${HEAD}#@ persist name="dr_a"
source: a is raw -> { select: cat, amt } extend {
  #(rollup) grain=["cat"]
  measure: total is amt.sum()
}
`,
      },
      upstream: "a",
      downstream: /^(?!a$)/,
      query: `run: a -> { group_by: cat; aggregate: total } ${AS_TEXT}`,
      servedBy: "rollup",
   },
   {
      label: "rollup over a #@ persist source in another file",
      files: {
         "base.malloy": `${HEAD}${PERSIST_A}`,
         "m.malloy": `##! experimental { persistence }
import "base.malloy"
source: rolled is a extend {
  #(rollup) grain=["cat"]
  measure: total is amt.sum()
}
`,
      },
      upstream: "a",
      downstream: /rolled/,
      query: `run: rolled -> { group_by: cat; aggregate: total } ${AS_TEXT}`,
      servedBy: "rollup",
   },
   {
      label: "rollup over an extension of a #(materialize) source",
      files: {
         "m.malloy": `${HEAD}#(materialize)
source: a is raw extend { dimension: cat2 is cat }

source: rolled is a extend {
  #(rollup) grain=["cat"]
  measure: total is amt.sum()
}
`,
      },
      upstream: "a",
      downstream: /rolled/,
      query: `run: rolled -> { group_by: cat; aggregate: total } ${AS_TEXT}`,
      servedBy: "rollup",
   },
   {
      label: "rollup over a #(materialize) source",
      files: {
         "m.malloy": `${HEAD}#(materialize)
source: a is raw extend {
  dimension: cat2 is cat
  #(rollup) grain=["cat"]
  measure: total is amt.sum()
}
`,
      },
      upstream: "a",
      downstream: /^(?!a$)/,
      query: `run: a -> { group_by: cat; aggregate: total } ${AS_TEXT}`,
      servedBy: "rollup",
   },
   {
      label: "extension of a #(materialize) parent two files away, through a pass-through file",
      files: {
         "a.malloy": `${HEAD}#(materialize)
source: a is raw extend { dimension: cat2 is cat }
`,
         "b.malloy": `##! experimental { persistence }
import "a.malloy"
source: mid is a extend { dimension: cat3 is cat }
`,
         "m.malloy": `##! experimental { persistence }
import "b.malloy"
#(materialize)
source: ext is mid extend { dimension: big is amt > 5 }
`,
      },
      upstream: "a",
      downstream: /^ext$/,
      query: `run: ext -> { group_by: cat; aggregate: total is amt.sum() } ${AS_TEXT}`,
      servedBy: "materialized",
   },
   {
      label: "extension of a #(materialize) parent in the file it imports",
      files: {
         "a.malloy": `${HEAD}#(materialize)
source: a is raw extend { dimension: cat2 is cat }
`,
         "m.malloy": `##! experimental { persistence }
import "a.malloy"
#(materialize)
source: ext is a extend { dimension: big is amt > 5 }
`,
      },
      upstream: "a",
      downstream: /^ext$/,
      query: `run: ext -> { group_by: cat; aggregate: total is amt.sum() } ${AS_TEXT}`,
      servedBy: "materialized",
   },
];

interface Answer {
   rows: unknown;
   kind?: string;
}

const tiers = [
   {
      label: "DuckDB",
      raw: "main.dr_raw",
      tablePrefix: "",
      rowsSQL: (sql: string) => sql,
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         } as unknown,
         drop: undefined as undefined | (() => Promise<void>),
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              raw: "public.dr_raw",
              // Postgres reads a table's columns only by a qualified name.
              tablePrefix: "public.",
              // Its runSQL reads each row from a `row` JSON column.
              rowsSQL: (sql: string) =>
                 `SELECT row_to_json(t) AS row FROM (${sql}) t`,
              warehouse: async () => {
                 const db = `dr_${process.pid}_${Date.now()}`;
                 const admin = new PostgresConnection({
                    name: "admin",
                    connectionString: POSTGRES_URL,
                 });
                 await admin.runSQL(`CREATE DATABASE ${db}`);
                 const parsed = new URL(POSTGRES_URL);
                 return {
                    config: {
                       name: "warehouse",
                       type: "postgres",
                       postgresConnection: {
                          host: parsed.hostname,
                          port: Number(parsed.port || 5432),
                          userName: decodeURIComponent(parsed.username),
                          password: decodeURIComponent(parsed.password),
                          databaseName: db,
                       },
                    } as unknown,
                    drop: async () => {
                       await admin
                          .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                          .catch(() => undefined);
                       await admin.close();
                    },
                 };
              },
           },
        ]
      : []),
];

interface Entry {
   sourceName?: string;
   physicalTableName: string;
   upstreamReuse?: string;
   upstreamRecomputeReason?: string;
   builtAt?: string;
   builtFrom?: {
      sourceEntityId: string;
      physicalTableName: string;
      builtAt?: string;
   }[];
}

const entriesOf = (m: Materialization): Record<string, Entry> =>
   (m.manifest as { entries?: Record<string, Entry> }).entries ?? {};

for (const [tier, mechanism] of tiers.flatMap((t) =>
   MECHANISMS.map((m) => [t, m] as const),
)) {
   describe(`a downstream builds from its upstream's table with its raw table gone (${mechanism.label}, ${tier.label})`, () => {
      let restore: () => void;
      let root: string;
      let env: Environment;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let service: MaterializationService;
      let pkg: Package;
      let drop: (() => Promise<void>) | undefined;
      let first: Materialization;
      let second: Materialization;
      let liveBefore: Answer;
      let served: Answer;

      const ask = async (query: string): Promise<Answer> => {
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: JSON.parse(JSON.stringify(result.compactResult)),
            kind: (result.path as { servedBy?: { kind?: string } } | undefined)
               ?.servedBy?.kind,
         };
      };
      const build = async (opts: object): Promise<Materialization> => {
         const created = await service.createMaterialization(ENV, PKG, opts);
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
               while (running.has(created.id)) {
                  await new Promise((r) => setTimeout(r, 10));
               }
               return (await repo.getMaterializationById(created.id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((r) => setTimeout(r, 20));
         }
      };

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "dr-")));
         const envPath = path.join(root, "env");
         fs.mkdirSync(envPath);
         writePackageFiles(
            path.join(envPath, PKG),
            Object.fromEntries(
               Object.entries(mechanism.files).map(([f, text]) => [
                  f,
                  text
                     .replace("__RAW__", tier.raw)
                     .replace(/name="dr_/g, `name="${tier.tablePrefix}dr_`),
               ]),
            ),
            {
               manifest: { name: PKG, description: "fixture" },
            },
         );
         db = new DuckDBConnection(":memory:");
         await db.initialize();
         await initializeSchema(db);
         await db.run(
            "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            ["env-1", ENV, envPath],
         );
         repo = createDuckDBRepository(db);
         const w = await tier.warehouse();
         drop = w.drop;
         env = await Environment.create(ENV, envPath, [w.config as never], []);
         const warehouse = await env.getMalloyConnection("warehouse");
         await warehouse.runSQL(`CREATE TABLE ${tier.raw} AS ${RAW_ROWS}`);
         await env.addPackage(PKG);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         liveBefore = await ask(mechanism.query);
         first = await build({ sourceNames: [mechanism.upstream] });
         // Out of band: the raw rows are gone. A downstream that recomputed
         // its upstream from raw would hold no rows.
         await warehouse.runSQL(`DELETE FROM ${tier.raw}`);
         second = await build({ strictUpstreams: true });
         served = await ask(mechanism.query);
      }, 300_000);
      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         await drop?.();
         fs.rmSync(root, { recursive: true, force: true });
         restore();
         await freshLoadPool();
      });

      it("builds the downstream under strict upstreams, reusing the upstream's table", () => {
         expect(first.status).toBe("MANIFEST_FILE_READY");
         expect([second.status, (second as { error?: unknown }).error]).toEqual(
            ["MANIFEST_FILE_READY", null],
         );
         const downstream = Object.values(entriesOf(second)).filter((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         );
         expect(downstream).toHaveLength(1);
         expect(downstream[0].upstreamReuse).toBe("reused");
      });

      it("names the upstream's table, and its build, in builtFrom", () => {
         const upstream = Object.entries(entriesOf(first)).filter(
            ([, e]) => e.sourceName === mechanism.upstream,
         );
         expect(upstream).toHaveLength(1);
         const [address, upEntry] = upstream[0];
         expect(upEntry.builtAt).toEqual(expect.any(String));
         const downstream = Object.values(entriesOf(second)).find((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         )!;
         expect(downstream.builtAt).toEqual(expect.any(String));
         expect(downstream.builtFrom).toEqual([
            {
               sourceEntityId: address,
               physicalTableName: upEntry.physicalTableName,
               builtAt: upEntry.builtAt,
            },
         ]);
      });

      it("records a column contract on every table it built, each string compared exact on its own engine", () => {
         for (const e of [
            ...Object.values(entriesOf(first)),
            ...Object.values(entriesOf(second)),
         ]) {
            const contract = (e as { contract?: unknown }).contract as {
               v: number;
               columns: { collation: string | null; compare: string }[];
            };
            expect(contract.v).toBe(1);
            expect(contract.columns.length).toBeGreaterThan(0);
            for (const c of contract.columns) {
               expect(c.compare).toBe("exact");
            }
         }
      });

      it("binds every table a build wrote as its contract recorded it: none incompatible or missing its contract", async () => {
         // The bind every run's commit, a manifest bind and a restart take.
         await pkg.reloadAllModels(
            pkg.materialization().boundDatabaseEntries() ?? {},
         );
         const judged = pkg
            .materialization()
            .serveVerdicts()
            .filter(
               (v) =>
                  v.reason === "incompatible" ||
                  v.reason === "contract_missing",
            );
         expect(judged).toEqual([]);
      });

      it("serves its table, equal to live before the drop, as text", () => {
         // On Postgres the router may answer from the upstream's served
         // copy instead, at its read cost; the stored rows are compared
         // directly below.
         const kinds: (string | undefined)[] =
            tier.label === "DuckDB"
               ? [mechanism.servedBy]
               : [mechanism.servedBy, "materialized"];
         expect(kinds).toContain(served.kind);
         expect(served.rows).toEqual(liveBefore.rows);
         expect((liveBefore.rows as unknown[]).length).toBe(2);
      });

      it("holds live's rows in its own table, read directly, as text", async () => {
         if (!mechanism.storesTotal) return;
         const downstream = Object.values(entriesOf(second)).find((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         )!;
         const warehouse = await env.getMalloyConnection("warehouse");
         const result = await warehouse.runSQL(
            tier.rowsSQL(
               `SELECT cat, CAST(total AS VARCHAR) AS total_text FROM ${downstream.physicalTableName} ORDER BY cat`,
            ),
         );
         expect(
            (result.rows as Record<string, unknown>[]).map((r) => ({
               cat: String(r["cat"]),
               total_text: String(r["total_text"]),
            })),
         ).toEqual(liveBefore.rows as { cat: string; total_text: string }[]);
      });
   });
}

describe("a host's instructions listing a view before its upstream", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;
   let run: Materialization;
   let order: string[];

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "dr-")));
      const envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         {
            "m.malloy": MECHANISMS[1].files["m.malloy"].replace(
               "__RAW__",
               "main.dr_raw",
            ),
         },
         {
            manifest: { name: PKG, description: "fixture" },
         },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         ["env-1", ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "warehouse",
               type: "duckdb",
               duckdbConnection: { setupSQL: "SELECT 1" },
            } as never,
         ],
         [],
      );
      const warehouse = await env.getMalloyConnection("warehouse");
      await warehouse.runSQL(`CREATE TABLE main.dr_raw AS ${RAW_ROWS}`);
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      // The view first: a run that followed the list would build it before
      // `a`'s table exists, which strict upstreams refuse.
      // One table per address: `s` extends `a`, so it shares `a`'s.
      const sources = Object.values(pkg.getBuildPlan()!.sources);
      const tableAt = (address: string) =>
         `host_${sources.find((s) => s.sourceEntityId === address)!.name}`;
      const instructions = sources
         .map((s) => ({
            sourceEntityId: s.sourceEntityId,
            sourceID: s.sourceID,
            materializedTableId: `mt-${s.name}`,
            physicalTableName: tableAt(s.sourceEntityId),
            realization: "COPY" as const,
         }))
         .sort((x, y) =>
            x.physicalTableName.includes("view")
               ? -1
               : y.physicalTableName.includes("view")
                 ? 1
                 : 0,
         );
      order = instructions.map((i) => i.physicalTableName);
      const created = await service.createMaterialization(ENV, PKG, {
         buildInstructions: instructions,
      });
      const deadline = Date.now() + 120_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
            run = m;
            break;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
   }, 300_000);
   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("builds the view after its upstream, over the upstream's table, under strict upstreams", () => {
      expect(order[0]).toBe("host_s__by_cat__view");
      expect([run.status, (run as { error?: unknown }).error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      const entries = Object.entries(entriesOf(run));
      const [aAddress] = entries.find(
         ([, e]) => e.physicalTableName === "host_a",
      )!;
      const view = entries.find(
         ([, e]) => e.physicalTableName === "host_s__by_cat__view",
      )![1];
      expect(view.upstreamReuse).toBe("reused");
      expect(view.builtFrom?.map((b) => b.sourceEntityId)).toEqual([aAddress]);
   });
});
