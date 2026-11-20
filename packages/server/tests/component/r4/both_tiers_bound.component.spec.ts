// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A source bound in both tiers: a host placed it in a storage destination
 * while a database table for it is still bound. For every leg, under every
 * PERSIST_STORAGE_MODE and every freshness of the two tables, the serve
 * verdict says what each query actually read: its tier, and `serves` exactly
 * when a table answered.
 *
 * The expected answers are written out as literals, not derived from the
 * rules the router or the verdict use, so a router change the verdict does
 * not follow fails here, and so does a verdict change the router does not.
 *
 * Before the fix the served copy re-rooted a source bound fresh in both
 * tiers on both tables, spliced its declaration twice and refused both, so
 * its queries were answered live under `on` while the verdict said
 * `storage/serves`; under `write-only` and `off` they were served from the
 * database table while the verdict said `storage/live/storage_mode`. Under
 * those modes every leg now reads a database table bound beside a storage
 * one, and its verdict judges it on the database tier. A
 * storage table the served copy then dropped (one missing a column) left
 * the source live too, though its database table was complete: it now falls
 * back to the database table. A column the storage table holds but cannot
 * be read back as live answers it (a list stored as its text) declines only
 * the queries that read it, as on a storage table alone, and does not fall
 * back.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { __setPackageLoadPoolForTests } from "../../../src/package_load/package_load_pool";
import { unrecordedAudit } from "../../../src/service/access_record";
import { recordConnectionsAsForTests } from "../../../src/service/connection_type";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import {
   castToDeclaredTypes,
   wideIntegerColumns,
} from "../../../src/service/materialization/stored_column_types";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { Package } from "../../../src/service/package";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";

const MODEL = `##! experimental { persistence givens }
given: ROLES :: string[]

source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'east', 10), (2, 'west', 20), (3, 'east', 5))
    AS t(id, region, amount)
""") extend {
  primary_key: id
}

source: tagged is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'east', ['x', 'y']), (2, 'west', ['y']), (3, 'east', ['z']))
    AS t(id, region, tags)
""") extend {
  primary_key: id
}

#@ persist name="copy_h"
source: copy_h is orders -> { group_by: region; aggregate: t is amount.sum() }

#(access_filter) region in $ROLES
#@ persist name="copy_g"
source: copy_g is orders -> { group_by: region; aggregate: tg is amount.sum() }

#@ persist name="copy_l"
source: copy_l is tagged -> { select: id, region, tags }

source: rollup_h is orders extend {
  #@ preaggregate grain="region"
  measure: total is amount.sum()
}

source: view_h is orders extend {
  #@ persist
  view: by_region is { group_by: region; aggregate: vt is amount.sum(); order_by: region }
}

#@ persist
query: query_h is orders -> { group_by: region; aggregate: qt is amount.sum(), n is count(); order_by: region }
`;

interface Verdict {
   sourceName: string;
   origin: string;
   tier: string;
   verdict: string;
   reason?: string;
}

interface Case {
   label: string;
   /** The path report's `servedBy.kind` when a table answers. */
   leg: "materialized" | "rollup" | "view" | "query";
   query: string;
   givens?: Record<string, unknown>;
   verdictOf: (v: Verdict) => boolean;
}

const COPY: Case = {
   label: "the served copy",
   leg: "materialized",
   query: "run: copy_h -> { group_by: region; aggregate: x is t.sum() }",
   verdictOf: (v) => v.sourceName === "copy_h" && v.origin === "persist",
};
const LEG_CASES: Case[] = [
   COPY,
   {
      label: "the served copy, through a row-level gate",
      leg: "materialized",
      query: "run: copy_g -> { group_by: region; aggregate: x is tg.sum() }",
      givens: { ROLES: ["east"] },
      verdictOf: (v) => v.sourceName === "copy_g" && v.origin === "persist",
   },
   {
      label: "the rollup leg",
      leg: "rollup",
      query: "run: rollup_h -> { group_by: region; aggregate: total }",
      verdictOf: (v) =>
         v.origin === "rollup" && v.sourceName.startsWith("rollup_h"),
   },
   {
      label: "the persisted view leg",
      leg: "view",
      query: "run: view_h -> by_region",
      verdictOf: (v) =>
         v.origin === "view" && v.sourceName.startsWith("view_h"),
   },
   {
      label: "the persisted query leg",
      leg: "query",
      query: "run: query_h",
      verdictOf: (v) =>
         v.origin === "query" && v.sourceName.startsWith("query_h"),
   },
];

interface Answer {
   rows: unknown[];
   servedBy: string;
   tier?: string;
}

/** Past its window, read live: what a build stamped long ago. */
const STALE = {
   dataAsOf: "2020-01-01T00:00:00Z",
   freshnessWindowSeconds: 60,
   freshnessFallback: "live",
};

interface Setup {
   mode: string;
   gatedSources?: boolean;
   storageStale?: boolean;
   databaseStale?: boolean;
   /**
    * Change one source's storage table after it is built, returning the
    * schema its binding carries (as the build that left it so recorded it).
    */
   alterStorage?: {
      sourceName: string;
      alter: (
         lake: DuckDBConnection,
         table: string,
      ) => Promise<{ name: string; type: string }[]>;
   };
}

interface Loaded {
   run: (c: Pick<Case, "query" | "givens">) => Promise<Answer>;
   live: Map<string, Answer>;
   verdicts: Verdict[];
   /** The package's served-copy warnings, after the bind. */
   servedCopyWarnings: {
      subject?: string;
      code?: string;
      reason?: string;
      message?: string;
   }[];
   dispose: () => Promise<void>;
}

/**
 * Load the package, run `liveQueries` live, bind every planned table in both
 * tiers with the freshness `setup` asks for, and hand back a runner.
 */
async function loadAndBind(
   setup: Setup,
   liveQueries: readonly Case[],
): Promise<Loaded> {
   const { restore } = installSettings({
      env: {
         PERSIST_STORAGE_MODE: setup.mode,
         PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         PUBLISHER_MATERIALIZE: "true",
         PUBLISHER_MATERIALIZE_GATED_SOURCES: String(
            setup.gatedSources ?? true,
         ),
      },
   });
   await __setPackageLoadPoolForTests(null);
   // A storage build reads a warehouse; the sandbox stands in for one.
   const restoreTypes = recordConnectionsAsForTests({ duckdb: "postgres" });
   const root = await fs.mkdtemp(path.join(os.tmpdir(), "both-tiers-"));
   const dispose = async () => {
      restoreTypes();
      restore();
      await __setPackageLoadPoolForTests(null);
      await fs.rm(root, { recursive: true, force: true }).catch(() => {});
   };
   try {
      const envPath = path.join(root, "env");
      await fs.mkdir(envPath, { recursive: true });
      writePackageFiles(
         path.join(envPath, "pkg"),
         { "m.malloy": MODEL },
         { manifest: { name: "pkg", description: "both tiers bound" } },
      );
      const env = await Environment.create("e", envPath, [] as never);
      await env.addPackage("pkg");
      const pkg: Package = await env.getPackage(parsePackageName("pkg"), false);
      const lake = new DuckDBConnection("lake", path.join(root, "lake.duckdb"));
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["lake", lake]]), "lake"),
      );
      pkg.materialization().setServeDestinationConfig(() => config);
      const warehouse = (await pkg.getMalloyConnection(
         "duckdb",
      )) as DuckDBConnection;

      const run = async (
         c: Pick<Case, "query" | "givens">,
      ): Promise<Answer> => {
         const r = await pkg.getModel("m.malloy")!.runQuery(
            unrecordedAudit(),
            {
               query: c.query,
               ...(c.givens ? { givens: c.givens } : {}),
            } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: false }),
         );
         const by = r.path?.servedBy as
            | { kind?: string; tier?: string }
            | undefined;
         return {
            rows: r.compactResult as unknown[],
            servedBy: by?.kind ?? "live",
            ...(by?.tier ? { tier: by.tier } : {}),
         };
      };
      const live = new Map<string, Answer>();
      for (const c of liveQueries) live.set(c.query, await run(c));

      const plan = pkg.getBuildPlan()!;
      const database: Record<string, unknown> = {};
      const stored: Record<string, unknown> = {};
      for (const entry of Object.values(plan.sources)) {
         if (stored[entry.sourceEntityId]) continue;
         const table = `t_${entry.sourceEntityId.slice(0, 12)}`;
         await warehouse.runSQL(
            `CREATE OR REPLACE TABLE ${table} AS ${entry.sql}`,
         );
         database[entry.sourceEntityId] = {
            tableName: table,
            connectionName: "duckdb",
            ...(setup.databaseStale ? STALE : {}),
         };
         const computed = await lake.runSQL(`DESCRIBE (${entry.sql})`);
         const select = castToDeclaredTypes(entry.sql, [], {
            exactIntegers: wideIntegerColumns(
               (
                  computed.rows as {
                     column_name: string;
                     column_type: string;
                  }[]
               ).map((r) => ({ name: r.column_name, type: r.column_type })),
            ),
         });
         await lake.runSQL(`CREATE OR REPLACE TABLE ${table} AS ${select}`);
         const described = await lake.runSQL(`DESCRIBE ${table}`);
         let schema = (
            described.rows as { column_name: string; column_type: string }[]
         ).map((r) => ({ name: r.column_name, type: r.column_type }));
         if (setup.alterStorage?.sourceName === entry.name) {
            schema = await setup.alterStorage.alter(lake, table);
         }
         stored[entry.sourceEntityId] = {
            sourceEntityId: entry.sourceEntityId,
            sourceName: entry.name,
            physicalTableName: table,
            storageDestinationName: "lake",
            ...(entry.origin === "rollup" ? { origin: "rollup" } : {}),
            ...(setup.storageStale ? STALE : {}),
            schema,
         };
      }
      pkg.materialization().bind({ database: database as never });
      pkg.materialization().bind({ storage: stored as never });
      for (const modelPath of pkg.getModelPaths()) {
         await pkg.getModel(modelPath)?.prepareServedCopy();
      }
      const verdicts = pkg.materialization().serveVerdicts() as Verdict[];
      const servedCopyWarnings = pkg.materialization().servedCopyWarnings();
      return { run, live, verdicts, servedCopyWarnings, dispose };
   } catch (error) {
      await dispose();
      throw error;
   }
}

/** What one query read: a table's tier, or live. */
type Read = "storage" | "database" | "live";

/** One case's expected read and verdict, as literals. */
interface Expected {
   read: Read;
   verdict: { tier: string; verdict: string; reason?: string };
}

const SERVES_STORAGE: Expected = {
   read: "storage",
   verdict: { tier: "storage", verdict: "serves" },
};
const SERVES_DATABASE: Expected = {
   read: "database",
   verdict: { tier: "database", verdict: "serves" },
};
const LIVE_STORAGE_STALE: Expected = {
   read: "live",
   verdict: { tier: "storage", verdict: "live", reason: "stale" },
};
const LIVE_DATABASE_STALE: Expected = {
   read: "live",
   verdict: { tier: "database", verdict: "live", reason: "stale" },
};

type Freshness = "both fresh" | "storage stale" | "database stale";

/**
 * The served copy's expectation, then the other legs' (rollup, persisted
 * view, persisted query), for each mode and freshness.
 */
const MATRIX: Record<
   string,
   Record<Freshness, { copy: Expected; legs: Expected }>
> = {
   on: {
      "both fresh": { copy: SERVES_STORAGE, legs: SERVES_STORAGE },
      "storage stale": { copy: SERVES_DATABASE, legs: LIVE_STORAGE_STALE },
      "database stale": { copy: SERVES_STORAGE, legs: SERVES_STORAGE },
   },
   "write-only": {
      "both fresh": { copy: SERVES_DATABASE, legs: SERVES_DATABASE },
      "storage stale": { copy: SERVES_DATABASE, legs: SERVES_DATABASE },
      "database stale": {
         copy: LIVE_DATABASE_STALE,
         legs: LIVE_DATABASE_STALE,
      },
   },
   off: {
      "both fresh": { copy: SERVES_DATABASE, legs: SERVES_DATABASE },
      "storage stale": { copy: SERVES_DATABASE, legs: SERVES_DATABASE },
      "database stale": {
         copy: LIVE_DATABASE_STALE,
         legs: LIVE_DATABASE_STALE,
      },
   },
};

/** What a query read, from its path report. */
function readOf(a: Answer): Read {
   if (a.servedBy === "live") return "live";
   return a.tier === "database" ? "database" : "storage";
}

/** One verdict, among the package's, as the literals compare it. */
function verdictOf(verdicts: Verdict[], c: Case) {
   const found = verdicts.filter(c.verdictOf);
   expect(found).toHaveLength(1);
   const [v] = found;
   return {
      tier: v.tier,
      verdict: v.verdict,
      ...(v.reason ? { reason: v.reason } : {}),
   };
}

/** The verdict says what the query read, whatever either rule is. */
function expectAgreement(v: { tier: string; verdict: string }, a: Answer) {
   expect(v.verdict).toBe(a.servedBy === "live" ? "live" : "serves");
   if (a.servedBy !== "live") expect(v.tier).toBe(readOf(a));
}

for (const mode of ["on", "write-only", "off"] as const) {
   for (const freshness of [
      "both fresh",
      "storage stale",
      "database stale",
   ] as const) {
      for (const gatedSources of [false, true]) {
         describe(`PERSIST_STORAGE_MODE=${mode}, ${freshness}, gated sources ${gatedSources ? "materialized" : "not materialized"}`, () => {
            let ctx: Loaded | undefined;
            beforeAll(async () => {
               ctx = await loadAndBind(
                  {
                     mode,
                     gatedSources,
                     storageStale: freshness === "storage stale",
                     databaseStale: freshness === "database stale",
                  },
                  LEG_CASES,
               );
            });
            afterAll(async () => {
               await ctx?.dispose();
            });

            for (const c of LEG_CASES) {
               const row = MATRIX[mode][freshness];
               const expected = c.leg === "materialized" ? row.copy : row.legs;
               it(`${c.label}: read ${expected.read}, verdict ${expected.verdict.tier}/${expected.verdict.verdict}, equal to live`, async () => {
                  const { run, live, verdicts } = ctx!;
                  const before = live.get(c.query)!;
                  const after = await run(c);
                  expect(before.servedBy).toBe("live");
                  expect(after.rows).toEqual(before.rows);
                  const v = verdictOf(verdicts, c);
                  expectAgreement(v, after);
                  expect(readOf(after)).toBe(expected.read);
                  if (expected.read !== "live") {
                     expect(after.servedBy).toBe(c.leg);
                  }
                  expect(v).toEqual(expected.verdict);
               });
            }
         });
      }
   }
}

// The storage table holds the source's rows but not its column `t`, while
// its database table holds every column: the served copy drops the storage
// table (it does not carry a field the source exposes) and falls back to the
// database one, so every query, one reading `t` or not, is served from the
// database, and the verdict says so.
describe("PERSIST_STORAGE_MODE=on, both fresh, the storage table missing a column", () => {
   const QUERIES: Case[] = [
      COPY,
      { ...COPY, query: "run: copy_h -> { group_by: region }" },
   ];
   let ctx: Loaded | undefined;
   beforeAll(async () => {
      ctx = await loadAndBind(
         {
            mode: "on",
            alterStorage: {
               sourceName: "copy_h",
               alter: async (lake, table) => {
                  await lake.runSQL(`ALTER TABLE ${table} DROP COLUMN t`);
                  return [{ name: "region", type: "VARCHAR" }];
               },
            },
         },
         QUERIES,
      );
   });
   afterAll(async () => {
      await ctx?.dispose();
   });

   it("warns why the storage table is not read, and that the database table serves instead", () => {
      const warned = ctx!.servedCopyWarnings.filter(
         (w) => w.subject === "copy_h",
      );
      expect(warned).toEqual([
         expect.objectContaining({
            code: "served-copy-not-served",
            reason: "fields_not_carried",
            message:
               "This source inherits fields (a measure, view or join declared on the source it is built from) that its stored table does not carry, so the served copy reads its database table instead.",
         }),
      ]);
   });

   for (const c of QUERIES) {
      it(`served from the database, as the verdict says: ${c.query}`, async () => {
         const { run, live, verdicts } = ctx!;
         const after = await run(c);
         expect(after.rows).toEqual(live.get(c.query)!.rows);
         expect(readOf(after)).toBe("database");
         expect(after.servedBy).toBe("materialized");
         const v = verdictOf(verdicts, c);
         expectAgreement(v, after);
         expect(v).toEqual({ tier: "database", verdict: "serves" });
      });
   }
});

// The storage table holds a list column as its text (a build that stored
// it so): the served copy leaves that column out of the table's type and
// keeps the re-root, so a query reading it declines and answers live, and a
// query that does not is served from storage. It does not fall back to the
// database table for either, as on a storage table alone.
describe("PERSIST_STORAGE_MODE=on, both fresh, a list column stored as its text", () => {
   const LIST: Case = {
      label: "the served copy, a list stored as text",
      leg: "materialized",
      query: "run: copy_l -> { select: id, tags; order_by: id }",
      verdictOf: (v) => v.sourceName === "copy_l" && v.origin === "persist",
   };
   const NO_LIST: Case = {
      ...LIST,
      query: "run: copy_l -> { group_by: region; aggregate: c is count(); order_by: region }",
   };
   let ctx: Loaded | undefined;
   beforeAll(async () => {
      ctx = await loadAndBind(
         {
            mode: "on",
            alterStorage: {
               sourceName: "copy_l",
               alter: async (lake, table) => {
                  await lake.runSQL(
                     `ALTER TABLE ${table} ALTER tags TYPE VARCHAR`,
                  );
                  const described = await lake.runSQL(`DESCRIBE ${table}`);
                  return (
                     described.rows as {
                        column_name: string;
                        column_type: string;
                     }[]
                  ).map((r) => ({ name: r.column_name, type: r.column_type }));
               },
            },
         },
         [LIST, NO_LIST],
      );
   });
   afterAll(async () => {
      await ctx?.dispose();
   });

   it("raises no warning that the database table serves instead", () => {
      expect(
         ctx!.servedCopyWarnings.filter((w) =>
            (w.message ?? "").includes("database table instead"),
         ),
      ).toEqual([]);
   });

   it("the source's verdict is storage/serves", () => {
      expect(verdictOf(ctx!.verdicts, LIST)).toEqual({
         tier: "storage",
         verdict: "serves",
      });
   });

   it("a query reading the list answers live, equal to live", async () => {
      const after = await ctx!.run(LIST);
      expect(after.rows).toEqual(ctx!.live.get(LIST.query)!.rows);
      expect(readOf(after)).toBe("live");
   });

   it("a query not reading it is served from storage, equal to live", async () => {
      const after = await ctx!.run(NO_LIST);
      expect(after.rows).toEqual(ctx!.live.get(NO_LIST.query)!.rows);
      expect(readOf(after)).toBe("storage");
      expect(after.servedBy).toBe("materialized");
   });
});
