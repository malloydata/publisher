// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of a v1 chained `storage=` build (known limitation 38):
 * a `#@ persist storage=` source over another one, built by stacking on the
 * parent's stored table through `StorageBuildStrategy.buildDownstreamViaParents`.
 * The transient model it compiles is pinned byte for byte, with what it builds
 * and how it refuses, so moving chained builds off the serve-shape lift (step
 * R) can show the same tables come out. The run's specs substitute this method
 * at a seam; here it runs for real, over a plain-DuckDB file destination.
 */

import type { StringReads } from "../domain/string_reads";
import { sourcesIn } from "../../../test_helpers/materialization_test_fixtures";
import { compiledTextReader } from "../../../utils";
import { DuckDBInstance } from "@duckdb/node-api";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   Runtime,
   type PersistSource,
   type URLReader,
} from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   ChainedShapeNotCarriedError,
   ChainedUpstreamMissingError,
   MaterializationEligibilityError,
} from "../../../errors";
import { buildEnvironmentStub } from "../../../test_helpers/materialization_service_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { storageDestinationRoot } from "../../connection_config";
import { collectModelBuildPlan, emptyBuildPlanParts } from "../plan/build_plan";
import { authorModelLiftContext } from "../text/author_model";
import type { ChainBuildRequest } from "../domain/chain_build";
import type { ManifestEntry } from "../domain/types";
import { chainBuildText } from "../serve/served_copy_serve";
import { servedCopyAlias } from "../serve/served_copy_config";
import * as session from "./materialization_build_session";
import { type ChainedPlanFacts, chainedPlanFacts } from "./build_upstreams";
import { StorageBuildStrategy } from "./storage_build_strategy";

const duckdb = new DuckDBConnection("duckdb", ":memory:");
const connections = new FixedConnectionMap(
   new Map([["duckdb", duckdb]]),
   "duckdb",
);
afterAll(() => duckdb.close());

const LAKE = { name: "lake", type: "duckdb" };

const RAW = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT * FROM (VALUES
    (DATE '2026-01-01', 'a', 10.0),
    (DATE '2026-01-02', 'b', 5.0),
    (DATE '2026-02-01', 'a', 7.0)
  ) AS t(d, cat, amt)
""")
`;

/** The parent's stored rows: what `daily`'s build would have written. */
const DAILY_ROWS =
   'CREATE TABLE "daily_t" AS SELECT * FROM (VALUES ' +
   "(DATE '2026-01-01', 'a', 10.0::DOUBLE), " +
   "(DATE '2026-01-02', 'b', 5.0::DOUBLE), " +
   "(DATE '2026-02-01', 'a', 7.0::DOUBLE)) AS t(d, cat, total)";

const DAILY_SCHEMA = [
   { name: "d", type: "DATE" },
   { name: "cat", type: "VARCHAR" },
   { name: "total", type: "DOUBLE" },
];

interface Chain {
   dir: string;
   sources: Record<string, PersistSource>;
   facts: ChainedPlanFacts;
   byName(name: string): { id: string; source: PersistSource };
}

let restore: () => void = () => {};
let dirs: string[] = [];
beforeEach(() => {
   restore = installSettings({ env: { PERSIST_STORAGE_MODE: "on" } }).restore;
});
afterEach(() => {
   restore();
   for (const dir of dirs) fs.rmSync(dir, { recursive: true, force: true });
   dirs = [];
});

function reader(): URLReader {
   return { readURL: async (url: URL) => fs.readFileSync(url, "utf8") };
}

/**
 * Plan one model file as a package load would, and the chained facts a run
 * derives. `others` are more files beside `m.malloy`, which it may import.
 */
async function planChain(
   text: string,
   others: Record<string, string> = {},
): Promise<Chain> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chained-build-"));
   dirs.push(dir);
   fs.writeFileSync(path.join(dir, "m.malloy"), text);
   for (const [name, other] of Object.entries(others)) {
      fs.writeFileSync(path.join(dir, name), other);
   }
   const base = pathToFileURL(dir + "/");
   const runtime = new Runtime({ urlReader: reader(), connections });
   const materializer = runtime.loadModel(new URL("m.malloy", base), {
      importBaseURL: base,
   });
   const parts = emptyBuildPlanParts();
   await collectModelBuildPlan(parts, {
      modelPath: "m.malloy",
      packagePath: dir,
      materializer,
      malloyModel: await materializer.getModel(),
      runtime,
      getRuntime: async () => ({ runtime, importBaseURL: base }),
   });
   const facts = chainedPlanFacts(
      parts.tables,
      { duckdb: "digest" },
      undefined,
      undefined,
   );
   return {
      dir,
      sources: sourcesIn(parts),
      facts,
      byName(name) {
         const hit = Object.entries(sourcesIn(parts)).find(
            ([, s]) => s.name === name,
         );
         if (!hit) throw new Error(`no planned source ${name}`);
         return { id: hit[0], source: hit[1] };
      },
   };
}

/** Run SQL against the plain-DuckDB `lake` file the build attaches. */
async function writeLake(dir: string, sql: string): Promise<void> {
   const root = storageDestinationRoot(dir);
   fs.mkdirSync(root, { recursive: true });
   const instance = await DuckDBInstance.create(path.join(root, "lake.duckdb"));
   const connection = await instance.connect();
   try {
      await connection.run(sql);
   } finally {
      connection.closeSync();
      instance.closeSync();
   }
}

async function readLake(dir: string, sql: string): Promise<unknown[]> {
   const instance = await DuckDBInstance.create(
      path.join(storageDestinationRoot(dir), "lake.duckdb"),
   );
   const connection = await instance.connect();
   try {
      return (await connection.runAndReadAll(sql)).getRowObjectsJson();
   } finally {
      connection.closeSync();
      instance.closeSync();
   }
}

/** The parent's manifest entry, as the run recorded building it into `lake`. */
function parentEntry(
   chain: Chain,
   name: string,
   over: Partial<ManifestEntry> = {},
): Record<string, ManifestEntry> {
   const address = chain.facts.addressBySourceId[chain.byName(name).id]!;
   return {
      [address]: {
         sourceEntityId: address,
         sourceName: name,
         physicalTableName: "daily_t",
         connectionName: "duckdb",
         storageDestinationName: "lake",
         realization: "COPY",
         rowCount: 3,
         schema: DAILY_SCHEMA,
         ...over,
      } as ManifestEntry,
   };
}

/** Stack `name` on its parents, recording the transient model it compiles. */
async function stack(
   chain: Chain,
   name: string,
   builtEntries: Record<string, ManifestEntry>,
   /**
    * The strings its plan recorded it compares: none by default, as for a
    * DuckDB source with no collation; null records none.
    */
   stringReads: StringReads | null = { reads: [] },
): Promise<{
   result?: Awaited<
      ReturnType<StorageBuildStrategy["buildDownstreamViaParents"]>
   >;
   error?: unknown;
   models: string[];
}> {
   const { id, source } = chain.byName(name);
   const strategy = new StorageBuildStrategy({
      buildDownstreamViaParents: () => {
         throw new Error("not used: the method is called directly");
      },
   });
   const spy = spyOn(session, "buildDownstreamIntoStorage");
   try {
      const result = await strategy.buildDownstreamViaParents(
         source,
         "lake",
         LAKE as never,
         builtEntries,
         {
            ...buildEnvironmentStub({
               destinations: { lake: LAKE },
               environmentPath: chain.dir,
            }),
            // The package's served copy writes the text (the host port).
            chainBuildText: (request: ChainBuildRequest) =>
               chainBuildText(request, {
                  modelPath: "m.malloy",
                  modelDef: source._model?._modelDef,
                  context: [chain.dir, {} as never, {}],
               }) ?? { declined: "no model declares the downstream" },
         } as never,
         `${name}_t`,
         chain.facts,
         chain.facts.addressBySourceId[id]!,
         authorModelLiftContext(
            source._model?._modelDef,
            compiledTextReader(source._model?._modelDef),
         ),
         stringReads ? { stringReads } : {},
      );
      return { result, models: transientModels(spy) };
   } catch (error) {
      return { error, models: transientModels(spy) };
   } finally {
      spy.mockRestore();
   }
}

/** A file with its blanked lines' spaces gone, as it reads. */
const trimmed = (text: string) => text.replace(/[ \t]+$/gm, "");

/** The downstream's file each compile read, as the build wrote it. */
function transientModels(spy: { mock: { calls: unknown[][] } }): string[] {
   return spy.mock.calls.map((call) => {
      const { model } = call[0] as {
         model: { files: ReadonlyMap<string, string>; entry: string };
      };
      return model.files.get(model.entry)!;
   });
}

const CHAIN = `${RAW}
#@ persist storage="lake"
source: daily is raw -> {
  group_by: d, cat
  aggregate: total is amt.sum()
}

#@ persist storage="lake"
source: monthly is daily -> {
  group_by: m is d.month
  aggregate: total is total.sum()
}
`;

describe.skipIf(process.platform === "win32")(
   "a v1 chained storage= build, stacked on its parent's stored table",
   () => {
      it("compiles the author's file with the parent re-rooted, and builds the downstream from the parent's rows", async () => {
         const chain = await planChain(CHAIN);
         await writeLake(chain.dir, DAILY_ROWS);
         const { result, error, models } = await stack(
            chain,
            "monthly",
            parentEntry(chain, "daily"),
         );
         expect(error).toBeUndefined();
         const address =
            chain.facts.addressBySourceId[chain.byName("daily").id];
         expect(models.map(trimmed)).toEqual([
            `##! experimental.persistence\n${"\n".repeat(7)}
##! experimental { virtual_source }
type: daily__served is {
   d::date,
   cat::string,
   total::number
}

#@ persist storage="lake"
source: daily is ${servedCopyAlias("lake")}.virtual('${address}')::daily__served

#@ persist storage="lake"
source: monthly is daily -> {
  group_by: m is d.month
  aggregate: total is total.sum()
}
`,
         ]);
         expect(result).toMatchObject({
            storageDestinationName: "lake",
            schema: [
               { name: "m", type: "DATE" },
               { name: "total", type: "DOUBLE" },
            ],
            rowCount: 2,
            readCost: null,
            upstreamReuse: "reused",
         });
         expect(
            await readLake(
               chain.dir,
               "SELECT CAST(m AS VARCHAR) AS m, total FROM monthly_t ORDER BY m",
            ),
         ).toEqual([
            { m: "2026-01-01", total: 15 },
            { m: "2026-02-01", total: 7 },
         ]);
      });

      it("leaves out a source off its path with the block doc written on it", async () => {
         const chain = await planChain(`${CHAIN}
#|(doc)
Off the downstream's path.
|#
source: other is raw extend {}
`);
         await writeLake(chain.dir, DAILY_ROWS);
         const { result, error } = await stack(
            chain,
            "monthly",
            parentEntry(chain, "daily"),
         );
         expect(error).toBeUndefined();
         expect(result).toMatchObject({ rowCount: 2, upstreamReuse: "reused" });
         expect(
            await readLake(
               chain.dir,
               "SELECT CAST(m AS VARCHAR) AS m, total FROM monthly_t ORDER BY m",
            ),
         ).toEqual([
            { m: "2026-01-01", total: 15 },
            { m: "2026-02-01", total: 7 },
         ]);
      });

      it("keeps a parent's own refinements over its table, and a rename reads the re-rooted parent", async () => {
         const chain = await planChain(`${RAW}
#@ persist storage="lake"
source: daily is raw -> {
  group_by: d, cat
  aggregate: total is amt.sum()
} extend {
  where: cat = 'a'
  dimension: loud_cat is upper(cat)
}

source: day_rows is daily

#@ persist storage="lake"
source: by_cat is day_rows -> {
  group_by: loud_cat
  aggregate: total is total.sum()
}
`);
         await writeLake(chain.dir, DAILY_ROWS);
         const { result, error, models } = await stack(
            chain,
            "by_cat",
            parentEntry(chain, "daily"),
         );
         expect(error).toBeUndefined();
         const address =
            chain.facts.addressBySourceId[chain.byName("daily").id];
         expect(models.map(trimmed)).toEqual([
            `##! experimental.persistence\n${"\n".repeat(7)}
##! experimental { virtual_source }
type: daily__served is {
   d::date,
   cat::string,
   total::number
}

#@ persist storage="lake"
source: daily is ${servedCopyAlias("lake")}.virtual('${address}')::daily__served extend {
  where: cat = 'a'
  dimension: loud_cat is upper(cat)
}

source: day_rows is daily

#@ persist storage="lake"
source: by_cat is day_rows -> {
  group_by: loud_cat
  aggregate: total is total.sum()
}
`,
         ]);
         expect(result?.rowCount).toBe(1);
         expect(
            await readLake(chain.dir, "SELECT loud_cat, total FROM by_cat_t"),
         ).toEqual([{ loud_cat: "A", total: 17 }]);
      });

      it("re-roots a parent in the file that declares it, under an intermediate the downstream reads it through", async () => {
         const chain = await planChain(
            `##! experimental.persistence
import 'base.malloy'

source: january is daily extend { where: d < @2026-02-01 }

#@ persist storage="lake"
source: jan_by_cat is january -> {
  group_by: cat
  aggregate: total is total.sum()
}
`,
            {
               "base.malloy": `${RAW}
#@ persist storage="lake"
source: daily is raw -> {
  group_by: d, cat
  aggregate: total is amt.sum()
}
`,
            },
         );
         // The stored rows differ from the warehouse's, so an answer read
         // from the warehouse would show.
         await writeLake(
            chain.dir,
            `${DAILY_ROWS}; UPDATE daily_t SET total = total * 100`,
         );
         const { result, error, models } = await stack(
            chain,
            "jan_by_cat",
            parentEntry(chain, "daily"),
         );
         expect(error).toBeUndefined();
         // The entry file, with the parent's own file re-rooted beside it.
         expect(models.map(trimmed)).toEqual([
            `##! experimental.persistence
import 'base.malloy'

source: january is daily extend { where: d < @2026-02-01 }

#@ persist storage="lake"
source: jan_by_cat is january -> {
  group_by: cat
  aggregate: total is total.sum()
}
`,
         ]);
         expect(result?.upstreamReuse).toBe("reused");
         expect(
            await readLake(
               chain.dir,
               "SELECT cat, total FROM jan_by_cat_t ORDER BY cat",
            ),
         ).toEqual([
            { cat: "a", total: 1000 },
            { cat: "b", total: 500 },
         ]);
      });

      it("leaves out a parent's definition that reads the warehouse, and stacks only a downstream that does not read it", async () => {
         const chain = await planChain(`${RAW}
#@ persist storage="lake"
source: daily is raw -> {
  group_by: d, cat
  aggregate: total is amt.sum()
} extend {
  join_one: r is raw on r.d = d
  measure: raw_rows is r.count()
  dimension: loud_cat is upper(cat)
}

#@ persist storage="lake"
source: by_loud is daily -> {
  group_by: loud_cat
  aggregate: total is total.sum()
}

#@ persist storage="lake"
source: by_raw is daily -> {
  group_by: cat
  aggregate: raw_rows
}
`);
         await writeLake(
            chain.dir,
            `${DAILY_ROWS}; UPDATE daily_t SET total = total * 100`,
         );
         const loud = await stack(
            chain,
            "by_loud",
            parentEntry(chain, "daily"),
         );
         expect(loud.error).toBeUndefined();
         expect(loud.result?.upstreamReuse).toBe("reused");
         expect(
            await readLake(
               chain.dir,
               "SELECT loud_cat, total FROM by_loud_t ORDER BY loud_cat",
            ),
         ).toEqual([
            { loud_cat: "A", total: 1700 },
            { loud_cat: "B", total: 500 },
         ]);
         // The measure reading the left-out join is gone too, so a downstream
         // that reads it does not compile over the stored parent, and is a
         // shape the build could not carry (the caller recomputes it), never a
         // table built without it.
         const raw = await stack(chain, "by_raw", parentEntry(chain, "daily"));
         expect(raw.error).toBeInstanceOf(ChainedShapeNotCarriedError);
         expect(
            await readLake(
               chain.dir,
               "SELECT count(*)::INTEGER AS n FROM information_schema.tables WHERE table_name = 'by_raw_t'",
            ),
         ).toEqual([{ n: 0 }]);
      });

      it("declines a downstream that would store fewer columns than a recompute writes", async () => {
         const chain = await planChain(`${RAW}
#@ persist storage="lake"
source: daily is raw -> {
  group_by: d, cat
  aggregate: total is amt.sum()
} extend {
  join_one: r is raw on r.d = d
  dimension: raw_cat is r.cat
  dimension: loud_cat is upper(cat)
}

#@ persist storage="lake"
source: every is daily -> { select: * }
`);
         await writeLake(chain.dir, DAILY_ROWS);
         // `raw_cat` reads the join left out over the stored parent, so the
         // stacked `select: *` would lack it; a recompute writes it.
         const { error } = await stack(
            chain,
            "every",
            parentEntry(chain, "daily"),
         );
         expect(error).toBeInstanceOf(ChainedShapeNotCarriedError);
         expect((error as Error).message).toContain("'raw_cat'");
         expect(
            await readLake(
               chain.dir,
               "SELECT count(*)::INTEGER AS n FROM information_schema.tables WHERE table_name = 'every_t'",
            ),
         ).toEqual([{ n: 0 }]);
      });

      it("names a parent with no table in this destination, before compiling anything", async () => {
         const chain = await planChain(CHAIN);
         const { error, models } = await stack(chain, "monthly", {});
         expect(error).toBeInstanceOf(ChainedUpstreamMissingError);
         const missing = error as ChainedUpstreamMissingError;
         expect(missing.missing).toEqual(["daily"]);
         expect(missing.message).toBe(
            "persisted upstream of 'monthly': 'daily' is not materialized in " +
               "destination 'lake' for this build — the build cannot stack on " +
               "it, and the recompute that remains would rebuild the stored " +
               "tables its SQL inlines",
         );
         expect(models).toEqual([]);
      });

      it("reads a parent entry in another destination as one at the wrong tier", async () => {
         const chain = await planChain(CHAIN);
         const { error, models } = await stack(
            chain,
            "monthly",
            parentEntry(chain, "daily", { storageDestinationName: "other" }),
         );
         expect(error).toBeInstanceOf(ChainedUpstreamMissingError);
         expect((error as Error).message).toBe(
            "persisted upstream of 'monthly': 'daily' is declared in storage " +
               "destination 'lake', but its manifest entry is a table in storage " +
               "destination 'other': supplied at the wrong tier, so this build " +
               "does not use it — the build cannot stack on it, and the " +
               "recompute that remains would rebuild the stored tables its SQL " +
               "inlines",
         );
         expect(models).toEqual([]);
      });

      it("refuses a downstream that also reads the source warehouse as a shape it cannot stack", async () => {
         const chain = await planChain(`${CHAIN}
#@ persist storage="lake"
source: mixed is daily extend {
  join_one: r is raw on r.d = d
} -> {
  group_by: d
  aggregate: n is r.count()
}
`);
         const { error, models } = await stack(
            chain,
            "mixed",
            parentEntry(chain, "daily"),
         );
         expect(error).toBeInstanceOf(MaterializationEligibilityError);
         expect((error as Error).message).toBe(
            "'mixed' reads the source warehouse (raw) through 'mixed', which " +
               "no stored table can stand in for",
         );
         expect(models).toEqual([]);
      });

      it("does not stack a downstream whose strings its parent's contract records compared otherwise, nor one whose strings were not recorded", async () => {
         const chain = await planChain(CHAIN);
         await writeLake(chain.dir, DAILY_ROWS);
         const entries = parentEntry(chain, "daily");
         for (const entry of Object.values(entries)) {
            entry.contract = {
               v: 1,
               dialect: "duckdb",
               columns: [
                  {
                     name: "cat",
                     nativeType: "VARCHAR",
                     collation: "binary",
                     compare: "none",
                     gateExact: true,
                  },
               ],
            };
         }
         const refused = await stack(chain, "monthly", entries, {
            reads: [
               {
                  upstream: chain.byName("daily").id,
                  names: ["cat"],
                  need: "equality",
               },
            ],
         });
         // A shape no build over the parents gives live's rows for: refused
         // as a shape it could not carry (recomputeAfterStackFailure), never
         // recomputed from raw.
         expect(refused.error).toBeInstanceOf(MaterializationEligibilityError);
         expect((refused.error as Error).message).toBe(
            "'monthly' compares a string over its stored upstreams ('daily') " +
               "that the destination, byte-exact, would compare otherwise " +
               "than its source does: it groups or compares 'cat', which its " +
               "upstream's table records as compared unlike live",
         );
         expect(refused.models).toEqual([]);
         // Nothing recorded at plan: it never stacks.
         const unrecorded = await stack(chain, "monthly", entries, null);
         expect(unrecorded.error).toBeInstanceOf(
            MaterializationEligibilityError,
         );
         expect((unrecorded.error as Error).message).toEndWith(
            "its string reads were not recorded",
         );
         expect(unrecorded.models).toEqual([]);
      });

      it("reports a downstream the destination cannot compile as a shape it could not carry", async () => {
         const chain = await planChain(`${RAW}
#@ persist storage="lake"
source: daily is raw -> {
  group_by: d, cat
  aggregate: total is amt.sum()
}

#@ persist storage="lake"
source: weird is daily -> {
  group_by: d
  aggregate: total is total.sum()
}
`);
         await writeLake(chain.dir, DAILY_ROWS);
         // The parent's entry says its table holds no `total`, so the
         // downstream compiles over no tier.
         const { error, models } = await stack(
            chain,
            "weird",
            parentEntry(chain, "daily", {
               schema: [
                  { name: "d", type: "DATE" },
                  { name: "cat", type: "VARCHAR" },
               ],
            }),
         );
         expect(error).toBeInstanceOf(ChainedShapeNotCarriedError);
         expect((error as Error).message).toStartWith(
            "'weird' reaches only stored upstreams ('daily'), but the build " +
               "could not express it over them",
         );
         // Nothing in the parent can be left out, so it is compiled once.
         expect(models.length).toBe(1);
      });

      describe("a parent's timestamp truncations, read in a zone", () => {
         // `a` truncates in the default zone; its table stores `d` and `m`,
         // which its re-root restates (`d is d__stored.day`) and so truncates
         // again in the zone the downstream reads in.
         const zoned = (downstreamZone: string) => `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, TIMESTAMP '2024-01-07 03:30:00'),
    (2, TIMESTAMP '2024-01-01 02:00:00'),
    (3, TIMESTAMP '2024-03-31 23:30:00')
  ) AS t(id, ts)
""")

#@ persist storage="lake"
source: a is raw -> { select: id, ts, d is ts.day, m is ts.month }

#@ persist storage="lake"
source: b is a -> {
  ${downstreamZone}
  group_by: id, dd is d, dm is d.month, mm is m, tday is ts.day
}

source: a_live is raw -> { select: id, ts, d is ts.day, m is ts.month }
source: b_live is a_live -> {
  ${downstreamZone}
  group_by: id, dd is d, dm is d.month, mm is m, tday is ts.day
}
`;
         /** What `a`'s build wrote: each truncation in the default zone. */
         const A_ROWS =
            'CREATE TABLE "daily_t" AS SELECT id, ts, ' +
            "date_trunc('day', ts) AS d, date_trunc('month', ts) AS m " +
            "FROM (VALUES (1, TIMESTAMP '2024-01-07 03:30:00'), " +
            "(2, TIMESTAMP '2024-01-01 02:00:00'), " +
            "(3, TIMESTAMP '2024-03-31 23:30:00')) AS t(id, ts)";
         const A_SCHEMA = [
            { name: "id", type: "INTEGER" },
            { name: "ts", type: "TIMESTAMP" },
            { name: "d", type: "TIMESTAMP" },
            { name: "m", type: "TIMESTAMP" },
         ];
         const COLUMNS = ["dd", "dm", "mm", "tday"];

         /** `b_live`'s rows, each value as text. */
         async function live(chain: Chain): Promise<unknown[]> {
            const base = pathToFileURL(chain.dir + "/");
            const runtime = new Runtime({ urlReader: reader(), connections });
            const result = await runtime
               .loadModel(new URL("m.malloy", base), { importBaseURL: base })
               .loadQuery(
                  `run: b_live -> { select: id, ${COLUMNS.map(
                     (c) => `${c} is ${c}::string`,
                  ).join(", ")}; order_by: id }`,
               )
               .run();
            return result.data
               .toObject()
               .map((row) =>
                  Object.fromEntries(
                     Object.entries(row).map(([k, v]) => [
                        k,
                        k === "id" ? Number(v) : String(v),
                     ]),
                  ),
               );
         }

         /** `b`'s stored rows, each value as text. */
         async function stored(chain: Chain): Promise<unknown[]> {
            return (
               (await readLake(
                  chain.dir,
                  `SELECT id, ${COLUMNS.map(
                     (c) => `${c}::VARCHAR AS ${c}`,
                  ).join(", ")} FROM "b_t" ORDER BY id`,
               )) as Record<string, unknown>[]
            ).map((row) => ({ ...row, id: Number(row.id) }));
         }

         it("declines a downstream that reads them in another zone, which a stored value truncated twice would answer wrong", async () => {
            const chain = await planChain(
               zoned("timezone: 'America/Los_Angeles'"),
            );
            await writeLake(chain.dir, A_ROWS);
            const { error } = await stack(
               chain,
               "b",
               parentEntry(chain, "a", { schema: A_SCHEMA, rowCount: 3 }),
            );
            if (error === undefined) {
               // Stacked: what it stored must be live's, which it is not.
               expect(await stored(chain)).toEqual(await live(chain));
            }
            expect(error).toBeInstanceOf(ChainedShapeNotCarriedError);
            expect((error as Error).message).toContain(
               "the downstream reads 'a' in another time zone than its table was built in",
            );
         });

         for (const [label, zone] of [
            ["sets no zone", ""],
            ["sets the zone they were truncated in", "timezone: 'UTC'"],
         ] as const) {
            it(`stacks a downstream that ${label}, storing live's rows`, async () => {
               const chain = await planChain(zoned(zone));
               await writeLake(chain.dir, A_ROWS);
               const { error, result } = await stack(
                  chain,
                  "b",
                  parentEntry(chain, "a", { schema: A_SCHEMA, rowCount: 3 }),
               );
               expect(error).toBeUndefined();
               expect(result).toMatchObject({ upstreamReuse: "reused" });
               expect(await stored(chain)).toEqual(await live(chain));
            });
         }
      });

      it("keeps a gated parent's gates on its re-root, as the author wrote them", async () => {
         const chain = await planChain(`${RAW.replace(
            "##! experimental.persistence",
            "##! experimental { persistence givens }",
         )}
given: ROLE :: string is 'admin'

#(authorize) $ROLE = 'admin'
#(access_filter) cat = 'a'
#@ persist storage="lake"
source: daily is raw -> {
  group_by: d, cat
  aggregate: total is amt.sum()
}

#@ persist storage="lake"
source: monthly is daily -> {
  group_by: m is d.month
  aggregate: total is total.sum()
}
`);
         await writeLake(chain.dir, DAILY_ROWS);
         const { error, models } = await stack(
            chain,
            "monthly",
            parentEntry(chain, "daily"),
         );
         // The gates stay on the re-rooted parent as the author wrote them,
         // so the copy declares the parent with every gate it has live. A
         // gate is a caller's, proved by govern() at query time, never baked
         // into a build: the eligibility gate refuses a stored source that
         // reaches one, so in a run this chain never builds.
         expect(models.length).toBe(1);
         expect(models[0]).toContain(
            "#(authorize) $ROLE = 'admin'\n#(access_filter) cat = 'a'\n#@ persist storage=\"lake\"\nsource: daily is ",
         );
         expect(error).toBeUndefined();
         expect(
            await readLake(
               chain.dir,
               "SELECT CAST(m AS VARCHAR) AS m, total FROM monthly_t ORDER BY m",
            ),
         ).toEqual([
            { m: "2026-01-01", total: 15 },
            { m: "2026-02-01", total: 7 },
         ]);
      });

      it("writes the text from the package's model as loaded, before any build has read its files", async () => {
         const chain = await planChain(CHAIN);
         // A fresh compile of the model, as the package holds it: nothing has
         // read its author files through it yet.
         const runtime = new Runtime({ urlReader: reader(), connections });
         const base = pathToFileURL(chain.dir + "/");
         const model = await runtime
            .loadModel(new URL("m.malloy", base), { importBaseURL: base })
            .getModel();
         const { id, source } = chain.byName("monthly");
         const address =
            chain.facts.addressBySourceId[chain.byName("daily").id]!;
         const text = chainBuildText(
            {
               downstream: { sourceID: id, name: source.name },
               destination: { name: "lake", schema: "main" },
               upstreams: [
                  {
                     entry: parentEntry(chain, "daily")[address],
                     names: { daily: [chain.byName("daily").id] },
                  },
               ],
            },
            {
               modelPath: "m.malloy",
               modelDef: model._modelDef,
               context: [chain.dir, {} as never, {}],
            },
         );
         expect(
            text && "declined" in text ? text.declined : undefined,
         ).toBeUndefined();
         expect(text && "rerooted" in text ? text.rerooted : []).toEqual([
            "daily",
         ]);
      });

      it("follows only the imports the compile resolved, never one inside a comment", async () => {
         // A secret outside the package, and a path that does not exist: the
         // text that names them is not an import, so neither is read, and the
         // answer is the same for both.
         const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), "outside-"));
         dirs.push(outsideDir);
         const secret = path.join(outsideDir, "secret.malloy");
         fs.writeFileSync(secret, "source: secret is duckdb.sql('SELECT 1')\n");
         const missing = path.join(outsideDir, "absent.malloy");
         const answers: string[] = [];
         for (const target of [secret, missing]) {
            const chain = await planChain(
               // A line that reads as an import, inside a block comment.
               `/*\nimport "${target}"\n*/\n${CHAIN}`,
            );
            const { id, source } = chain.byName("monthly");
            const address =
               chain.facts.addressBySourceId[chain.byName("daily").id]!;
            const text = chainBuildText(
               {
                  downstream: { sourceID: id, name: source.name },
                  destination: { name: "lake", schema: "main" },
                  upstreams: [
                     {
                        entry: parentEntry(chain, "daily")[address],
                        names: { daily: [chain.byName("daily").id] },
                     },
                  ],
               },
               {
                  modelPath: "m.malloy",
                  modelDef: source._model?._modelDef,
                  context: [chain.dir, {} as never, {}],
               },
            );
            expect(
               text && "declined" in text ? text.declined : undefined,
            ).toBeUndefined();
            const files = text && "files" in text ? [...text.files.keys()] : [];
            expect(files.some((url) => url.includes(outsideDir))).toBe(false);
            answers.push(
               JSON.stringify(
                  text && "rerooted" in text ? text.rerooted : null,
               ),
            );
         }
         expect(answers[0]).toBe(answers[1]);
      });
   },
);
