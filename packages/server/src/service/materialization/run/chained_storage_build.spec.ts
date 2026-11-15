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
import {
   chainedPlanFacts,
   readAuthorFile,
   type ChainedPlanFacts,
} from "./materialization_run_support";
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
   const facts = chainedPlanFacts(parts.sources, { duckdb: "digest" });
   return {
      dir,
      sources: parts.sources,
      facts,
      byName(name) {
         const hit = Object.entries(parts.sources).find(
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
         authorModelLiftContext(source._model?._modelDef, readAuthorFile),
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
   },
);
