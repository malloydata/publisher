// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every v1 chained `storage=` source in the repo's persist corpus (the persist
 * fixtures and every hammer scenario's models), built by stacking on its
 * parents' stored tables in a plain-DuckDB destination, pinned against a
 * golden: its content address, which stored upstreams it reaches, how the
 * chained build ends (stacked, or which refusal), and the schema the table it
 * writes has.
 *
 * A chained source is a `#@ persist storage=` source whose definition reaches
 * another persisted source (other than its own table). Its parents are built
 * first: a parent that is not itself chained is written from its public build
 * SQL, run on the corpus's DuckDB stand-in for its warehouse; a chained one is
 * the table its own chained build wrote, or, when that build refused, the
 * recompute's. Moving chained builds onto another text must leave every row
 * of the golden as it is, or change it in a commit that says why.
 *
 *    UPDATE_GOLDEN=1 bun test src/service/materialization/run/chained_storage_corpus.spec.ts
 */

import { DuckDBInstance } from "@duckdb/node-api";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   Runtime,
   type PersistSource,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { buildEnvironmentStub } from "../../../test_helpers/materialization_service_harness";
import {
   closeCorpus,
   type CorpusModel,
   fixtureModels,
   hammerModels,
} from "../../../test_helpers/persist_corpus";
import { installSettings } from "../../../test_helpers/settings_env";
import { storageDestinationRoot } from "../../connection_config";
import { buildSQLOf } from "../plan/addressing";
import {
   collectModelBuildPlan,
   emptyBuildPlanParts,
   projectToPublicColumns,
} from "../plan/build_plan";
import { declaredStorage } from "../plan/persist_source_fields";
import { reachPersisted } from "../plan/source_map";
import { authorModelLiftContext } from "../serve/materialization_serve_transform";
import type { ManifestEntry } from "../types";
import {
   chainedPlanFacts,
   readAuthorFile,
} from "./materialization_run_support";
import { StorageBuildStrategy } from "./storage_build_strategy";

const GOLDEN = path.join(__dirname, "chained_storage_corpus.golden.json");

/** One chained source, as the golden records it. */
interface ChainedRow {
   model: string;
   source: string;
   address: string | null;
   destination: string;
   upstreams: string[];
   outcome: string;
   schema?: { name: string; type: string }[];
}

const scratch: string[] = [];
afterAll(async () => {
   for (const dir of scratch) fs.rmSync(dir, { recursive: true, force: true });
   await closeCorpus();
});

/**
 * The model's files on disk, so the author-model reads a build makes by
 * `location.url` find them: a fixture's already are; a hammer scenario's are
 * written under a scratch directory. Returns the package root and the entry.
 */
function onDisk(model: CorpusModel): { root: string; entry: string } {
   const entryPath = fileURLToPath(model.entry);
   if (!model.entry.startsWith("file:///hammer/")) {
      return { root: path.dirname(entryPath), entry: entryPath };
   }
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chained-corpus-"));
   scratch.push(dir);
   for (const [url, text] of model.files) {
      const rel = url.slice("file:///hammer/".length);
      fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
      fs.writeFileSync(path.join(dir, rel), text);
   }
   const rel = model.entry.slice("file:///hammer/".length);
   const entry = path.join(dir, rel);
   return { root: path.dirname(entry), entry };
}

async function lake(
   envDir: string,
   destination: string,
   run: (c: Awaited<ReturnType<DuckDBInstance["connect"]>>) => Promise<void>,
): Promise<void> {
   const root = storageDestinationRoot(envDir);
   fs.mkdirSync(root, { recursive: true });
   const instance = await DuckDBInstance.create(
      path.join(root, `${destination}.duckdb`),
   );
   const connection = await instance.connect();
   try {
      await run(connection);
   } finally {
      connection.closeSync();
      instance.closeSync();
   }
}

/** Write `source`'s public rows to `table` in its destination, as a recompute would. */
async function writeFromWarehouse(
   model: CorpusModel,
   source: PersistSource,
   envDir: string,
   destination: string,
   table: string,
): Promise<{ name: string; type: string }[]> {
   const warehouse = model.connections.get(source.connectionName) as
      | DuckDBConnection
      | undefined;
   if (!warehouse) throw new Error(`no stand-in for ${source.connectionName}`);
   const sql = projectToPublicColumns(
      source,
      buildSQLOf(source, { entries: {}, strict: false }),
   );
   const file = path.join(envDir, `${table}.parquet`);
   await warehouse.runSQL(
      `COPY (${sql}) TO '${file.replace(/'/g, "''")}' (FORMAT parquet)`,
   );
   let schema: { name: string; type: string }[] = [];
   await lake(envDir, destination, async (c) => {
      await c.run(
         `CREATE OR REPLACE TABLE "${table}" AS SELECT * FROM read_parquet('${file.replace(/'/g, "''")}')`,
      );
      schema = (await c.runAndReadAll(`DESCRIBE "${table}"`))
         .getRowObjectsJson()
         .map((r) => ({
            name: String(r.column_name),
            type: String(r.column_type),
         }));
   });
   return schema;
}

/** An error as the golden records it: its class and message, with no scratch path. */
function errorOutcome(err: unknown): string {
   const e = err as Error;
   return `${e?.constructor?.name ?? "Error"}: ${e?.message ?? String(err)}`
      .replace(
         /(file:\/\/)?(\/private)?\/[^\s'"]*\/chained-corpus-[A-Za-z0-9]+/g,
         "<scratch>",
      )
      .replace(/\/[^\s'"]*\/persist-corpus-[A-Za-z0-9]+/g, "<corpus>");
}

/** Plan one model and stack every chained `storage=` source in it. */
async function chainedRows(model: CorpusModel): Promise<ChainedRow[]> {
   const { root, entry } = onDisk(model);
   const base = pathToFileURL(root + "/");
   const runtime = new Runtime({
      urlReader: {
         readURL: async (url: URL) => fs.readFileSync(url, "utf8"),
      },
      connections: new FixedConnectionMap(
         new Map([...model.connections]),
         [...model.connections.keys()][0],
      ),
   });
   const materializer = runtime.loadModel(pathToFileURL(entry), {
      importBaseURL: base,
   });
   const parts = emptyBuildPlanParts();
   await collectModelBuildPlan(parts, {
      modelPath: path.relative(root, entry),
      packagePath: root,
      materializer,
      malloyModel: await materializer.getModel(),
      runtime,
      getRuntime: async () => ({ runtime, importBaseURL: base }),
   });
   const digests = Object.fromEntries(
      [...model.connections.keys()].map((n) => [n, `${n}-digest`]),
   );
   const facts = chainedPlanFacts(parts.sources, digests);

   // Each storage= source, with what it reaches.
   const storage = Object.entries(parts.sources).flatMap(([id, source]) => {
      let destination: string | undefined;
      try {
         destination = declaredStorage(source);
      } catch {
         return [];
      }
      if (!destination || destination === source.connectionName) return [];
      const address = facts.addressBySourceId[id];
      const lift = authorModelLiftContext(
         source._model?._modelDef,
         readAuthorFile,
      );
      const reached = reachPersisted(lift, source.name, (_n, sid) =>
         facts.persistSourceIds.has(sid),
      ).persisted.filter(
         (p) => facts.addressBySourceId[p.sourceID] !== address,
      );
      return [{ id, source, destination, address, lift, reached }];
   });
   if (!storage.some((s) => s.reached.length > 0)) return [];

   const envDir = fs.mkdtempSync(path.join(os.tmpdir(), "chained-corpus-env-"));
   scratch.push(envDir);
   const destinations = Object.fromEntries(
      storage.map((s) => [
         s.destination,
         { name: s.destination, type: "duckdb" },
      ]),
   );
   const environment = buildEnvironmentStub({
      destinations,
      environmentPath: envDir,
   });
   const strategy = new StorageBuildStrategy({
      buildDownstreamViaParents: () => {
         throw new Error("not used: the method is called directly");
      },
   });

   const built: Record<string, ManifestEntry> = {};
   const done = new Set<string>();
   const rows: ChainedRow[] = [];
   let pending = storage.slice();
   for (let pass = 0; pending.length > 0 && pass < 20; pass++) {
      const ready = pending.filter((s) =>
         s.reached.every(
            (p) =>
               !storage.some((o) => o.id === p.sourceID) ||
               done.has(p.sourceID),
         ),
      );
      if (ready.length === 0) break;
      for (const s of ready) {
         const table = `t${done.size}_${s.source.name}`.replace(/\W/g, "_");
         let schema: { name: string; type: string }[] | undefined;
         let outcome = "not built";
         if (s.reached.length > 0) {
            try {
               const result = await strategy.buildDownstreamViaParents(
                  s.source,
                  s.destination,
                  destinations[s.destination] as never,
                  built,
                  environment as never,
                  table,
                  facts,
                  s.address ?? "",
                  s.lift,
               );
               outcome = `stacked (${result.upstreamReuse})`;
               schema = result.schema.map((c) => ({
                  name: String(c.name),
                  type: String(c.type),
               }));
            } catch (err) {
               outcome = errorOutcome(err);
            }
            rows.push({
               model: model.label,
               source: s.source.name,
               address: s.address ?? null,
               destination: s.destination,
               upstreams: s.reached.map((p) => p.name).sort(),
               outcome,
               ...(schema ? { schema } : {}),
            });
         }
         if (schema === undefined) {
            try {
               schema = await writeFromWarehouse(
                  model,
                  s.source,
                  envDir,
                  s.destination,
                  table,
               );
            } catch {
               schema = undefined;
            }
         }
         if (schema !== undefined && s.address !== undefined) {
            built[s.address] = {
               sourceEntityId: s.address,
               sourceName: s.source.name,
               physicalTableName: table,
               connectionName: s.source.connectionName,
               storageDestinationName: s.destination,
               realization: "COPY",
               schema,
            } as ManifestEntry;
         }
         done.add(s.id);
      }
      pending = pending.filter((s) => !done.has(s.id));
   }
   for (const s of pending) {
      rows.push({
         model: model.label,
         source: s.source.name,
         address: s.address ?? null,
         destination: s.destination,
         upstreams: s.reached.map((p) => p.name).sort(),
         outcome: "upstreams never built",
      });
   }
   return rows;
}

describe.skipIf(process.platform === "win32")(
   "v1 chained storage= builds over the repo's persist corpus",
   () => {
      it(
         "keep every chained source's address, outcome and stored schema",
         async () => {
            const { restore } = installSettings({
               env: { PERSIST_STORAGE_MODE: "on" },
            });
            try {
               const hammer = await hammerModels();
               const corpus = [...fixtureModels(), ...hammer.models];
               const rows: ChainedRow[] = [];
               const failed: string[] = [];
               // Not released per model: a scenario's models share connections,
               // and a later model of the scenario still reads them.
               for (const model of corpus) {
                  try {
                     rows.push(...(await chainedRows(model)));
                  } catch (err) {
                     failed.push(`${model.label}: ${errorOutcome(err)}`);
                  }
               }
               const actual = {
                  rows: rows.sort((a, b) =>
                     `${a.model}\0${a.source}`.localeCompare(
                        `${b.model}\0${b.source}`,
                     ),
                  ),
                  modelsThatDidNotPlan: failed.sort(),
               };
               if (process.env.UPDATE_GOLDEN === "1") {
                  fs.writeFileSync(
                     GOLDEN,
                     JSON.stringify(actual, null, 2) + "\n",
                  );
               }
               const golden = JSON.parse(fs.readFileSync(GOLDEN, "utf8"));
               expect(actual).toEqual(golden);
               // The corpus holds chained sources at all, and most stack.
               expect(actual.rows.length).toBeGreaterThan(5);
            } finally {
               restore();
            }
         },
         { timeout: 600_000 },
      );
   },
);
