// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The plan-time build of a `storage=` persisted view or named query over
 * upstreams stored in its own destination: the text it compiles there, with
 * each upstream re-rooted on a stand-in of its table as the served copy
 * re-roots it (`chainBuildText`, handed in as a port), compiled against a
 * stand-in for the destination that has DuckDB's dialect and no table. The
 * caller proves the compile builds the planned table (build_plan.ts) and
 * records it, so a run builds from this text or not at all, and never by
 * recomputing its upstreams from raw.
 *
 * Each upstream's stand-in columns are its output fields, each typed as a
 * `storage=` build writes that type. A view or query reaching anything but
 * its upstreams' tables there (the warehouse, another destination) has no
 * text, and is refused.
 */

import { FixedConnectionMap, type PersistSource } from "@malloydata/malloy";
import * as path from "path";
import { pathToFileURL } from "url";
import type {
   BoundStorageColumns,
   ChainBuildRequest,
   PlanChainBuildText,
} from "../domain/chain_build";
import type {
   MaterializePlan,
   PersistedQueryPlan,
   PersistedViewPlan,
   StoredInDestination,
} from "../domain/table_plans";
import { compileInMemory } from "../text/generated_files";
import {
   generatePersistedResultFile,
   persistedViewCompanionHref,
} from "../text/read_text";
import { SourceMap } from "../text/source_map";
import { plannedSources } from "./addressing";
import { reachedUpstreams } from "./downstream_upstreams";
import { destinationStandIn } from "./materialize_compile";

/** A view or query compiled over its upstreams' stand-ins, or why not. */
export type StorageOverStored =
   | { source: PersistSource; stored: StoredInDestination }
   | { refused: string };

/**
 * Each `storage=` view or query in `plans` and `queries` that reads an
 * upstream stored in its own destination, by build sourceID: its compile
 * over those upstreams' stand-ins, or why it has none. One that reads no
 * such upstream is not listed.
 */
export async function compileStorageOverStored(args: {
   modelDef: unknown;
   modelPath: string;
   modelFile: string;
   plans: readonly PersistedViewPlan[];
   queries: readonly PersistedQueryPlan[];
   chainBuildText?: PlanChainBuildText;
   boundStorageColumns?: BoundStorageColumns;
   materializePlans?: Readonly<Record<string, MaterializePlan>>;
}): Promise<Map<string, StorageOverStored>> {
   const out = new Map<string, StorageOverStored>();
   const map = SourceMap.of(args.modelDef);
   const contents =
      (args.modelDef as { contents?: Record<string, unknown> }).contents ?? {};
   const candidates = [
      ...args.plans.map((plan) => ({
         plan,
         root: map.node(plan.sourceID)?.def,
         authorName: plan.sourceName,
         views: [plan],
         queries: [] as PersistedQueryPlan[],
      })),
      ...args.queries.map((plan) => ({
         plan,
         root: contents[plan.queryName],
         // The query's named run target, its path kept, and the query too.
         authorName: plan.sourceName,
         keepAlso: [plan.queryName],
         views: [] as PersistedViewPlan[],
         queries: [plan],
      })),
   ];
   for (const c of candidates) {
      const destination = c.plan.storage;
      if (destination === undefined || c.root === undefined) continue;
      // An extension of a stored source reads its base's table: the base,
      // a query, is the upstream re-rooted.
      // A `#(materialize)` upstream planned as a table is one too, unless
      // it is the parent an extension table reached here builds over: the
      // extension's table is read, not its parent's.
      const materialized = args.materializePlans ?? {};
      const reached = reachedUpstreams(c.root, args.modelDef, map).filter(
         (u) =>
            u.storage === destination &&
            (materialized[u.sourceID] !== undefined ||
               (map.node(u.sourceID)?.def as { type?: unknown } | undefined)
                  ?.type === "query_source"),
      );
      const parents = new Set(
         reached.flatMap((u) => {
            const parent = materialized[u.sourceID]?.buildsOver?.sourceID;
            return parent ? [parent] : [];
         }),
      );
      const upstreams = reached.filter((u) => !parents.has(u.sourceID));
      if (upstreams.length === 0) continue;
      // A pair table holds a row per pair of its inlined join_many, not one
      // per row of its source: the stand-in would be read as the source's
      // rows, and a count over it would count pairs. An extension of a pair
      // parent is built over the parent's table, so it is refused the same.
      const pair = upstreams.find((u) =>
         overPairTable(u.sourceID, materialized),
      );
      out.set(
         c.plan.buildSourceID,
         pair
            ? {
                 refused:
                    `its upstream '${pair.name}' is a pair table, one row ` +
                    `per pair of an inlined join_many, which a view or ` +
                    `query is not yet built over in its destination`,
              }
            : await compileOne({ ...args, map, destination, upstreams, ...c }),
      );
   }
   return out;
}

/**
 * Whether `sourceID`'s `#(materialize)` table is a pair table, or is built
 * over one through its `buildsOver` chain (an extension of a pair parent,
 * whose own plan declares no pair).
 */
function overPairTable(
   sourceID: string,
   materialized: Readonly<Record<string, MaterializePlan>>,
): boolean {
   const seen = new Set<string>();
   for (
      let id: string | undefined = sourceID;
      id !== undefined && !seen.has(id);
      id = materialized[id]?.buildsOver?.sourceID
   ) {
      seen.add(id);
      const plan = materialized[id];
      if (plan?.pair || plan?.pairBlocks) return true;
   }
   return false;
}

async function compileOne(args: {
   modelDef: unknown;
   modelPath: string;
   modelFile: string;
   map: SourceMap;
   destination: string;
   upstreams: readonly { sourceID: string; name: string }[];
   plan: PersistedViewPlan | PersistedQueryPlan;
   authorName: string | undefined;
   keepAlso?: readonly string[];
   views: PersistedViewPlan[];
   queries: PersistedQueryPlan[];
   chainBuildText?: PlanChainBuildText;
   boundStorageColumns?: BoundStorageColumns;
   materializePlans?: Readonly<Record<string, MaterializePlan>>;
}): Promise<StorageOverStored> {
   const why = (refused: string): StorageOverStored => ({ refused });
   if (!args.chainBuildText) {
      return why("no served copy writes its text over its stored upstreams");
   }
   if (args.authorName === undefined) {
      return why("its run target is written inline, not a source by name");
   }
   const standIns: StoredInDestination["upstreams"][number][] = [];
   for (const [i, u] of args.upstreams.entries()) {
      // As bound, when the package binds its table: the plan is then
      // proven over the table's real types. Else as its build writes them.
      const columns =
         args.boundStorageColumns?.(u.sourceID, args.destination) ??
         args.materializePlans?.[u.sourceID]?.storedColumns ??
         storedColumnsOf(args.map.node(u.sourceID)?.def);
      if (columns === undefined) {
         return why(
            `its upstream '${u.name}' is not a query whose output columns a stored table holds`,
         );
      }
      standIns.push({
         sourceID: u.sourceID,
         handle: `publisher_stand_in_${i}`,
         columns,
      });
   }
   const companionURL = persistedViewCompanionHref(
      pathToFileURL(args.modelFile).href,
   );
   const companion = generatePersistedResultFile(
      args.views,
      args.queries,
      path.basename(args.modelPath),
   );
   if (companion === undefined)
      return why("its build file could not be written");
   // A view's source, or a query's run target (absent: none to judge the
   // zone by, so every zone-dependent upstream declines).
   const runTarget = args.plan.sourceID;
   const request: ChainBuildRequest = {
      downstream: {
         sourceID: runTarget ?? args.plan.buildSourceID,
         name: args.plan.buildSourceName,
      },
      destination: { name: args.destination, schema: "main" },
      upstreams: args.upstreams.map((u, i) => ({
         entry: {
            sourceEntityId: standIns[i].handle,
            sourceName: u.name,
            physicalTableName: standIns[i].handle,
            connectionName: args.destination,
            storageDestinationName: args.destination,
            schema: standIns[i].columns.map((c) => ({ ...c })),
         },
         names: { [u.name]: [u.sourceID] },
      })),
      view: {
         authorName: args.authorName,
         ...(args.keepAlso ? { keepAlso: args.keepAlso } : {}),
         companion: { url: companionURL, text: companion },
      },
   };
   const text = args.chainBuildText(request, {
      modelPath: args.modelPath,
      modelDef: args.modelDef,
      ...(args.materializePlans
         ? { materializePlans: args.materializePlans }
         : {}),
   });
   if (text === undefined) return why("its model does not declare it");
   if ("declined" in text) return why(text.declined);
   try {
      const { runtime, model } = await compileInMemory(
         text.files,
         text.entry,
         new FixedConnectionMap(
            new Map([[text.connection, destinationStandIn(text.connection)]]),
            text.connection,
         ),
      );
      const source = plannedSources(
         await runtime.getBuildTargets(model, {
            virtualMap: text.virtualMap as Map<string, Map<string, string>>,
         }),
      ).find((s) => s.name === text.downstreamName);
      if (!source) return why("its build source is not planned there");
      return {
         source,
         stored: {
            files: [...text.files],
            entry: text.entry,
            downstreamName: text.downstreamName,
            connection: text.connection,
            rerootedSpans: text.rerootedSpans.map((s) => ({ ...s })),
            // Only those it re-rooted: an extension of one reads it.
            upstreams: standIns.filter((_, i) =>
               text.rerooted.includes(args.upstreams[i].name),
            ),
         },
      };
   } catch (error) {
      return why(
         `it does not compile there (${error instanceof Error ? error.message : String(error)})`,
      );
   }
}

/**
 * The columns a `storage=` table of a query-shaped source holds, as its build
 * writes them: each output field that is a column, typed as DuckDB stores
 * it. Undefined for a source that is not a query, or an output a stored
 * table does not carry as one column (a nest, a record).
 */
export function storedColumnsOf(
   def: unknown,
): { name: string; type: string }[] | undefined {
   const d = def as { type?: unknown; fields?: unknown } | undefined;
   if (d?.type !== "query_source" || !Array.isArray(d.fields)) return undefined;
   const out: { name: string; type: string }[] = [];
   for (const f of d.fields as {
      name?: unknown;
      as?: unknown;
      type?: unknown;
      e?: unknown;
      join?: unknown;
      numberType?: unknown;
   }[]) {
      // A dimension its `extend` computes, a join or a view is not a column.
      if (f.e !== undefined || f.join !== undefined || f.type === "turtle") {
         continue;
      }
      const name = String(f.as ?? f.name ?? "");
      const type = storedTypeOf(f.type, f.numberType);
      if (!name) continue;
      if (type === undefined) return undefined;
      out.push({ name, type });
   }
   return out.length > 0 ? out : undefined;
}

/** The DuckDB type a `storage=` build writes a Malloy type as. */
function storedTypeOf(type: unknown, numberType: unknown): string | undefined {
   switch (type) {
      case "string":
         return "VARCHAR";
      case "number":
         return numberType === "integer" ? "BIGINT" : "DOUBLE";
      case "boolean":
         return "BOOLEAN";
      case "date":
         return "DATE";
      case "timestamp":
         return "TIMESTAMP";
      case "timestamptz":
         return "TIMESTAMP WITH TIME ZONE";
      default:
         return undefined;
   }
}
