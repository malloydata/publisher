// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A one-model package on disk, compiled through the real runtime over an
 * in-memory DuckDB, with the persisted view and query legs
 * (`PersistedViewServe`, `PersistedQueryServe`) and the rollup leg
 * (`RollupVariantServe`) wired to it as `Model` wires them: for unit specs of the legs' decisions without a package load,
 * a build or a bind. Nothing here runs a query; a leg's answer is checked
 * by the SQL it compiles.
 */

import { isPersistedQuerySourceID } from "../service/materialization/domain/table_plans";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   MalloyConfig,
   type ModelDef,
   type ModelMaterializer,
} from "@malloydata/malloy";
import { createModelRuntime } from "../service/model_runtime";
import { computePackageBuildPlan } from "../service/materialization/plan/build_plan_compile";
import {
   planModelPersistedQueries,
   planModelPersistedViews,
} from "../service/materialization/plan/persisted_view";
import {
   planSourceRollups,
   rollupGateEnforcedAtRead,
} from "../service/materialization/plan/rollup_plan";
import { RollupVariantServe } from "../service/materialization/serve/rollup_variants";
import type {
   MaterializedTableCatalog,
   PlannedTable,
} from "../service/materialization/serve/query_path";
import { PersistedQueryServe } from "../service/materialization/serve/query_variants";
import { PersistedViewServe } from "../service/materialization/serve/view_variants";
import {
   type Binding,
   makeSnapshot,
   pin,
   type Pinned,
} from "../service/materialization/domain/binding";
import type { FreshnessManifest } from "../service/materialization/domain/types";
import { tempBuildPlanPackages } from "./incremental_test_harness";

export interface PersistedResultFixture {
   packagePath: string;
   malloyConfig: MalloyConfig;
   modelDef: ModelDef;
   materializer: ModelMaterializer;
   /** The package's planned tables, as `Model` hands the legs its catalog. */
   catalog: MaterializedTableCatalog;
   views: PersistedViewServe;
   queries: PersistedQueryServe;
   rollups: RollupVariantServe;
   /** The planned table id of the build source named `buildSourceName`. */
   tableIdOf(buildSourceName: string): string;
   /** The build source's sourceID, by its name. */
   buildSourceIDOf(buildSourceName: string): string;
   cleanup(): Promise<void>;
}

/** What a fixture's legs read that a spec may vary. */
export interface PersistedResultOptions {
   /** The destinations a `storage=` table is read from, by destination name. */
   destinations?: Record<string, DuckDBConnection>;
   /** Skip the build plan (a model it would refuse); the catalog is then empty. */
   noBuildPlan?: boolean;
}

/** Compile `model` as `m.malloy` and wire both legs to it. */
export async function persistedResultFixture(
   model: string,
   options: PersistedResultOptions = {},
): Promise<PersistedResultFixture> {
   const packages = tempBuildPlanPackages();
   const pkg = await packages.create(model);
   const packagePath = pkg.getPackagePath();
   const malloyConfig = pkg.getMalloyConfig();
   const planned = new Map<string, PlannedTable>();
   if (!options.noBuildPlan) {
      const outcome = await computePackageBuildPlan(pkg);
      for (const [sourceID, source] of Object.entries(
         outcome.plan?.sources ?? {},
      )) {
         if (!source.sourceEntityId) continue;
         const compilerKey = outcome.compilerKeys?.[source.sourceEntityId];
         planned.set(sourceID, {
            sourceID,
            name: source.name,
            sourceEntityId: source.sourceEntityId,
            ...(compilerKey ? { compilerKey } : {}),
            origin: source.origin ?? "persist",
         });
      }
   }
   const catalog: MaterializedTableCatalog = { planned, bound: {} };
   const { runtime, modelURL } = await createModelRuntime(
      packagePath,
      "m.malloy",
      malloyConfig,
   );
   const materializer = runtime.loadModel(modelURL);
   const modelDef = (await materializer.getModel())._modelDef;
   const destinations = options.destinations;
   const destinationConfig = destinations
      ? (() => {
           const config = new MalloyConfig({ connections: {} });
           const map = new Map(Object.entries(destinations));
           const [first] = map.keys();
           config.wrapConnections(
              () => new FixedConnectionMap(map, first ?? "lake"),
           );
           return config;
        })()
      : undefined;
   const host = {
      modelPath: () => "m.malloy",
      modelDef: () => modelDef,
      modelMaterializer: () => materializer,
      serveDestinationConfig: () =>
         destinationConfig ? () => destinationConfig : undefined,
      catalog: () => catalog,
      planModelPersistedViews,
      planModelPersistedQueries,
   };
   const views = new PersistedViewServe(host);
   const queries = new PersistedQueryServe(host, views);
   const rollups = new RollupVariantServe({
      ...host,
      planSourceRollups,
      rollupGateEnforcedAtRead,
      // No served copy, so no join branch.
      joinRollupBasis: async () => undefined,
   });
   views.setContext(packagePath, malloyConfig);
   queries.setContext(packagePath, malloyConfig);
   rollups.setContext(packagePath, malloyConfig);
   const byName = (buildSourceName: string) => {
      const hit = [...planned.values()].find((t) => t.name === buildSourceName);
      if (!hit) throw new Error(`no planned table ${buildSourceName}`);
      return hit;
   };
   return {
      packagePath,
      malloyConfig,
      modelDef,
      materializer,
      catalog,
      views,
      queries,
      rollups,
      tableIdOf: (name) => byName(name).sourceEntityId,
      buildSourceIDOf: (name) => {
         const plan = [
            ...planModelPersistedViews(modelDef).plans,
            ...planModelPersistedQueries(modelDef).plans,
         ].find((p) => p.buildSourceName === name);
         if (!plan) throw new Error(`no persisted result ${name}`);
         return plan.buildSourceID;
      },
      cleanup: () => packages.cleanup(),
   };
}

/** A pin of `bindings` and database `entries`, now. */
export function pinnedOf(
   bindings: Binding[] = [],
   entries: FreshnessManifest = {},
   asOf = new Date(),
): Pinned {
   return pin(makeSnapshot(bindings, entries), asOf);
}

/** A `storage=` view-kind binding of `buildSourceID`, in destination `lake`. */
export function storedViewBinding(
   buildSourceID: string,
   overrides: Partial<Binding> = {},
): Binding {
   return {
      kind: isPersistedQuerySourceID(buildSourceID) ? "query" : "view",
      tier: "storage",
      sourceName: buildSourceID.split("@")[0],
      sourceIDs: [buildSourceID],
      destinationName: "lake",
      virtualHandle: "vh_1",
      tablePath: "main.stored_view_t",
      schema: [],
      ...overrides,
   };
}
