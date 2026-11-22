// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's compiled build plan as the run receives it: each connection's
 * build order (`SourceBuild`), the table planned for every source, the
 * connection digests and connections a build runs on, and the two pure
 * readers the run walks it with.
 *
 * Compiled in `plan/` (`build_plan_compile.ts`); read by the run's builds,
 * its instructions, its ownership checks and its telemetry.
 *
 * Pure.
 */

import type {
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import type { KeyCheckCompiler } from "./key_check";
import type { Omission } from "./omitted_sources";
import type {
   ModelGateLines,
   PersistedViewName,
   SourceBuild,
} from "./package_plan";
import type { MaterializePlan, TablePlan } from "./table_plans";
import type { UpstreamSchemaReadings } from "./upstream_schemas";

export type { SourceBuild } from "./package_plan";

/**
 * Result of compiling a package's persist sources. The wire `BuildPlan` is a
 * projection of this (`deriveBuildPlan` in `plan/`); the build engine consumes
 * the full structure.
 *
 * `Finding` is the type of an annotation finding: the planner's
 * `PackageAnnotationFinding`, which the run never reads.
 */
export interface CompiledBuildPlan<Finding = unknown> {
   builds: SourceBuild[];
   /**
    * sourceID -> the table planned for it, one per planned source, with its
    * kind fixed here: an authored `#@ persist` source, a generated
    * `#(materialize)` build source (under the author's sourceID), a rollup
    * or a persisted view.
    */
   tables: Record<string, TablePlan>;
   connectionDigests: Record<string, string>;
   /**
    * Each table the plan's sources read, with its columns' native types as
    * the load read them from the warehouse's catalog, by `upstreamTableKey`
    * (`domain/upstream_schemas.ts`). Every address folds the readings of the
    * tables its source reads. Pass it to every address computation of a plan
    * source, with `tableFolds`. Absent: none read (a spec's plan).
    */
   upstreamSchemas?: UpstreamSchemaReadings;
   /**
    * Each connection's configured type, by name (`postgres`, `duckdb`,
    * `ducklake`, ...), resolved through the package, so the package's DuckDB
    * sandbox reads `duckdb`. Data, like the digests, so the plan derives the
    * same in the package-load worker. The `storage=` gate reads it
    * (materialization_eligibility.ts). Absent: none recorded.
    */
   connectionTypes?: Record<string, string>;
   /**
    * The connections configured `managed` (a catalog an orchestrator
    * provisions, whose tables are built inside the catalog by SQL Malloy
    * generates), by name, sorted. Data, as `connectionTypes` is. Absent: none.
    */
   managedConnections?: string[];
   connections: Map<string, MalloyConnection>;
   /**
    * The package is a published version of a `scope: "version"` package:
    * every table name, `name=` included, is a readable name with its
    * source's identity hash.
    */
   versionScoped?: boolean;
   /**
    * What each moved source's address folds in (`foldDistinctTableNames` in
    * `plan/`), by sourceID: decided over the whole package before any
    * refusal. Pass it to every address computation of a plan source.
    */
   tableFolds?: Readonly<Record<string, string>>;
   /**
    * Each planned source's content address, its fold applied, by sourceID,
    * or why it cannot be addressed (`PlanAddresses` in `plan/`). Computed
    * once the folds are decided; every plan-side reader of an address reads
    * it.
    */
   addresses?: Readonly<Record<string, string | { unaddressable: string }>>;
   /**
    * `#@ persist` sources Malloy's build walk did NOT recognize as persistable
    * (`detectDroppedPersistSources` in `plan/`). Callers surface these as a
    * load-time warning and a hard build failure so the annotation is never a
    * silent no-op.
    */
   droppedPersistSources?: { name: string; modelPath: string }[];
   /** Findings on the authored `#@` annotations (see annotation_reader). */
   annotationFindings?: Finding[];
   /**
    * Each model's own load-failure lines (`load_gate_text.ts` in `plan/`), so
    * a run over the files reads them as the load does.
    */
   modelGateLines?: ModelGateLines;
   /** Each persisted view's build source, named as its author wrote it. */
   persistedViews?: Record<string, PersistedViewName>;
   /**
    * Each `#(materialize)` uniqueness check's compiler over a run's compiler
    * manifest, by `keyCheckCompilerKey`; set only on a plan compiled on the thread
    * that builds (`build_plan_compile.ts`).
    */
   keyCheckCompilers?: ReadonlyMap<string, KeyCheckCompiler>;
   /**
    * Sources planning left out because it FAILED (a companion that did not
    * compile), for the wire plan's `omittedSources`. Absent: none.
    */
   omittedSources?: Omission[];
   /**
    * Every sourceID an access decision of the package reads: a dependency,
    * at any depth, of a definition that carries a gate or reads a given
    * (`collectAccessReads` in `plan/`). A relation that reads the current
    * time is refused when it is one. `"*"` when a walk could not finish.
    */
   accessReads?: readonly string[];
}

/**
 * The name `accessReads` holds when a walk could not finish: every stored
 * relation counts as one an access decision reads (fail closed).
 */
export const EVERY_SOURCE = "*";

/** The planned sources of `build` in build order, skipping any not in `tables`. */
export function* sourcesInBuildOrder(
   build: SourceBuild,
   tables: Readonly<Record<string, { source: PersistSource }>>,
): Iterable<PersistSource> {
   for (const sourceID of build.order) {
      const source = tables[sourceID]?.source;
      if (source) yield source;
   }
}

/**
 * Every planned source of `builds`, each with the connection whose build
 * names it, in an order where each comes after every source it depends on
 * that the builds name, across builds: a downstream's edges may name a
 * source another build (another model's, or Malloy's own build of the
 * author's model) orders. Otherwise in the builds' own order, which is
 * Malloy's within a build. A source named by several builds is kept at each
 * place, as the builds name it. A cycle, which no plan has, is broken in the
 * builds' order rather than dropping a source.
 */
export function sourcesInDependencyOrder(
   builds: readonly SourceBuild[],
): { sourceID: string; connectionName: string }[] {
   const items = builds.flatMap((build) =>
      build.order.map((sourceID) => ({
         sourceID,
         connectionName: build.connectionName,
      })),
   );
   const named = new Set(items.map((i) => i.sourceID));
   const upstreams = new Map<string, Set<string>>();
   for (const build of builds) {
      for (const [sourceID, reads] of Object.entries(build.dependsOn)) {
         const set = upstreams.get(sourceID) ?? new Set<string>();
         for (const r of reads) if (r !== sourceID && named.has(r)) set.add(r);
         upstreams.set(sourceID, set);
      }
   }
   const out: typeof items = [];
   const placed = new Set<string>();
   const pending = [...items];
   while (pending.length > 0) {
      let at = pending.findIndex((item) =>
         [...(upstreams.get(item.sourceID) ?? [])].every((u) => placed.has(u)),
      );
      if (at < 0) at = 0;
      const [item] = pending.splice(at, 1);
      out.push(item);
      placed.add(item.sourceID);
   }
   return out;
}

/** The `#(materialize)` plans of `tables`, by the author's sourceID. */
export function materializePlansOf(
   tables: Readonly<Record<string, Pick<TablePlan, "materialize">>>,
): Record<string, MaterializePlan> {
   return Object.fromEntries(
      Object.entries(tables).flatMap(([sourceID, table]) =>
         table.materialize ? [[sourceID, table.materialize]] : [],
      ),
   );
}
