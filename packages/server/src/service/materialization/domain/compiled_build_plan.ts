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
import type { MaterializePlan, TablePlan } from "./table_plans";

/**
 * One connection's persist sources in the order they build, from Malloy's
 * `Runtime.getBuildTargets`: every source on every table, the tables a source
 * reads before it. Sources whose SQL does not compile on their own come last;
 * they have no table, and are refused per source like any other.
 *
 * Holds sourceIDs, resolved against a plan's `sources` when walked, so a
 * pre-aggregation companion that reaches an author's persisted source walks
 * the author's own `PersistSource` for it.
 */
export interface SourceBuild {
   connectionName: string;
   /** sourceIDs, dependencies first; the sources of one table are adjacent */
   order: string[];
   /**
    * sourceID -> the sourceIDs on the tables it reads. Sources on one table
    * read none of each other: an extension is its base's table, not a reader
    * of it. An unbuildable source lists none.
    */
   dependsOn: Record<string, string[]>;
}

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
    * Each connection's configured type, by name (`postgres`, `duckdb`,
    * `ducklake`, ...), resolved through the package, so the package's DuckDB
    * sandbox reads `duckdb`. Data, like the digests, so the plan derives the
    * same in the package-load worker. The `storage=` gate reads it
    * (materialization_eligibility.ts). Absent: none recorded.
    */
   connectionTypes?: Record<string, string>;
   connections: Map<string, MalloyConnection>;
   /**
    * What each moved source's address folds in (`foldDistinctTableNames` in
    * `plan/`), by sourceID: decided over the whole package before any
    * refusal. Pass it to every address computation of a plan source.
    */
   tableFolds?: Readonly<Record<string, string>>;
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
    * Each model's invalid `#@ preaggregate` declarations and misspelled
    * materialization routes, as load-failure lines (`load_gate_text.ts` in
    * `plan/`), so a run over the files reads them as the load does.
    */
   modelGateLines?: { preaggregate: string[]; nearMiss: string[] };
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
}

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
