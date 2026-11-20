// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's build plan as plain data (`PackagePlan`): what the planner
 * decided for every declaration, the wire projection, and what serving and
 * the load gate read beside it. It is the one plan shape that crosses the
 * package-load worker, and the one the facade holds; it carries no compiler
 * object, so `structuredClone` leaves it as it was.
 *
 * A run compiles its own view of it (live sources, connections and key
 * checks); nothing here holds one.
 *
 * Pure.
 */

import type { components } from "../../../api";
import type { IncrementalDeclaration } from "./incremental";
import type { MaterializePlan, TablePlan } from "./table_plans";
import type { BuildPlan } from "./types";

type RefusedReason = components["schemas"]["RefusedSource"]["reason"];

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
 * A planned table as plain data: its {@link TablePlan} less the compiled
 * source, its build target and the rollup's over-stored-base compile, which
 * hold the compiler that planned them.
 */
export type TablePlanData = DistributiveOmit<
   TablePlan,
   "source" | "target" | "overStoredBase"
> & {
   /** The planned source's name: the author's, for a `#(materialize)` table. */
   name: string;
   /** The connection the planned source builds on, by name. */
   connectionName: string;
   /**
    * A rollup a run may compile again over its base's stored table, and
    * build from that table when its manifest holds it.
    */
   overStoredBaseCandidate?: true;
};

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
   ? Omit<T, K>
   : never;

/**
 * Which sources may be served from a `storage=` table, decided where the
 * compiled sources exist: binding a host's manifest skips the recompile, so
 * the serve side cannot decide it again.
 *
 * Carries the ELIGIBLE sources so the serve side requires a positive answer.
 * Absence is not eligibility: a `#@ persist` filtered pass-through (the
 * row-level-access shape) never reaches this record, and a refusal-only
 * record would read that silence as consent.
 *
 * Keyed by `sourceID`, never by name (the source map's keying rule): two
 * models may declare same-named sources, and a rollup is recorded under its
 * own `sourceID` rather than its generated name.
 */
export interface SourceEligibility {
   /** sourceIDs compiled, examined, and found eligible. */
   eligible: string[];
   /** sourceID -> why it was refused; for the operator-facing log. */
   refused: Record<string, string>;
   /**
    * The eligible sourceIDs admitted past their own row-level gate
    * (`gateEnforcedAtRead`), whose bindings the router lets a served answer
    * read past that gate. Absent, as in a record made before it: none.
    */
   gateKept?: string[];
   /**
    * The eligible sourceIDs whose tables hold only ungated rows: no gate
    * anywhere beneath the source, its joins and the sources it derives from
    * included (`holdsOnlyUngatedRows`). A `storage=` table every source on
    * whose address is listed here is read past a gated entry point's gate, as
    * a database table is. Absent, as in a record made before it: none.
    */
   ungated?: string[];
}

/**
 * Which sources may be served from a DATABASE table, decided where the
 * compiled sources exist, as {@link SourceEligibility} is.
 *
 * Keyed by `sourceEntityId`, not name: a database binding carries only the
 * id, and two models may declare same-named sources, so a name key would let
 * one's eligibility authorize the other.
 *
 * Two DIFFERENT sources can share an id (identical SQL, different gates,
 * since the build SQL excludes annotations). Refusal therefore dominates: one
 * refusal retracts the id for good, whatever the iteration order.
 */
export interface DatabaseSourceEligibility {
   /** sourceEntityId of database sources examined and found eligible. */
   eligibleEntityIds: Set<string>;
   /** sourceEntityId -> why it was refused; for the operator-facing log. */
   refused: Record<string, string>;
}

/** What a refused rollup's warning names: its base and its grain. */
export interface RefusedRollup {
   baseSourceName: string;
   grainDimensions: string[];
}

/**
 * The warning code and subject of each refusal a finding made, by its
 * `refusedSources` key: the record says why, the notice says how the
 * package warning names it.
 */
export type RefusalNotices = Record<string, { code: string; subject?: string }>;

/**
 * A `#@ persist name=` the raw model text holds that its DDL cannot take (not
 * quoted, or not a plain identifier path): one load-gate line, and the unsafe
 * value when there is one, whose own line on the plan the raw line replaces.
 */
export interface PersistNameProblem {
   line: string;
   unsafeValue?: string;
}

/**
 * The load-gate lines each model's own text and annotations make, outside
 * any planned table: invalid `#@ preaggregate` declarations, misspelled
 * materialization routes, and `#@ persist name=` values the raw text refuses.
 */
export interface ModelGateLines {
   preaggregate: string[];
   nearMiss: string[];
   nameProblems: PersistNameProblem[];
}

/** A persisted view's generated build source, named as its author wrote it. */
export interface PersistedViewName {
   sourceName: string;
   viewName: string;
}

/**
 * A package's build plan as plain data. `Finding` is the planner's
 * annotation finding (`PackageAnnotationFinding`).
 */
export interface PackagePlan<Finding = unknown> {
   /**
    * The wire projection (`deriveBuildPlan` in `plan/`); null when the
    * package declares nothing to build and lost nothing to a failure.
    */
   plan: BuildPlan | null;
   /** The table planned for each planned source, by sourceID. */
   tables: Record<string, TablePlanData>;
   /** Each connection's build order. */
   builds: SourceBuild[];
   connectionDigests: Record<string, string>;
   /** Each connection's configured type, by name (see `CompiledBuildPlan`). */
   connectionTypes: Record<string, string>;
   /** The connections configured `managed` (see `CompiledBuildPlan`). */
   managedConnections: string[];
   /**
    * Every connection a persist source builds on, including sources the plan
    * skipped (a connection that did not resolve) or refused, so a connection
    * edit that creates or fixes one re-derives the plan.
    */
   buildConnectionNames: string[];
   /** The package names its tables per version (`scope: "version"`). */
   versionScoped: boolean;
   /** What each moved source's address folds in, by sourceID. */
   tableFolds: Record<string, string>;
   /** Each planned source's content address, or why it has none, by sourceID. */
   addresses: Record<string, string | { unaddressable: string }>;
   /**
    * The key Malloy files each table under (its target's `buildId`), by
    * content address, where the two differ.
    */
   compilerKeys: Record<string, string>;
   annotationFindings: Finding[];
   droppedPersistSources: { name: string; modelPath: string }[];
   modelGateLines: ModelGateLines;
   /** Each persisted view's build source, by its sourceID. */
   persistedViews: Record<string, PersistedViewName>;
   refusalNotices: RefusalNotices;
   /** The base and grain of each rollup in `plan.refusedSources`, by sourceID. */
   refusedRollups: Record<string, RefusedRollup>;
   sourceEligibility: SourceEligibility;
   databaseSourceEligibility: DatabaseSourceEligibility;
   /**
    * The rollups over a gated base whose gates are grafted on their variant
    * at read, by sourceID, so the serve verdict does not call them gated out.
    */
   rollupsKeepingGates: string[];
   incrementalDeclarations: Record<string, IncrementalDeclaration>;
   /** The `#(materialize)` plans, by the author's sourceID. */
   materializePlans: Record<string, MaterializePlan>;
   /**
    * The planned tables stored as of their build (`storedAsOfBuild`: a v1
    * `#@ persist`, persisted view or query whose build reads the current
    * time), by sourceID: each one's name, model and where it reads the clock.
    */
   asOfBuild?: Record<string, AsOfBuildSource>;
   /** See `CompiledBuildPlan.accessReads`. */
   accessReads?: readonly string[];
   /**
    * The declarations this derivation refused for eligibility, counted by
    * reason, for the caller to record: deriving a plan counts nothing.
    */
   eligibilityRefused: EligibilityRefusals;
}

/** A planned table stored as of its build: see {@link PackagePlan.asOfBuild}. */
export interface AsOfBuildSource {
   name: string;
   modelPath?: string;
   /** Where its build reads the current time, in the author's words. */
   reads: string[];
}

/** Eligibility refusals, counted by reason. */
export type EligibilityRefusals = Partial<Record<RefusedReason, number>>;

/** Count one refusal under `reason` in `counts`. */
export function countRefusal(
   counts: EligibilityRefusals,
   reason: RefusedReason,
): void {
   counts[reason] = (counts[reason] ?? 0) + 1;
}

/** No model gate lines. */
export function noModelGateLines(): ModelGateLines {
   return { preaggregate: [], nearMiss: [], nameProblems: [] };
}
