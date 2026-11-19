// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   BuildTarget,
   ConnectionBuild,
   PersistSource,
} from "@malloydata/malloy";
import { Materialization } from "../storage/DatabaseInterface";
import {
   BuildInstruction,
   BuildPlan,
} from "../service/materialization/domain/types";
import type { PackagePlan } from "../service/materialization/domain/package_plan";
import {
   CompiledBuildPlan,
   setTestAddressForTarget,
   plannedSources,
} from "../service/materialization/plan/build_plan";
import type { WorkerBuildPlan } from "../package_load/protocol";
import type {
   PersistSourceGateOutcome,
   TablePlan,
} from "../service/materialization/domain/table_plans";

/**
 * Shared fixtures for the materialization unit specs (service, controller,
 * build-plan). Centralizing the builders keeps the per-source stand-ins and
 * record shapes consistent across files and avoids drift when the wire types
 * change.
 */

/** A persisted materialization record with sensible PENDING defaults. */
export function makeMaterialization(
   overrides: Partial<Materialization> = {},
): Materialization {
   return {
      id: "mat-1",
      environmentId: "env-1",
      packageName: "pkg",
      status: "PENDING",
      manifest: null,
      startedAt: null,
      completedAt: null,
      error: null,
      metadata: null,
      version: null,
      createdAt: new Date("2026-01-01"),
      updatedAt: new Date("2026-01-01"),
      ...overrides,
   };
}

/** A single-source wire BuildPlan (the `Package.buildPlan` artifact). */
export function makeBuildPlan(overrides: Partial<BuildPlan> = {}): BuildPlan {
   return {
      graphs: [
         {
            connectionName: "duckdb",
            nodes: [[{ sourceID: "orders@m.malloy", dependsOn: [] }]],
         },
      ],
      sources: {
         "orders@m.malloy": {
            name: "orders",
            sourceID: "orders@m.malloy",
            connectionName: "duckdb",
            dialect: "duckdb",
            sourceEntityId: "build-orders",
            sql: "SELECT 1",
            columns: [],
         },
      },
      ...overrides,
   };
}

/** A caller-supplied build instruction matching {@link makeBuildPlan}. */
export function makeInstruction(
   overrides: Partial<BuildInstruction> = {},
): BuildInstruction {
   return {
      sourceEntityId: "build-orders",
      materializedTableId: "mt-1",
      physicalTableName: '"orders_v1"',
      realization: "COPY",
      ...overrides,
   };
}

/**
 * A minimal stand-in for a Malloy {@link PersistSource} exposing only what the
 * build internals touch (name/id, deterministic sourceEntityId, SQL, and the
 * `#@ persist name=` annotation reader, defaulted to "unset").
 */
/** A freshness layer for the fake source's `#@` or `##` tag. */
interface FakeFreshnessSchedule {
   freshness?: { window?: string; fallback?: string };
}

/** A nested tag tree: scalar leaves and property collections. */
type FakeTagTree = { [key: string]: string | FakeTagTree };

/**
 * Build a fake Malloy `Tag` over a nested tree, supporting all three readers the
 * build plan uses: `entries()` (scalar `#@ persist` key=value pairs, for
 * deriveAnnotationFields), the path-based `text(...at)` (dotted
 * `freshness.window`, for resolveFreshness) and `tag(...at)` (a property
 * collection — `queryMetadata { … }`, or the `materialization` envelope).
 */
function fakeTagFromTree(tree: FakeTagTree) {
   const walk = (at: string[]): string | FakeTagTree | undefined => {
      let node: string | FakeTagTree | undefined = tree;
      for (const segment of at) {
         if (node === undefined || typeof node !== "object") return undefined;
         node = node[segment];
      }
      return node;
   };
   return {
      *entries(): Generator<[string, { text(): string | undefined }]> {
         for (const [key, value] of Object.entries(tree)) {
            yield [
               key,
               { text: () => (typeof value === "string" ? value : undefined) },
            ];
         }
      },
      text(...at: string[]): string | undefined {
         const node = walk(at);
         return typeof node === "string" ? node : undefined;
      },
      has(...at: string[]): boolean {
         return walk(at) !== undefined;
      },
      tag(...at: string[]) {
         const node = walk(at);
         return node !== undefined && typeof node === "object"
            ? fakeTagFromTree(node)
            : undefined;
      },
   };
}

/** Assemble one tag layer's tree from the pieces a fake source declares. */
function tagTree(pieces: {
   fields?: Record<string, string>;
   freshness?: FakeFreshnessSchedule;
   queryMetadata?: Record<string, string>;
   /** Contents of the `materialization` envelope (model-file layer). */
   materialization?: {
      freshness?: FakeFreshnessSchedule;
      queryMetadata?: Record<string, string>;
   };
}): FakeTagTree {
   const tree: FakeTagTree = { ...(pieces.fields ?? {}) };
   if (pieces.freshness?.freshness) {
      tree.freshness = { ...pieces.freshness.freshness } as FakeTagTree;
   }
   if (pieces.queryMetadata) tree.queryMetadata = { ...pieces.queryMetadata };
   if (pieces.materialization) {
      const envelope: FakeTagTree = {};
      if (pieces.materialization.freshness?.freshness) {
         envelope.freshness = {
            ...pieces.materialization.freshness.freshness,
         } as FakeTagTree;
      }
      if (pieces.materialization.queryMetadata) {
         envelope.queryMetadata = { ...pieces.materialization.queryMetadata };
      }
      tree.materialization = envelope;
   }
   return tree;
}

export function fakeSource(opts: {
   name: string;
   sourceEntityId: string;
   sql?: string;
   connectionName?: string;
   dialectName?: string;
   /** key=value fields of the `#@ persist` annotation (e.g. name, refresh). */
   annotationFields?: Record<string, string>;
   /** Source-level (`#@`) freshness (dotted keys). */
   freshnessSchedule?: FakeFreshnessSchedule;
   /** Model-file-level (`##`) freshness default, in the deprecated bare form. */
   modelFreshnessSchedule?: FakeFreshnessSchedule;
   /** Model-file-level (`## materialization.*`) envelope declarations. */
   modelMaterialization?: {
      freshness?: FakeFreshnessSchedule;
      queryMetadata?: Record<string, string>;
   };
   /** Source-level (`#@ persist queryMetadata.*`) per-query metadata. */
   queryMetadata?: Record<string, string>;
   /** Model-file-level bare (`## queryMetadata.*`) per-query metadata. */
   modelQueryMetadata?: Record<string, string>;
   /**
    * Spy on the args Malloy's SQL generation is handed — chiefly the
    * `buildManifest` that resolves upstream persist references — so a test can
    * assert what physical name a downstream build sees for its upstream.
    */
   onGetSQL?: (sqlOpts: unknown) => string | void;
   /**
    * The compiled `_sourceDef` the materialization-eligibility gate walks
    * (parameters/givens). Defaults to an empty def (eligible); the gate only
    * runs for `storage=` sources, so database fakes never touch it.
    */
   sourceDef?: unknown;
   /**
    * The source's compiled output columns, which deriveColumns reads off
    * `_explore.intrinsicFields`. An incremental delta names these in its DML, so
    * a test exercising that path has to declare them.
    */
   columns?: string[];
   /**
    * The compiled model's `_modelDef`, for the walk that finds a source's
    * stored upstreams (`reachPersisted` in plan/source_map). Absent, the walk finds
    * nothing, as for a source with no persisted upstream.
    */
   modelDef?: unknown;
}): PersistSource {
   const fields = opts.annotationFields;
   const sql = opts.sql ?? "SELECT 1";
   const source = {
      name: opts.name,
      sourceID: opts.name,
      ...(opts.modelDef !== undefined
         ? { _model: { _modelDef: opts.modelDef } }
         : {}),
      connectionName: opts.connectionName ?? "duckdb",
      dialectName: opts.dialectName ?? "duckdb",
      _sourceDef: opts.sourceDef ?? {},
      _explore: {
         intrinsicFields: (opts.columns ?? []).map((name) => ({
            name,
            type: "string",
            isAtomicField: () => true,
         })),
      },
      annotations: {
         // No notes, so `parseRouteTag` drops nothing and calls `parseAsTag`.
         forRoute: () => [],
         parseAsTag: () => ({
            tag: fakeTagFromTree(
               tagTree({
                  fields,
                  freshness: opts.freshnessSchedule,
                  queryMetadata: opts.queryMetadata,
               }),
            ),
         }),
      },
      modelAnnotations: {
         forRoute: () => [],
         parseAsTag: () => ({
            tag: fakeTagFromTree(
               tagTree({
                  freshness: opts.modelFreshnessSchedule,
                  queryMetadata: opts.modelQueryMetadata,
                  materialization: opts.modelMaterialization,
               }),
            ),
         }),
      },
   } as unknown as PersistSource;
   // The build target the source maps onto, recorded the way a real plan
   // records it (plannedSources), keyed by the test's chosen address; its
   // build SQL reports the manifest it was asked for, as getSQL() once did.
   const target = {
      buildId: opts.sourceEntityId,
      connectionName: opts.connectionName ?? "duckdb",
      sql,
      buildSQL: (manifest: unknown) => {
         // A string return stands for the SQL Malloy renders for this
         // manifest, for a test whose assertion depends on what it reads.
         return opts.onGetSQL?.({ buildManifest: manifest }) ?? sql;
      },
      readsGivens: false,
      dependsOn: [],
      sources: [source],
   } as unknown as BuildTarget;
   setTestAddressForTarget(target, opts.sourceEntityId);
   plannedSources({
      connections: [
         { connectionName: target.connectionName, targets: [target] },
      ] as unknown as ConnectionBuild[],
      unbuildable: [],
   });
   return source;
}

/**
 * Assemble a {@link CompiledBuildPlan} from a sources map and dependency
 * levels (one connection, "duckdb"), each source a `persist` table.
 * `connections` is supplied by the caller because build vs. plan-derivation
 * tests need different connection mocks. A source's gate is left undefined
 * unless a test explicitly supplies one — a fixture that doesn't set it
 * exercises the fail-closed default (no compile-time gate outcome ⇒ the
 * database authorize relaxation never applies, same as before that outcome
 * existed).
 */
export function compiledWith(
   sources: Record<string, PersistSource>,
   levels: string[][],
   connections: CompiledBuildPlan["connections"] = new Map(),
   gates?: Record<string, PersistSourceGateOutcome>,
): CompiledBuildPlan {
   return {
      builds: [
         {
            connectionName: "duckdb",
            order: levels.flat(),
            dependsOn: {},
         },
      ],
      tables: persistTables(sources, gates),
      connectionDigests: { duckdb: "dig" },
      connections,
   };
}

/** Each source as a planned `persist` table, with its gate when `gates` has one. */
export function persistTables(
   sources: Record<string, PersistSource>,
   gates?: Record<string, PersistSourceGateOutcome>,
): Record<string, TablePlan> {
   return Object.fromEntries(
      Object.entries(sources).map(([sourceID, source]) => [
         sourceID,
         {
            kind: "persist",
            source,
            ...(gates?.[sourceID] ? { gate: gates[sourceID] } : {}),
         },
      ]),
   );
}

/** A plan's tables, as the per-field records specs read them by sourceID. */
type Planned = { tables: Readonly<Record<string, TablePlan>> };

function fieldIn<K extends keyof TablePlan>(
   plan: Planned,
   key: K,
): Record<string, NonNullable<TablePlan[K]>> {
   const out: Record<string, NonNullable<TablePlan[K]>> = {};
   for (const [sourceID, table] of Object.entries(plan.tables)) {
      const value = table[key];
      if (value !== undefined && value !== null) {
         out[sourceID] = value as NonNullable<TablePlan[K]>;
      }
   }
   return out;
}

/** Each planned table's source, by sourceID. */
export function sourcesIn(plan: Planned): Record<string, PersistSource> {
   return fieldIn(plan, "source");
}

/** Each planned table's build target, by sourceID (an unkeyable one has none). */
export function targetsIn(plan: Planned): Record<string, BuildTarget> {
   return fieldIn(plan, "target");
}

/** Each `materialize` or `pair` table's `#(materialize)` plan, by sourceID. */
export function materializePlansIn(
   plan: Planned,
): Record<string, NonNullable<TablePlan["materialize"]>> {
   return fieldIn(plan, "materialize");
}

/** Each `rollup` table's rollup, by sourceID. */
export function rollupPlansIn(
   plan: Planned,
): Record<string, NonNullable<TablePlan["rollup"]>> {
   return fieldIn(plan, "rollup");
}

/** Each planned table's declaring model, by sourceID. */
export function modelPathsIn(plan: Planned): Record<string, string> {
   return fieldIn(plan, "modelPath");
}

/** Each planned table's classified gate, by sourceID. */
export function gatesIn(
   plan: Planned,
): Record<string, PersistSourceGateOutcome> {
   return fieldIn(plan, "gate");
}

/** Each planned table's stored-table key check, by sourceID. */
export function storedKeyChecksIn(plan: Planned): Record<string, string> {
   return fieldIn(plan, "storedKeyCheck");
}

/**
 * A two-source wire plan over two connections (`conn_a`, `conn_b`), for the
 * specs that need sources on more than one connection: `a@m.malloy` at
 * address `addr-a` on `conn_a`, and `b@m.malloy` at `addr-b` on `conn_b`.
 */
export function makeTwoConnectionPlan(): BuildPlan {
   const source = (name: string, connectionName: string) => ({
      name,
      sourceID: `${name}@m.malloy`,
      connectionName,
      dialect: "duckdb",
      sourceEntityId: `addr-${name}`,
      sql: "SELECT 1",
      columns: [],
   });
   return makeBuildPlan({
      graphs: [],
      sources: {
         "a@m.malloy": source("a", "conn_a"),
         "b@m.malloy": source("b", "conn_b"),
      },
   });
}

/**
 * A package plan as the load worker hands it over, every side table empty
 * unless given: what `PlanLifecycle.load` adopts.
 */
export function makePlanOutcome(
   overrides: Partial<PackagePlan<never>> = {},
): PackagePlan<never> {
   return {
      plan: makeBuildPlan(),
      tables: {},
      builds: [],
      connectionDigests: {},
      connectionTypes: {},
      versionScoped: false,
      tableFolds: {},
      addresses: {},
      persistedViews: {},
      eligibilityRefused: {},
      droppedPersistSources: [],
      annotationFindings: [],
      modelGateLines: { preaggregate: [], nearMiss: [], nameProblems: [] },
      sourceEligibility: { eligible: [], refused: {} },
      databaseSourceEligibility: { eligibleEntityIds: new Set(), refused: {} },
      incrementalDeclarations: {},
      buildConnectionNames: [],
      materializePlans: {},
      rollupsKeepingGates: [],
      refusedRollups: {},
      refusalNotices: {},
      compilerKeys: {},
      ...overrides,
   };
}

/** The load worker's plan, derived. */
export function okWorkerPlan(
   packagePlan: PackagePlan<never> = makePlanOutcome(),
): WorkerBuildPlan {
   return {
      ok: true,
      packagePlan,
      durationMs: 1,
      digestSkipped: [],
   };
}

/** The load worker's plan, failed in the compute. */
export function failedWorkerPlan(
   error = "the plan compute failed",
): WorkerBuildPlan {
   return {
      ok: false,
      error,
      durationMs: 1,
      digestSkipped: [],
      eligibilityRefused: {},
   };
}
