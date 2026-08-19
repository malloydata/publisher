import type { PersistSource } from "@malloydata/malloy";
import {
   BuildInstruction,
   BuildPlan,
   Materialization,
} from "../storage/DatabaseInterface";
import { CompiledBuildPlan } from "./build_plan";

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
   onGetSQL?: (sqlOpts: unknown) => void;
   /**
    * The compiled `_sourceDef` the materialization-eligibility gate walks
    * (parameters/givens). Defaults to an empty def (eligible); the gate only
    * runs for `storage=` sources, so colocated fakes never touch it.
    */
   sourceDef?: unknown;
   /**
    * The source's compiled output columns, which deriveColumns reads off
    * `_explore.intrinsicFields`. An incremental delta names these in its DML, so
    * a test exercising that path has to declare them.
    */
   columns?: string[];
}): PersistSource {
   const fields = opts.annotationFields;
   return {
      name: opts.name,
      sourceID: opts.name,
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
      makeBuildId: () => opts.sourceEntityId,
      getSQL: (sqlOpts?: unknown) => {
         opts.onGetSQL?.(sqlOpts);
         return opts.sql ?? "SELECT 1";
      },
      annotations: {
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
}

/**
 * Assemble a {@link CompiledBuildPlan} from a sources map and dependency
 * levels (one connection, "duckdb"). `connections` is supplied by the caller
 * because build vs. plan-derivation tests need different connection mocks.
 * `sourceGateOutcomes` is left undefined unless a test explicitly supplies
 * one — a fixture that doesn't set it exercises the fail-closed default (no
 * compile-time gate outcome ⇒ the colocated authorize relaxation never
 * applies, same as before that outcome existed).
 */
export function compiledWith(
   sources: Record<string, PersistSource>,
   levels: string[][],
   connections: CompiledBuildPlan["connections"] = new Map(),
   sourceGateOutcomes?: CompiledBuildPlan["sourceGateOutcomes"],
): CompiledBuildPlan {
   return {
      graphs: [
         {
            connectionName: "duckdb",
            nodes: levels.map((level) =>
               level.map((sourceID) => ({ sourceID, dependsOn: [] })),
            ),
         },
      ] as unknown as CompiledBuildPlan["graphs"],
      sources,
      connectionDigests: { duckdb: "dig" },
      connections,
      sourceGateOutcomes,
   };
}
