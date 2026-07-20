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

/**
 * Build a fake Malloy `Tag` supporting both readers the build plan uses:
 * `entries()` (scalar `#@ persist` key=value pairs, for deriveAnnotationFields)
 * and the path-based `text(...at)` (dotted `freshness.window`, for
 * resolveFreshness).
 */
function fakeTag(
   fields: Record<string, string> | undefined,
   fs: FakeFreshnessSchedule | undefined,
) {
   return {
      *entries() {
         for (const [key, value] of Object.entries(fields ?? {})) {
            yield [key, { text: () => value }];
         }
      },
      text(...at: string[]): string | undefined {
         if (at.length === 1) {
            return fields?.[at[0]];
         }
         if (at.length === 2 && at[0] === "freshness") {
            return fs?.freshness?.[at[1] as "window" | "fallback"];
         }
         return undefined;
      },
   };
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
   /** Model-file-level (`##`) freshness default. */
   modelFreshnessSchedule?: FakeFreshnessSchedule;
   /**
    * Spy on the args Malloy's SQL generation is handed — chiefly the
    * `buildManifest` that resolves upstream persist references — so a test can
    * assert what physical name a downstream build sees for its upstream.
    */
   onGetSQL?: (sqlOpts: unknown) => void;
}): PersistSource {
   const fields = opts.annotationFields;
   return {
      name: opts.name,
      sourceID: opts.name,
      connectionName: opts.connectionName ?? "duckdb",
      dialectName: opts.dialectName ?? "duckdb",
      makeBuildId: () => opts.sourceEntityId,
      getSQL: (sqlOpts?: unknown) => {
         opts.onGetSQL?.(sqlOpts);
         return opts.sql ?? "SELECT 1";
      },
      annotations: {
         parseAsTag: () => ({
            tag: fakeTag(fields, opts.freshnessSchedule),
         }),
      },
      modelAnnotations: {
         parseAsTag: () => ({
            tag: fakeTag(undefined, opts.modelFreshnessSchedule),
         }),
      },
   } as unknown as PersistSource;
}

/**
 * Assemble a {@link CompiledBuildPlan} from a sources map and dependency
 * levels (one connection, "duckdb"). `connections` is supplied by the caller
 * because build vs. plan-derivation tests need different connection mocks.
 */
export function compiledWith(
   sources: Record<string, PersistSource>,
   levels: string[][],
   connections: CompiledBuildPlan["connections"] = new Map(),
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
   };
}
