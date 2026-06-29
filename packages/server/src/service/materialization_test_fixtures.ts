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
            buildId: "build-orders",
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
      buildId: "build-orders",
      materializedTableId: "mt-1",
      physicalTableName: '"orders_v1"',
      realization: "COPY",
      ...overrides,
   };
}

/**
 * A minimal stand-in for a Malloy {@link PersistSource} exposing only what the
 * build internals touch (name/id, deterministic buildId, SQL, and the
 * `#@ persist name=` annotation reader, defaulted to "unset").
 */
export function fakeSource(opts: {
   name: string;
   buildId: string;
   sql?: string;
   connectionName?: string;
   dialectName?: string;
}): PersistSource {
   return {
      name: opts.name,
      sourceID: opts.name,
      connectionName: opts.connectionName ?? "duckdb",
      dialectName: opts.dialectName ?? "duckdb",
      makeBuildId: () => opts.buildId,
      getSQL: () => opts.sql ?? "SELECT 1",
      annotations: {
         parseAsTag: () => ({ tag: { text: () => undefined } }),
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
