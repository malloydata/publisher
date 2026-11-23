// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a package must expose for its build plan to be compiled: its model
 * paths and root, its Malloy config and connections, and its package-level
 * materialization config.
 *
 * Implemented by `Package` and the materialization service; read by the
 * planner (plan/build_plan.ts) and the host port (host.ts).
 *
 * Types only.
 */

import type {
   BoundStorageColumns,
   BoundStorageContract,
   BoundTableContract,
   PlanChainBuildText,
   PlanChainStringReads,
} from "./chain_build";
import type {
   Connection as MalloyConnection,
   MalloyConfig,
} from "@malloydata/malloy";
import type { components } from "../../../api";
import type { UpstreamSchemaReadings, UpstreamTable } from "./upstream_schemas";

type WirePackageMaterialization =
   components["schemas"]["PackageMaterializationConfig"];

/**
 * Minimal surface a package must expose to compile its build plan; satisfied by
 * {@link Package} and the materialization service.
 */
export interface BuildPlanPackage {
   getModelPaths(): string[];
   getPackagePath(): string;
   getMalloyConfig(): MalloyConfig;
   getMalloyConnection(name: string): Promise<MalloyConnection>;
   /**
    * The package-level `materialization` config (publisher.json), the
    * least-specific layer for per-source resolution.
    */
   getMaterializationConfig(): WirePackageMaterialization | null;
   /**
    * The package's metadata, for its materialization `scope`: under
    * `scope: "version"` every table name takes the version's suffix, which
    * decides which sources share a table.
    */
   getPackageMetadata(): { scope?: string | null };
   /**
    * The published version this package is, if any. Only a published
    * version of a `scope: "version"` package names its tables per version;
    * an unversioned package writes an author's `name=` as given whatever its
    * scope.
    */
   getVersionId(): string | undefined;
   /**
    * The native column types of `tables` (the tables its persist sources
    * read), for the plan's addresses to fold in. `load`: as this load reads
    * them (again, under an explicit reload or publish). `run`: as the plan
    * in force read them, so a run addresses each source as its load did.
    * Absent (a spec's package), none is read and every address is its SQL's
    * alone.
    */
   readUpstreamSchemas?(
      tables: UpstreamTable[],
      use: "load" | "run",
   ): Promise<UpstreamSchemaReadings>;
   /**
    * The served copy's chain text, for a `storage=` view or query over
    * upstreams in its own destination (plan/storage_over_stored.ts). Absent
    * (a spec's package), such a view or query is refused at plan.
    */
   planChainBuildText?: PlanChainBuildText;
   /**
    * The stored columns bound for an upstream, which a plan stands in for
    * its table in place of the ones its build would write by default.
    * Absent (the load worker, a spec's package): the defaults.
    */
   boundStorageColumns?: BoundStorageColumns;
   /**
    * Reads the strings a `storage=` build over stored upstreams compares
    * (serve/chain_string_reads.ts). Absent (a spec's package), every such
    * build is refused at plan.
    */
   planChainStringReads?: PlanChainStringReads;
   /**
    * The contract bound for an upstream, which a plan judges those strings
    * against. Absent (the load worker, a spec's package): judged at build.
    */
   boundStorageContract?: BoundStorageContract;
   /**
    * The contract bound for a table at its tier, which a plan judges an
    * extension's build over a pair-table parent by. Absent (the load worker,
    * a spec's package): judged at build.
    */
   boundTableContract?: BoundTableContract;
}
