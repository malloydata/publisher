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
   Connection as MalloyConnection,
   MalloyConfig,
} from "@malloydata/malloy";
import type { components } from "../../../api";

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
}
