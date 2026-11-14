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
    * least-specific layer for per-source resolution. Optional: callers without
    * it resolve with no package default.
    */
   getMaterializationConfig?(): WirePackageMaterialization | null;
}
