// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The query metadata a build's statements and a run's own statements carry:
 * a connection's two metadata layers under the run's context. Each
 * warehouse's tagging grammar is its passthrough build's
 * (passthrough/bigquery.ts, passthrough/snowflake.ts).
 */

import { errorCodeOf } from "../../../error_code";
import type { components } from "../../../api";
import { logger } from "../../../logger";
import {
   mergeQueryMetadata,
   type QueryContext,
   type QueryMetadata,
} from "../../query_metadata";
import type { BuildEnvironment } from "./build_upstreams";

/**
 * What a build needs to tag its statements: the package-level metadata layer
 * and the run's context. Constant for the run; a source adds only its name.
 */
export interface BuildQueryMetadata {
   packageMaterialization:
      | components["schemas"]["PackageMaterializationConfig"]
      | null;
   context: QueryContext;
}

/**
 * A connection's two metadata layers: the overridable default and the
 * deployment-enforced properties. Fails open: an unreadable connection costs
 * the tags, never the statement.
 */
export function connectionMetadataLayers(
   environment: BuildEnvironment,
   connectionName: string,
): { default: QueryMetadata | null; enforced: QueryMetadata | null } {
   try {
      const connection = environment.getApiConnection(connectionName);
      return {
         default: connection?.queryMetadata ?? null,
         enforced: connection?.queryMetadataEnforced ?? null,
      };
   } catch (error) {
      // Logged because the lost enforced layer has no metric behind it.
      logger.debug("No query-metadata layers for connection", {
         connectionName,
         code: errorCodeOf(error),
      });
      return { default: null, enforced: null };
   }
}

/**
 * The query metadata a run's own statement on `connectionName` carries (an
 * ownership probe, a mark, a drop): the connection's layers under the run's
 * context. Fails open, as a build's does.
 */
export function statementRunOptions(
   environment: BuildEnvironment,
   connectionName: string,
   context: QueryContext,
): { queryMetadata?: QueryMetadata } {
   const layers = connectionMetadataLayers(environment, connectionName);
   const resolved = mergeQueryMetadata({
      connection: layers.default,
      enforced: layers.enforced,
      context,
   });
   return resolved.metadata ? { queryMetadata: resolved.metadata } : {};
}
