// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The names a build writes besides its table's: the staging name a table is
 * built under and the backup name a served table is moved aside to, both
 * derived from its content address, and the name an auto-run gives a table
 * from its source. Pure.
 */

import type { PersistSource } from "@malloydata/malloy";
import { deriveAnnotationFields } from "./run_plan";

/**
 * Length of the sourceEntityId prefix used when synthesizing staging table
 * names. 12 hex chars is 48 bits of entropy, well inside every dialect's
 * identifier limit (Postgres is the tightest at 63).
 */
export const STAGING_ID_LEN = 12;

/** Staging suffix appended to a table name while it is being built. */
export function stagingSuffix(sourceEntityId: string): string {
   // Drop hyphens so a UUID-shaped id still yields a bare identifier fragment.
   return `_${sourceEntityId.replace(/-/g, "").substring(0, STAGING_ID_LEN)}`;
}

/**
 * Suffix of the name a served table is moved aside to while its rebuilt
 * replacement is renamed into place. The same length as {@link stagingSuffix},
 * so it fits wherever a staging name does, and never equal to it.
 */
export function backupSuffix(sourceEntityId: string): string {
   return `_o${sourceEntityId.replace(/-/g, "").substring(0, STAGING_ID_LEN - 1)}`;
}

/**
 * Physical table name the publisher self-assigns in auto-run mode: the
 * `#@ persist name=<table>` value if present, else the Malloy source name.
 * The author owns quoting the `name=` value for the dialect.
 */
export function selfAssignTableName(persistSource: PersistSource): string {
   return deriveAnnotationFields(persistSource).name || persistSource.name;
}
