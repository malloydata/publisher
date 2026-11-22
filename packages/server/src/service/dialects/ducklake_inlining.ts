// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DuckLake data inlining on the catalogs Publisher writes into. A catalog that
 * records a `data_inlining_row_limit` above zero keeps a table of that many rows
 * or fewer as rows in its catalog database, not as a file, and a read-only view
 * scoped to the catalog's files cannot keep a reader from them. A build
 * session's `ducklake_default_data_inlining_row_limit=0` does not override a
 * limit the catalog records.
 *
 * So the attach that creates a catalog records 0, and an attach of an existing
 * catalog that records more leaves it as it is (it is the host's), logs
 * {@link DUCKLAKE_DATA_INLINING_ON}, and keeps the limit here for the package
 * warning of that code.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../api";
import { logger } from "../../logger";
import { redactPgSecrets } from "../../pg_helpers";
import { errMessage } from "../../utils";
import { runInternalSQL } from "../duckdb_lockdown";
import { runSQLRows } from "../duckdb_session_setup";
import { quoteIdentifier } from "../quoting";
import {
   metadataCatalogRef,
   recordedCatalogSettings,
} from "./ducklake_catalog";

type DucklakeConnection = components["schemas"]["DucklakeConnection"];
type PostgresConnection = components["schemas"]["PostgresConnection"];

/** The log and package-warning code of a catalog that records inlining on. */
export const DUCKLAKE_DATA_INLINING_ON = "ducklake-data-inlining-on";

/**
 * The nonzero limit each attached catalog records, by
 * {@link ducklakeCatalogIdentity}; a catalog recording 0, or none, is absent.
 */
const inliningOn = new Map<string, number>();

/** The catalog database a DuckLake config names, normalized. */
export function catalogKey(
   pg: PostgresConnection | undefined,
): string | undefined {
   if (!pg) return undefined;
   if (pg.connectionString?.trim()) return pg.connectionString.trim();
   if (!pg.databaseName) return undefined;
   return `${(pg.host ?? "localhost").toLowerCase()}:${pg.port ?? 5432}/${pg.databaseName}`;
}

/** One DuckLake catalog: its catalog database and metadata schema. */
export function ducklakeCatalogIdentity(
   config: DucklakeConnection | undefined,
): string | undefined {
   const key = catalogKey(config?.catalog?.postgresConnection);
   return key === undefined
      ? undefined
      : `${key}|${config?.catalog?.metadataSchema ?? ""}`;
}

/**
 * The `data_inlining_row_limit` above zero that the catalog `config` names was
 * found recording when Publisher last attached it, or undefined.
 */
export function recordedInliningLimit(
   config: DucklakeConnection | undefined,
): number | undefined {
   const identity = ducklakeCatalogIdentity(config);
   return identity === undefined ? undefined : inliningOn.get(identity);
}

/**
 * After a DuckLake attach under `alias`: record inlining off on a catalog this
 * attach created (`created`), or note an existing catalog's nonzero limit.
 * `created` is `"unknown"` when the format preflight could not tell, and then
 * a catalog with no snapshot past its creation's counts as created. A catalog
 * create that records its own settings next (`settingsFollow`, the admin
 * API's POST /catalogs) is not judged here.
 *
 * Best effort: a failure here is logged and never fails the attach.
 */
export async function reconcileDataInlining(
   connection: DuckDBConnection,
   alias: string,
   config: DucklakeConnection,
   input: {
      created: boolean | "unknown";
      readOnly: boolean;
      settingsFollow: boolean;
      origin: string;
   },
): Promise<void> {
   const metadataSchema = config.catalog?.metadataSchema;
   const identity = ducklakeCatalogIdentity(config);
   try {
      const recorded = (
         await recordedCatalogSettings(connection, alias, metadataSchema)
      ).data_inlining_row_limit;
      if (
         !input.readOnly &&
         recorded === undefined &&
         (input.created === true ||
            (input.created === "unknown" &&
               (await onlyCreationSnapshot(connection, alias, metadataSchema))))
      ) {
         await connection.runSQL(
            `CALL ${quoteIdentifier(alias, "duckdb")}.set_option('data_inlining_row_limit', 0)`,
         );
         if (identity !== undefined) inliningOn.delete(identity);
         logger.info(
            `DuckLake catalog ${alias} created with data inlining off`,
         );
         return;
      }
      if (input.settingsFollow) return;
      const limit = recorded === undefined ? 0 : Number(recorded);
      if (identity !== undefined) {
         if (limit > 0) inliningOn.set(identity, limit);
         else inliningOn.delete(identity);
      }
      if (limit > 0) {
         logger.warn(
            `DuckLake catalog ${alias} records data_inlining_row_limit ${limit}: ` +
               `a table of ${limit} rows or fewer written into it is kept as rows ` +
               `in the catalog database rather than a file, which a read-only ` +
               `view scoped to the catalog's files does not protect. Publisher ` +
               `leaves the catalog's setting as it is; record 0 with ` +
               `CALL ${alias}.set_option('data_inlining_row_limit', 0) or the ` +
               `admin API's PATCH /catalogs/{catalog}.`,
            {
               code: DUCKLAKE_DATA_INLINING_ON,
               connection: alias,
               limit,
               origin: input.origin,
            },
         );
      }
   } catch (error) {
      logger.warn(`Could not read DuckLake catalog ${alias}'s inlining limit`, {
         connection: alias,
         error: redactPgSecrets(errMessage(error)),
      });
   }
}

/** Whether the catalog records no snapshot past the one its creation wrote. */
async function onlyCreationSnapshot(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
): Promise<boolean> {
   const row = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT max(snapshot_id) AS m FROM ${metadataCatalogRef(alias, metadataSchema)}.ducklake_snapshot`,
      ),
   )[0];
   return Number(row?.m ?? -1) === 0;
}
