// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The real warehouses the warehouse conformance variant runs against
 * (tests/component/r4/warehouse_conformance.component.spec.ts), each found
 * from its credentials in the environment, and the per-run scratch area each
 * run builds in and always drops.
 *
 * - BigQuery: `GOOGLE_APPLICATION_CREDENTIALS` (a service-account key file)
 *   and `BIGQUERY_TEST_PROJECT_ID`, as connection-integration-tests.yml sets
 *   them from `BQ_PRESTO_TRINO_KEY`. Optional: `WAREHOUSE_CONFORMANCE_BQ_LOCATION`
 *   (default `US`) and `WAREHOUSE_CONFORMANCE_BQ_DATASET`, an existing dataset
 *   to fall back to, with run-prefixed tables, when the key cannot create one.
 * - Snowflake: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`,
 *   `SNOWFLAKE_DATABASE`, and `SNOWFLAKE_PRIVATE_KEY` (or
 *   `SNOWFLAKE_PRIVATE_KEY_RAW`, as Malloy's CI names it) or
 *   `SNOWFLAKE_PASSWORD`; optional `SNOWFLAKE_ROLE`,
 *   `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, and `SNOWFLAKE_SCHEMA`, the schema to
 *   fall back to, with run-prefixed tables, when the role cannot create one.
 * - Postgres, a stand-in that proves the harness itself on a laptop:
 *   `WAREHOUSE_CONFORMANCE_POSTGRES_URL`. The nightly job does not run it;
 *   the Postgres-gated suites in connection-integration-tests.yml cover
 *   Postgres.
 *
 * Every scratch area is named `pub_whc_<UTC yyyymmddhhmm>_<run>` (upper-cased
 * on Snowflake), so the sweeper can drop one a crashed run left behind from
 * its name alone, once it is older than {@link SWEEP_AFTER_HOURS}.
 */

import type { Connection } from "@malloydata/malloy";
import * as crypto from "crypto";
import * as fs from "fs";
import type { components } from "../../../../../src/api";

type ApiConnection = components["schemas"]["Connection"];

export type Dialect = "bigquery" | "snowflake" | "postgres";

/** Scratch areas (or table prefixes) start with this, then a UTC stamp. */
export const SCRATCH_PREFIX = "pub_whc_";

/** A scratch area older than this is a crashed run's, and the sweeper drops it. */
export const SWEEP_AFTER_HOURS = 24;

/**
 * BigQuery's cap on every job the native driver runs (live queries, colocated
 * builds, the fixture's DDL): 1 GiB. The fixture is a few kilobytes, so a job
 * that would bill more is a bug, and BigQuery refuses it rather than bill it.
 */
export const BIGQUERY_MAXIMUM_BYTES_BILLED = String(1024 ** 3);

/** One warehouse, configured from the environment. */
export interface WarehouseTarget {
   dialect: Dialect;
   /** The environment connection, as a publisher.config.json names it. */
   connection(): ApiConnection;
}

/** Why each warehouse was not configured, for the skip message. */
export interface TargetDiscovery {
   targets: WarehouseTarget[];
   skipped: { dialect: Dialect; reason: string }[];
}

/** The connection name every warehouse fixture's model reads. */
export const WAREHOUSE_CONNECTION = "wh";

function missing(names: string[]): string[] {
   return names.filter((n) => !process.env[n]);
}

/**
 * A Snowflake private key as the secret holds it: a PEM (multi-line or
 * single-line), a PEM base64-encoded whole, or the bare base64 body of a
 * PKCS#8 key. Returned as a PEM; Publisher normalizes it further.
 */
export function snowflakePrivateKeyPem(raw: string): string {
   const text = raw.trim();
   if (text.includes("-----BEGIN")) return text;
   const decoded = Buffer.from(text, "base64").toString("utf8");
   if (decoded.includes("-----BEGIN")) return decoded.trim();
   const body = text.replace(/\s+/g, "");
   const lines = body.match(/.{1,64}/g) ?? [];
   return `-----BEGIN PRIVATE KEY-----\n${lines.join("\n")}\n-----END PRIVATE KEY-----\n`;
}

/** The warehouses whose credentials are present, and why the others are not. */
export function discoverTargets(): TargetDiscovery {
   const targets: WarehouseTarget[] = [];
   const skipped: TargetDiscovery["skipped"] = [];

   const bqMissing = missing([
      "GOOGLE_APPLICATION_CREDENTIALS",
      "BIGQUERY_TEST_PROJECT_ID",
   ]);
   if (bqMissing.length === 0) {
      targets.push({
         dialect: "bigquery",
         connection: () => ({
            name: WAREHOUSE_CONNECTION,
            type: "bigquery",
            bigqueryConnection: {
               serviceAccountKeyJson: fs.readFileSync(
                  process.env.GOOGLE_APPLICATION_CREDENTIALS!,
                  "utf8",
               ),
               defaultProjectId: process.env.BIGQUERY_TEST_PROJECT_ID!,
               location: bigQueryLocation(),
               maximumBytesBilled: BIGQUERY_MAXIMUM_BYTES_BILLED,
            },
         }),
      });
   } else {
      skipped.push({
         dialect: "bigquery",
         reason: `no BigQuery credentials (${bqMissing.join(", ")} unset)`,
      });
   }

   const key =
      process.env.SNOWFLAKE_PRIVATE_KEY ??
      process.env.SNOWFLAKE_PRIVATE_KEY_RAW;
   const sfMissing = [
      ...missing([
         "SNOWFLAKE_ACCOUNT",
         "SNOWFLAKE_USER",
         "SNOWFLAKE_WAREHOUSE",
         "SNOWFLAKE_DATABASE",
      ]),
      ...(key || process.env.SNOWFLAKE_PASSWORD
         ? []
         : ["SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PASSWORD"]),
   ];
   if (sfMissing.length === 0) {
      targets.push({
         dialect: "snowflake",
         connection: () => ({
            name: WAREHOUSE_CONNECTION,
            type: "snowflake",
            snowflakeConnection: {
               account: process.env.SNOWFLAKE_ACCOUNT!,
               username: process.env.SNOWFLAKE_USER!,
               ...(key
                  ? {
                       privateKey: snowflakePrivateKeyPem(key),
                       ...(process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
                          ? {
                               privateKeyPass:
                                  process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
                            }
                          : {}),
                    }
                  : { password: process.env.SNOWFLAKE_PASSWORD! }),
               warehouse: process.env.SNOWFLAKE_WAREHOUSE!,
               database: process.env.SNOWFLAKE_DATABASE!,
               ...(process.env.SNOWFLAKE_ROLE
                  ? { role: process.env.SNOWFLAKE_ROLE }
                  : {}),
               // A runaway statement on the shared warehouse fails instead.
               responseTimeoutMilliseconds: 300_000,
            },
         }),
      });
   } else {
      skipped.push({
         dialect: "snowflake",
         reason: `no Snowflake credentials (${sfMissing.join(", ")} unset)`,
      });
   }

   const pgUrl = process.env.WAREHOUSE_CONFORMANCE_POSTGRES_URL;
   if (pgUrl) {
      const u = new URL(pgUrl);
      targets.push({
         dialect: "postgres",
         connection: () => ({
            name: WAREHOUSE_CONNECTION,
            type: "postgres",
            postgresConnection: {
               host: u.hostname,
               port: Number(u.port || 5432),
               databaseName: u.pathname.replace(/^\//, ""),
               userName: decodeURIComponent(u.username),
               password: decodeURIComponent(u.password),
            },
         }),
      });
   }

   // Narrowed by name, so a job can run one dialect.
   const only = process.env.WAREHOUSE_CONFORMANCE_DIALECTS?.split(",")
      .map((d) => d.trim())
      .filter(Boolean);
   if (only?.length) {
      for (const t of targets.filter((t) => !only.includes(t.dialect))) {
         skipped.push({
            dialect: t.dialect,
            reason: `not in WAREHOUSE_CONFORMANCE_DIALECTS=${only.join(",")}`,
         });
      }
      return {
         targets: targets.filter((t) => only.includes(t.dialect)),
         skipped,
      };
   }
   return { targets, skipped };
}

function bigQueryLocation(): string {
   return process.env.WAREHOUSE_CONFORMANCE_BQ_LOCATION || "US";
}

/** `yyyymmddhhmm` in UTC. */
function stamp(at: Date): string {
   return at.toISOString().replace(/[-:T]/g, "").slice(0, 12);
}

/**
 * This run's scratch name: the CI run's id when there is one (so the
 * workflow's cleanup step can name it), else random.
 */
export function scratchName(at = new Date()): string {
   const run = process.env.WAREHOUSE_CONFORMANCE_RUN_ID
      ? runIdPart(process.env.WAREHOUSE_CONFORMANCE_RUN_ID)
      : crypto.randomBytes(4).toString("hex");
   return `${SCRATCH_PREFIX}${stamp(at)}_${run}`;
}

/** A run id as a scratch name carries it: lower-case letters, digits and `x`. */
function runIdPart(runId: string): string {
   return runId.toLowerCase().replace(/[^a-z0-9]/g, "x");
}

/** Whether `name` (a scratch area, or a prefixed table) is the run `runId`'s. */
function isRuns(name: string, runId: string): boolean {
   return new RegExp(
      `^${SCRATCH_PREFIX}\\d{12}_${runIdPart(runId)}(_|$)`,
      "i",
   ).test(name);
}

/** When a scratch name says it was made, or undefined when it is not one. */
export function scratchCreatedAt(name: string): Date | undefined {
   const m = new RegExp(`^${SCRATCH_PREFIX}(\\d{12})_`, "i").exec(name);
   if (!m) return undefined;
   const s = m[1];
   const at = new Date(
      `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}T${s.slice(8, 10)}:${s.slice(10, 12)}:00Z`,
   );
   return Number.isNaN(at.getTime()) ? undefined : at;
}

/**
 * Where a run's tables go: its own schema or dataset (`container`, no
 * `prefix`), or, when the credentials cannot create one, run-prefixed tables
 * in an existing one.
 */
export interface Scratch {
   dialect: Dialect;
   /** The schema or dataset, as a model's `name=` and a table path write it. */
   container: string;
   /** Prepended to every table this run creates; empty in a container of its own. */
   prefix: string;
   /** True when the container is this run's, and dropping it drops everything. */
   ownsContainer: boolean;
   /** A table in the scratch area, as a model's `name=` writes it. */
   name(table: string): string;
   /** A fixture table's path, as `wh.table('…')` takes it. */
   tablePath(table: string): string;
}

const run = (c: Connection, sql: string) => c.runSQL(sql, { rowLimit: 10_000 });

function rowsOf(result: unknown): Record<string, unknown>[] {
   return ((result as { rows?: unknown[] }).rows ?? []) as Record<
      string,
      unknown
   >[];
}

/** Snowflake folds an unquoted name to upper case; the scratch is created unquoted. */
function containerCase(dialect: Dialect, name: string): string {
   return dialect === "snowflake" ? name.toUpperCase() : name;
}

function makeScratch(
   dialect: Dialect,
   container: string,
   prefix: string,
   ownsContainer: boolean,
): Scratch {
   const project = process.env.BIGQUERY_TEST_PROJECT_ID;
   const database = process.env.SNOWFLAKE_DATABASE?.toUpperCase();
   return {
      dialect,
      container,
      prefix,
      ownsContainer,
      name: (table) => `${container}.${prefix}${table}`,
      tablePath: (table) =>
         dialect === "bigquery"
            ? `${project}.${container}.${prefix}${table}`
            : dialect === "snowflake"
              ? `${database}.${container}.${(prefix + table).toUpperCase()}`
              : `${container}.${prefix}${table}`,
   };
}

/** A permission refusal, which the fallback answers, as opposed to any other failure. */
function isPermissionError(error: unknown): boolean {
   const text = error instanceof Error ? error.message : String(error);
   return /permission|denied|not authorized|insufficient privileges|access/i.test(
      text,
   );
}

/**
 * Create this run's scratch area: a schema or dataset of its own, or, when
 * the credentials may not create one and a fallback is configured, a prefix
 * in the fallback. Throws, naming the grant, when neither is possible.
 */
export async function createScratch(
   dialect: Dialect,
   connection: Connection,
   name = scratchName(),
): Promise<Scratch> {
   const container = containerCase(dialect, name);
   try {
      switch (dialect) {
         case "bigquery":
            // A table the drop missed expires on its own after a day.
            await run(
               connection,
               `CREATE SCHEMA \`${process.env.BIGQUERY_TEST_PROJECT_ID}.${container}\` ` +
                  `OPTIONS(location="${bigQueryLocation()}", default_table_expiration_days=1, ` +
                  `labels=[("purpose", "publisher-warehouse-conformance")])`,
            );
            break;
         case "snowflake":
            await run(
               connection,
               `CREATE SCHEMA ${process.env.SNOWFLAKE_DATABASE}.${container} ` +
                  `COMMENT = 'publisher warehouse conformance scratch; dropped by the run or the sweeper'`,
            );
            break;
         case "postgres":
            await run(connection, `CREATE SCHEMA "${container}"`);
            break;
      }
      return makeScratch(dialect, container, "", true);
   } catch (error) {
      const fallback =
         dialect === "bigquery"
            ? process.env.WAREHOUSE_CONFORMANCE_BQ_DATASET
            : dialect === "snowflake"
              ? process.env.SNOWFLAKE_SCHEMA
              : undefined;
      if (!isPermissionError(error) || !fallback) {
         const grant =
            dialect === "bigquery"
               ? "bigquery.datasets.create on the project (roles/bigquery.user), or set WAREHOUSE_CONFORMANCE_BQ_DATASET to a dataset it can write tables in"
               : dialect === "snowflake"
                 ? `CREATE SCHEMA on database ${process.env.SNOWFLAKE_DATABASE}, or set SNOWFLAKE_SCHEMA to a schema it can create tables in`
                 : "CREATE on the database";
         throw new Error(
            `could not create the ${dialect} scratch area ${container}: ${
               error instanceof Error ? error.message : String(error)
            }. The run needs ${grant}.`,
         );
      }
      return makeScratch(
         dialect,
         containerCase(dialect, fallback),
         `${name}_`,
         false,
      );
   }
}

/** Drop everything a scratch area holds. Best effort per table; throws if anything is left. */
export async function dropScratch(
   connection: Connection,
   scratch: Scratch,
): Promise<void> {
   if (scratch.ownsContainer) {
      await dropContainer(connection, scratch.dialect, scratch.container);
      return;
   }
   for (const table of await listPrefixedTables(
      connection,
      scratch.dialect,
      scratch.container,
      scratch.prefix,
   )) {
      await dropTable(connection, scratch.dialect, scratch.container, table);
   }
}

async function dropContainer(
   connection: Connection,
   dialect: Dialect,
   container: string,
): Promise<void> {
   switch (dialect) {
      case "bigquery":
         await run(
            connection,
            `DROP SCHEMA IF EXISTS \`${process.env.BIGQUERY_TEST_PROJECT_ID}.${container}\` CASCADE`,
         );
         return;
      case "snowflake":
         await run(
            connection,
            `DROP SCHEMA IF EXISTS ${process.env.SNOWFLAKE_DATABASE}.${container} CASCADE`,
         );
         return;
      case "postgres":
         await run(connection, `DROP SCHEMA IF EXISTS "${container}" CASCADE`);
         return;
   }
}

async function dropTable(
   connection: Connection,
   dialect: Dialect,
   container: string,
   table: string,
): Promise<void> {
   const kind = await run(
      connection,
      dialect === "bigquery"
         ? `SELECT table_type FROM \`${process.env.BIGQUERY_TEST_PROJECT_ID}.${container}\`.INFORMATION_SCHEMA.TABLES WHERE table_name = '${table}'`
         : dialect === "snowflake"
           ? `SELECT table_type FROM ${process.env.SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '${container}' AND table_name = '${table}'`
           : `SELECT table_type FROM information_schema.tables WHERE table_schema = '${container}' AND table_name = '${table}'`,
   );
   const isView = rowsOf(kind).some((r) =>
      /view/i.test(String(r.table_type ?? r.TABLE_TYPE)),
   );
   const what = isView ? "VIEW" : "TABLE";
   const path =
      dialect === "bigquery"
         ? `\`${process.env.BIGQUERY_TEST_PROJECT_ID}.${container}.${table}\``
         : dialect === "snowflake"
           ? `${process.env.SNOWFLAKE_DATABASE}.${container}."${table}"`
           : `"${container}"."${table}"`;
   await run(connection, `DROP ${what} IF EXISTS ${path}`);
}

async function listPrefixedTables(
   connection: Connection,
   dialect: Dialect,
   container: string,
   prefix: string,
): Promise<string[]> {
   const like = prefix.replace(/_/g, "\\_");
   const result = await run(
      connection,
      dialect === "bigquery"
         ? `SELECT table_name FROM \`${process.env.BIGQUERY_TEST_PROJECT_ID}.${container}\`.INFORMATION_SCHEMA.TABLES WHERE STARTS_WITH(LOWER(table_name), '${prefix.toLowerCase()}')`
         : dialect === "snowflake"
           ? `SELECT table_name FROM ${process.env.SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '${container}' AND table_name ILIKE '${like}%'`
           : `SELECT table_name FROM information_schema.tables WHERE table_schema = '${container}' AND table_name ILIKE '${like}%'`,
   );
   return rowsOf(result).map((r) => String(r.table_name ?? r.TABLE_NAME));
}

async function listScratchContainers(
   connection: Connection,
   dialect: Dialect,
): Promise<string[]> {
   const result = await run(
      connection,
      dialect === "bigquery"
         ? `SELECT schema_name FROM \`${process.env.BIGQUERY_TEST_PROJECT_ID}\`.\`region-${bigQueryLocation().toLowerCase()}\`.INFORMATION_SCHEMA.SCHEMATA WHERE STARTS_WITH(LOWER(schema_name), '${SCRATCH_PREFIX}')`
         : dialect === "snowflake"
           ? `SELECT schema_name FROM ${process.env.SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name ILIKE '${SCRATCH_PREFIX.replace(/_/g, "\\_")}%'`
           : `SELECT schema_name FROM information_schema.schemata WHERE schema_name ILIKE '${SCRATCH_PREFIX.replace(/_/g, "\\_")}%'`,
   );
   return rowsOf(result).map((r) => String(r.schema_name ?? r.SCHEMA_NAME));
}

/**
 * Drop the scratch areas (and, in a fallback container, the prefixed tables)
 * a run left behind: every one older than `olderThanHours`, or, with
 * `runId`, the ones that run made, whatever their age (the workflow's
 * cleanup step, after a cancelled or timed-out run). Returns what it
 * dropped. One failed drop does not stop the rest; the failures are
 * returned too.
 */
export async function sweepScratch(
   connection: Connection,
   dialect: Dialect,
   options: { olderThanHours?: number; runId?: string } = {},
): Promise<{ dropped: string[]; failed: string[] }> {
   const olderThan = options.olderThanHours ?? SWEEP_AFTER_HOURS;
   const cutoff = Date.now() - olderThan * 3_600_000;
   const due = (name: string) => {
      if (options.runId) return isRuns(name, options.runId);
      const at = scratchCreatedAt(name);
      return at !== undefined && at.getTime() < cutoff;
   };
   const dropped: string[] = [];
   const failed: string[] = [];
   for (const name of await listScratchContainers(connection, dialect)) {
      if (!due(name)) continue;
      try {
         await dropContainer(connection, dialect, name);
         dropped.push(name);
      } catch (error) {
         failed.push(`${name}: ${String(error)}`);
      }
   }
   const fallback =
      dialect === "bigquery"
         ? process.env.WAREHOUSE_CONFORMANCE_BQ_DATASET
         : dialect === "snowflake"
           ? process.env.SNOWFLAKE_SCHEMA
           : undefined;
   if (fallback) {
      const container = containerCase(dialect, fallback);
      let tables: string[] = [];
      try {
         tables = await listPrefixedTables(
            connection,
            dialect,
            container,
            SCRATCH_PREFIX,
         );
      } catch (error) {
         failed.push(`${container}: ${String(error)}`);
      }
      for (const table of tables) {
         if (!due(table)) continue;
         try {
            await dropTable(connection, dialect, container, table);
            dropped.push(`${container}.${table}`);
         } catch (error) {
            failed.push(`${container}.${table}: ${String(error)}`);
         }
      }
   }
   return { dropped, failed };
}
