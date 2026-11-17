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
 *   `SNOWFLAKE_DATABASE`, and a key or a password: `SNOWFLAKE_PRIVATE_KEY_PATH`
 *   (a PEM file: what the workflow prepares, so the key never passes through
 *   this process's output), `SNOWFLAKE_PRIVATE_KEY` (a PEM, or a PEM or bare
 *   PKCS#8 body base64-encoded), or `SNOWFLAKE_PASSWORD`; optional `SNOWFLAKE_ROLE` and
 *   `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`. The role must be able to create a
 *   schema: there is no fallback, so a run never writes into a schema other
 *   CI reads.
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
   /**
    * The environment connection, as a publisher.config.json names it; with a
    * scratch schema, that schema as its default (Snowflake), so an
    * unqualified name a build writes lands in the run's own schema rather
    * than the user's default.
    */
   connection(scratchSchema?: string): ApiConnection;
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
 * A Snowflake private key as `SNOWFLAKE_PRIVATE_KEY` may hold it: a PEM
 * (multi-line or single-line), a PEM base64-encoded whole, or the bare
 * base64 body of a PKCS#8 key. Returned as a PEM; Publisher normalizes it
 * further. Nothing here prints: in CI the workflow decodes and masks the key
 * in a step of its own and hands this process a file
 * (`SNOWFLAKE_PRIVATE_KEY_PATH`), since anything a test process writes to
 * its output may be kept somewhere a mask does not reach.
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

/** The Snowflake key, from its file or its variable; undefined when neither is set. */
function snowflakeKey(): string | undefined {
   const file = process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
   if (file) return fs.readFileSync(file, "utf8");
   const raw = process.env.SNOWFLAKE_PRIVATE_KEY;
   return raw ? snowflakePrivateKeyPem(raw) : undefined;
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

   const hasKey = !!(
      process.env.SNOWFLAKE_PRIVATE_KEY_PATH ||
      process.env.SNOWFLAKE_PRIVATE_KEY
   );
   const sfMissing = [
      ...missing([
         "SNOWFLAKE_ACCOUNT",
         "SNOWFLAKE_USER",
         "SNOWFLAKE_WAREHOUSE",
         "SNOWFLAKE_DATABASE",
      ]),
      ...(hasKey || process.env.SNOWFLAKE_PASSWORD
         ? []
         : [
              "SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PASSWORD",
           ]),
   ];
   if (sfMissing.length === 0) {
      targets.push({
         dialect: "snowflake",
         connection: (scratchSchema) => {
            const key = snowflakeKey();
            return {
               name: WAREHOUSE_CONNECTION,
               type: "snowflake",
               snowflakeConnection: {
                  account: process.env.SNOWFLAKE_ACCOUNT!,
                  username: process.env.SNOWFLAKE_USER!,
                  ...(key
                     ? {
                          privateKey: key,
                          ...(process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
                             ? {
                                  privateKeyPass:
                                     process.env
                                        .SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
                               }
                             : {}),
                       }
                     : { password: process.env.SNOWFLAKE_PASSWORD! }),
                  warehouse: process.env.SNOWFLAKE_WAREHOUSE!,
                  database: process.env.SNOWFLAKE_DATABASE!,
                  ...(process.env.SNOWFLAKE_ROLE
                     ? { role: process.env.SNOWFLAKE_ROLE }
                     : {}),
                  ...(scratchSchema ? { schema: scratchSchema } : {}),
                  // A runaway statement on the shared warehouse fails instead.
                  responseTimeoutMilliseconds: 300_000,
               },
            };
         },
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

/** Snowflake folds an unquoted name to upper case; a scratch schema is spelled upper-case there. */
function containerCase(dialect: Dialect, name: string): string {
   return dialect === "snowflake" ? name.toUpperCase() : name;
}

/** A run's own scratch area, exactly: the prefix, a UTC stamp, a run id. */
const CONTAINER_NAME = new RegExp(`^${SCRATCH_PREFIX}\\d{12}_[a-z0-9]+$`, "i");

/** A run-prefixed table in the fallback dataset: a run's prefix, then a plain name. */
const PREFIXED_TABLE = new RegExp(
   `^${SCRATCH_PREFIX}\\d{12}_[a-z0-9]+_[a-z0-9_]+$`,
   "i",
);

/** An existing dataset the BigQuery fallback writes into: a plain identifier. */
const DATASET_NAME = /^[A-Za-z0-9_]+$/;

/** Refuse any name a drop would reach that is not one of ours, before any SQL is written. */
function assertName(kind: string, name: string, pattern: RegExp): void {
   if (!pattern.test(name)) {
      throw new Error(
         `refusing to touch ${kind} '${name}': not a warehouse conformance name`,
      );
   }
}

/** Snowflake's database, quoted as it was configured (upper-cased, as Snowflake folds it). */
function snowflakeDatabase(): string {
   const db = process.env.SNOWFLAKE_DATABASE ?? "";
   assertName("database", db, /^[A-Za-z0-9_]+$/);
   return `"${db.toUpperCase()}"`;
}

function bigQueryProject(): string {
   const project = process.env.BIGQUERY_TEST_PROJECT_ID ?? "";
   assertName("project", project, /^[a-z0-9-]+$/);
   return project;
}

/** A schema or dataset as DDL names it, quoted. */
function quotedContainer(dialect: Dialect, container: string): string {
   switch (dialect) {
      case "bigquery":
         return `\`${bigQueryProject()}.${container}\``;
      case "snowflake":
         return `${snowflakeDatabase()}."${container}"`;
      case "postgres":
         return `"${container}"`;
   }
}

function makeScratch(
   dialect: Dialect,
   container: string,
   prefix: string,
   ownsContainer: boolean,
): Scratch {
   return {
      dialect,
      container,
      prefix,
      ownsContainer,
      name: (table) => `${container}.${prefix}${table}`,
      tablePath: (table) =>
         dialect === "bigquery"
            ? `${bigQueryProject()}.${container}.${prefix}${table}`
            : dialect === "snowflake"
              ? `${process.env.SNOWFLAKE_DATABASE!.toUpperCase()}.${container}.${(prefix + table).toUpperCase()}`
              : `${container}.${prefix}${table}`,
   };
}

/**
 * BigQuery's refusal of a dataset create for want of the grant, which the
 * fallback dataset answers; any other failure (a bad location, a network
 * error) is not one.
 */
function isPermissionError(error: unknown): boolean {
   const text = error instanceof Error ? error.message : String(error);
   return /access denied|permission denied|does not have [\w.]+ permission|\bPERMISSION_DENIED\b/i.test(
      text,
   );
}

/**
 * Create this run's scratch area, a schema or dataset of its own. On
 * BigQuery, when the key may not create a dataset and
 * `WAREHOUSE_CONFORMANCE_BQ_DATASET` names one it can write, run-prefixed
 * tables there instead. Throws, naming the grant, otherwise: a run never
 * writes into a schema other CI reads (Snowflake's `malloytest`).
 */
export async function createScratch(
   dialect: Dialect,
   connection: Connection,
   name = scratchName(),
): Promise<Scratch> {
   assertName("scratch", name, CONTAINER_NAME);
   const container = containerCase(dialect, name);
   try {
      switch (dialect) {
         case "bigquery":
            // A table the drop missed expires on its own after a day.
            await run(
               connection,
               `CREATE SCHEMA ${quotedContainer(dialect, container)} ` +
                  `OPTIONS(location="${bigQueryLocation()}", default_table_expiration_days=1, ` +
                  `labels=[("purpose", "publisher-warehouse-conformance")])`,
            );
            break;
         case "snowflake":
            await run(
               connection,
               `CREATE SCHEMA ${quotedContainer(dialect, container)} ` +
                  `COMMENT = 'publisher warehouse conformance scratch; dropped by the run or the sweeper'`,
            );
            break;
         case "postgres":
            await run(
               connection,
               `CREATE SCHEMA ${quotedContainer(dialect, container)}`,
            );
            break;
      }
      return makeScratch(dialect, container, "", true);
   } catch (error) {
      const fallback =
         dialect === "bigquery"
            ? process.env.WAREHOUSE_CONFORMANCE_BQ_DATASET
            : undefined;
      if (fallback && isPermissionError(error)) {
         assertName("dataset", fallback, DATASET_NAME);
         return makeScratch(dialect, fallback, `${name}_`, false);
      }
      const grant =
         dialect === "bigquery"
            ? `bigquery.datasets.create on project ${process.env.BIGQUERY_TEST_PROJECT_ID} (for example roles/bigquery.user), or the repository variable WAREHOUSE_CONFORMANCE_BQ_DATASET naming a dataset it can create tables in`
            : dialect === "snowflake"
              ? `CREATE SCHEMA on database ${process.env.SNOWFLAKE_DATABASE} for role ${process.env.SNOWFLAKE_ROLE ?? "(the user's default)"}`
              : "CREATE on the database";
      throw new Error(
         `could not create the ${dialect} scratch area ${container}: ${
            error instanceof Error ? error.message : String(error)
         }. The run needs ${grant}.`,
      );
   }
}

/** Drop everything a scratch area holds; throws if a drop fails. */
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
      scratch.container,
      scratch.prefix,
   )) {
      await dropTable(connection, scratch.container, table);
   }
}

async function dropContainer(
   connection: Connection,
   dialect: Dialect,
   container: string,
): Promise<void> {
   assertName("scratch", container, CONTAINER_NAME);
   await run(
      connection,
      `DROP SCHEMA IF EXISTS ${quotedContainer(dialect, container)} CASCADE`,
   );
}

/** A run-prefixed table (or a view Publisher made) in the BigQuery fallback dataset. */
async function dropTable(
   connection: Connection,
   dataset: string,
   table: string,
): Promise<void> {
   assertName("dataset", dataset, DATASET_NAME);
   assertName("table", table, PREFIXED_TABLE);
   const kind = await run(
      connection,
      `SELECT table_type FROM ${quotedContainer("bigquery", dataset)}.INFORMATION_SCHEMA.TABLES WHERE table_name = '${table}'`,
   );
   const isView = rowsOf(kind).some((r) =>
      /view/i.test(String(r.table_type ?? r.TABLE_TYPE)),
   );
   await run(
      connection,
      `DROP ${isView ? "VIEW" : "TABLE"} IF EXISTS \`${bigQueryProject()}.${dataset}.${table}\``,
   );
}

/** The fallback dataset's tables that start with `prefix` and are ours by name. */
async function listPrefixedTables(
   connection: Connection,
   dataset: string,
   prefix: string,
): Promise<string[]> {
   assertName("dataset", dataset, DATASET_NAME);
   const result = await run(
      connection,
      `SELECT table_name FROM ${quotedContainer("bigquery", dataset)}.INFORMATION_SCHEMA.TABLES WHERE STARTS_WITH(LOWER(table_name), '${prefix.toLowerCase().replace(/[^a-z0-9_]/g, "")}')`,
   );
   return rowsOf(result)
      .map((r) => String(r.table_name ?? r.TABLE_NAME))
      .filter((t) => PREFIXED_TABLE.test(t));
}

/** The scratch areas present, by exact name; anything merely starting with the prefix is left alone. */
async function listScratchContainers(
   connection: Connection,
   dialect: Dialect,
): Promise<string[]> {
   const like = SCRATCH_PREFIX.replace(/_/g, "\\_");
   const result = await run(
      connection,
      dialect === "bigquery"
         ? `SELECT schema_name FROM \`${bigQueryProject()}\`.\`region-${bigQueryLocation().toLowerCase()}\`.INFORMATION_SCHEMA.SCHEMATA WHERE STARTS_WITH(LOWER(schema_name), '${SCRATCH_PREFIX}')`
         : dialect === "snowflake"
           ? `SELECT schema_name FROM ${snowflakeDatabase()}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name ILIKE '${like}%'`
           : `SELECT schema_name FROM information_schema.schemata WHERE schema_name ILIKE '${like}%'`,
   );
   return rowsOf(result)
      .map((r) => String(r.schema_name ?? r.SCHEMA_NAME))
      .filter((n) => CONTAINER_NAME.test(n));
}

/**
 * Drop the scratch areas (and, in the BigQuery fallback dataset, the
 * prefixed tables) a run left behind: every one older than
 * `olderThanHours`, or, with `runId`, the ones that run made, whatever their
 * age (the workflow's cleanup step, after a cancelled or timed-out run).
 * Only a name of exactly our form is ever dropped. Returns what it dropped;
 * one failed drop does not stop the rest, and the failures are returned too.
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
         : undefined;
   if (fallback) {
      let tables: string[] = [];
      try {
         tables = await listPrefixedTables(
            connection,
            fallback,
            SCRATCH_PREFIX,
         );
      } catch (error) {
         failed.push(`${fallback}: ${String(error)}`);
      }
      for (const table of tables) {
         if (!due(table)) continue;
         try {
            await dropTable(connection, fallback, table);
            dropped.push(`${fallback}.${table}`);
         } catch (error) {
            failed.push(`${fallback}.${table}: ${String(error)}`);
         }
      }
   }
   return { dropped, failed };
}
