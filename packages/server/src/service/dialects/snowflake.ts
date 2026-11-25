// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `snowflake` connection type's registry entry. */

import { createPrivateKey } from "crypto";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../api";
import { logger } from "../../logger";
import { runInternalSQL } from "../duckdb_lockdown";
import {
   escapeSQL,
   installAndLoadExtension,
   sanitizeSecretName,
} from "../duckdb_session_setup";
import { quoteIdentifier } from "../quoting";
import type { Connection } from "@malloydata/malloy";
import { standardizeRunSQLResult } from "../gcs_s3_utils";
import { runIntrospectionSQL, sqlLiteral } from "../introspection_sql";
import {
   assertSafeSqlIdentifier,
   groupColumnRowsIntoTables,
   sqlInFilter,
   withIntrospectionFailure,
} from "../introspection_helpers";
import type {
   ApiConnection,
   ApiSchema,
   ApiTable,
   AttachedDatabase,
   ConnectionDialect,
   CoreConnectionEntry,
   FederatedHandle,
   FederationConfig,
   NamedConnection,
} from "./types";

export const SNOWFLAKE: ConnectionDialect = Object.freeze({
   type: "snowflake",
   attributes: Object.freeze({
      dialectName: "snowflake",
      isPool: true,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateSnowflakeShape,
   coreEntry: buildSnowflakeEntry,
   introspector: Object.freeze({
      schemas: getSchemasForSnowflake,
      tables: listTablesForSnowflake,
   }),
   attached: Object.freeze({
      attach: attachSnowflake,
      test: testAttachedSnowflake,
      secretName: snowflakeDetachSecret,
   }),
   passthrough: Object.freeze({
      federate: federateSnowflake,
      wrap: wrapSnowflake,
   }),
});

/**
 * Collapse `null` to `undefined` for an optional connection field. The Malloy
 * config layer treats only `undefined` as absent: `makeDigest` reads `.length`
 * off every part and special-cases `undefined` alone, so a `null` throws.
 * `removeUndefined` does not help, since a `null` survives it.
 */
function nullToUndefined<T>(value: T | null | undefined): T | undefined {
   return value ?? undefined;
}

export function normalizeSnowflakePrivateKey(privateKey: string): string {
   let privateKeyContent = privateKey.trim();

   if (!privateKeyContent.includes("\n")) {
      const keyPatterns = [
         {
            beginRegex: /-----BEGIN\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN ENCRYPTED PRIVATE KEY-----",
            endMarker: "-----END ENCRYPTED PRIVATE KEY-----",
         },
         {
            beginRegex: /-----BEGIN\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN PRIVATE KEY-----",
            endMarker: "-----END PRIVATE KEY-----",
         },
         {
            beginRegex: /-----BEGIN\s+RSA\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+RSA\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN RSA PRIVATE KEY-----",
            endMarker: "-----END RSA PRIVATE KEY-----",
         },
      ];

      for (const pattern of keyPatterns) {
         const beginMatch = privateKeyContent.match(pattern.beginRegex);
         const endMatch = privateKeyContent.match(pattern.endRegex);

         if (beginMatch && endMatch) {
            const beginPos = beginMatch.index! + beginMatch[0].length;
            const endPos = endMatch.index!;
            const keyData = privateKeyContent
               .substring(beginPos, endPos)
               .replace(/\s+/g, "");

            const lines: string[] = [];
            for (let i = 0; i < keyData.length; i += 64) {
               lines.push(keyData.slice(i, i + 64));
            }
            privateKeyContent = `${pattern.beginMarker}\n${lines.join("\n")}\n${pattern.endMarker}\n`;
            break;
         }
      }
   } else if (!privateKeyContent.endsWith("\n")) {
      privateKeyContent += "\n";
   }

   // Snowflake's Node SDK requires PKCS#8 ("BEGIN PRIVATE KEY"). Convert
   // PKCS#1 ("BEGIN RSA PRIVATE KEY") so users can paste either format.
   if (/-----BEGIN\s+RSA\s+PRIVATE\s+KEY-----/i.test(privateKeyContent)) {
      try {
         privateKeyContent = createPrivateKey({
            key: privateKeyContent,
            format: "pem",
         })
            .export({ type: "pkcs8", format: "pem" })
            .toString();
      } catch (err) {
         throw new Error(
            `Failed to convert Snowflake RSA private key (PKCS#1) to PKCS#8: ${
               err instanceof Error ? err.message : String(err)
            }`,
         );
      }
      if (!privateKeyContent.endsWith("\n")) {
         privateKeyContent += "\n";
      }
   }

   return privateKeyContent;
}

function validateSnowflakeShape(connection: ApiConnection): void {
   const snowflakeConnection = connection.snowflakeConnection;
   if (!snowflakeConnection) {
      throw new Error("Snowflake connection configuration is missing.");
   }
   if (!snowflakeConnection.account) {
      throw new Error("Snowflake account is required.");
   }
   if (!snowflakeConnection.username) {
      throw new Error("Snowflake username is required.");
   }
   if (!snowflakeConnection.password && !snowflakeConnection.privateKey) {
      throw new Error(
         "Snowflake password or private key or private key path is required.",
      );
   }
   if (!snowflakeConnection.warehouse) {
      throw new Error("Snowflake warehouse is required.");
   }
}

function buildSnowflakeEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "snowflake",
      account: connection.snowflakeConnection?.account,
      username: connection.snowflakeConnection?.username,
      password: nullToUndefined(connection.snowflakeConnection?.password),
      privateKey: connection.snowflakeConnection?.privateKey
         ? normalizeSnowflakePrivateKey(
              connection.snowflakeConnection.privateKey,
           )
         : undefined,
      privateKeyPass: nullToUndefined(
         connection.snowflakeConnection?.privateKeyPass,
      ),
      warehouse: connection.snowflakeConnection?.warehouse,
      // An explicit `null` here would throw in Malloy's `makeDigest` (see
      // nullToUndefined). Publisher builds every Snowflake connection itself
      // (snowflake_connection.ts), which drops them too; this entry only
      // registers the name.
      database: nullToUndefined(connection.snowflakeConnection?.database),
      schema: nullToUndefined(connection.snowflakeConnection?.schema),
      role: nullToUndefined(connection.snowflakeConnection?.role),
      timeoutMs: connection.snowflakeConnection?.responseTimeoutMilliseconds,
      // Pool sizing is server-owned policy, not exposed through the API.
      poolMin: 1,
      poolMax: 20,
   };
}

/** Each named Snowflake field must be non-empty. */
function assertSnowflakeFieldsPresent(
   name: string | undefined,
   fields: Record<string, string | null | undefined>,
): void {
   for (const [field, value] of Object.entries(fields)) {
      if (!value) {
         throw new Error(`Snowflake ${field} is required for: ${name}`);
      }
   }
}

/** The SQL-escaped identity fields every Snowflake secret is built from. */
function escapeSnowflakeIdentity(
   config: components["schemas"]["SnowflakeConnection"],
) {
   return {
      account: escapeSQL(config.account || ""),
      user: escapeSQL(config.username || ""),
      database: config.database ? escapeSQL(config.database) : undefined,
      warehouse: config.warehouse ? escapeSQL(config.warehouse) : undefined,
      schema: config.schema ? escapeSQL(config.schema) : undefined,
      role: config.role ? escapeSQL(config.role) : undefined,
   };
}

/** The secret a Snowflake attachment named `name` creates. */
function snowflakeSecretName(name: string): string {
   return `${name}_secret`;
}

/**
 * The statements that attach a Snowflake database under `name`: its secret
 * (from the already-escaped `secretLines`), a test query through that secret,
 * and the ATTACH. Every use of the name is quoted: as a DuckDB identifier
 * (the alias, the secret) or escaped as a string literal (the database to
 * attach, the secret the test query names), so none can end its statement.
 */
export function snowflakeAttachSQL(
   name: string,
   secretLines: string[],
): { createSecret: string; test: string; attach: string } {
   const secret = snowflakeSecretName(name);
   const quotedSecret = quoteIdentifier(secret, "duckdb");
   return {
      createSecret: `CREATE OR REPLACE SECRET ${quotedSecret} (\n${secretLines.join(",\n")}\n   );`,
      test: `SELECT * FROM snowflake_query('SELECT 1', '${escapeSQL(secret)}');`,
      attach: `ATTACH '${escapeSQL(name)}' AS ${quoteIdentifier(name, "duckdb")} (TYPE snowflake, SECRET ${quotedSecret}, READ_ONLY);`,
   };
}

async function attachSnowflake(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.snowflakeConnection) {
      throw new Error(
         `Snowflake connection configuration missing for: ${attachedDb.name}`,
      );
   }

   const config = attachedDb.snowflakeConnection;

   assertSnowflakeFieldsPresent(attachedDb.name, {
      account: config.account,
      username: config.username,
      password: config.password,
   });

   await installAndLoadExtension(connection, "snowflake", true);

   try {
      const version = await connection.runSQL("SELECT snowflake_version();");
      logger.info(`Snowflake ADBC driver verified with version:`, version.rows);
   } catch (error) {
      throw new Error(
         `Snowflake ADBC driver verification failed: ${error instanceof Error ? error.message : String(error)}`,
      );
   }

   const params = {
      ...escapeSnowflakeIdentity(config),
      password: escapeSQL(config.password || ""),
   };

   // Optional fields are emitted only when set: an absent one would otherwise
   // interpolate the literal 'undefined', which Snowflake reads as a real
   // database or warehouse name. ROLE and SCHEMA are carried so the attached
   // database queries under the configured role, as the passthrough build does.
   const secretLines = [
      `      TYPE snowflake`,
      `      ACCOUNT '${params.account}'`,
      `      USER '${params.user}'`,
      `      PASSWORD '${params.password}'`,
      ...(params.database ? [`      DATABASE '${params.database}'`] : []),
      ...(params.warehouse ? [`      WAREHOUSE '${params.warehouse}'`] : []),
      ...(params.schema ? [`      SCHEMA '${params.schema}'`] : []),
      ...(params.role ? [`      ROLE '${params.role}'`] : []),
   ];
   const statements = snowflakeAttachSQL(attachedDb.name ?? "", secretLines);

   await connection.runSQL(statements.createSecret);

   const testresult = await connection.runSQL(statements.test);
   logger.info(`Testing Snowflake connection:`, testresult.rows);

   await connection.runSQL(statements.attach);
   logger.info(`Successfully attached Snowflake database: ${attachedDb.name}`);
}

/** The probe a connection test runs on an attached Snowflake database. */
async function testAttachedSnowflake(
   duckdbConnection: DuckDBConnection,
   name: string,
): Promise<void> {
   await runInternalSQL(
      duckdbConnection,
      `SELECT database_name FROM duckdb_databases() WHERE database_name = '${escapeSQL(name)}'`,
   );
   logger.info(`Attached Snowflake database test passed: ${name}`);
}

/** The secret a Snowflake attachment creates, as its detach drops it. */
function snowflakeDetachSecret(name: string): string {
   return quoteIdentifier(snowflakeSecretName(name), "duckdb");
}

async function federateSnowflake(
   connection: DuckDBConnection,
   config: FederationConfig,
): Promise<FederatedHandle> {
   const sf = config.snowflakeConnection;
   if (!sf) {
      throw new Error(
         `Snowflake connection configuration missing for: ${config.name}`,
      );
   }
   assertSnowflakeFieldsPresent(config.name, {
      account: sf.account,
      username: sf.username,
   });
   // Key pair or password, as a live Snowflake connection accepts; requiring a
   // password would make key-pair connections (Snowflake's preferred
   // programmatic auth) unbuildable.
   const usesKeyPair = !!sf.privateKey;
   if (!usesKeyPair && !sf.password) {
      throw new Error(
         `Snowflake privateKey or password is required for: ${config.name}`,
      );
   }

   await installAndLoadExtension(connection, "snowflake", true);

   const params = {
      ...escapeSnowflakeIdentity(sf),
      password: sf.password ? escapeSQL(sf.password) : undefined,
      // Normalized here because this config is an unnormalized clone (the live
      // path normalizes at its own call site): a single-line PEM makes Go's
      // pem.Decode return nil, and the extension wants PKCS#8 where a user may
      // have pasted PKCS#1.
      privateKey: sf.privateKey
         ? escapeSQL(normalizeSnowflakePrivateKey(sf.privateKey))
         : undefined,
      privateKeyPass: sf.privateKeyPass
         ? escapeSQL(sf.privateKeyPass)
         : undefined,
   };
   const secretName = sanitizeSecretName(`snowflake_${config.name}`);
   // Optional fields are emitted only when supplied, or the literal 'undefined'
   // would be used as a db/warehouse name.
   //
   // ROLE and SCHEMA are part of the connection's identity (its digest).
   // Dropping them would run the build under the user's default role while live
   // queries use the configured one: a build that fails on permissions, or
   // reads under wider ones.
   const secretLines = [
      `   TYPE snowflake`,
      `   ACCOUNT '${params.account}'`,
      `   USER '${params.user}'`,
      // PRIVATE_KEY_PASSWORD, not the documented PRIVATE_KEY_PASSPHRASE, which
      // is only a backward-compatible alias for it.
      ...(usesKeyPair
         ? [
              `   AUTH_TYPE 'key_pair'`,
              `   PRIVATE_KEY '${params.privateKey}'`,
              ...(params.privateKeyPass
                 ? [`   PRIVATE_KEY_PASSWORD '${params.privateKeyPass}'`]
                 : []),
           ]
         : [`   PASSWORD '${params.password}'`]),
      ...(params.database ? [`   DATABASE '${params.database}'`] : []),
      ...(params.warehouse ? [`   WAREHOUSE '${params.warehouse}'`] : []),
      ...(params.schema ? [`   SCHEMA '${params.schema}'`] : []),
      ...(params.role ? [`   ROLE '${params.role}'`] : []),
   ];
   await connection.runSQL(
      `CREATE OR REPLACE SECRET ${secretName} (\n${secretLines.join(",\n")}\n);`,
   );
   logger.info(`Federated Snowflake source for passthrough: ${secretName}`);
   // snowflake_query(sql, secret): the secret name is the handle.
   return { handle: secretName, sourceType: "snowflake" };
}

/** A SELECT as a snowflake_query call: the SQL, then the secret. */
function wrapSnowflake(handle: string, innerSQL: string): string {
   const sql = `'${escapeSQL(innerSQL)}'`;
   const h = `'${escapeSQL(handle)}'`;
   // snowflake_query(<sql>, <secret name>) — SQL FIRST for this engine.
   return `SELECT * FROM snowflake_query(${sql}, ${h})`;
}

/**
 * Snowflake-managed databases (the metadata share and the sample-data share),
 * hidden from an account-wide schema listing: neither is user data, and
 * together they contribute most schemas on an empty account.
 */
const SNOWFLAKE_SYSTEM_DATABASES = new Set([
   "SNOWFLAKE",
   "SNOWFLAKE_SAMPLE_DATA",
]);

/** One schema row, normalized across the two sources that can produce it. */
interface SnowflakeSchemaRow {
   catalogName: string;
   schemaName: string;
   owner: string;
}

/**
 * Schemas within one configured database, via that database's
 * INFORMATION_SCHEMA. Column names come back upper-case.
 */
async function listSnowflakeSchemasInDatabase(
   connection: ApiConnection,
   malloyConnection: Connection,
   database: string,
   // `null` as well as `undefined`: the API contract declares the optional
   // Snowflake fields nullable, so an omitted schema arrives as either.
   schema: string | null | undefined,
): Promise<SnowflakeSchemaRow[]> {
   const filters = [
      `CATALOG_NAME = '${sqlLiteral(database, connection.type)}'`,
   ];
   if (schema) {
      filters.push(`SCHEMA_NAME = '${sqlLiteral(schema, connection.type)}'`);
   }
   // Identifier position, so validated rather than escaped. The name is
   // operator config, so this is consistency with the catalog handling rather
   // than a trust boundary.
   assertSafeSqlIdentifier(database, "database name");
   const result = await runIntrospectionSQL(
      malloyConnection,
      `SELECT CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER FROM ${database}.INFORMATION_SCHEMA.SCHEMATA WHERE ${filters.join(
         " AND ",
      )} ORDER BY SCHEMA_NAME`,
   );
   return standardizeRunSQLResult(result).map((row: unknown) => {
      const r = row as Record<string, unknown>;
      return {
         catalogName: String(r.CATALOG_NAME ?? r.catalog_name ?? ""),
         schemaName: String(r.SCHEMA_NAME ?? r.schema_name ?? ""),
         owner: String(r.SCHEMA_OWNER ?? r.schema_owner ?? ""),
      };
   });
}

/**
 * Snowflake's maximum `LIMIT` for a SHOW command. Stated explicitly because
 * without it SHOW SCHEMAS ERRORS above ten thousand rows and returns nothing;
 * with it, a large account gets a partial list, and the caller warns on hitting
 * the limit because a partial list looks complete.
 *
 * {@link runIntrospectionSQL}'s cap cannot substitute: the Snowflake driver
 * applies `rowLimit` by slicing rows it already fetched, so it never reaches the
 * server. 10,000 is Snowflake's ceiling (`LIMIT 10001` is rejected); an account
 * above it needs SHOW per database, the `FROM '<name>'` cursor, or
 * SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA.
 */
const SNOWFLAKE_SHOW_ROW_LIMIT = 10_000;

/**
 * Every role-visible schema in the account, for a connection with no configured
 * database. SHOW needs no current database.
 *
 * Its output columns are LOWER-case, unlike INFORMATION_SCHEMA's; read with
 * upper-case keys every owner is blank, and the caller's `isHidden` rule would
 * hide every row. SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA is avoided on purpose: it
 * needs elevated grants and lags by up to ~2 hours.
 */
async function listSnowflakeSchemasInAccount(
   malloyConnection: Connection,
): Promise<SnowflakeSchemaRow[]> {
   const result = await runIntrospectionSQL(
      malloyConnection,
      `SHOW SCHEMAS IN ACCOUNT LIMIT ${SNOWFLAKE_SHOW_ROW_LIMIT}`,
   );
   const returnedRows = standardizeRunSQLResult(result);
   if (returnedRows.length >= SNOWFLAKE_SHOW_ROW_LIMIT) {
      logger.warn(
         "Snowflake account-wide schema listing hit the SHOW row limit; the schema list is incomplete and some tables will not be discoverable",
         {
            rowLimit: SNOWFLAKE_SHOW_ROW_LIMIT,
            returnedRows: returnedRows.length,
         },
      );
   }
   const parsed = returnedRows.map((row: unknown) => {
      const r = row as Record<string, unknown>;
      return {
         catalogName: String(r.database_name ?? r.DATABASE_NAME ?? ""),
         schemaName: String(r.name ?? r.NAME ?? ""),
         owner: String(r.owner ?? r.OWNER ?? ""),
      };
   });
   // A row missing either half cannot form a DATABASE.SCHEMA name that
   // listTablesForSnowflake can parse.
   const usable = parsed.filter((r) => r.catalogName && r.schemaName);
   if (usable.length < parsed.length) {
      // Warn, so unrecognized SHOW columns (a rename, or a role that cannot see
      // them) are not mistaken for an account with no schemas.
      logger.warn(
         "Dropped Snowflake schema rows missing a database or schema name; the schema list is incomplete",
         { dropped: parsed.length - usable.length, returned: parsed.length },
      );
   }
   return usable.sort(
      (a, b) =>
         a.catalogName.localeCompare(b.catalogName) ||
         a.schemaName.localeCompare(b.schemaName),
   );
}

async function getSchemasForSnowflake(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const snowflake = connection.snowflakeConnection;
   if (!snowflake) {
      throw new Error("Snowflake connection is required");
   }
   // "before-log": a rejected database name is the config's to fix and must
   // stay a BadRequestError (see BadRequestPassThrough).
   return withIntrospectionFailure(
      `schemas for Snowflake connection ${connection.name}`,
      async () => {
         const database = snowflake.database;
         const schema = snowflake.schema;

         // INFORMATION_SCHEMA.SCHEMATA is per-database: without a current
         // database the unqualified form fails outright, and no qualified form
         // spans databases. So with no configured database, use SHOW SCHEMAS IN
         // ACCOUNT. Both branches emit `DATABASE.SCHEMA`, which
         // listTablesForSnowflake parses back apart.
         const rows = database
            ? await listSnowflakeSchemasInDatabase(
                 connection,
                 malloyConnection,
                 database,
                 schema,
              )
            : await listSnowflakeSchemasInAccount(malloyConnection);

         return rows.map(({ catalogName, schemaName, owner }) => ({
            name: `${catalogName}.${schemaName}`,
            isHidden:
               owner === "SNOWFLAKE" ||
               // A blank owner means system-owned only within a configured
               // database. Account-wide, SHOW reports a blank owner for every
               // schema of an IMPORTED DATABASE (a Marketplace or partner share
               // the operator subscribed to), which must stay visible. The other
               // account-wide blank-owner schemas are caught by the
               // INFORMATION_SCHEMA and system-database rules below.
               (Boolean(database) && owner === "") ||
               schemaName === "INFORMATION_SCHEMA" ||
               // Snowflake's own databases are noise account-wide; a connection
               // pointed at one by name still lists it.
               (!database && SNOWFLAKE_SYSTEM_DATABASES.has(catalogName)),
            // Only within a configured database: account-wide, a same-named
            // schema in every database would be a default.
            isDefault:
               Boolean(database) && Boolean(schema) && schemaName === schema,
         }));
      },
      "before-log",
   );
}

async function listTablesForSnowflake(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const snowflake = connection.snowflakeConnection;
   if (!snowflake) {
      throw new Error("Snowflake connection is required");
   }
   return withIntrospectionFailure(
      `tables for Snowflake schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const parts = schemaName.split(".");
         let databaseName: string;
         let schemaOnly: string;

         if (parts.length >= 2) {
            databaseName = parts[0];
            schemaOnly = parts[1];
         } else {
            databaseName = snowflake.database ?? "";
            schemaOnly = parts[0];
         }

         if (!databaseName) {
            throw new Error(
               `Cannot resolve database for schema "${schemaName}": provide DATABASE.SCHEMA or configure a database on the connection`,
            );
         }

         // Identifier position below, so this is validated, not escaped.
         assertSafeSqlIdentifier(databaseName, "database name");
         const qualifiedSchema = `${databaseName}.${schemaOnly}`;
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ${databaseName}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${sqlLiteral(schemaOnly, connection.type)}' ${sqlInFilter("TABLE_NAME", tableNames, connection.type)} ORDER BY TABLE_NAME, ORDINAL_POSITION`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(
            rows,
            (t) => `${qualifiedSchema}.${t}`,
         );
      },
      "after-log",
   );
}
