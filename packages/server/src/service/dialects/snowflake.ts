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
import type {
   ApiConnection,
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
