// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createPrivateKey } from "crypto";
import { existsSync } from "fs";
import path from "path";
import { components } from "../api";
import {
   ALLOW_DUCKDB_SETUP_SQL_ENV,
   getExtensionFetchPolicy,
   isDuckdbSetupSqlAllowed,
   parseBoolEnv,
} from "../config";
import { BadRequestError } from "../errors";
import { logger } from "../logger";
import {
   resolveCloudStorageCredentials,
   validateS3ProviderShape,
} from "./gcs_s3_utils";
import {
   ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV,
   allowUnverifiedHostKey,
   parseHostKeys,
} from "./proxy";
import {
   queryMetadataAdvisoryWarnings,
   queryMetadataBudgetWarning,
   queryMetadataViolations,
} from "./query_metadata";

type ApiConnection = components["schemas"]["Connection"];
type AttachedDatabase = components["schemas"]["AttachedDatabase"];

// TLS modes accepted on a postgres connection, proxied or direct. Defined here
// (connection.ts imports this module) so config-load validation and the
// connect-time builders share one list. Mirrors `sslmode` in api-doc.yaml.
export const PROXIED_SSLMODES = [
   "disable",
   "no-verify",
   "verify-ca",
   "verify-full",
] as const;

/**
 * Throws unless `sslmode` is one of PROXIED_SSLMODES and, for `verify-ca`, a
 * readable CA bundle is available to pin it against. `subject` leads each
 * error message.
 */
function validatePostgresSslmode(subject: string, sslmode: string): void {
   if (!(PROXIED_SSLMODES as readonly string[]).includes(sslmode)) {
      throw new Error(
         `${subject} has unsupported sslmode '${sslmode}' ` +
            `(expected ${PROXIED_SSLMODES.join(" | ")}).`,
      );
   }
   if (sslmode === "verify-ca") {
      const caBundle = process.env.NODE_EXTRA_CA_CERTS;
      if (!caBundle || !existsSync(caBundle)) {
         throw new Error(
            `${subject} uses sslmode 'verify-ca' but no readable ` +
               `CA bundle is available (NODE_EXTRA_CA_CERTS is unset or points at a missing file). ` +
               `Add the CA bundle to the image or use sslmode 'no-verify'.`,
         );
      }
   }
}

// Postgres's own ceiling for statement_timeout, which is an int in ms. A larger
// value makes the session SET fail on every connection that applies it.
const POSTGRES_MAX_STATEMENT_TIMEOUT_MS = 2_147_483_647;

/**
 * The connection's statement timeout, validated. Returns undefined when unset.
 *
 * Every consumer interpolates this value into SQL (`SET statement_timeout`) or
 * a libpq conninfo string, so anything but an integer from 1 to
 * POSTGRES_MAX_STATEMENT_TIMEOUT_MS is rejected here rather than trusted: a JSON
 * body is not held to the schema before it reaches this code.
 */
export function postgresStatementTimeoutMs(
   name: string,
   pg: components["schemas"]["PostgresConnection"],
): number | undefined {
   const value: unknown = pg.statementTimeoutMilliseconds;
   if (value === undefined || value === null) return undefined;
   if (
      typeof value !== "number" ||
      !Number.isInteger(value) ||
      value < 1 ||
      value > POSTGRES_MAX_STATEMENT_TIMEOUT_MS
   ) {
      throw new Error(
         `Connection '${name}' has an invalid statementTimeoutMilliseconds ` +
            `${JSON.stringify(value)} (expected an integer from 1 to ${POSTGRES_MAX_STATEMENT_TIMEOUT_MS}).`,
      );
   }
   return value;
}

export type CoreConnectionEntry = {
   is: string;
   [key: string]: unknown;
};

export type CoreConnectionsPojo = {
   connections: Record<string, CoreConnectionEntry>;
};

export type EnvironmentConnectionMetadata = {
   apiConnection: ApiConnection;
   attachedDatabases: AttachedDatabase[];
   hasAzureAttachment: boolean;
   hasSnowflakePrivateKey: boolean;
   isDuckLake: boolean;
   databasePath?: string;
   workingDirectory: string;
   proxy?: ApiConnection["proxy"];
};

export type AssembledEnvironmentConnections = {
   pojo: CoreConnectionsPojo;
   metadata: Map<string, EnvironmentConnectionMetadata>;
   apiConnections: ApiConnection[];
};

const PUBLISHER_DUCKDB_API_FIELDS = new Set<string>([
   "attachedDatabases",
   "setupSQL",
]);

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

// Narrows the environment-author API surface: rejects securityPolicy,
// allowedDirectories, etc., and admits setupSQL only behind
// PUBLISHER_ALLOW_DUCKDB_SETUP_SQL. It is NOT a filesystem isolation boundary:
// attachedDatabases[].path is not constrained to the environment root, and
// DuckDB's local-file access is unchanged. Adversarial filesystem isolation
// (path-traversal checks, an allowlist) is not provided.
export function validateDuckdbApiSurface(connection: ApiConnection): void {
   if (connection.type !== "duckdb" || !connection.duckdbConnection) return;

   const unsupportedFields = Object.keys(connection.duckdbConnection).filter(
      (field) =>
         !PUBLISHER_DUCKDB_API_FIELDS.has(field) &&
         (connection.duckdbConnection as Record<string, unknown>)[field] !==
            undefined,
   );

   if (unsupportedFields.length > 0) {
      throw new Error(
         `Unsupported DuckDB connection field(s): ${unsupportedFields.join(
            ", ",
         )}. Publisher only supports attachedDatabases and setupSQL for environment-authored DuckDB connections.`,
      );
   }

   const setupSQL = connection.duckdbConnection.setupSQL;
   const hasSetupSQL =
      typeof setupSQL === "string" && setupSQL.trim().length > 0;
   if (!hasSetupSQL) return;
   // Every path that builds a connection passes through here: config load,
   // create and update, and the connection test, which runs setupSQL without
   // storing anything. Refusing here covers all of them.
   if (!isDuckdbSetupSqlAllowed()) {
      throw new Error(
         `setupSQL on DuckDB connection "${connection.name}" is disabled in this deployment. ` +
            `setupSQL runs arbitrary DuckDB statements on the server when the connection is set up. ` +
            `Fix: set the environment variable ${ALLOW_DUCKDB_SETUP_SQL_ENV}=true to enable it.`,
      );
   }
   if (getExtensionFetchPolicy() === "local-only") {
      throw new Error(
         `setupSQL is not allowed on DuckDB connection "${connection.name}" when EXTENSION_FETCH_POLICY is "local-only".`,
      );
   }
}

function cloneApiConnection(connection: ApiConnection): ApiConnection {
   return { ...connection };
}

export function getStaticConnectionAttributes(
   type: ApiConnection["type"],
): components["schemas"]["ConnectionAttributes"] | undefined {
   switch (type) {
      case "postgres":
         return {
            dialectName: "postgres",
            isPool: true,
            canPersist: true,
            canStream: true,
         };
      case "bigquery":
         return {
            dialectName: "standardsql",
            isPool: false,
            canPersist: true,
            canStream: true,
         };
      case "snowflake":
         return {
            dialectName: "snowflake",
            isPool: true,
            canPersist: true,
            canStream: true,
         };
      case "trino":
         return {
            dialectName: "trino",
            isPool: false,
            canPersist: true,
            canStream: false,
         };
      case "databricks":
         return {
            dialectName: "databricks",
            isPool: false,
            canPersist: true,
            canStream: false,
         };
      case "mysql":
         return {
            dialectName: "mysql",
            isPool: false,
            canPersist: true,
            canStream: false,
         };
      case "sqlserver":
         // The connector is read-only: it creates no tables and has no
         // PersistSQLResults, so a colocated `#@ persist` on a SQL Server
         // source is refused at load (READ_ONLY_DIALECTS in
         // materialization_eligibility.ts) rather than failing mid-build.
         return {
            dialectName: "sqlserver",
            isPool: true,
            canPersist: false,
            canStream: true,
         };
      case "duckdb":
      case "motherduck":
      case "ducklake":
         return {
            dialectName: "duckdb",
            isPool: false,
            canPersist: true,
            canStream: true,
         };
      default:
         return undefined;
   }
}

// Field names follow Google's service-account key file format, so they are
// fixed by that format and do not track Publisher's own vocabulary.
type ServiceAccountKey = {
   type?: string;
   project_id?: string;
   private_key?: string;
   client_email?: string;
   [key: string]: unknown;
};

export function parseServiceAccountKey(
   json?: string,
): ServiceAccountKey | undefined {
   if (!json) return undefined;
   const keyData = JSON.parse(json) as ServiceAccountKey;
   const requiredFields = ["type", "project_id", "private_key", "client_email"];
   for (const field of requiredFields) {
      if (!keyData[field]) {
         throw new Error(
            `Invalid service account key: missing "${field}" field`,
         );
      }
   }
   if (keyData.type !== "service_account") {
      throw new Error('Invalid service account key: incorrect "type" field');
   }
   return keyData;
}

/**
 * The pg connectionString query params for a direct connection's own sslmode.
 * Same meanings as the proxied query path (buildProxiedSslQuery): `no-verify`
 * encrypts without verifying, `verify-ca` pins the chain to NODE_EXTRA_CA_CERTS
 * without the hostname (libpq-compatible parsing is what makes pg honor that),
 * and `verify-full` checks the chain and the real hostname.
 */
function directSslParams(
   sslmode: (typeof PROXIED_SSLMODES)[number],
): Record<string, string> {
   switch (sslmode) {
      case "disable":
      case "no-verify":
      case "verify-full":
         return { sslmode };
      case "verify-ca":
         return {
            uselibpqcompat: "true",
            sslmode: "verify-ca",
            sslrootcert: process.env.NODE_EXTRA_CA_CERTS ?? "",
         };
   }
}

/**
 * The connectionString a postgres connection is opened with, or undefined when
 * the individual host/port/user/password/database fields should be used as-is.
 *
 * A raw `connectionString` always wins. Otherwise the sslmode is the
 * connection's own `sslmode` when `applySslmode` is set and the field is
 * present, else the deployment PGSSLMODE, else none. `applySslmode` is false
 * for a proxied connection, which applies its sslmode on the tunnel instead
 * and must not have it baked into a string that targets the real host.
 */
export function buildPostgresConnectionString(
   config: components["schemas"]["PostgresConnection"],
   { applySslmode }: { applySslmode: boolean },
): string | undefined {
   if (config.connectionString) {
      return config.connectionString;
   }

   const params = new URLSearchParams();
   if (applySslmode && config.sslmode != null) {
      for (const [key, value] of Object.entries(
         directSslParams(config.sslmode),
      )) {
         params.set(key, value);
      }
   } else if (process.env.PGSSLMODE) {
      params.set("sslmode", process.env.PGSSLMODE);
   } else {
      return undefined;
   }
   const auth =
      config.userName && config.password
         ? `${encodeURIComponent(config.userName)}:${encodeURIComponent(
              config.password,
           )}@`
         : config.userName
           ? `${encodeURIComponent(config.userName)}@`
           : "";
   const host = config.host ?? "localhost";
   const port = config.port ? `:${config.port}` : "";
   const database = config.databaseName
      ? `/${encodeURIComponent(config.databaseName)}`
      : "";
   return `postgresql://${auth}${host}${port}${database}?${params.toString()}`;
}

function buildDuckdbEntry(
   name: string,
   environmentPath: string,
   databaseFilename = `${name}.duckdb`,
   setupSQL?: string,
): CoreConnectionEntry {
   const entry: CoreConnectionEntry = {
      is: "duckdb",
      databasePath: path.join(environmentPath, databaseFilename),
   };
   if (typeof setupSQL === "string" && setupSQL.trim().length > 0) {
      entry.setupSQL = setupSQL;
   }
   return entry;
}

/**
 * Report a connection default that will not do what it says — a property name
 * the contract rejects, one BigQuery would drop, a bag with no room for the
 * server's own context.
 *
 * Warns rather than throws, unlike everything else in this file: query metadata
 * is observability, and an environment that refuses to load because a tag has a
 * hyphen in it would trade a missing label for an outage. The connection update
 * API rejects the same bag outright (see validateAdminAuthoredConnection) —
 * strict where a human is waiting, lenient where a config is being loaded.
 */
function warnOnConnectionQueryMetadata(connection: ApiConnection): void {
   let declared = 0;
   for (const field of ["queryMetadata", "queryMetadataEnforced"] as const) {
      const metadata = connection[field];
      if (!metadata) continue;
      declared += Object.keys(metadata).length;
      const problems = [
         ...queryMetadataViolations(metadata),
         ...queryMetadataAdvisoryWarnings(metadata),
      ];
      for (const problem of problems) {
         logger.warn("Connection query metadata will not apply as declared", {
            connectionName: connection.name,
            field,
            problem,
         });
      }
   }
   // The budget is checked over BOTH maps: they merge into one bag, so 6
   // defaults plus 6 enforced can be over it while neither map is.
   const overBudget = queryMetadataBudgetWarning(declared);
   if (overBudget) {
      logger.warn("Connection query metadata will not apply as declared", {
         connectionName: connection.name,
         problem: overBudget,
      });
   }
}

function validateProxyShape(
   connection: ApiConnection,
   proxy: NonNullable<ApiConnection["proxy"]>,
): void {
   // A connection proxy makes THIS server open an outbound SSH tunnel to a
   // tenant-configured bastion. It is a normal connection capability,
   // deliberately NOT gated by an env flag, and separate from the `publisher`
   // type's PUBLISHER_ALLOW_PROXY_CONNECTIONS gate (publisher-to-publisher
   // proxying is a distinct operator decision). Host-key pinning is fail-closed
   // at connect time unless PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY is set (see
   // openProxy). Proxy fields are validated here so a permanent misconfig fails
   // at config load, not by repeatedly dialing the bastion at query time.
   if (proxy.type !== "ssh") {
      throw new Error(
         `Connection '${connection.name}' has an unsupported proxy type '${proxy.type}'. Only 'ssh' is supported.`,
      );
   }
   if (connection.type !== "postgres") {
      throw new Error(
         `Connection proxy is not supported for type '${connection.type}' (only 'postgres' today).`,
      );
   }
   if (!proxy.ssh) {
      throw new Error(
         `Connection proxy on '${connection.name}' has type 'ssh' but no 'ssh' config object.`,
      );
   }
   // The tunnel forwards to an explicit host:port, and a connectionString
   // cannot be rewritten to the local endpoint. A config carrying both would
   // tunnel to host/port and ignore the connectionString (which normally takes
   // precedence), reaching a different database than configured.
   if (connection.postgresConnection?.connectionString) {
      throw new Error(
         `Connection proxy on '${connection.name}' does not support the connectionString form; ` +
            `provide discrete host and port instead (the tunnel forwards to an explicit endpoint).`,
      );
   }
   if (
      !connection.postgresConnection?.host ||
      !connection.postgresConnection?.port
   ) {
      throw new Error(
         `Connection proxy on '${connection.name}' requires explicit host and port on the ` +
            `postgres connection; the connectionString form is not supported with a proxy.`,
      );
   }

   // An omitted or empty hostKey is unpinned, which the tunnel refuses at
   // connect time unless PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY is set. A
   // non-empty hostKey that parses to zero keys (only blanks or `#` comments,
   // e.g. just ssh-keyscan's header) is a misconfigured pin and is rejected.
   // Truthiness, not trim(): "" means unpinned; "   " must yield a key.
   const hostKey = proxy.ssh?.hostKey;
   // The tunnel is dialed lazily, so an unpinned connection would otherwise be
   // discovered only by a failing query. Warn rather than throw: refusing to
   // start over a connection nobody may use would be worse.
   // Config load can throw, so a misspelled opt-in (`=ture`) is caught here
   // rather than read as "off"; the connect-time check in proxy.ts falls back
   // instead (see allowUnverifiedHostKey).
   parseBoolEnv(ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV);
   if (!hostKey && !allowUnverifiedHostKey()) {
      logger.warn(
         `Connection proxy on '${connection.name}' pins no SSH host key, so the tunnel will ` +
            `be refused when a query first uses it. Set ssh.hostKey to the bastion's host ` +
            `key, or set ${ALLOW_UNVERIFIED_SSH_HOST_KEY_ENV}=true to accept an unverified ` +
            `key for this deployment.`,
      );
   }
   if (hostKey && parseHostKeys(hostKey).size === 0) {
      throw new Error(
         `Connection proxy on '${connection.name}' has a hostKey with no usable host-key line ` +
            `(only blanks/comments). Provide an OpenSSH known_hosts line or base64 blob, or omit ` +
            `hostKey to connect unpinned (which the tunnel refuses unless ` +
            `PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY is set).`,
      );
   }

   // A failed tunnel build is retried on every query, so a bad sslmode left to
   // connect time would re-dial the bastion indefinitely. `!= null`, not
   // truthiness, so an empty sslmode is rejected here; null/undefined is unset.
   const sslmode = connection.postgresConnection?.sslmode;
   if (sslmode != null) {
      validatePostgresSslmode(
         `Connection proxy on '${connection.name}'`,
         sslmode,
      );
      // No bundle precondition for `verify-full`: it verifies against Node's
      // ambient trust anchors (bundled roots + NODE_EXTRA_CA_CERTS), so a
      // publicly-trusted CA must be accepted. Its CA trust is therefore broader
      // than verify-ca's pinned bundle; unlike libpq, verify-full here is not a
      // strict superset of verify-ca. Intentional.
   }

   // The proxied path builds a connectionString to the tunnel endpoint, and pg
   // decodes the database path with decodeURI, which leaves URI-reserved
   // characters encoded, so such a name would resolve to the wrong database.
   // (user/password use decodeURIComponent and round-trip fine.)
   const dbName = connection.postgresConnection?.databaseName;
   if (dbName && decodeURI(encodeURIComponent(dbName)) !== dbName) {
      throw new Error(
         `Connection proxy on '${connection.name}' has a database name with characters that can't ` +
            `be carried over a proxied connection (${JSON.stringify(dbName)}). Use a database name ` +
            `without URI-reserved characters (; , / ? : @ & = + $ #).`,
      );
   }
}

function validateSqlServerShape(connection: ApiConnection): void {
   const sqlserver = connection.sqlserverConnection;
   if (!sqlserver) {
      throw new Error("SQL Server connection configuration is missing.");
   }
   // The connector reads a non-numeric port as no port and dials 1433, so a
   // typo would surface as a failure naming a port nobody configured.
   const port: unknown = sqlserver.port;
   if (
      port !== undefined &&
      port !== null &&
      (typeof port !== "number" ||
         !Number.isInteger(port) ||
         port < 1 ||
         port > 65535)
   ) {
      throw new Error(
         `Connection '${connection.name}' has an invalid sqlserverConnection.port ` +
            `${JSON.stringify(port)} (expected an integer from 1 to 65535).`,
      );
   }
   // The driver takes the instance name over the port when given both.
   if (sqlserver.instanceName && sqlserver.port) {
      logger.warn(
         `Connection '${connection.name}' sets both sqlserverConnection.instanceName ` +
            `and port; the instance is located through SQL Browser and the port is not used.`,
      );
   }
}

function validateBigQueryShape(connection: ApiConnection): void {
   const bigquery = connection.bigqueryConnection;
   if (bigquery?.impersonateServiceAccount) {
      // An authClient replaces the credential entirely, so a key alongside it
      // would look live while the impersonated identity runs every query.
      // Core refuses this too; refusing here makes it a load-time error.
      if (bigquery.serviceAccountKeyJson) {
         throw new Error(
            `Connection '${connection.name}' sets impersonateServiceAccount ` +
               `and serviceAccountKeyJson. Impersonation replaces the ` +
               `credential entirely — the key would be ignored — so supply ` +
               `one or the other.`,
         );
      }
      // With an authClient the SDK resolves the project through the
      // impersonated credential, not ADC, so auto-detection is no fallback.
      if (!bigquery.billingProjectId) {
         throw new Error(
            `Connection '${connection.name}' sets impersonateServiceAccount ` +
               `but no billingProjectId. Impersonated connections resolve ` +
               `the project through the impersonated credential, so the ` +
               `project that runs (and is billed for) jobs must be named ` +
               `explicitly.`,
         );
      }
   }
}

function validateDuckDBShape(connection: ApiConnection): void {
   if (!connection.duckdbConnection) {
      throw new Error("DuckDB connection configuration is missing.");
   }
   const attached = connection.duckdbConnection.attachedDatabases ?? [];
   // AttachedDatabase reuses the BigqueryConnection schema, but the ATTACH
   // path builds a DuckDB BIGQUERY secret from key JSON (the extension takes a
   // key, not a token), so impersonation would be silently ignored.
   for (const attachedDb of attached) {
      if (
         attachedDb.type === "bigquery" &&
         attachedDb.bigqueryConnection?.impersonateServiceAccount
      ) {
         throw new Error(
            `Attached database '${attachedDb.name}' on DuckDB connection ` +
               `'${connection.name}' sets impersonateServiceAccount, which ` +
               `is not supported on attached databases: DuckDB's BIGQUERY ` +
               `secret authenticates with a service account key, not a ` +
               `token. Use serviceAccountKeyJson for attached BigQuery ` +
               `databases.`,
         );
      }
   }
   const setupSQL = connection.duckdbConnection.setupSQL;
   const hasSetupSQL =
      typeof setupSQL === "string" && setupSQL.trim().length > 0;
   if (attached.length === 0 && !hasSetupSQL) {
      throw new Error(
         `DuckDB connection "${connection.name}" must provide either attachedDatabases or non-empty setupSQL.`,
      );
   }
   // Shape only, deliberately not the full credential check: this throw fails
   // the ENTIRE environment (assembleEnvironmentConnections has no
   // per-connection recovery), so demanding a key pair here would turn a
   // config that fails late on one attachment into one that loads nothing.
   // Only shapes no path accepts are rejected.
   for (const attachedDb of attached) {
      if (attachedDb.type === "s3" && attachedDb.s3Connection) {
         validateS3ProviderShape(
            attachedDb.s3Connection,
            attachedDb.name ?? connection.name,
         );
      }
   }
}

function validateMotherDuckShape(connection: ApiConnection): void {
   if (!connection.motherduckConnection) {
      throw new Error("MotherDuck connection configuration is missing.");
   }
   if (!connection.motherduckConnection.accessToken) {
      throw new Error("MotherDuck access token is required.");
   }
}

function validateDuckLakeShape(connection: ApiConnection): void {
   // Every field the ATTACH demands (see attachDuckLakeWithMode). A
   // destination is only attached at its first BUILD, so a field missed here
   // would surface hours after the config change that caused it.
   if (!connection.ducklakeConnection) {
      throw new Error("DuckLake connection configuration is missing.");
   }
   if (!connection.ducklakeConnection.catalog?.postgresConnection) {
      throw new Error(
         `PostgreSQL connection configuration is required for DuckLake catalog: ${connection.name}`,
      );
   }
   if (!connection.ducklakeConnection.storage?.bucketUrl) {
      throw new Error(
         `Storage bucketUrl is required for DuckLake: ${connection.name}`,
      );
   }
   // Shape only, as in the duckdb branch: a throw here fails the ENTIRE
   // environment. The mutual-exclusion and enum guards are safe; the key-pair
   // requirement is NOT applied, because failing every other connection over
   // an incomplete s3Connection is worse than reporting it when used.
   // validateStorageDestinations rejects one destination at a time, so it
   // applies the full check.
   const storage = connection.ducklakeConnection.storage;
   if (storage.s3Connection) {
      validateS3ProviderShape(storage.s3Connection, connection.name);
   }
   // metadataSchema reaches the ATTACH as a quoted string literal AND the
   // catalog-format preflight as a quoted identifier. Rather than escape one
   // value for two grammars, restrict it to a plain identifier.
   //
   // The typeof check is load-bearing: the value comes from untyped JSON and
   // RegExp.test() coerces, so `true` or `null` would pass the pattern and
   // reach escapeSQL as a non-string, a TypeError at the first attach.
   if (connection.ducklakeConnection.catalog.metadataSchema !== undefined) {
      const schema = connection.ducklakeConnection.catalog.metadataSchema;
      if (
         typeof schema !== "string" ||
         !/^[A-Za-z_][A-Za-z0-9_]*$/.test(schema)
      ) {
         throw new Error(
            `DuckLake catalog metadataSchema must be a plain identifier ` +
               `([A-Za-z_][A-Za-z0-9_]*), got '${schema}' for connection: ` +
               `${connection.name}`,
         );
      }
   }
   // destinationSchema becomes part of every stored table's path, quoted as an
   // identifier by quoteTablePath; a plain identifier keeps that one grammar.
   const destinationSchema = connection.ducklakeConnection.destinationSchema;
   if (destinationSchema !== undefined && destinationSchema !== null) {
      if (
         typeof destinationSchema !== "string" ||
         !/^[A-Za-z_][A-Za-z0-9_]*$/.test(destinationSchema)
      ) {
         throw new Error(
            `DuckLake destinationSchema must be a plain identifier ` +
               `([A-Za-z_][A-Za-z0-9_]*), got '${destinationSchema}' for connection: ` +
               `${connection.name}`,
         );
      }
   }
}

function validateTrinoShape(connection: ApiConnection): void {
   if (!connection.trinoConnection) {
      throw new Error("Trino connection configuration is missing.");
   }
}

function validateDatabricksShape(connection: ApiConnection): void {
   const databricks = connection.databricksConnection;
   if (!databricks) {
      throw new Error("Databricks connection configuration is missing.");
   }
   if (!databricks.host) {
      throw new Error("Databricks host is required.");
   }
   if (!databricks.path) {
      throw new Error("Databricks SQL warehouse HTTP path is required.");
   }
   const hasToken = !!databricks.token;
   const hasOAuth =
      !!databricks.oauthClientId && !!databricks.oauthClientSecret;
   if (!hasToken && !hasOAuth) {
      throw new Error(
         "Databricks requires either a personal access token or OAuth M2M client ID and secret.",
      );
   }
   const hasDefaultCatalog = !!databricks.defaultCatalog;
   if (!hasDefaultCatalog) {
      throw new Error("Databricks default catalog is required.");
   }
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

function validatePublisherShape(connection: ApiConnection): void {
   // SSRF gate (default-deny, fail-closed). A `publisher` connection makes
   // THIS server issue outbound HTTP to a tenant-controlled `connectionUri`
   // (the query path in db-publisher and the introspection path in db_utils).
   // Intended for local `--watch-env` authoring, but an SSRF surface in a
   // hosted multi-tenant deployment, so it needs an explicit opt-in. Every
   // connection passes validateConnectionShape before it can be assembled,
   // queried, or introspected, so denying here shuts off all three.
   if (process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS !== "true") {
      throw new Error(
         `Publisher proxy connection '${connection.name}' is disabled in this deployment. ` +
            `'publisher' connections make the server issue outbound requests to a configured connectionUri, ` +
            `which is only appropriate for local --watch-env authoring. ` +
            `Fix: set the environment variable PUBLISHER_ALLOW_PROXY_CONNECTIONS=true to enable them.`,
      );
   }
   const publisher = connection.publisherConnection;
   if (!publisher?.connectionUri) {
      throw new Error(
         `Invalid publisher connection '${connection.name}': missing connectionUri. ` +
            `Fix: { "name": "${connection.name}", "type": "publisher", ` +
            `"publisherConnection": { "connectionUri": "https://…/connections/${connection.name}", "accessToken": "<jwt>" } }`,
      );
   }
   // Reject a malformed connectionUri at config load: deep in the request
   // path the error can echo the raw value, leaking embedded credentials
   // (`redactUrlCredentials` returns an unparseable URI unchanged). Never
   // include the raw connectionUri in these messages; the scheme is safe.
   let parsedUri: URL;
   try {
      parsedUri = new URL(publisher.connectionUri);
   } catch {
      throw new Error(
         `Invalid publisher connection '${connection.name}': connectionUri is not a valid URL. ` +
            `Fix: set connectionUri to an absolute https URL like "https://…/connections/${connection.name}".`,
      );
   }
   if (parsedUri.protocol !== "http:" && parsedUri.protocol !== "https:") {
      throw new Error(
         `Invalid publisher connection '${connection.name}': connectionUri must use http or https (got '${parsedUri.protocol}'). ` +
            `Fix: set connectionUri to an absolute https URL like "https://…/connections/${connection.name}".`,
      );
   }
}

/**
 * Type-specific shape checks; postgres and mysql have none of their own. A
 * switch rather than a lookup table, so a caller-supplied type selects only a
 * function named here.
 */
function validateTypeShape(connection: ApiConnection): void {
   switch (connection.type) {
      case "sqlserver":
         return validateSqlServerShape(connection);
      case "bigquery":
         return validateBigQueryShape(connection);
      case "duckdb":
         return validateDuckDBShape(connection);
      case "motherduck":
         return validateMotherDuckShape(connection);
      case "ducklake":
         return validateDuckLakeShape(connection);
      case "trino":
         return validateTrinoShape(connection);
      case "databricks":
         return validateDatabricksShape(connection);
      case "snowflake":
         return validateSnowflakeShape(connection);
      case "publisher":
         return validatePublisherShape(connection);
   }
}

function validateConnectionShape(connection: ApiConnection): void {
   if (connection.proxy) {
      validateProxyShape(connection, connection.proxy);
   }

   // A direct connection's sslmode, when set, replaces the deployment
   // PGSSLMODE. A connectionString carries its own sslmode, so alongside one
   // the field is ignored; warn once per config load.
   if (!connection.proxy && connection.postgresConnection?.sslmode != null) {
      validatePostgresSslmode(
         `Connection '${connection.name}'`,
         connection.postgresConnection.sslmode,
      );
      if (connection.postgresConnection.connectionString) {
         logger.warn(
            `Connection '${connection.name}' sets both postgresConnection.sslmode and ` +
               `connectionString; sslmode is ignored and the connectionString's own sslmode applies.`,
         );
      }
   }
   if (connection.postgresConnection) {
      postgresStatementTimeoutMs(
         connection.name ?? "",
         connection.postgresConnection,
      );
   }

   validateTypeShape(connection);
}

/**
 * Warehouse types a storage destination may be DECLARED as. Deliberately
 * narrower than the connection types: a destination is attached read-write by
 * the build path, and every type admitted here is another way to define a
 * warehouse that no connection endpoint audits.
 *
 * Also narrower than `STORAGE_DESTINATION_TYPES` in
 * materialization_build_session, which accepts `duckdb` too; this set is the
 * only way a destination is configured, so widen it deliberately.
 */
export const DECLARABLE_STORAGE_DESTINATION_TYPES: ReadonlySet<string> =
   new Set(["ducklake"]);

/**
 * Subdirectory of the environment root holding the local DuckDB files of
 * storage destinations, keeping them out of the directory connection
 * files derive into.
 *
 * Not cosmetic. A DuckDB instance is pooled by `databasePath` +
 * `workingDirectory`; the share key excludes the connection name
 * (`buildDuckDBShareKey`), and both lists derive the path as
 * `<root>/<name>…duckdb`. A connection and a destination may share a name, so
 * without separate roots they would share one pooled instance, and
 * `ATTACH OR REPLACE … AS <name>` would let one replace the other's attach and
 * a query read across the two. (If malloydata/malloy#3006 changes the share
 * key, re-check that it still excludes the name.)
 *
 * Dot-prefixed like `.staging`/`.retired`, so a walk that enumerates an
 * environment for package trees cannot mistake it for one.
 *
 * Deliberately asymmetric: a CONNECTION's local DuckDB file still derives into
 * the environment root. Moving it would change every deployment's pooled
 * instance identity, re-attaching each connection against a new empty local
 * primary and orphaning the old file.
 */
export const STORAGE_DESTINATIONS_DIR = ".storage-destinations";

/** Where {@link STORAGE_DESTINATIONS_DIR} sits for an environment. */
export function storageDestinationRoot(environmentPath: string): string {
   return path.join(environmentPath, STORAGE_DESTINATIONS_DIR);
}

/** An entry of a `storageDestinations` list that cannot be used, and why. */
export type RejectedStorageDestination = {
   /** Absent when the entry carried no usable name to attribute the defect to. */
   name?: string;
   /** The defect, as a sentence naming what is wrong with the entry. */
   reason: string;
};

/**
 * Sorts a `storageDestinations` list into the entries that may be built into and
 * served from, and the entries that cannot be used. The one place destinations
 * are checked, so no caller can seat an unvalidated destination on an
 * Environment.
 *
 * Names are unique within the list and are NOT checked against the
 * environment's connections. The two lists are separate namespaces: one
 * environment may hold a connection and a destination that share a name and
 * mean different warehouses, and both must keep working.
 *
 * Reports what it rejected rather than deciding what that means: a list read
 * from config or from storage drops the bad entry and keeps serving
 * ({@link processStorageDestinations}), while a list that arrived on a request
 * body is refused whole ({@link processStorageDestinationsOrThrow}). Dropping is
 * safe because it is not a substitution: build and serve resolve destinations
 * only through this list, so a `storage=` build naming a dropped destination
 * fails rather than falling through to a same-named connection.
 */
export function validateStorageDestinations(
   destinations: ApiConnection[] = [],
): {
   accepted: ApiConnection[];
   rejected: RejectedStorageDestination[];
} {
   const accepted: ApiConnection[] = [];
   const rejected: RejectedStorageDestination[] = [];

   if (!Array.isArray(destinations)) {
      return {
         accepted,
         rejected: [{ reason: "the value is not a list of destinations." }],
      };
   }

   const seen = new Set<string>();

   for (const destination of destinations) {
      if (!destination || typeof destination !== "object") {
         rejected.push({ reason: "the entry is not an object." });
         continue;
      }

      const { name, type } = destination;
      if (!name || typeof name !== "string") {
         rejected.push({
            reason: `the "name" field is missing or not a string.`,
         });
         continue;
      }
      if (!type || !DECLARABLE_STORAGE_DESTINATION_TYPES.has(type)) {
         rejected.push({
            name,
            reason:
               `the type "${type}" is not a supported destination type ` +
               `(supported: ${[...DECLARABLE_STORAGE_DESTINATION_TYPES].join(", ")}).`,
         });
         continue;
      }
      // A destination named `duckdb` could never be reached: the package
      // connection lookup answers that name with the per-package :memory:
      // sandbox before any environment list is consulted, so a serve compile
      // would read an empty sandbox instead of the destination.
      if (name === "duckdb") {
         rejected.push({
            name,
            reason: `the name "duckdb" is reserved for per-package sandboxes.`,
         });
         continue;
      }
      if (seen.has(name)) {
         rejected.push({
            name,
            reason: "an earlier entry already uses this name.",
         });
         continue;
      }

      try {
         validateConnectionShape(destination);
         // The full credential check, which validateConnectionShape cannot
         // afford (its throw fails a whole environment): a destination is only
         // attached at its first BUILD, so a missing credential would surface
         // hours after the config change. Both arms, in the attach path's
         // precedence (attachDuckLakeWithMode takes s3 over gcs).
         const storage = destination.ducklakeConnection?.storage;
         if (storage?.s3Connection) {
            resolveCloudStorageCredentials(
               {
                  name,
                  type: "s3",
                  s3Connection: storage.s3Connection,
               },
               // The OPERATOR's own configuration, not an author's: a managed
               // storage tier running on the host's identity is the intended use
               // of credential_chain, so `destinations-only` permits it here.
               "storage-destination",
            );
         } else if (storage?.gcsConnection) {
            resolveCloudStorageCredentials({
               name,
               type: "gcs",
               gcsConnection: storage.gcsConnection,
            });
         }
      } catch (error) {
         rejected.push({ name, reason: (error as Error).message });
         continue;
      }

      seen.add(name);
      // `resource` addresses a connection endpoint. A destination has none, and
      // carrying the field would make it look addressable to anything that
      // reads one.
      const { resource: _resource, ...body } = destination;
      accepted.push(body);
   }

   return { accepted, rejected };
}

/** One rejection, as a phrase that reads inside a longer sentence. */
function describeRejection(rejection: RejectedStorageDestination): string {
   return rejection.name
      ? `storage destination "${rejection.name}": ${rejection.reason}`
      : `storage destination: ${rejection.reason}`;
}

/**
 * The usable entries of a list whose source cannot be asked to fix it — the
 * config file, or the rows restored from storage. An unusable entry is dropped
 * with a warning rather than failing the whole environment, so one malformed
 * destination does not take a tenant's packages offline.
 */
export function processStorageDestinations(
   destinations: ApiConnection[] = [],
): ApiConnection[] {
   const { accepted, rejected } = validateStorageDestinations(destinations);
   for (const rejection of rejected) {
      logger.warn(`Invalid ${describeRejection(rejection)} Skipping.`);
   }
   return accepted;
}

/**
 * The entries of a list that arrived on a request body, or a `BadRequestError`
 * naming every defect. Nothing is applied unless the whole list is understood:
 * the list has replace semantics and the caller's set becomes the set to
 * reconcile stored rows against, so quietly dropping the part we could not read
 * would un-register destinations the caller believes it just re-affirmed. A
 * caller can be told, and can fix it.
 *
 * The rejection reasons reach that caller in an HTTP body, so a reason must name
 * the defect without quoting a credential. The ones assembled here are literals,
 * and the validator's own messages interpolate only a name, a type, or the
 * offending identifier — keep it that way when adding one.
 */
export function processStorageDestinationsOrThrow(
   destinations: ApiConnection[] = [],
): ApiConnection[] {
   const { accepted, rejected } = validateStorageDestinations(destinations);
   if (rejected.length > 0) {
      throw new BadRequestError(
         `storageDestinations was not accepted — ${rejected
            .map(describeRejection)
            .join(" ")}`,
      );
   }
   return accepted;
}

/**
 * Whether two validated destination lists describe the same set of
 * destinations, so a caller re-pushing its desired state can be recognized as a
 * no-op instead of swapping every destination and dropping the serve shapes
 * compiled against them.
 *
 * Neither the order a caller listed the destinations in nor the key order inside
 * a config carries meaning: the list is a set keyed by name, and a config is
 * plain JSON that may have been assembled or parsed in any order. Both are
 * normalized away, so equality is decided by content alone.
 *
 * Compares every field rather than a chosen subset. A destination's config is
 * what the build attaches — a different bucket, a different catalog host, a
 * rotated credential — so anything less would let a real change go unapplied.
 */
export function storageDestinationsEqual(
   left: ApiConnection[],
   right: ApiConnection[],
): boolean {
   if (left.length !== right.length) {
      return false;
   }
   return canonicalDestinationList(left) === canonicalDestinationList(right);
}

/**
 * Content-determined, order-independent serialization of a destination list.
 * Sorted by code unit rather than by collation, which is a strict total order on
 * the distinct names a validated list holds and does not vary with the locale the
 * process happens to run under.
 */
function canonicalDestinationList(destinations: ApiConnection[]): string {
   return JSON.stringify(
      [...destinations]
         .sort((left, right) =>
            (left.name ?? "") < (right.name ?? "") ? -1 : 1,
         )
         .map(canonicalizeJsonValue),
   );
}

/**
 * Rewrites a JSON value with every object's keys in sorted order, so
 * `JSON.stringify` of the result depends only on content. Arrays keep their
 * order: within a connection config an array's order is part of its meaning.
 */
function canonicalizeJsonValue(value: unknown): unknown {
   if (Array.isArray(value)) {
      return value.map(canonicalizeJsonValue);
   }
   if (value === null || typeof value !== "object") {
      return value;
   }
   const entries = value as Record<string, unknown>;
   const sorted: Record<string, unknown> = {};
   for (const key of Object.keys(entries).sort()) {
      sorted[key] = canonicalizeJsonValue(entries[key]);
   }
   return sorted;
}

/** An optional string field, with the empty string a form sends for "not set" read as unset. */
function blankAsUnset(value: string | undefined): string | undefined {
   return value === "" ? undefined : value;
}

function buildPostgresEntry(connection: NamedConnection): CoreConnectionEntry {
   const postgresConnection = connection.postgresConnection;
   return {
      is: "postgres",
      host: postgresConnection?.host,
      port: postgresConnection?.port,
      username: postgresConnection?.userName,
      password: postgresConnection?.password,
      databaseName: postgresConnection?.databaseName,
      connectionString: postgresConnection
         ? buildPostgresConnectionString(postgresConnection, {
              applySslmode: !connection.proxy,
           })
         : undefined,
   };
}

function buildMySQLEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "mysql",
      host: connection.mysqlConnection?.host,
      port: connection.mysqlConnection?.port,
      user: connection.mysqlConnection?.user,
      password: connection.mysqlConnection?.password,
      database: connection.mysqlConnection?.database,
   };
}

function buildSqlServerEntry(connection: NamedConnection): CoreConnectionEntry {
   // A pass-through: the API field names are the connector's own. The two
   // optional names are read as unset when blank: the connector drops the
   // port whenever instanceName is DEFINED, so "" would send the driver to SQL
   // Browser for an instance called nothing, and a "" certificate name would
   // be checked against the certificate.
   const sqlserver = connection.sqlserverConnection;
   return {
      is: "sqlserver",
      server: sqlserver?.server,
      port: sqlserver?.port,
      database: sqlserver?.database,
      user: sqlserver?.user,
      password: sqlserver?.password,
      encrypt: sqlserver?.encrypt,
      trustServerCertificate: sqlserver?.trustServerCertificate,
      hostNameInCertificate: blankAsUnset(sqlserver?.hostNameInCertificate),
      instanceName: blankAsUnset(sqlserver?.instanceName),
   };
}

function buildBigQueryEntry(connection: NamedConnection): CoreConnectionEntry {
   const serviceAccountKey = parseServiceAccountKey(
      connection.bigqueryConnection?.serviceAccountKeyJson as
         | string
         | undefined,
   );
   // Impersonation rides the config-overlay mechanism: the pojo carries a
   // plain-JSON reference, which the `gcpImpersonation` overlay registered by
   // buildEnvironmentMalloyConfig resolves to a live auth client. The property
   // is opaque, overlay-sourced and mustHaveValue, so a literal can never
   // satisfy it and an unresolved reference errors instead of falling back to
   // ambient ADC. The reference embeds the SA email, so core's connection
   // digest (and so the BuildID) is per identity.
   const impersonateServiceAccount =
      connection.bigqueryConnection?.impersonateServiceAccount;
   return {
      is: "bigquery",
      projectId:
         connection.bigqueryConnection?.defaultProjectId ||
         serviceAccountKey?.project_id,
      serviceAccountKey,
      // Spread rather than `authClient: x ?? undefined`: the property is
      // mustHaveValue and core checks whether it is SET, so an undefined
      // entry would fail every plain connection with "no value arrived".
      ...(impersonateServiceAccount
         ? {
              authClient: {
                 gcpImpersonation: impersonateServiceAccount,
              },
           }
         : {}),
      location: connection.bigqueryConnection?.location,
      maximumBytesBilled: connection.bigqueryConnection?.maximumBytesBilled,
      timeoutMs: connection.bigqueryConnection?.queryTimeoutMilliseconds,
      billingProjectId: connection.bigqueryConnection?.billingProjectId,
   };
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
      // nullToUndefined). Defense in depth: Malloy's connection lookup drops
      // nulls today, but that guard is not a contract and the dependency is a
      // caret range. The key-pair path that bypasses the lookup is handled by
      // `removeUndefined` in connection.ts.
      database: nullToUndefined(connection.snowflakeConnection?.database),
      schema: nullToUndefined(connection.snowflakeConnection?.schema),
      role: nullToUndefined(connection.snowflakeConnection?.role),
      timeoutMs: connection.snowflakeConnection?.responseTimeoutMilliseconds,
      // Pool sizing is server-owned policy, not exposed through the API.
      poolMin: 1,
      poolMax: 20,
   };
}

function buildTrinoEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "trino",
      ...validateAndBuildTrinoCoreConfig(connection.trinoConnection),
   };
}

function buildDatabricksEntry(
   connection: NamedConnection,
): CoreConnectionEntry {
   const databricks = connection.databricksConnection;
   return {
      is: "databricks",
      host: databricks?.host,
      path: databricks?.path,
      token: databricks?.token,
      oauthClientId: databricks?.oauthClientId,
      oauthClientSecret: databricks?.oauthClientSecret,
      defaultCatalog: databricks?.defaultCatalog,
      defaultSchema: databricks?.defaultSchema,
      setupSQL: databricks?.setupSQL,
   };
}

function buildDuckDBEntry(
   connection: NamedConnection,
   environmentPath: string,
): CoreConnectionEntry {
   const attachedDatabases =
      connection.duckdbConnection?.attachedDatabases ?? [];
   if (
      attachedDatabases.some((database) => database.name === connection.name)
   ) {
      throw new Error(
         `DuckDB attached database names cannot conflict with connection name ${connection.name}`,
      );
   }
   return buildDuckdbEntry(
      connection.name,
      environmentPath,
      `${connection.name}.duckdb`,
      connection.duckdbConnection?.setupSQL,
   );
}

function buildMotherDuckEntry(
   connection: NamedConnection,
): CoreConnectionEntry {
   if (!connection.motherduckConnection?.accessToken) {
      throw new Error("MotherDuck access token is required.");
   }

   return {
      is: "duckdb",
      databasePath: connection.motherduckConnection.database
         ? `md:${connection.motherduckConnection.database}?attach_mode=single`
         : "md:",
      motherDuckToken: connection.motherduckConnection.accessToken,
   };
}

function buildDuckLakeEntry(
   connection: NamedConnection,
   environmentPath: string,
): CoreConnectionEntry {
   // Shape is validated up front by validateConnectionShape.
   return buildDuckdbEntry(
      connection.name,
      environmentPath,
      `${connection.name}_ducklake.duckdb`,
   );
}

function buildPublisherEntry(connection: NamedConnection): CoreConnectionEntry {
   // connectionUri presence is validated by validateConnectionShape. The
   // proxied dataplane owns auth and read-only enforcement; PublisherConnection
   // itself does not reject writes. The dialect is the remote connection's,
   // resolved at runtime, so getStaticConnectionAttributes returns undefined.
   const publisher = connection.publisherConnection!;
   return {
      is: "publisher",
      connectionUri: publisher.connectionUri,
      accessToken: publisher.accessToken,
   };
}

/** A connection past assembly's name check. */
type NamedConnection = ApiConnection & { name: string };

/**
 * The Malloy core connection entry a connection assembles into, by type. A
 * switch rather than a lookup table, so a caller-supplied type selects only a
 * builder named here.
 */
function buildCoreEntry(
   connection: NamedConnection,
   environmentPath: string,
): CoreConnectionEntry {
   switch (connection.type) {
      case "postgres":
         return buildPostgresEntry(connection);
      case "mysql":
         return buildMySQLEntry(connection);
      case "sqlserver":
         return buildSqlServerEntry(connection);
      case "bigquery":
         return buildBigQueryEntry(connection);
      case "snowflake":
         return buildSnowflakeEntry(connection);
      case "trino":
         return buildTrinoEntry(connection);
      case "databricks":
         return buildDatabricksEntry(connection);
      case "duckdb":
         return buildDuckDBEntry(connection, environmentPath);
      case "motherduck":
         return buildMotherDuckEntry(connection);
      case "ducklake":
         return buildDuckLakeEntry(connection, environmentPath);
      case "publisher":
         return buildPublisherEntry(connection);
      default:
         throw new Error(`Unsupported connection type: ${connection.type}`);
   }
}

export function assembleEnvironmentConnections(
   connections: ApiConnection[] = [],
   environmentPath = "",
): AssembledEnvironmentConnections {
   const pojo: CoreConnectionsPojo = { connections: {} };
   const metadata = new Map<string, EnvironmentConnectionMetadata>();
   const apiConnections: ApiConnection[] = [];
   const processedConnections = new Set<string>();

   for (const connection of connections) {
      if (!connection.name) {
         throw new Error("Invalid connection configuration. No name.");
      }

      if (processedConnections.has(connection.name)) {
         continue;
      }

      if (connection.name === "duckdb") {
         throw new Error(
            "Connection name 'duckdb' is reserved for per-package sandboxes. Choose a different name for environment-level DuckDB connections (e.g. 'shared_duckdb').",
         );
      }

      processedConnections.add(connection.name);
      validateDuckdbApiSurface(connection);
      validateConnectionShape(connection);
      warnOnConnectionQueryMetadata(connection);

      const apiConnection = cloneApiConnection(connection);
      apiConnection.attributes = getStaticConnectionAttributes(connection.type);
      const attachedDatabases =
         connection.duckdbConnection?.attachedDatabases ?? [];
      const isDuckLake = connection.type === "ducklake";
      const isDuckdb = connection.type === "duckdb";
      const databasePath = isDuckLake
         ? path.join(environmentPath, `${connection.name}_ducklake.duckdb`)
         : isDuckdb
           ? path.join(environmentPath, `${connection.name}.duckdb`)
           : undefined;

      metadata.set(connection.name, {
         apiConnection,
         attachedDatabases,
         hasAzureAttachment: attachedDatabases.some(
            (database) => database.type === "azure",
         ),
         hasSnowflakePrivateKey:
            connection.type === "snowflake" &&
            !!connection.snowflakeConnection?.privateKey,
         isDuckLake,
         databasePath,
         workingDirectory: environmentPath,
         proxy: connection.proxy,
      });

      pojo.connections[connection.name] = buildCoreEntry(
         connection as NamedConnection,
         environmentPath,
      );

      apiConnections.push(apiConnection);
   }

   return { pojo, metadata, apiConnections };
}

function validateAndBuildTrinoCoreConfig(
   trinoConfig: components["schemas"]["TrinoConnection"] | undefined,
): Record<string, unknown> {
   if (!trinoConfig) {
      return {};
   }

   const server =
      trinoConfig.server && trinoConfig.port
         ? trinoConfig.server.includes(trinoConfig.port.toString())
            ? trinoConfig.server
            : `${trinoConfig.server}:${trinoConfig.port}`
         : trinoConfig.server;

   const baseConfig: Record<string, unknown> = {
      server,
      port: trinoConfig.port,
      catalog: trinoConfig.catalog,
      schema: trinoConfig.schema,
      user: trinoConfig.user,
   };

   if (trinoConfig.peakaKey) {
      baseConfig.extraCredential = {
         peakaKey: trinoConfig.peakaKey,
      };
      return baseConfig;
   }

   if (server?.startsWith("https://") && trinoConfig.password) {
      baseConfig.password = trinoConfig.password;
   }

   if (server?.startsWith("http://") || server?.startsWith("https://")) {
      return baseConfig;
   }

   throw new Error(
      `Invalid Trino connection: expected "http://server:port" or "https://server:port".`,
   );
}
