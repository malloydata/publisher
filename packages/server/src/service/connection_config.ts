// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { settings } from "../settings";
import path from "path";
import { components } from "../api";
import {
   ALLOW_DUCKDB_SETUP_SQL_ENV,
   ConnectionOrigin,
   getExtensionFetchPolicy,
   isDuckdbSetupSqlAllowed,
} from "../config";
import { BadRequestError } from "../errors";
import {
   postgresStatementTimeoutMs,
   validatePostgresSslmode,
} from "./postgres_conninfo";
import { connectionDialectOf } from "./dialects";
import { configBoolean } from "./dialects/bigquery";
import type { CoreConnectionEntry, NamedConnection } from "./dialects/types";
import { logger } from "../logger";
import { resolveCloudStorageCredentials } from "./gcs_s3_utils";
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

export type CoreConnectionsPojo = {
   connections: Record<string, CoreConnectionEntry>;
};

export type EnvironmentConnectionMetadata = {
   apiConnection: ApiConnection;
   attachedDatabases: AttachedDatabase[];
   hasAzureAttachment: boolean;
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
 * The pattern `AttachedDatabase.name` documents (api-doc.yaml): a letter or
 * underscore, then letters, digits and underscores. The name becomes a DuckDB
 * catalog alias and part of a secret's name, so it is held to it wherever a
 * connection's config is accepted.
 */
export const ATTACHED_DATABASE_NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

/**
 * The shape an S3 region takes (`us-east-1`, an S3-compatible store's own
 * name). The region becomes a string literal in the attach's secret, so it is
 * held to it wherever a connection's config is accepted, as the name is.
 */
export const S3_REGION_PATTERN = /^[a-zA-Z0-9_-]+$/;

/** Throws unless `region`, when set, matches {@link S3_REGION_PATTERN}. */
function assertS3Region(
   region: unknown,
   field: string,
   connectionName: string | undefined,
): void {
   if (region === undefined || region === null || region === "") return;
   if (typeof region === "string" && S3_REGION_PATTERN.test(region)) return;
   throw new Error(
      `${field} on connection "${connectionName}" is invalid: an S3 region ` +
         `must match ${S3_REGION_PATTERN.source} (letters, digits, hyphens or ` +
         `underscores, such as us-east-1).`,
   );
}

/**
 * Throws unless every connection field Publisher places in setup SQL holds
 * its shape: each `attachedDatabases[]` name that is set matches
 * {@link ATTACHED_DATABASE_NAME_PATTERN}, no `attachedDatabases[]` type names
 * an `Object.prototype` member (`constructor`, `toString`), which a lookup by
 * type would find on any plain object, and each S3 region that is set (an
 * attached database's, a DuckLake connection's storage) matches
 * {@link S3_REGION_PATTERN}. An absent value is left to the attach that reads
 * it. The message names the field by path, not by the rejected text.
 */
export function validateConnectionSqlFields(
   connection: Partial<ApiConnection>,
): void {
   assertS3Region(
      (
         connection.ducklakeConnection?.storage?.s3Connection as
            | { region?: unknown }
            | null
            | undefined
      )?.region,
      "ducklakeConnection.storage.s3Connection.region",
      connection.name,
   );
   const attached = connection.duckdbConnection?.attachedDatabases;
   if (!Array.isArray(attached)) return;
   attached.forEach((attachedDb, index) => {
      const entry = attachedDb as {
         name?: unknown;
         type?: unknown;
         s3Connection?: { region?: unknown } | null;
      } | null;
      if (typeof entry?.type === "string" && entry.type in Object.prototype) {
         throw new Error(
            `attachedDatabases[${index}].type on connection "${connection.name}" ` +
               `is invalid: it is not an attached database type.`,
         );
      }
      const name = entry?.name;
      if (
         name !== undefined &&
         name !== null &&
         !(
            typeof name === "string" &&
            ATTACHED_DATABASE_NAME_PATTERN.test(name)
         )
      ) {
         throw new Error(
            `attachedDatabases[${index}].name on connection "${connection.name}" ` +
               `is invalid: an attached database name must match ` +
               `${ATTACHED_DATABASE_NAME_PATTERN.source} (a letter or underscore, ` +
               `then letters, digits or underscores).`,
         );
      }
      assertS3Region(
         entry?.s3Connection?.region,
         `attachedDatabases[${index}].s3Connection.region`,
         connection.name,
      );
   });
}

// Narrows the environment-author API surface: rejects securityPolicy,
// allowedDirectories, etc., and admits setupSQL only behind
// PUBLISHER_ALLOW_DUCKDB_SETUP_SQL. It is NOT a filesystem isolation boundary:
// attachedDatabases[].path is not constrained to the environment root, and
// DuckDB's local-file access is unchanged. Adversarial filesystem isolation
// (path-traversal checks, an allowlist) is not provided.
export function validateDuckdbApiSurface(connection: ApiConnection): void {
   // Before the type test: a PATCH may send attachedDatabases without a type,
   // and a DuckLake connection's storage region is checked here too.
   validateConnectionSqlFields(connection);
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

/** The connection as stored and returned by the API, `dbmeta` as a boolean. */
function cloneApiConnection(connection: ApiConnection): ApiConnection {
   const clone = { ...connection };
   const bigquery = connection.bigqueryConnection;
   if (bigquery?.dbmeta !== undefined) {
      clone.bigqueryConnection = {
         ...bigquery,
         dbmeta: configBoolean(
            bigquery.dbmeta,
            `dbmeta on BigQuery connection "${connection.name}"`,
         ),
      };
   }
   return clone;
}

/**
 * What the API reports for a connection of `type` before it is opened: the
 * registry entry's attributes (service/dialects), as a fresh object the
 * caller may keep. Undefined for `publisher` and an unregistered type.
 */
export function getStaticConnectionAttributes(
   type: ApiConnection["type"],
): components["schemas"]["ConnectionAttributes"] | undefined {
   const attributes = connectionDialectOf(type)?.attributes;
   return attributes === undefined ? undefined : { ...attributes };
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
   settings.get("allow.unverifiedSshHostKey");
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

function validateConnectionShape(
   connection: ApiConnection,
   origin: ConnectionOrigin = "connection",
): void {
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

   // Type-specific shape checks; postgres and mysql have none of their own.
   connectionDialectOf(connection.type)?.validateShape?.(connection, origin);
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

/**
 * The connection every package gets of its own: an in-memory DuckDB sandbox,
 * so nothing built in it survives a restart. The name is reserved: a
 * destination or environment connection may not take it.
 */
export const PACKAGE_SANDBOX_CONNECTION = "duckdb";

/** Where {@link STORAGE_DESTINATIONS_DIR} sits for an environment. */
export function storageDestinationRoot(environmentPath: string): string {
   return path.join(environmentPath, STORAGE_DESTINATIONS_DIR);
}

/**
 * A plain DuckDB destination's name as a file name: letters, digits, `_`,
 * `-` and `.`, never `..`, so it cannot leave the destinations root.
 */
const DESTINATION_FILE_NAME = /^(?!.*\.\.)[A-Za-z0-9_.-]+$/;

/**
 * Why `destinationName` cannot name a plain DuckDB destination's file
 * ({@link DESTINATION_FILE_NAME}), or undefined when it can: checked where
 * the destination list is validated, so a bad name is a config error when
 * the list loads, and again on every path to the file.
 */
export function destinationFileNameRefusal(
   destinationName: string,
): string | undefined {
   return DESTINATION_FILE_NAME.test(destinationName)
      ? undefined
      : `the name "${destinationName}" names the destination's database ` +
           `file, so it may use only ASCII letters, digits, '_', '-' and '.', ` +
           `never '..'.`;
}

/**
 * A destination type whose database file is named from its name: one that is
 * no catalog (its registry entry declares no destination dialect), as a plain
 * DuckDB file is.
 */
function namesItsFile(type: string): boolean {
   return connectionDialectOf(type)?.destination === undefined;
}

/**
 * Where a plain DuckDB destination's database file lives, under
 * {@link storageDestinationRoot}. Every path to that file goes through here:
 * a name that could reach outside the root (a separator, `..`) is refused.
 */
export function storageDestinationFile(
   environmentPath: string,
   destinationName: string,
): string {
   const refused = destinationFileNameRefusal(destinationName);
   if (refused !== undefined) {
      throw new BadRequestError(`Storage destination: ${refused}`);
   }
   const root = storageDestinationRoot(environmentPath);
   const file = path.join(root, `${destinationName}.duckdb`);
   if (path.dirname(file) !== root) {
      throw new BadRequestError(
         `Storage destination name '${destinationName}' leaves its root.`,
      );
   }
   return file;
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
      const fileName = namesItsFile(type)
         ? destinationFileNameRefusal(name)
         : undefined;
      if (fileName !== undefined) {
         rejected.push({ name, reason: fileName });
         continue;
      }

      try {
         validateConnectionShape(destination, "storage-destination");
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

/**
 * Drop keys whose value is `undefined` or `null`.
 *
 * The null guard for connectors Publisher builds itself, outside Malloy's
 * registry (which strips nulls on its own path). A surviving `null` reaches
 * `makeDigest`, which reads `.length` off each part, and throws on the first
 * digest; the package-load worker takes that digest, so the whole package fails
 * to load ("import reference failure") while /compile still succeeds.
 *
 * Nulls come from an explicit `null` in publisher.config.json or a client
 * serializing unset optionals over POST /connections, and persist through the
 * repository's JSON round trip. An explicit null and an omitted field produce
 * the same digest.
 */
export function removeUndefined<T extends object>(
   value: T,
): { [K in keyof T]?: Exclude<T[K], null> } {
   return Object.fromEntries(
      Object.entries(value).filter(
         ([, fieldValue]) => fieldValue !== undefined && fieldValue !== null,
      ),
   ) as { [K in keyof T]?: Exclude<T[K], null> };
}

/**
 * The Malloy core connection entry a connection assembles into, by its
 * type's registry entry (looked up with `Object.hasOwn`, so a caller-supplied
 * type selects only a registered builder).
 */
function buildCoreEntry(
   connection: NamedConnection,
   environmentPath: string,
): CoreConnectionEntry {
   const dialect = connectionDialectOf(connection.type);
   if (dialect === undefined) {
      throw new Error(`Unsupported connection type: ${connection.type}`);
   }
   return dialect.coreEntry(connection, environmentPath);
}

export function assembleEnvironmentConnections(
   connections: ApiConnection[] = [],
   environmentPath = "",
   origin: ConnectionOrigin = "connection",
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
      validateConnectionShape(connection, origin);
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
