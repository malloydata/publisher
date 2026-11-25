// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The secret a GCS or S3 storage root is read through on a DuckDB session:
 * its CREATE SECRET, the attach that creates it (and arms a chain secret's
 * renewer), and the secret lookup the object-storage attaches share.
 *
 * A leaf: it imports no connection module, so the connection-type registry
 * (service/dialects/) and connection.ts can both use it.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../api";
import type { ConnectionOrigin } from "../config";
import { logger } from "../logger";
import {
   allowForLockdown,
   cloudStorageAllowPrefixes,
   duckDBInstanceOf,
   runAsInternalStatement,
   runInternalSQL,
} from "./duckdb_lockdown";
import {
   escapeSQL,
   installAndLoadExtension,
   sanitizeSecretName,
} from "./duckdb_session_setup";
import {
   CloudStorageCredentials,
   checkS3Endpoint,
   DEFAULT_S3_CREDENTIAL_CHAIN,
   resolveCloudStorageCredentials,
} from "./gcs_s3_utils";
import { quoteIdentifier } from "./quoting";

type AttachedDatabase = components["schemas"]["AttachedDatabase"];

/** A connection whose object-storage secret can be re-resolved without a reattach. */
interface RenewableStorageSecret {
   setStorageSecretRenewer(renew: () => Promise<void>): void;
}

/**
 * The CREATE OR REPLACE SECRET for a GCS or S3 storage root. Separate from
 * `attachCloudStorage` so the statement can be asserted without a live DuckDB.
 */
export function buildCloudStorageSecretSQL(
   secretName: string,
   credentials: CloudStorageCredentials,
): string {
   const isGCS = credentials.type === "gcs";
   const escapedKeyId = escapeSQL(credentials.accessKeyId);
   const escapedSecret = escapeSQL(credentials.secretAccessKey);
   const scopeClause = credentials.scope?.length
      ? `SCOPE [${credentials.scope.map((prefix) => `'${escapeSQL(prefix)}'`).join(", ")}]`
      : "";

   let createSecretCommand: string;

   if (isGCS) {
      createSecretCommand = `
         CREATE OR REPLACE SECRET ${secretName} (
            TYPE gcs,
            KEY_ID '${escapedKeyId}',
            SECRET '${escapedSecret}'${scopeClause ? `,\n            ${scopeClause}` : ""}
         );
      `;
   } else {
      // The schema carries no `default:` (a generated client would not apply
      // it), so the fallback lives here. Warned because a bucket in another
      // region then fails as "no such bucket", which reads as a wrong name.
      const region = credentials.region || "us-east-1";
      if (!credentials.region) {
         logger.warn(
            "S3 connection has no region; defaulting to us-east-1. A bucket in " +
               "another region will not be found.",
            { secretName },
         );
      }

      let clauses: string[];
      if (credentials.provider === "credential_chain") {
         // CHAIN is required: omitted, DuckDB resolves against `config` alone,
         // which cannot work in a container.
         const chain = escapeSQL(
            credentials.chain?.trim() || DEFAULT_S3_CREDENTIAL_CHAIN,
         );
         // A chain secret stores the credentials it resolved, so a temporary
         // credential (web-identity assume-role: an hour by default) expires
         // while a serve-path secret lives on. `auto` is the only value that
         // arms refresh (the aws extension stores `refresh_info` only for it;
         // any other value silently does nothing). Not caller-configurable: a
         // frozen snapshot of an expiring credential has no use.
         clauses = [
            "TYPE s3",
            "PROVIDER credential_chain",
            `CHAIN '${chain}'`,
            "REFRESH 'auto'",
            `REGION '${escapeSQL(region)}'`,
         ];
      } else {
         clauses = [
            "TYPE s3",
            `KEY_ID '${escapedKeyId}'`,
            `SECRET '${escapedSecret}'`,
            `REGION '${escapeSQL(region)}'`,
         ];
      }
      // Additive on both arms: an S3-compatible endpoint behind a host role is a
      // real combination, and so is one with temporary keys.
      if (credentials.endpoint) {
         const { hostAndPath, useSsl } = checkS3Endpoint(
            credentials.endpoint,
            credentials.provider,
         );
         clauses.push(
            `ENDPOINT '${escapeSQL(hostAndPath)}'`,
            "URL_STYLE 'path'",
         );
         // `true` is never written: it is DuckDB's default, so a secret for an
         // https or bare endpoint is what a bare one always was.
         if (!useSsl) {
            clauses.push("USE_SSL false");
         }
      }
      if (
         credentials.provider !== "credential_chain" &&
         credentials.sessionToken
      ) {
         clauses.push(`SESSION_TOKEN '${escapeSQL(credentials.sessionToken)}'`);
      }
      if (scopeClause) {
         clauses.push(scopeClause);
      }
      createSecretCommand = `
            CREATE OR REPLACE SECRET ${secretName} (
               ${clauses.join(",\n               ")}
            );
         `;
   }

   return createSecretCommand;
}

export async function attachCloudStorage(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
   origin: ConnectionOrigin = "connection",
   scope?: string[],
): Promise<void> {
   const storageType = attachedDb.type?.toUpperCase() || "";
   // The scope rides on the credentials so the renewer re-emits it.
   const credentials = {
      ...resolveCloudStorageCredentials(attachedDb, origin),
      scope,
   };

   await installAndLoadExtension(connection, "httpfs");
   // `credential_chain` is implemented by the `aws` extension and resolves when
   // the secret is created, so load it first (the generic attach path does not
   // otherwise). `aws` is baked into the image, so local-only policy holds.
   if (credentials.provider === "credential_chain") {
      await installAndLoadExtension(connection, "aws");
   }

   const secretName = sanitizeSecretName(
      `${attachedDb.type}_${attachedDb.name}`,
   );
   const createSecretCommand = buildCloudStorageSecretSQL(
      secretName,
      credentials,
   );

   if (await doesSecretExistInDuckDB(connection, secretName)) {
      // Force-refresh attachments using this storage.
      await connection
         .runSQL(`DETACH ${quoteIdentifier(attachedDb.name ?? "", "duckdb")};`)
         .catch(() => {});
   }
   await connection.runSQL(createSecretCommand);

   // Only a chain secret goes stale; a renewer for a key pair would only retry a
   // wrong key. This backs up `REFRESH 'auto'`, which does not reach every read
   // path: reads through an attached DuckLake catalog have surfaced ExpiredToken
   // from an armed secret. Structural check, because only some of the
   // connection types reaching here can renew.
   const renewable = connection as unknown as Partial<RenewableStorageSecret>;
   if (
      credentials.provider === "credential_chain" &&
      typeof renewable.setStorageSecretRenewer === "function"
   ) {
      const renewOn = async (target: DuckDBConnection) => {
         // Replaced in place on the live connection; no in-flight query is
         // disturbed. Admitted past a locked instance's secret gate for this
         // one statement only (duckdb_lockdown.ts).
         const sql = buildCloudStorageSecretSQL(secretName, credentials);
         await runAsInternalStatement(sql, () => target.runSQL(sql));
      };
      renewable.setStorageSecretRenewer(() => renewOn(connection));
      // The secret is the instance's: a later connection joining the locked
      // instance, which runs no attach of its own, renews it the same way.
      const instance = duckDBInstanceOf(connection);
      if (instance) instanceSecretRenewers.set(instance, renewOn);
   }

   if (scope === undefined) {
      // An attached database names no bucket: its reads are any URL of its
      // scheme the secret reaches.
      for (const prefix of cloudStorageAllowPrefixes(attachedDb.type ?? "")) {
         allowForLockdown(connection, prefix);
      }
   }

   logger.info(`Created ${storageType} secret: ${secretName}`);
   logger.info(`${storageType} connection configured for: ${attachedDb.name}`);
}

export async function doesSecretExistInDuckDB(
   connection: DuckDBConnection,
   secretName: string,
): Promise<boolean> {
   const escapedSecretName = escapeSQL(secretName);
   const result = await runInternalSQL(
      connection,
      `SELECT COUNT(*) AS count FROM duckdb_secrets() WHERE name = '${escapedSecretName}';`,
   );
   const rows = result.rows;
   return Number(rows?.[0]?.count ?? 0) > 0;
}

/** Each instance's storage-secret renewer, for connections that join it. */
export const instanceSecretRenewers = new WeakMap<
   object,
   (target: DuckDBConnection) => Promise<void>
>();
