// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   ListBucketsCommand,
   ListObjectsV2Command,
   S3Client,
} from "@aws-sdk/client-s3";
import { Connection } from "@malloydata/malloy";
import { components } from "../api";
import { getS3CredentialChainPolicy } from "../config";
import { logger } from "../logger";
import { runIntrospectionSQL, sqlLiteral } from "./introspection_sql";

type ApiTable = components["schemas"]["Table"];
type CloudStorageType = "gcs" | "s3";
type ApiS3Connection = components["schemas"]["S3Connection"];
type ApiAttachedDatabase = components["schemas"]["AttachedDatabase"];

// The providers `S3Connection.provider` admits. Canonical here rather than in
// connection.ts, which imports this module, so the config-load validator and the
// connect-time builder derive from one list. Mirrors the enum in api-doc.yaml.
const S3_PROVIDERS = ["config", "credential_chain"] as const;

// The provider order Publisher supplies when a chain-auth S3 connection names
// none. DuckDB's own default is `config` alone — a shared credentials file,
// which is the one source a container does not have — so leaving it unset is not
// an option. `env` first so an explicit credential in the environment wins,
// then `web_identity` for a projected service-account token (EKS IRSA, GKE
// Workload Identity), then `instance` for a node's own profile.
// Deliberately excluded: `config` (no credentials file in an image), `sts`
// (needs ASSUME_ROLE_ARN, which this schema has no field for), and `sso` and
// `process` (workstation flows).
export const DEFAULT_S3_CREDENTIAL_CHAIN = "env;web_identity;instance";

export interface CloudStorageCredentials {
   type: CloudStorageType;
   accessKeyId: string;
   secretAccessKey: string;
   region?: string;
   endpoint?: string;
   sessionToken?: string;
   // S3 only. `credential_chain` means the host supplies the credentials, so
   // accessKeyId and secretAccessKey are empty and must not be signed with;
   // `chain` is the provider order to try. GCS has no chain equivalent through
   // this path — its secret is HMAC-only.
   provider?: "config" | "credential_chain";
   chain?: string;
}

interface CloudStorageBucket {
   name: string;
   creationDate?: Date;
}

interface CloudStorageObject {
   key: string;
   size?: number;
   lastModified?: Date;
}

export function gcsConnectionToCredentials(gcsConnection: {
   keyId?: string;
   secret?: string;
}): CloudStorageCredentials {
   return {
      type: "gcs",
      accessKeyId: gcsConnection.keyId || "",
      secretAccessKey: gcsConnection.secret || "",
   };
}

export function s3ConnectionToCredentials(s3Connection: {
   accessKeyId?: string;
   secretAccessKey?: string;
   region?: string;
   endpoint?: string;
   sessionToken?: string;
   provider?: "config" | "credential_chain";
   chain?: string;
}): CloudStorageCredentials {
   return {
      type: "s3",
      accessKeyId: s3Connection.accessKeyId || "",
      secretAccessKey: s3Connection.secretAccessKey || "",
      region: s3Connection.region,
      endpoint: s3Connection.endpoint,
      sessionToken: s3Connection.sessionToken,
      provider: s3Connection.provider,
      chain: s3Connection.chain,
   };
}

/**
 * Checks the parts of an S3 connection's credential *shape* that are wrong on their
 * face, independent of whether a key pair is required.
 *
 * Separate from {@link resolveCloudStorageCredentials} because of where each is safe
 * to call. These checks reject only configurations that no path ever accepted, so a
 * config that loads today still loads; the key-pair requirement is a longer-standing
 * rule whose enforcement point is load-bearing (see the callers in
 * connection_config.ts).
 *
 * The typeof checks are load-bearing rather than defensive: the values arrive from
 * untyped JSON, so `provider: true` would otherwise slip past a string comparison
 * into the `config` path and be reported as a missing access key, and a non-string
 * `chain` would reach `.trim()` as a TypeError at the first attach.
 */
export function validateS3ProviderShape(
   s3Connection: ApiS3Connection,
   connectionName: string | undefined,
   // Where this connection came from, which is what the credential-chain policy
   // turns on. Defaults to "connection" so a caller that does not say is treated
   // as the author-supplied case -- the one worth refusing.
   origin: "connection" | "storage-destination" = "connection",
): void {
   const provider = s3Connection.provider as unknown;
   if (provider !== undefined) {
      if (
         typeof provider !== "string" ||
         !S3_PROVIDERS.includes(provider as (typeof S3_PROVIDERS)[number])
      ) {
         throw new Error(
            `S3 provider must be one of ${S3_PROVIDERS.join(", ")} for: ${connectionName}`,
         );
      }
   }
   const chain = s3Connection.chain as unknown;
   if (chain !== undefined && typeof chain !== "string") {
      throw new Error(`S3 chain must be a string for: ${connectionName}`);
   }

   if (provider === "credential_chain") {
      // Host-resolved credentials, gated by deployment policy. `destinations-only`
      // draws the line where authorship changes hands: a storage destination is
      // the operator's own configuration, while an environment connection may be
      // supplied by whoever can reach the connection endpoints. See
      // getS3CredentialChainPolicy for why that distinction is the useful one.
      const policy = getS3CredentialChainPolicy();
      if (
         policy === "off" ||
         (policy === "destinations-only" && origin === "connection")
      ) {
         throw new Error(
            `S3 provider 'credential_chain' is not permitted for: ${connectionName}` +
               ` (S3_CREDENTIAL_CHAIN_POLICY=${policy}). It resolves the host's own` +
               ` credentials; supply an accessKeyId and secretAccessKey instead.`,
         );
      }

      // A static credential supplied alongside chain auth is rejected rather than
      // ignored: it would sit unused in the config, reading as the thing granting
      // access while something else actually does.
      if (
         s3Connection.accessKeyId ||
         s3Connection.secretAccessKey ||
         s3Connection.sessionToken
      ) {
         throw new Error(
            `S3 accessKeyId, secretAccessKey, and sessionToken must not be set when provider is 'credential_chain' for: ${connectionName}`,
         );
      }
   } else if (s3Connection.chain) {
      // The mirror argument: a chain named under key-based auth reads as though the
      // host supplies the credentials when the key pair beside it is what does.
      throw new Error(
         `S3 chain is only valid when provider is 'credential_chain' for: ${connectionName}`,
      );
   }
}

/**
 * Validates a GCS or S3 attachment's credential configuration and reduces it to the
 * shape both the DuckDB secret and Publisher's own storage client read.
 *
 * Lives here rather than in connection.ts so the config-load validator can reach it
 * without importing that module, which imports this one. Each guard is therefore
 * assertable directly: whether a key pair is required depends on the S3 provider, and
 * a guard only reachable through a live DuckDB session is a guard nothing pins.
 */
export function resolveCloudStorageCredentials(
   attachedDb: ApiAttachedDatabase,
   // Forwarded to the shape validator, which is where the credential-chain policy
   // is applied. Same default and same reason: unstated means author-supplied.
   origin: "connection" | "storage-destination" = "connection",
): CloudStorageCredentials {
   if (attachedDb.type === "gcs") {
      if (!attachedDb.gcsConnection) {
         throw new Error(
            `GCS connection configuration missing for: ${attachedDb.name}`,
         );
      }
      if (!attachedDb.gcsConnection.keyId || !attachedDb.gcsConnection.secret) {
         throw new Error(
            `GCS keyId and secret are required for: ${attachedDb.name}`,
         );
      }
      return {
         type: "gcs",
         accessKeyId: attachedDb.gcsConnection.keyId,
         secretAccessKey: attachedDb.gcsConnection.secret,
      };
   }

   if (attachedDb.type !== "s3") {
      throw new Error(`Invalid cloud storage type: ${attachedDb.type}`);
   }

   if (!attachedDb.s3Connection) {
      throw new Error(
         `S3 connection configuration missing for: ${attachedDb.name}`,
      );
   }
   const s3 = attachedDb.s3Connection;
   validateS3ProviderShape(s3, attachedDb.name, origin);
   if (
      s3.provider !== "credential_chain" &&
      (!s3.accessKeyId || !s3.secretAccessKey)
   ) {
      throw new Error(
         `S3 accessKeyId and secretAccessKey are required for: ${attachedDb.name}`,
      );
   }
   return {
      type: "s3",
      accessKeyId: s3.accessKeyId || "",
      secretAccessKey: s3.secretAccessKey || "",
      region: s3.region,
      endpoint: s3.endpoint,
      sessionToken: s3.sessionToken,
      provider: s3.provider,
      chain: s3.chain,
   };
}

// Exported for tests: whether the client signs with the configured key pair or
// leaves resolution to the AWS SDK is the whole of the chain-auth behaviour on
// this path, and it is not observable through the listing functions below
// without reaching a real bucket.
export function createCloudStorageClient(
   credentials: CloudStorageCredentials,
): S3Client {
   const isGCS = credentials.type === "gcs";

   // Under `credential_chain` there is no key pair to sign with, so the
   // `credentials` key is omitted entirely and the AWS SDK resolves its own
   // default chain. Passing the empty strings through instead would sign every
   // request with an empty key and fail on signature rather than on configuration.
   //
   // KNOWN DIVERGENCE, and it runs both ways. The SDK's default chain covers a
   // web-identity token natively, which is why the DuckDB secret must name
   // `web_identity` and this need not. But it ALSO covers sources DuckDB's chain
   // does not — the shared credentials file, AWS_PROFILE, SSO, process — and it
   // ignores `chain` entirely. So on a host that has one of those, this path can
   // browse as one principal while the query path resolves another: a mounted
   // credentials file is invisible to DuckDB and wins here.
   //
   // Not corrected by narrowing this to `chain`, deliberately: doing so needs a
   // DuckDB-provider-to-SDK-provider mapping for all seven of DuckDB's names, plus
   // direct dependencies on providers that are currently only transitive, and the
   // table would drift against both sides. The divergence cannot arise in the
   // deployment this exists for — a container image has no credentials file,
   // profile, or SSO cache — and where it can arise the query path is the
   // authoritative one and fails visibly.
   const useHostCredentials =
      !isGCS && credentials.provider === "credential_chain";

   const client = new S3Client({
      endpoint: isGCS ? "https://storage.googleapis.com" : credentials.endpoint,
      region: isGCS ? "auto" : credentials.region || "us-east-1",
      ...(useHostCredentials
         ? {}
         : {
              credentials: {
                 accessKeyId: credentials.accessKeyId,
                 secretAccessKey: credentials.secretAccessKey,
                 sessionToken: credentials.sessionToken,
              },
           }),
      forcePathStyle: isGCS || !!credentials.endpoint,
   });

   if (isGCS) {
      client.middlewareStack.add(
         (next) => async (args) => {
            const request = args.request as { query?: Record<string, string> };
            if (request.query) {
               delete request.query["x-id"];
            }
            return next(args);
         },
         { step: "build", name: "removeXIdParam" },
      );
   }

   return client;
}

async function listCloudBuckets(
   credentials: CloudStorageCredentials,
): Promise<CloudStorageBucket[]> {
   const client = createCloudStorageClient(credentials);
   const storageType = credentials.type.toUpperCase();

   try {
      const response = await client.send(new ListBucketsCommand({}));
      return (response.Buckets || []).map((bucket) => ({
         name: bucket.Name || "",
         creationDate: bucket.CreationDate,
      }));
   } catch (error) {
      logger.error(`Failed to list ${storageType} buckets`, { error });
      throw new Error(
         `Failed to list ${storageType} buckets: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

// Flat listing with pagination - much faster than recursive DFS
// Makes only O(total_files / 1000) API calls instead of O(num_folders)
async function listAllCloudFiles(
   credentials: CloudStorageCredentials,
   bucket: string,
   prefix: string = "",
): Promise<CloudStorageObject[]> {
   const client = createCloudStorageClient(credentials);
   const storageType = credentials.type.toUpperCase();
   const allFiles: CloudStorageObject[] = [];

   try {
      let continuationToken: string | undefined;

      // Paginate through all objects (1000 per page)
      do {
         const response = await client.send(
            new ListObjectsV2Command({
               Bucket: bucket,
               Prefix: prefix,
               ContinuationToken: continuationToken,
               // No Delimiter = flat listing of ALL objects
            }),
         );

         for (const content of response.Contents || []) {
            if (content.Key) {
               allFiles.push({
                  key: content.Key,
                  size: content.Size,
                  lastModified: content.LastModified,
               });
            }
         }

         continuationToken = response.IsTruncated
            ? response.NextContinuationToken
            : undefined;
      } while (continuationToken);

      logger.info(
         `Listed ${allFiles.length} files in ${storageType} bucket ${bucket}`,
      );

      return allFiles;
   } catch (error) {
      logger.error(
         `Failed to list ${storageType} objects in bucket ${bucket}`,
         {
            error,
         },
      );
      throw new Error(
         `Failed to list objects in ${storageType} bucket ${bucket}: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

function isDataFile(key: string): boolean {
   const lowerKey = key.toLowerCase();
   return (
      lowerKey.endsWith(".csv") ||
      lowerKey.endsWith(".parquet") ||
      lowerKey.endsWith(".json") ||
      lowerKey.endsWith(".jsonl") ||
      lowerKey.endsWith(".ndjson")
   );
}

function buildCloudUri(
   type: CloudStorageType,
   bucket: string,
   key: string,
): string {
   const scheme = type === "gcs" ? "gs" : "s3";
   return `${scheme}://${bucket}/${key}`;
}

function getFileType(key: string): string {
   const lowerKey = key.toLowerCase();
   if (lowerKey.endsWith(".csv")) return "csv";
   if (lowerKey.endsWith(".parquet")) return "parquet";
   if (lowerKey.endsWith(".json")) return "json";
   if (lowerKey.endsWith(".jsonl") || lowerKey.endsWith(".ndjson"))
      return "jsonl";
   return "unknown";
}

function standardizeRunSQLResult(result: unknown): unknown[] {
   return Array.isArray(result)
      ? result
      : (result as { rows?: unknown[] }).rows || [];
}

const SCHEMA_FETCH_BATCH_SIZE = 10;
const BUCKET_SCAN_BATCH_SIZE = 3;

async function getTableSchema(
   malloyConnection: Connection,
   credentials: CloudStorageCredentials,
   bucketName: string,
   fileKey: string,
): Promise<ApiTable> {
   const uri = buildCloudUri(credentials.type, bucketName, fileKey);
   const fileType = getFileType(fileKey);

   try {
      let describeQuery: string;

      switch (fileType) {
         case "csv":
            describeQuery = `DESCRIBE SELECT * FROM read_csv('${sqlLiteral(uri, "duckdb")}', auto_detect=true) LIMIT 1`;
            break;
         case "parquet":
            describeQuery = `DESCRIBE SELECT * FROM read_parquet('${sqlLiteral(uri, "duckdb")}') LIMIT 1`;
            break;
         case "json":
            describeQuery = `DESCRIBE SELECT * FROM read_json('${sqlLiteral(uri, "duckdb")}', auto_detect=true) LIMIT 1`;
            break;
         case "jsonl":
            describeQuery = `DESCRIBE SELECT * FROM read_json('${sqlLiteral(uri, "duckdb")}', format='newline_delimited', auto_detect=true) LIMIT 1`;
            break;
         default:
            logger.warn(`Unsupported file type for ${fileKey}`);
            return { resource: uri, columns: [] };
      }

      // DESCRIBE returns one row per COLUMN, so the driver's small default
      // row limit caps a cloud file's schema at ten columns and silently drops
      // the rest. listTablesForDuckDB routes every cloud schema through here.
      const result = await runIntrospectionSQL(malloyConnection, describeQuery);
      const rows = standardizeRunSQLResult(result);
      const columns = rows.map((row: unknown) => {
         const typedRow = row as Record<string, unknown>;
         return {
            name: (typedRow.column_name || typedRow.name) as string,
            type: (typedRow.column_type || typedRow.type) as string,
         };
      });

      return { resource: uri, columns };
   } catch (error) {
      logger.warn(
         `Failed to get schema for ${credentials.type.toUpperCase()} file: ${uri}`,
         { error },
      );
      return { resource: uri, columns: [] };
   }
}

export async function getCloudTablesWithColumns(
   malloyConnection: Connection,
   credentials: CloudStorageCredentials,
   bucketName: string,
   fileKeys: string[],
): Promise<ApiTable[]> {
   const allTables: ApiTable[] = [];

   for (let i = 0; i < fileKeys.length; i += SCHEMA_FETCH_BATCH_SIZE) {
      const batch = fileKeys.slice(i, i + SCHEMA_FETCH_BATCH_SIZE);

      const batchResults = await Promise.all(
         batch.map((fileKey) =>
            getTableSchema(malloyConnection, credentials, bucketName, fileKey),
         ),
      );

      allTables.push(...batchResults);

      logger.info(
         `Processed batch ${Math.floor(i / SCHEMA_FETCH_BATCH_SIZE) + 1}/${Math.ceil(fileKeys.length / SCHEMA_FETCH_BATCH_SIZE)} (${allTables.length}/${fileKeys.length} files)`,
      );
   }

   return allTables;
}

export function parseCloudUri(uri: string): {
   type: CloudStorageType;
   bucket: string;
   path: string;
} | null {
   const gsMatch = uri.match(/^gs:\/\/([^/]+)(?:\/(.*))?$/);
   if (gsMatch) {
      return {
         type: "gcs",
         bucket: gsMatch[1],
         path: gsMatch[2] || "",
      };
   }

   const s3Match = uri.match(/^s3:\/\/([^/]+)(?:\/(.*))?$/);
   if (s3Match) {
      return {
         type: "s3",
         bucket: s3Match[1],
         path: s3Match[2] || "",
      };
   }

   return null;
}

export async function listDataFilesInDirectory(
   credentials: CloudStorageCredentials,
   bucketName: string,
   directoryPath: string,
): Promise<string[]> {
   const prefix = directoryPath ? `${directoryPath}/` : "";
   const client = createCloudStorageClient(credentials);
   const storageType = credentials.type.toUpperCase();
   const dataFiles: string[] = [];

   try {
      let continuationToken: string | undefined;

      do {
         const response = await client.send(
            new ListObjectsV2Command({
               Bucket: bucketName,
               Prefix: prefix,
               Delimiter: "/",
               ContinuationToken: continuationToken,
            }),
         );

         for (const content of response.Contents || []) {
            if (content.Key && isDataFile(content.Key)) {
               dataFiles.push(content.Key);
            }
         }

         continuationToken = response.IsTruncated
            ? response.NextContinuationToken
            : undefined;
      } while (continuationToken);

      logger.info(
         `Listed ${dataFiles.length} data files in ${storageType} ${bucketName}/${directoryPath}`,
      );
      return dataFiles;
   } catch (error) {
      logger.error(
         `Failed to list files in ${storageType} ${bucketName}/${directoryPath}`,
         { error },
      );
      throw new Error(
         `Failed to list files in ${storageType} ${bucketName}/${directoryPath}: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

/**
 * Scans an entire bucket and returns unique directory paths that contain data files.
 * Uses flat listing for efficiency — O(total_files / 1000) API calls.
 */
async function listDirectorySchemas(
   credentials: CloudStorageCredentials,
   bucketName: string,
): Promise<string[]> {
   const allFiles = await listAllCloudFiles(credentials, bucketName);
   const directories = new Set<string>();

   for (const file of allFiles) {
      if (!isDataFile(file.key)) continue;

      const lastSlashIndex = file.key.lastIndexOf("/");
      const dir =
         lastSlashIndex > 0 ? file.key.substring(0, lastSlashIndex) : "";
      directories.add(dir);
   }

   const scheme = credentials.type === "gcs" ? "gs" : "s3";
   const sortedDirs = Array.from(directories).sort();

   logger.info(
      `Found ${sortedDirs.length} directories with data files in ${credentials.type.toUpperCase()} bucket ${bucketName}`,
   );

   return sortedDirs.map((dir) =>
      dir ? `${scheme}://${bucketName}/${dir}` : `${scheme}://${bucketName}`,
   );
}

export async function listCloudDirectorySchemas(
   credentials: CloudStorageCredentials,
): Promise<{ name: string; isHidden: boolean; isDefault: boolean }[]> {
   const storageType = credentials.type.toUpperCase();
   const buckets = await listCloudBuckets(credentials);

   logger.info(
      `Listed ${buckets.length} ${storageType} buckets, scanning for directories...`,
   );

   const allDirArrays: string[][] = [];

   for (let i = 0; i < buckets.length; i += BUCKET_SCAN_BATCH_SIZE) {
      const batch = buckets.slice(i, i + BUCKET_SCAN_BATCH_SIZE);
      const batchResults = await Promise.all(
         batch.map((bucket) =>
            listDirectorySchemas(credentials, bucket.name).catch((err) => {
               logger.warn(
                  `Failed to scan ${storageType} bucket ${bucket.name}`,
                  { error: err },
               );
               return [] as string[];
            }),
         ),
      );
      allDirArrays.push(...batchResults);
   }

   return allDirArrays.flat().map((dirUri) => ({
      name: dirUri,
      isHidden: false,
      isDefault: false,
   }));
}
