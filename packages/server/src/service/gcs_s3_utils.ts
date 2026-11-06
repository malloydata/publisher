// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   ListBucketsCommand,
   ListObjectsV2Command,
   S3Client,
} from "@aws-sdk/client-s3";
import { Connection } from "@malloydata/malloy";
import { components } from "../api";
import { ConnectionOrigin, getS3CredentialChainPolicy } from "../config";
import { logger } from "../logger";
import { runIntrospectionSQL, sqlLiteral } from "./introspection_sql";

type ApiTable = components["schemas"]["Table"];
type CloudStorageType = "gcs" | "s3";
type ApiS3Connection = components["schemas"]["S3Connection"];
type ApiAttachedDatabase = components["schemas"]["AttachedDatabase"];

// The providers `S3Connection.provider` admits (mirrors api-doc.yaml). Here
// rather than in connection.ts, which imports this module, so config-load
// validation and the connect-time builder share one list.
const S3_PROVIDERS = ["config", "credential_chain"] as const;

// The provider order used when a chain-auth S3 connection names none. DuckDB's
// default is `config` alone, a credentials file a container does not have.
// `env` first so an explicit environment credential wins, then `web_identity`
// (EKS IRSA, GKE Workload Identity), then `instance`. Excluded: `config`, `sts`
// (needs ASSUME_ROLE_ARN, which the schema lacks), `sso` and `process`
// (workstation flows).
export const DEFAULT_S3_CREDENTIAL_CHAIN = "env;web_identity;instance";

/**
 * An S3-compatible endpoint as DuckDB and the AWS SDK each need it. DuckDB's
 * `ENDPOINT` is a bare `host[:port][/path]` and speaks TLS unless the secret
 * says `USE_SSL false`; given a scheme it requests `https://http://…`. The SDK
 * needs the full URL and rejects a bare host as `Invalid URL`. So an endpoint
 * may be written either way, and `http://` is the one form that turns TLS off.
 *
 * Anything the two read differently is refused: another scheme, whitespace, a
 * query or fragment, userinfo, and a path with no host. No message repeats the
 * endpoint, which may carry a credential.
 */
export function parseS3Endpoint(endpoint: string): {
   hostAndPath: string;
   useSsl: boolean;
   url: string;
} {
   const scheme = /^([a-z][a-z0-9+.-]*):\/\//i.exec(endpoint);
   const protocol = scheme?.[1].toLowerCase();
   if (protocol !== undefined && protocol !== "http" && protocol !== "https") {
      throw new Error(
         `S3 endpoint scheme must be http:// or https://, got ${protocol}://`,
      );
   }
   if (/\s/.test(endpoint)) {
      throw new Error("S3 endpoint must not contain whitespace");
   }
   if (/[?#]/.test(endpoint)) {
      throw new Error(
         "S3 endpoint must not contain a query string or fragment",
      );
   }
   // A loop, not /\/+$/: that regex backtracks on a long run of slashes.
   let end = endpoint.length;
   while (end > 0 && endpoint[end - 1] === "/") end--;
   const hostAndPath = endpoint.slice(scheme ? scheme[0].length : 0, end);
   if (!hostAndPath) {
      throw new Error("S3 endpoint must include a host");
   }
   if (hostAndPath.startsWith("/")) {
      throw new Error("S3 endpoint must include a host before any path");
   }
   if (hostAndPath.split("/", 1)[0].includes("@")) {
      throw new Error("S3 endpoint must not contain userinfo");
   }
   const useSsl = protocol !== "http";
   return {
      hostAndPath,
      useSsl,
      url: `${useSsl ? "https" : "http"}://${hostAndPath}`,
   };
}

/**
 * The endpoint a connection may use with its credentials: {@link parseS3Endpoint},
 * and no plain `http://` for `credential_chain`, whose signed requests carry the
 * host role's session token. Connection authors are less trusted than the host
 * operator (the chain is policy-gated for that reason), so an author must not be
 * able to send the host's credentials in cleartext.
 */
export function checkS3Endpoint(
   endpoint: string,
   provider: string | undefined,
): ReturnType<typeof parseS3Endpoint> {
   const parsed = parseS3Endpoint(endpoint);
   if (!parsed.useSsl && provider === "credential_chain") {
      throw new Error(
         "S3 endpoint must use https:// with provider credential_chain: its requests carry the host's credentials",
      );
   }
   return parsed;
}

export interface CloudStorageCredentials {
   type: CloudStorageType;
   accessKeyId: string;
   secretAccessKey: string;
   region?: string;
   endpoint?: string;
   sessionToken?: string;
   // S3 only (GCS is HMAC-only here). `credential_chain` means the host
   // supplies credentials, so the key pair is empty and must not be signed
   // with; `chain` is the provider order to try.
   provider?: "config" | "credential_chain";
   chain?: string;
   // The URL prefix the secret is restricted to (`SCOPE`); unset keeps DuckDB's
   // default scope, the whole scheme.
   scope?: string;
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
 * Checks the parts of an S3 connection's credential shape that are wrong on
 * their face, independent of whether a key pair is required. Separate from
 * {@link resolveCloudStorageCredentials} because the key-pair rule's
 * enforcement point matters (see the callers in connection_config.ts).
 *
 * The typeof checks are needed: values arrive from untyped JSON, so
 * `provider: true` would slip into the `config` path as a missing access key,
 * and a non-string `chain` would throw a TypeError at the first attach.
 */
export function validateS3ProviderShape(
   s3Connection: ApiS3Connection,
   connectionName: string | undefined,
   // The credential-chain policy turns on this. Unstated means the
   // author-supplied case, the one worth refusing.
   origin: ConnectionOrigin = "connection",
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
      // Host-resolved credentials, gated by deployment policy. A storage
      // destination is the operator's own config; an environment connection may
      // come from whoever can reach the connection endpoints (see
      // getS3CredentialChainPolicy).
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

      // Rejected rather than ignored: an unused static credential would read as
      // what grants access while something else does.
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
      // The mirror case: a chain under key-based auth misstates what grants access.
      throw new Error(
         `S3 chain is only valid when provider is 'credential_chain' for: ${connectionName}`,
      );
   }
}

/**
 * Validates a GCS or S3 attachment's credentials and reduces them to the shape
 * both the DuckDB secret and Publisher's storage client read. Here rather than
 * in connection.ts so config-load validation (and tests) can reach each guard
 * without a live DuckDB session.
 */
export function resolveCloudStorageCredentials(
   attachedDb: ApiAttachedDatabase,
   // Forwarded to validateS3ProviderShape; unstated means author-supplied.
   origin: ConnectionOrigin = "connection",
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
   // Here rather than at attach, so a storage destination with an unusable
   // endpoint is refused when it is configured, not at its first build.
   if (typeof s3.endpoint === "string" && s3.endpoint !== "") {
      try {
         checkS3Endpoint(s3.endpoint, s3.provider);
      } catch (error) {
         throw new Error(`${(error as Error).message} (${attachedDb.name})`);
      }
   }
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

// Exported for tests: whether the client signs with the key pair or leaves
// resolution to the AWS SDK is not observable through the listing functions
// without a real bucket.
export function createCloudStorageClient(
   credentials: CloudStorageCredentials,
): S3Client {
   const isGCS = credentials.type === "gcs";

   // Under `credential_chain` the `credentials` key is omitted so the AWS SDK
   // resolves its default chain; empty strings would sign every request with
   // an empty key and fail on signature rather than configuration.
   //
   // Known divergence: the SDK chain also covers sources DuckDB's does not
   // (credentials file, AWS_PROFILE, SSO, process) and ignores `chain`, so on
   // such a host browsing can run as a different principal than queries.
   // Narrowing it would need a DuckDB-to-SDK provider mapping that drifts on
   // both sides; a container image has none of those sources, and elsewhere the
   // query path is authoritative and fails visibly.
   const useHostCredentials =
      !isGCS && credentials.provider === "credential_chain";

   const client = new S3Client({
      endpoint: isGCS
         ? "https://storage.googleapis.com"
         : credentials.endpoint
           ? checkS3Endpoint(credentials.endpoint, credentials.provider).url
           : undefined,
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

// Flat (no Delimiter) paginated listing: O(total_files / 1000) API calls rather
// than one per folder.
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

      do {
         const response = await client.send(
            new ListObjectsV2Command({
               Bucket: bucket,
               Prefix: prefix,
               ContinuationToken: continuationToken,
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

export function isDataFile(key: string): boolean {
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

export function getFileType(key: string): string {
   const lowerKey = key.toLowerCase();
   if (lowerKey.endsWith(".csv")) return "csv";
   if (lowerKey.endsWith(".parquet")) return "parquet";
   if (lowerKey.endsWith(".json")) return "json";
   if (lowerKey.endsWith(".jsonl") || lowerKey.endsWith(".ndjson"))
      return "jsonl";
   return "unknown";
}

export function standardizeRunSQLResult(result: unknown): unknown[] {
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

      // DESCRIBE returns one row per column, so the driver's default row limit
      // would silently cap the schema at ten columns.
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
 * The URIs of every directory in a bucket that holds a data file.
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
