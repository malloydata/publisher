import {
   ListBucketsCommand,
   ListObjectsV2Command,
   S3Client,
} from "@aws-sdk/client-s3";
import { Connection } from "@malloydata/malloy";
import { components } from "../api";
import { logger } from "../logger";

type ApiTable = components["schemas"]["Table"];

type CloudStorageType = "gcs" | "s3";

export interface CloudStorageCredentials {
   type: CloudStorageType;
   accessKeyId: string;
   secretAccessKey: string;
   region?: string;
   endpoint?: string;
   sessionToken?: string;
}

interface CloudStorageBucket {
   name: string;
   creationDate?: Date;
}

interface CloudStorageObject {
   key: string;
   size?: number;
   lastModified?: Date;
   isFolder: boolean;
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
}): CloudStorageCredentials {
   return {
      type: "s3",
      accessKeyId: s3Connection.accessKeyId || "",
      secretAccessKey: s3Connection.secretAccessKey || "",
      region: s3Connection.region,
      endpoint: s3Connection.endpoint,
      sessionToken: s3Connection.sessionToken,
   };
}

function createCloudStorageClient(
   credentials: CloudStorageCredentials,
): S3Client {
   const isGCS = credentials.type === "gcs";

   const client = new S3Client({
      endpoint: isGCS ? "https://storage.googleapis.com" : credentials.endpoint,
      region: isGCS ? "auto" : credentials.region || "us-east-1",
      credentials: {
         accessKeyId: credentials.accessKeyId,
         secretAccessKey: credentials.secretAccessKey,
         sessionToken: credentials.sessionToken,
      },
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

export async function listCloudBuckets(
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
                  isFolder: false,
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

function getFileType(key: string): string {
   const lowerKey = key.toLowerCase();
   if (lowerKey.endsWith(".csv")) return "csv";
   if (lowerKey.endsWith(".parquet")) return "parquet";
   if (lowerKey.endsWith(".json")) return "json";
   if (lowerKey.endsWith(".jsonl") || lowerKey.endsWith(".ndjson"))
      return "jsonl";
   return "unknown";
}

function buildCloudUri(
   type: CloudStorageType,
   bucket: string,
   key: string,
): string {
   const scheme = type === "gcs" ? "gs" : "s3";
   return `${scheme}://${bucket}/${key}`;
}

function standardizeRunSQLResult(result: unknown): unknown[] {
   return Array.isArray(result)
      ? result
      : (result as { rows?: unknown[] }).rows || [];
}

// Batch size for parallel schema fetching to avoid overwhelming the connection
const SCHEMA_FETCH_BATCH_SIZE = 10;

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
            describeQuery = `DESCRIBE SELECT * FROM read_csv('${uri}', auto_detect=true) LIMIT 1`;
            break;
         case "parquet":
            describeQuery = `DESCRIBE SELECT * FROM read_parquet('${uri}') LIMIT 1`;
            break;
         case "json":
            describeQuery = `DESCRIBE SELECT * FROM read_json('${uri}', auto_detect=true) LIMIT 1`;
            break;
         case "jsonl":
            describeQuery = `DESCRIBE SELECT * FROM read_json('${uri}', format='newline_delimited', auto_detect=true) LIMIT 1`;
            break;
         default:
            logger.warn(`Unsupported file type for ${fileKey}`);
            return { resource: uri, columns: [] };
      }

      const result = await malloyConnection.runSQL(describeQuery);
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

   // Process in batches to avoid overwhelming the connection
   for (let i = 0; i < fileKeys.length; i += SCHEMA_FETCH_BATCH_SIZE) {
      const batch = fileKeys.slice(i, i + SCHEMA_FETCH_BATCH_SIZE);

      // Process batch in parallel
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

export async function listFilesInCloudDirectory(
   credentials: CloudStorageCredentials,
   bucketName: string,
   directoryPath: string,
): Promise<string[]> {
   const files = await listAllCloudFiles(credentials, bucketName);

   const filesInDirectory = files
      .filter((obj) => {
         if (!isDataFile(obj.key)) return false;

         const lastSlashIndex = obj.key.lastIndexOf("/");
         const fileDir =
            lastSlashIndex > 0 ? obj.key.substring(0, lastSlashIndex) : "";

         return fileDir === directoryPath;
      })
      .map((obj) => {
         const lastSlashIndex = obj.key.lastIndexOf("/");
         return lastSlashIndex > 0
            ? obj.key.substring(lastSlashIndex + 1)
            : obj.key;
      });

   return filesInDirectory;
}

// List all data files in a bucket with their full relative paths
export async function listAllDataFilesInBucket(
   credentials: CloudStorageCredentials,
   bucketName: string,
): Promise<string[]> {
   const files = await listAllCloudFiles(credentials, bucketName);
   return files.filter((obj) => isDataFile(obj.key)).map((obj) => obj.key);
}
