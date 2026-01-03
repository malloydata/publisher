import {
   ListBucketsCommand,
   ListObjectsV2Command,
   S3Client,
} from "@aws-sdk/client-s3";
import { Connection } from "@malloydata/malloy";
import { components } from "../api";
import { logger } from "../logger";

type ApiTable = components["schemas"]["Table"];

export type CloudStorageType = "gcs" | "s3";

export interface CloudStorageCredentials {
   type: CloudStorageType;
   accessKeyId: string;
   secretAccessKey: string;
   region?: string;
   endpoint?: string;
   sessionToken?: string;
}

export interface CloudStorageBucket {
   name: string;
   creationDate?: Date;
}

export interface CloudStorageObject {
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

async function listCloudObjectsInFolder(
   credentials: CloudStorageCredentials,
   bucket: string,
   prefix: string = "",
): Promise<CloudStorageObject[]> {
   const client = createCloudStorageClient(credentials);
   const storageType = credentials.type.toUpperCase();
   const uri = buildCloudUri(credentials.type, bucket, prefix);

   try {
      const response = await client.send(
         new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix,
            Delimiter: "/",
         }),
      );

      const objects: CloudStorageObject[] = [];

      for (const folderPrefix of response.CommonPrefixes || []) {
         if (folderPrefix.Prefix) {
            objects.push({
               key: folderPrefix.Prefix,
               isFolder: true,
            });
         }
      }

      for (const content of response.Contents || []) {
         if (content.Key && content.Key !== prefix) {
            objects.push({
               key: content.Key,
               size: content.Size,
               lastModified: content.LastModified,
               isFolder: false,
            });
         }
      }

      return objects;
   } catch (error) {
      logger.error(`Failed to list ${storageType} objects`, {
         error,
         bucket,
         prefix,
      });
      throw new Error(
         `Failed to list objects in ${uri}: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

export async function listAllCloudFiles(
   credentials: CloudStorageCredentials,
   bucket: string,
   prefix: string = "",
): Promise<CloudStorageObject[]> {
   const allFiles: CloudStorageObject[] = [];

   async function traverse(currentPrefix: string): Promise<void> {
      const objects = await listCloudObjectsInFolder(
         credentials,
         bucket,
         currentPrefix,
      );

      for (const obj of objects) {
         if (obj.isFolder) {
            await traverse(obj.key);
         } else {
            allFiles.push(obj);
         }
      }
   }

   await traverse(prefix);
   return allFiles;
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

export function getFileType(key: string): string {
   const lowerKey = key.toLowerCase();
   if (lowerKey.endsWith(".csv")) return "csv";
   if (lowerKey.endsWith(".parquet")) return "parquet";
   if (lowerKey.endsWith(".json")) return "json";
   if (lowerKey.endsWith(".jsonl") || lowerKey.endsWith(".ndjson"))
      return "jsonl";
   return "unknown";
}

export function buildCloudUri(
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

export async function getCloudTablesWithColumns(
   malloyConnection: Connection,
   credentials: CloudStorageCredentials,
   bucketName: string,
   fileKeys: string[],
): Promise<ApiTable[]> {
   const tables: ApiTable[] = [];

   for (const fileKey of fileKeys) {
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
               tables.push({
                  resource: uri,
                  columns: [],
               });
               continue;
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

         tables.push({
            resource: uri,
            columns,
         });

         logger.info(
            `Got schema for ${credentials.type.toUpperCase()} file: ${uri}`,
            {
               columnCount: columns.length,
            },
         );
      } catch (error) {
         logger.warn(
            `Failed to get schema for ${credentials.type.toUpperCase()} file: ${uri}`,
            {
               error,
            },
         );
         tables.push({
            resource: uri,
            columns: [],
         });
      }
   }

   return tables;
}
