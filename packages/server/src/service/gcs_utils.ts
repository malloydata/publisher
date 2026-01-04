import {
   ListBucketsCommand,
   ListObjectsV2Command,
   S3Client,
} from "@aws-sdk/client-s3";
import { logger } from "../logger";

export interface GCSCredentials {
   keyId: string;
   secret: string;
}

export interface GCSBucket {
   name: string;
   creationDate?: Date;
}

export interface GCSObject {
   key: string;
   size?: number;
   lastModified?: Date;
   isFolder: boolean;
}

function createGCSClient(credentials: GCSCredentials): S3Client {
   const client = new S3Client({
      endpoint: "https://storage.googleapis.com",
      region: "auto",
      credentials: {
         accessKeyId: credentials.keyId,
         secretAccessKey: credentials.secret,
      },
      forcePathStyle: true,
   });

   // Remove x-id query parameter that GCS doesn't support
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

   return client;
}

export async function listGCSBuckets(
   credentials: GCSCredentials,
): Promise<GCSBucket[]> {
   const client = createGCSClient(credentials);

   try {
      const response = await client.send(new ListBucketsCommand({}));
      return (response.Buckets || []).map((bucket) => ({
         name: bucket.Name || "",
         creationDate: bucket.CreationDate,
      }));
   } catch (error) {
      logger.error("Failed to list GCS buckets", { error });
      throw new Error(
         `Failed to list GCS buckets: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

async function listGCSObjectsInFolder(
   credentials: GCSCredentials,
   bucket: string,
   prefix: string = "",
): Promise<GCSObject[]> {
   const client = createGCSClient(credentials);

   try {
      const response = await client.send(
         new ListObjectsV2Command({
            Bucket: bucket,
            Prefix: prefix,
            Delimiter: "/",
         }),
      );

      const objects: GCSObject[] = [];

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
      logger.error("Failed to list GCS objects", { error, bucket, prefix });
      throw new Error(
         `Failed to list objects in gs://${bucket}/${prefix}: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

export async function listAllGCSFiles(
   credentials: GCSCredentials,
   bucket: string,
   prefix: string = "",
): Promise<GCSObject[]> {
   const allFiles: GCSObject[] = [];

   async function traverse(currentPrefix: string): Promise<void> {
      const objects = await listGCSObjectsInFolder(
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

export function buildGCSUri(bucket: string, key: string): string {
   return `gs://${bucket}/${key}`;
}
