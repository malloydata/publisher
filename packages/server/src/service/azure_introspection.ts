// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How a DuckDB connection's table listing reads Azure: the blobs a glob URL
 * names, a remote file's columns, and whether an attached Azure database
 * covers a location (and its SAS token with it).
 *
 * A leaf: it imports no connection module, so the connection-type registry
 * (service/dialects/) can use it.
 */

import { ClientSecretCredential } from "@azure/identity";
import { ContainerClient } from "@azure/storage-blob";
import type { Connection } from "@malloydata/malloy";
import type { components } from "../api";
import { logger } from "../logger";
import {
   getFileType,
   isDataFile,
   standardizeRunSQLResult,
} from "./gcs_s3_utils";
import { runIntrospectionSQL, sqlLiteral } from "./introspection_sql";

type ApiTable = components["schemas"]["Table"];

type ApiAzureConnection = components["schemas"]["AzureConnection"];

/**
 * Lists blobs in an Azure container matching a glob-like prefix/extension filter.
 * Parses an HTTPS SAS URL like:
 *   https://account.blob.core.windows.net/container/path/*.parquet?sasToken
 * Returns individual file URLs with the SAS token appended.
 */
async function listAzureBlobs(
   fileUrl: string,
   azureConnection?: ApiAzureConnection,
): Promise<{ url: string; blobName: string }[]> {
   // Split URL and SAS token carefully to avoid encoding issues with signatures
   const queryStart = fileUrl.indexOf("?");
   const baseUrl = queryStart >= 0 ? fileUrl.substring(0, queryStart) : fileUrl;
   const sasToken = queryStart >= 0 ? fileUrl.substring(queryStart) : "";

   let accountUrl: string;
   let container: string;
   let blobPath: string;

   if (baseUrl.startsWith("abfss://")) {
      // abfss://container/path or abfss://account.dfs.core.windows.net/container/path
      const withoutScheme = baseUrl.substring("abfss://".length);
      const parts = withoutScheme.split("/").filter(Boolean);
      if (parts[0].includes(".")) {
         // Fully qualified: abfss://account.dfs.core.windows.net/container/path
         const accountName = parts[0].split(".")[0];
         accountUrl = `https://${accountName}.blob.core.windows.net`;
         container = parts[1];
         blobPath = parts.slice(2).join("/");
      } else {
         // Short form: abfss://container/path — need accountName from config
         if (!azureConnection?.accountName) {
            throw new Error(
               "accountName is required to list blobs with abfss:// URLs",
            );
         }
         accountUrl = `https://${azureConnection.accountName}.blob.core.windows.net`;
         container = parts[0];
         blobPath = parts.slice(1).join("/");
      }
   } else {
      // https://account.blob.core.windows.net/container/path
      const url = new URL(baseUrl);
      const pathParts = url.pathname.split("/").filter(Boolean);
      container = pathParts[0];
      blobPath = pathParts.slice(1).join("/");
      accountUrl = `${url.protocol}//${url.host}`;
   }

   // Three supported glob patterns:
   //   path/file.ext       → single file (handled upstream by isAzureSingleFileUrl)
   //   path/*.ext          → files directly in path/ with that extension (no subdirs)
   //   path/**             → all valid data files in path/ and nested dirs (recursive)
   let prefix: string;
   let extensionFilter = ""; // for *.ext pattern
   let recursive = true; // for ** pattern

   if (blobPath.endsWith("**")) {
      prefix = blobPath.slice(0, -2);
      recursive = true;
   } else if (blobPath.includes("*")) {
      const starIndex = blobPath.indexOf("*");
      prefix = blobPath.substring(0, starIndex);
      extensionFilter = blobPath.substring(starIndex + 1); // e.g. ".parquet"
      recursive = false;
   } else {
      // No glob — use blobPath as prefix (container-level listing)
      prefix = blobPath;
      recursive = true;
   }

   let containerClient: ContainerClient;
   if (
      azureConnection?.authType === "service_principal" &&
      azureConnection.tenantId &&
      azureConnection.clientId &&
      azureConnection.clientSecret
   ) {
      const credential = new ClientSecretCredential(
         azureConnection.tenantId,
         azureConnection.clientId,
         azureConnection.clientSecret,
      );
      containerClient = new ContainerClient(
         `${accountUrl}/${container}`,
         credential,
      );
   } else {
      // SAS token auth — append token to container URL
      const containerUrl = `${accountUrl}/${container}${sasToken}`;
      containerClient = new ContainerClient(containerUrl);
   }

   const matchingFiles: { url: string; blobName: string }[] = [];
   for await (const blob of containerClient.listBlobsFlat({
      prefix: prefix || undefined,
   })) {
      if (extensionFilter && !blob.name.endsWith(extensionFilter)) continue;
      // For *.ext (non-recursive): only allow files directly in prefix dir
      if (!recursive) {
         const nameAfterPrefix = blob.name.substring(prefix.length);
         if (nameAfterPrefix.includes("/")) continue;
      }
      if (!isDataFile(blob.name)) continue;
      // For SPN: use abfss:// URLs that DuckDB's azure extension can read
      // For SAS: use https:// URLs with token appended
      let url: string;
      if (azureConnection?.authType === "service_principal") {
         const account =
            azureConnection.accountName ||
            accountUrl.split("//")[1]?.split(".")[0];
         url = `abfss://${account}.dfs.core.windows.net/${container}/${blob.name}`;
      } else {
         url = `${accountUrl}/${container}/${blob.name}${sasToken}`;
      }
      matchingFiles.push({ url, blobName: blob.name });
   }

   logger.info(
      `Listed ${matchingFiles.length} matching blobs in Azure container ${container} with prefix "${prefix}"`,
   );
   return matchingFiles;
}

/** These builders always run against DuckDB, whatever connection reached them. */
const DUCKDB_DIALECT = "duckdb";

/**
 * DESCRIBE a single remote data file.
 *
 * `fileUri` is caller-controlled: `listTablesForDuckDB` routes any `schemaName`
 * beginning with `https://`, `abfss://` or `az://` here straight from an MCP
 * tool argument, with no warehouse credentials needed, so it is escaped like any
 * literal (DuckDB's ANSI quote-doubling).
 */
async function describeRemoteFile(
   malloyConnection: Connection,
   fileUri: string,
): Promise<ApiTable> {
   const pathWithoutQuery = fileUri.split("?")[0];
   const fileType = getFileType(pathWithoutQuery);

   let describeQuery: string;
   switch (fileType) {
      case "csv":
         describeQuery = `DESCRIBE SELECT * FROM read_csv('${sqlLiteral(fileUri, DUCKDB_DIALECT)}', auto_detect=true) LIMIT 1`;
         break;
      case "parquet":
         describeQuery = `DESCRIBE SELECT * FROM read_parquet('${sqlLiteral(fileUri, DUCKDB_DIALECT)}') LIMIT 1`;
         break;
      case "json":
         describeQuery = `DESCRIBE SELECT * FROM read_json('${sqlLiteral(fileUri, DUCKDB_DIALECT)}', auto_detect=true) LIMIT 1`;
         break;
      case "jsonl":
         describeQuery = `DESCRIBE SELECT * FROM read_json('${sqlLiteral(fileUri, DUCKDB_DIALECT)}', format='newline_delimited', auto_detect=true) LIMIT 1`;
         break;
      default:
         logger.warn(`Unsupported file type for file: ${fileUri}`);
         return { resource: fileUri, columns: [] };
   }

   const result = await runIntrospectionSQL(malloyConnection, describeQuery);
   const rows = standardizeRunSQLResult(result);
   const columns = rows.map((row: unknown) => {
      const typedRow = row as Record<string, unknown>;
      return {
         name: (typedRow.column_name || typedRow.name) as string,
         type: (typedRow.column_type || typedRow.type) as string,
      };
   });

   const fileName = pathWithoutQuery.split("/").pop() || fileUri;
   return { resource: fileName, columns };
}

function isAzureSingleFileUrl(fileUri: string): boolean {
   const pathWithoutQuery = fileUri.split("?")[0];
   // Has a glob — not a single file
   if (pathWithoutQuery.includes("*")) return false;
   // Ends with / — directory listing
   if (pathWithoutQuery.endsWith("/")) return false;
   // Check if the last path segment has a data file extension
   const lastSegment = pathWithoutQuery.split("/").pop() || "";
   return isDataFile(lastSegment);
}

/**
 * The storage account and container an Azure URL names, as a comparable key,
 * or undefined when it names none. Hosts under `core.windows.net` compare by
 * account name, so a blob URL and a dfs URL for the same account match; any
 * other host compares in full. A short `abfss://container/path` takes its
 * account from `accountName`.
 */
function azureLocationKey(
   url: string,
   accountName?: string,
): string | undefined {
   const base = url.split("?")[0];
   const scheme = base.slice(0, base.indexOf("://"));
   const rest = base
      .slice(scheme.length + 3)
      .split("/")
      .filter(Boolean);
   if (rest.length < 2 && !(rest.length === 1 && accountName)) return undefined;
   let host: string;
   let container: string;
   if (scheme !== "https" && !rest[0].includes(".")) {
      if (!accountName) return undefined;
      host = `${accountName}.blob.core.windows.net`;
      container = rest[0];
   } else {
      host = rest[0];
      container = rest[1];
   }
   if (!container) return undefined;
   host = host.toLowerCase();
   const account = host.endsWith(".core.windows.net")
      ? host.split(".")[0]
      : host;
   return `${account}/${container}`;
}

/** Does `url` fall inside the account and container `attachment` is configured for? */
export function azureAttachmentCovers(
   attachment: ApiAzureConnection,
   url: string,
): boolean {
   const configured =
      attachment.authType === "sas_token"
         ? attachment.sasUrl
         : attachment.fileUrl;
   if (!configured) return false;
   const want = azureLocationKey(url, attachment.accountName);
   return (
      want !== undefined &&
      want === azureLocationKey(configured, attachment.accountName)
   );
}

/** A SAS attachment's token, appended to a URL in its container that carries none. */
export function withAttachmentSasToken(
   url: string,
   attachment: ApiAzureConnection,
): string {
   if (attachment.authType !== "sas_token" || url.includes("?")) return url;
   const token = attachment.sasUrl?.split("?")[1];
   return token ? `${url}?${token}` : url;
}

export async function describeAzureFile(
   malloyConnection: Connection,
   fileUri: string,
   azureConnection?: ApiAzureConnection,
): Promise<ApiTable[]> {
   try {
      if (isAzureSingleFileUrl(fileUri)) {
         // Single file — describe directly via DuckDB
         return [await describeRemoteFile(malloyConnection, fileUri)];
      }

      // Glob pattern or container/directory URL — list blobs via Azure SDK
      const blobs = await listAzureBlobs(fileUri, azureConnection);
      if (blobs.length === 0) {
         return [{ resource: fileUri, columns: [] }];
      }

      const results = await Promise.all(
         blobs.map(async ({ url, blobName }) => {
            try {
               const table = await describeRemoteFile(malloyConnection, url);
               return { ...table, resource: blobName };
            } catch (error) {
               logger.warn(`Failed to describe Azure blob: ${url}`, { error });
               return { resource: blobName, columns: [] } as ApiTable;
            }
         }),
      );
      return results;
   } catch (error) {
      logger.error(`Failed to describe Azure file: ${fileUri}`, { error });
      throw new Error(
         `Failed to describe Azure file: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}
