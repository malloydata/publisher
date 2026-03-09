import { HTMLInputTypeAttribute } from "react";
import type {
   BigqueryConnection,
   ConnectionTypeEnum,
   DucklakeConnection,
   GCSConnection,
   MotherDuckConnection,
   MysqlConnection,
   PostgresConnection,
   S3Connection,
   SnowflakeConnection,
   TrinoConnection,
} from "../../client/api";

type ConnectionField = {
   label: string;
   name: keyof (PostgresConnection &
      BigqueryConnection &
      SnowflakeConnection &
      TrinoConnection &
      MysqlConnection &
      MotherDuckConnection &
      S3Connection &
      GCSConnection &
      DucklakeConnection);
   type: HTMLInputTypeAttribute;
   required?: boolean;
};

export const connectionFieldsByType: Record<
   ConnectionTypeEnum,
   Array<ConnectionField>
> = {
   postgres: [
      {
         label: "Host",
         name: "host",
         type: "text",
         required: false, // Required only if connectionString is not provided
      },
      {
         label: "Port",
         name: "port",
         type: "number",
         required: false, // Required only if connectionString is not provided
      },
      {
         label: "Database Name",
         name: "databaseName",
         type: "text",
         required: false, // Required only if connectionString is not provided
      },
      {
         label: "User Name",
         name: "userName",
         type: "text",
         required: false, // Required only if connectionString is not provided
      },
      {
         label: "Password",
         name: "password",
         type: "password",
         required: false, // Required only if connectionString is not provided
      },
      {
         label: "Connection String",
         name: "connectionString",
         type: "text",
         required: false,
      },
   ],
   bigquery: [
      {
         label: "Project ID",
         name: "defaultProjectId",
         type: "text",
         required: false,
      },
      {
         label: "Billing Project ID",
         name: "billingProjectId",
         type: "text",
         required: false,
      },
      {
         label: "Location",
         name: "location",
         type: "text",
         required: false,
      },
      {
         label: "Service Account Key JSON",
         name: "serviceAccountKeyJson",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Maximum Bytes Billed",
         name: "maximumBytesBilled",
         type: "text",
         required: false,
      },
      {
         label: "Query Timeout Milliseconds",
         name: "queryTimeoutMilliseconds",
         type: "text",
         required: false,
      },
   ],
   snowflake: [
      {
         label: "Account",
         name: "account",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Username",
         name: "username",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Password",
         name: "password",
         type: "password",
         required: false, // Either password or privateKey is required
      },
      {
         label: "Role",
         name: "role",
         type: "text",
         required: false,
      },
      {
         label: "Warehouse",
         name: "warehouse",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Database",
         name: "database",
         type: "text",
         required: false,
      },
      {
         label: "Schema",
         name: "schema",
         type: "text",
         required: false,
      },
      {
         label: "Response Timeout Milliseconds",
         name: "responseTimeoutMilliseconds",
         type: "text",
         required: false,
      },
      {
         label: "Private Key",
         name: "privateKey",
         type: "password",
         required: false, // Either password or privateKey is required
      },
      {
         label: "Private Key Passphrase",
         name: "privateKeyPass",
         type: "password",
         required: false,
      },
   ],
   trino: [
      {
         label: "Server",
         name: "server",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Port",
         name: "port",
         type: "number",
         required: false,
      },
      {
         label: "Catalog",
         name: "catalog",
         type: "text",
         required: false,
      },
      {
         label: "Schema",
         name: "schema",
         type: "text",
         required: false,
      },
      {
         label: "User",
         name: "user",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Password",
         name: "password",
         type: "password",
         required: false, // Required for HTTPS unless peakaKey is used
      },
      {
         label: "Peaka Key",
         name: "peakaKey",
         type: "password",
         required: false, // Can be used instead of password for HTTPS
      },
   ],
   mysql: [
      {
         label: "Host",
         name: "host",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Port",
         name: "port",
         type: "number",
         required: true, // Always required
      },
      {
         label: "User",
         name: "user",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Password",
         name: "password",
         type: "password",
         required: true, // Always required
      },
      {
         label: "Database",
         name: "database",
         type: "text",
         required: true, // Always required
      },
   ],
   // DuckDB connections use attached databases configured via duckdbConnection.attachedDatabases
   duckdb: [],
   motherduck: [
      {
         label: "Access Token",
         name: "accessToken",
         type: "text",
         required: true, // Always required
      },
      {
         label: "Database",
         name: "database",
         type: "text",
         required: false,
      },
   ],
   // DuckLake uses custom form rendering (catalog + storage dropdowns)
   ducklake: [],
};

export const attributesFieldName: Record<ConnectionTypeEnum, string> = {
   postgres: "postgresConnection",
   bigquery: "bigqueryConnection",
   snowflake: "snowflakeConnection",
   trino: "trinoConnection",
   mysql: "mysqlConnection",
   duckdb: "duckdbConnection",
   motherduck: "motherduckConnection",
   ducklake: "ducklakeConnection",
};

// Mapping for attached database types to their connection field names
export const attachedDatabaseConnectionFieldName: Record<string, string> = {
   postgres: "postgresConnection",
   bigquery: "bigqueryConnection",
   snowflake: "snowflakeConnection",
   gcs: "gcsConnection",
   s3: "s3Connection",
};

// Fields for S3 attached database
export const s3AttachedDatabaseFields: Array<ConnectionField> = [
   {
      label: "Access Key ID",
      name: "accessKeyId" as keyof S3Connection,
      type: "text",
      required: true,
   },
   {
      label: "Secret Access Key",
      name: "secretAccessKey" as keyof S3Connection,
      type: "password",
      required: true,
   },
   {
      label: "Region",
      name: "region" as keyof S3Connection,
      type: "text",
      required: false,
   },
   {
      label: "Endpoint",
      name: "endpoint" as keyof S3Connection,
      type: "text",
      required: false,
   },
   {
      label: "Session Token",
      name: "sessionToken" as keyof S3Connection,
      type: "password",
      required: false,
   },
];

// Fields for GCS attached database
export const gcsAttachedDatabaseFields: Array<ConnectionField> = [
   {
      label: "Key ID",
      name: "keyId" as keyof GCSConnection,
      type: "text",
      required: true,
   },
   {
      label: "Secret",
      name: "secret" as keyof GCSConnection,
      type: "password",
      required: true,
   },
];

// Helper to get fields for attached database types
export function getAttachedDatabaseFields(
   dbType: string,
): Array<ConnectionField> {
   // For types that exist in ConnectionTypeEnum, use connectionFieldsByType
   if (
      dbType === "postgres" ||
      dbType === "bigquery" ||
      dbType === "snowflake"
   ) {
      return connectionFieldsByType[dbType as ConnectionTypeEnum] || [];
   }
   // For S3
   if (dbType === "s3") {
      return s3AttachedDatabaseFields;
   }
   // For GCS
   if (dbType === "gcs") {
      return gcsAttachedDatabaseFields;
   }
   return [];
}
