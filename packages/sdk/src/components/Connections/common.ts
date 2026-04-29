import { HTMLInputTypeAttribute } from "react";
import type {
   AzureConnection,
   BigqueryConnection,
   ConnectionTypeEnum,
   DatabricksConnection,
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
      DatabricksConnection &
      MysqlConnection &
      MotherDuckConnection &
      S3Connection &
      GCSConnection &
      AzureConnection &
      DucklakeConnection);
   type: HTMLInputTypeAttribute;
   required?: boolean;
   selectOptions?: Array<{ label: string; value: string }>;
   visibleWhen?: { field: string; value: string };
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
   databricks: [
      {
         label: "Host",
         name: "host",
         type: "text",
         required: true,
      },
      {
         label: "HTTP Path",
         name: "path",
         type: "text",
         required: true,
      },
      {
         label: "Access Token",
         name: "token",
         type: "password",
         required: false, // Either token or OAuth client credentials required
      },
      {
         label: "OAuth Client ID",
         name: "oauthClientId",
         type: "text",
         required: false, // Required with oauthClientSecret if no token
      },
      {
         label: "OAuth Client Secret",
         name: "oauthClientSecret",
         type: "password",
         required: false, // Required with oauthClientId if no token
      },
      {
         label: "Default Catalog",
         name: "defaultCatalog",
         type: "text",
         required: false,
      },
      {
         label: "Default Schema",
         name: "defaultSchema",
         type: "text",
         required: false,
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
   databricks: "databricksConnection",
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
   azure: "azureConnection",
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

// Fields for Azure ADLS attached database
export const azureAttachedDatabaseFields: Array<ConnectionField> = [
   {
      label: "Auth Type",
      name: "authType" as keyof AzureConnection,
      type: "text",
      required: true,
      selectOptions: [
         { label: "SAS Token", value: "sas_token" },
         { label: "Service Principal (SPN)", value: "service_principal" },
      ],
   },
   {
      label: "SAS URL (e.g. https://account.blob.core.windows.net/container/file.parquet?sp=r&st=...)",
      name: "sasUrl" as keyof AzureConnection,
      type: "text",
      required: false,
      visibleWhen: { field: "authType", value: "sas_token" },
   },
   {
      label: "Tenant ID",
      name: "tenantId" as keyof AzureConnection,
      type: "text",
      required: false,
      visibleWhen: { field: "authType", value: "service_principal" },
   },
   {
      label: "Client ID",
      name: "clientId" as keyof AzureConnection,
      type: "text",
      required: false,
      visibleWhen: { field: "authType", value: "service_principal" },
   },
   {
      label: "Client Secret",
      name: "clientSecret" as keyof AzureConnection,
      type: "password",
      required: false,
      visibleWhen: { field: "authType", value: "service_principal" },
   },
   {
      label: "Account Name",
      name: "accountName" as keyof AzureConnection,
      type: "text",
      required: false,
      visibleWhen: { field: "authType", value: "service_principal" },
   },
   {
      label: "File URL (e.g. abfss://container/path/file.parquet)",
      name: "fileUrl" as keyof AzureConnection,
      type: "text",
      required: false,
      visibleWhen: { field: "authType", value: "service_principal" },
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
   // For Azure ADLS
   if (dbType === "azure") {
      return azureAttachedDatabaseFields;
   }
   return [];
}
