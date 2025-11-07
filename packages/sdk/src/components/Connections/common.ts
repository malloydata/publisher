import { HTMLInputTypeAttribute } from "react";
import type {
   BigqueryConnection,
   ConnectionTypeEnum,
   MotherDuckConnection,
   MysqlConnection,
   PostgresConnection,
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
      MotherDuckConnection);
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
      },
      {
         label: "Port",
         name: "port",
         type: "number",
      },
      {
         label: "Database Name",
         name: "databaseName",
         type: "text",
      },
      {
         label: "User Name",
         name: "userName",
         type: "text",
      },
      {
         label: "Password",
         name: "password",
         type: "password",
      },
      {
         label: "Connection String",
         name: "connectionString",
         type: "text",
      },
   ],
   bigquery: [
      {
         label: "Project ID",
         name: "defaultProjectId",
         type: "text",
      },
      {
         label: "Billing Project ID",
         name: "billingProjectId",
         type: "text",
      },
      {
         label: "Location",
         name: "location",
         type: "text",
      },
      {
         label: "Service Account Key JSON",
         name: "serviceAccountKeyJson",
         type: "text",
      },
      {
         label: "Maximum Bytes Billed",
         name: "maximumBytesBilled",
         type: "text",
      },
      {
         label: "Query Timeout Milliseconds",
         name: "queryTimeoutMilliseconds",
         type: "text",
      },
   ],
   snowflake: [
      {
         label: "Account",
         name: "account",
         type: "text",
      },
      {
         label: "Username",
         name: "username",
         type: "text",
      },
      {
         label: "Password",
         name: "password",
         type: "password",
      },
      {
         label: "Role",
         name: "role",
         type: "text",
      },
      {
         label: "Warehouse",
         name: "warehouse",
         type: "text",
      },
      {
         label: "Database",
         name: "database",
         type: "text",
      },
      {
         label: "Schema",
         name: "schema",
         type: "text",
      },
      {
         label: "Response Timeout Milliseconds",
         name: "responseTimeoutMilliseconds",
         type: "text",
      },
   ],
   trino: [
      {
         label: "Server",
         name: "server",
         type: "text",
      },
      {
         label: "Port",
         name: "port",
         type: "number",
      },
      {
         label: "Catalog",
         name: "catalog",
         type: "text",
      },
      {
         label: "Schema",
         name: "schema",
         type: "text",
      },
      {
         label: "User",
         name: "user",
         type: "text",
      },
      {
         label: "Password",
         name: "password",
         type: "password",
      },
      {
         label: "Peaka Key",
         name: "peakaKey",
         type: "password",
      },
   ],
   mysql: [
      {
         label: "Host",
         name: "host",
         type: "text",
      },
      {
         label: "Port",
         name: "port",
         type: "number",
      },
      {
         label: "User",
         name: "user",
         type: "text",
      },
      {
         label: "Password",
         name: "password",
         type: "password",
      },
      {
         label: "Database",
         name: "database",
         type: "text",
      },
   ],
   duckdb: [],
   motherduck: [
      {
         label: "Access Token",
         name: "accessToken",
         type: "text",
      },
      {
         label: "Database",
         name: "database",
         type: "text",
      },
   ],
};

export const attributesFieldName: Record<ConnectionTypeEnum, string> = {
   postgres: "postgresConnection",
   bigquery: "bigqueryConnection",
   snowflake: "snowflakeConnection",
   trino: "trinoConnection",
   mysql: "mysqlConnection",
   duckdb: "duckdbConnection",
   motherduck: "motherduckConnection",
};
