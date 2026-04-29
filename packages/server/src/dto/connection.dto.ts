import { Type } from "class-transformer";
import {
   IsEnum,
   IsArray,
   IsNumber,
   IsOptional,
   IsString,
   ValidateNested,
} from "class-validator";
import "reflect-metadata";
import { components } from "../api";
import { ApiConnection } from "../service/model";

type AttachedDatabase = components["schemas"]["AttachedDatabase"];

export class PostgresConnectionDto {
   @IsOptional()
   @IsString()
   host?: string;

   @IsOptional()
   @IsNumber()
   port?: number;

   @IsOptional()
   @IsString()
   databaseName?: string;

   @IsOptional()
   @IsString()
   userName?: string;

   @IsOptional()
   @IsString()
   password?: string;

   @IsOptional()
   @IsString()
   connectionString?: string;
}

export class MysqlConnectionDto {
   @IsOptional()
   @IsString()
   host?: string;

   @IsOptional()
   @IsNumber()
   port?: number;

   @IsOptional()
   @IsString()
   database?: string;

   @IsOptional()
   @IsString()
   user?: string;

   @IsOptional()
   @IsString()
   password?: string;
}

export class BigqueryConnectionDto {
   @IsOptional()
   @IsString()
   defaultProjectId?: string;

   @IsOptional()
   @IsString()
   billingProjectId?: string;

   @IsOptional()
   @IsString()
   location?: string;

   @IsOptional()
   @IsString()
   serviceAccountKeyJson?: string;

   @IsOptional()
   @IsString()
   maximumBytesBilled?: string;

   @IsOptional()
   @IsString()
   queryTimeoutMilliseconds?: string;
}

export class SnowflakeConnectionDto {
   @IsOptional()
   @IsString()
   account?: string;

   @IsOptional()
   @IsString()
   username?: string;

   @IsOptional()
   @IsString()
   password?: string;

   @IsOptional()
   @IsString()
   privateKey?: string;

   @IsOptional()
   @IsString()
   privateKeyPass?: string;

   @IsOptional()
   @IsString()
   warehouse?: string;

   @IsOptional()
   @IsString()
   database?: string;

   @IsOptional()
   @IsString()
   schema?: string;

   @IsOptional()
   @IsString()
   role?: string;

   @IsOptional()
   @IsNumber()
   responseTimeoutMilliseconds?: number;
}

export class TrinoConnectionDto {
   @IsOptional()
   @IsString()
   server?: string;

   @IsOptional()
   @IsString()
   port?: number;

   @IsOptional()
   @IsString()
   catalog?: string;

   @IsOptional()
   @IsString()
   schema?: string;

   @IsOptional()
   @IsString()
   user?: string;

   @IsOptional()
   @IsString()
   password?: string;

   @IsOptional()
   @IsString()
   peakaKey?: string;
}

export class DuckdbConnectionDto {
   @IsOptional()
   @IsArray()
   attachedDatabases?: AttachedDatabase[];
}

export class ConnectionDto implements ApiConnection {
   @IsOptional()
   @IsString()
   name?: string;

   @IsOptional()
   @IsEnum([
      "postgres",
      "bigquery",
      "snowflake",
      "trino",
      "mysql",
      "duckdb",
      "motherduck",
      "ducklake",
   ])
   type?:
      | "postgres"
      | "bigquery"
      | "snowflake"
      | "trino"
      | "mysql"
      | "duckdb"
      | "motherduck"
      | "ducklake";

   @IsOptional()
   @ValidateNested()
   @Type(() => PostgresConnectionDto)
   postgresConnection?: PostgresConnectionDto;

   @IsOptional()
   @ValidateNested()
   @Type(() => BigqueryConnectionDto)
   bigqueryConnection?: BigqueryConnectionDto;

   @IsOptional()
   @ValidateNested()
   @Type(() => SnowflakeConnectionDto)
   snowflakeConnection?: SnowflakeConnectionDto;

   @IsOptional()
   @ValidateNested()
   @Type(() => TrinoConnectionDto)
   TrinoConnection?: TrinoConnectionDto;

   @IsOptional()
   @ValidateNested()
   @Type(() => DuckdbConnectionDto)
   duckdbConnection?: DuckdbConnectionDto;
}
