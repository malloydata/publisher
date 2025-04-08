import { expect, it, describe } from "bun:test";

import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import {
   BigqueryConnectionDto,
   ConnectionDto,
   PostgresConnectionDto,
   SnowflakeConnectionDto,
} from "./connection.dto";

describe("dto/connection", () => {
   describe("Connection Validation", () => {
      it("should validate a valid PostgresConnection object", async () => {
         const validData = {
            host: "localhost",
            port: 5432,
            databaseName: "testdb",
            userName: "user",
            password: "pass",
            connectionString: "postgres://user:pass@localhost:5432/testdb",
         };
         const postgresConnection = plainToInstance(
            PostgresConnectionDto,
            validData,
         );

         const errors = await validate(postgresConnection);
         expect(errors).toBe.empty;
      });

      it("should return errors for invalid PostgresConnection object", async () => {
         const invalidData = {
            host: 123, // Invalid type
            port: "not-a-number", // Invalid type
         };
         const postgresConnection = plainToInstance(
            PostgresConnectionDto,
            invalidData,
         );

         const errors = await validate(postgresConnection);
         expect(errors).not.toHaveLength(0);
         expect(errors).toHaveLength(2);
      });

      it("should validate a valid BigqueryConnection object", async () => {
         const validData = {
            defaultProjectId: "default-project",
            billingProjectId: "billing-project",
            location: "US",
            serviceAccountKeyJson: "{}",
            maximumBytesBilled: "1000000",
            queryTimeoutMilliseconds: "1000",
         };
         const bigqueryConnection = plainToInstance(
            BigqueryConnectionDto,
            validData,
         );

         const errors = await validate(bigqueryConnection);
         expect(errors).toHaveLength(0);
      });

      it("should validate a valid SnowflakeConnection object", async () => {
         const validData = {
            account: "my-account",
            username: "user",
            password: "pass",
            warehouse: "my-warehouse",
            database: "my-database",
            schema: "my-schema",
            responseTimeoutMilliseconds: 5000,
         };
         const snowflakeConnection = plainToInstance(
            SnowflakeConnectionDto,
            validData,
         );

         const errors = await validate(snowflakeConnection);
         expect(errors).toHaveLength(0);
      });

      it("should validate a valid Connection object with postgres type", async () => {
         const validData = {
            name: "My Postgres Connection",
            type: "postgres",
            postgresConnection: {
               host: "localhost",
               port: 5432,
               databaseName: "testdb",
               userName: "user",
               password: "pass",
            },
         };
         const connection = plainToInstance(ConnectionDto, validData);

         const errors = await validate(connection);
         expect(errors).toHaveLength(0);
      });

      it("should return errors for invalid Connection object", async () => {
         const invalidData = {
            type: "invalid-type", // Invalid enum value
            postgresConnection: {
               port: "invalid-port", // Invalid type
            },
         };
         const connection = plainToInstance(ConnectionDto, invalidData);

         const errors = await validate(connection);
         expect(errors).not.toHaveLength(0);
         expect(errors.length).toBeGreaterThan(0);
      });
   });
});
