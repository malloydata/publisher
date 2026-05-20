import { describe, expect, it } from "bun:test";
import { generateKeyPairSync } from "crypto";
import { components } from "../api";
import {
   assembleEnvironmentConnections,
   normalizeSnowflakePrivateKey,
} from "./connection_config";

type ApiConnection = components["schemas"]["Connection"];

describe("assembleEnvironmentConnections — databricks", () => {
   const validBase: ApiConnection = {
      name: "dbx",
      type: "databricks",
      databricksConnection: {
         host: "dbc.cloud.databricks.com",
         path: "/sql/1.0/warehouses/abc",
         token: "dapiXXXX",
         defaultCatalog: "main",
         defaultSchema: "default",
      },
   };

   it("emits a databricks core entry with all known fields preserved", () => {
      const { pojo, apiConnections } = assembleEnvironmentConnections([
         validBase,
      ]);

      const entry = pojo.connections["dbx"];
      expect(entry.is).toBe("databricks");
      expect(entry.host).toBe("dbc.cloud.databricks.com");
      expect(entry.path).toBe("/sql/1.0/warehouses/abc");
      expect(entry.token).toBe("dapiXXXX");
      expect(entry.defaultCatalog).toBe("main");
      expect(entry.defaultSchema).toBe("default");

      expect(apiConnections).toHaveLength(1);
      expect(apiConnections[0].attributes?.dialectName).toBe("databricks");
   });

   it("accepts OAuth M2M auth (clientId + secret) without a token", () => {
      const conn: ApiConnection = {
         name: "dbx-oauth",
         type: "databricks",
         databricksConnection: {
            host: "dbc.cloud.databricks.com",
            path: "/sql/1.0/warehouses/abc",
            oauthClientId: "client-id",
            oauthClientSecret: "client-secret",
            defaultCatalog: "main",
         },
      };
      const { pojo } = assembleEnvironmentConnections([conn]);
      const entry = pojo.connections["dbx-oauth"];
      expect(entry.is).toBe("databricks");
      expect(entry.oauthClientId).toBe("client-id");
      expect(entry.oauthClientSecret).toBe("client-secret");
      expect(entry.token).toBeUndefined();
   });

   it("rejects connections missing the databricksConnection block", () => {
      const conn: ApiConnection = {
         name: "dbx",
         type: "databricks",
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks connection configuration is missing.",
      );
   });

   it("rejects connections with a missing host", () => {
      const conn: ApiConnection = {
         ...validBase,
         databricksConnection: {
            ...validBase.databricksConnection!,
            host: undefined,
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks host is required",
      );
   });

   it("rejects connections with a missing path", () => {
      const conn: ApiConnection = {
         ...validBase,
         databricksConnection: {
            ...validBase.databricksConnection!,
            path: undefined,
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks SQL warehouse HTTP path is required",
      );
   });

   it("rejects when defaultCatalog is missing", () => {
      const conn: ApiConnection = {
         name: "dbx",
         type: "databricks",
         databricksConnection: {
            host: "dbc.cloud.databricks.com",
            path: "/sql/1.0/warehouses/abc",
            token: "dapiXXXX",
            // defaultCatalog deliberately omitted
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks default catalog is required",
      );
   });

   it("rejects when neither token nor full OAuth credentials are provided", () => {
      const conn: ApiConnection = {
         name: "dbx",
         type: "databricks",
         databricksConnection: {
            host: "dbc.cloud.databricks.com",
            path: "/sql/1.0/warehouses/abc",
            // Only oauthClientId, missing secret → rejected.
            oauthClientId: "client-id",
            defaultCatalog: "main",
         },
      };
      expect(() => assembleEnvironmentConnections([conn])).toThrow(
         "Databricks requires",
      );
   });
});

describe("normalizeSnowflakePrivateKey", () => {
   const { privateKey: pkcs8Pem } = generateKeyPairSync("rsa", {
      modulusLength: 2048,
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
      publicKeyEncoding: { type: "spki", format: "pem" },
   });
   const { privateKey: pkcs1Pem } = generateKeyPairSync("rsa", {
      modulusLength: 2048,
      privateKeyEncoding: { type: "pkcs1", format: "pem" },
      publicKeyEncoding: { type: "pkcs1", format: "pem" },
   });

   it("passes a multi-line PKCS#8 key through and adds a trailing newline", () => {
      const trimmed = (pkcs8Pem as string).trimEnd();
      const result = normalizeSnowflakePrivateKey(trimmed);
      expect(result).toContain("-----BEGIN PRIVATE KEY-----");
      expect(result.endsWith("\n")).toBe(true);
   });

   it("converts a multi-line PKCS#1 RSA key to PKCS#8", () => {
      const result = normalizeSnowflakePrivateKey(pkcs1Pem as string);
      expect(result).toContain("-----BEGIN PRIVATE KEY-----");
      expect(result).not.toContain("BEGIN RSA PRIVATE KEY");
      expect(result.endsWith("\n")).toBe(true);
   });

   it("converts a single-line PKCS#1 RSA key to PKCS#8", () => {
      const singleLine = (pkcs1Pem as string).replace(/\n/g, "");
      const result = normalizeSnowflakePrivateKey(singleLine);
      expect(result).toContain("-----BEGIN PRIVATE KEY-----");
      expect(result).not.toContain("BEGIN RSA PRIVATE KEY");
   });

   it("reconstructs a single-line PKCS#8 key without conversion", () => {
      const singleLine = (pkcs8Pem as string).replace(/\n/g, "");
      const result = normalizeSnowflakePrivateKey(singleLine);
      expect(result.startsWith("-----BEGIN PRIVATE KEY-----\n")).toBe(true);
      expect(result.endsWith("-----END PRIVATE KEY-----\n")).toBe(true);
   });
});
