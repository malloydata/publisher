import { describe, expect, it } from "bun:test";
import { assembleProjectConnections } from "./connection_config";
import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

describe("assembleProjectConnections — databricks", () => {
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
      const { pojo, apiConnections } = assembleProjectConnections([validBase]);

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
      const { pojo } = assembleProjectConnections([conn]);
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
      expect(() => assembleProjectConnections([conn])).toThrow(
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
      expect(() => assembleProjectConnections([conn])).toThrow(
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
      expect(() => assembleProjectConnections([conn])).toThrow(
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
      expect(() => assembleProjectConnections([conn])).toThrow(
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
      expect(() => assembleProjectConnections([conn])).toThrow(
         "Databricks requires",
      );
   });
});
