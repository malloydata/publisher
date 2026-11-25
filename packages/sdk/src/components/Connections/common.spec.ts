// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { typedConnectionConfig } from "./common";

describe("typedConnectionConfig", () => {
   it("sends a SQL Server port as an integer and its TLS switches as booleans", () => {
      // FormData yields strings; the connector reads port as a number and
      // encrypt/trustServerCertificate as booleans, and drops a string.
      expect(
         typedConnectionConfig("sqlserver", {
            server: "db.example",
            port: "1433",
            encrypt: "true",
            trustServerCertificate: "false",
            instanceName: "SQLEXPRESS",
         }),
      ).toEqual({
         server: "db.example",
         port: 1433,
         encrypt: true,
         trustServerCertificate: false,
         instanceName: "SQLEXPRESS",
      });
   });

   it("sends BigQuery dbmeta as a boolean", () => {
      expect(
         typedConnectionConfig("bigquery", {
            defaultProjectId: "data-project",
            dbmeta: "true",
         }),
      ).toEqual({ defaultProjectId: "data-project", dbmeta: true });
   });

   it("leaves out a blank optional name rather than storing an empty one", () => {
      // The edit form submits "" for an untouched optional field; stored, the
      // connector would read a defined instanceName and drop the port.
      expect(
         typedConnectionConfig("sqlserver", {
            server: "db.example",
            port: "1434",
            hostNameInCertificate: "",
            instanceName: "",
         }),
      ).toEqual({ server: "db.example", port: 1434 });
   });

   it("leaves out a blank port rather than sending 0", () => {
      // A named instance needs no port; Number("") would send 0.
      expect(
         typedConnectionConfig("sqlserver", {
            server: "db.example",
            port: "",
            instanceName: "SQLEXPRESS",
         }),
      ).toEqual({ server: "db.example", instanceName: "SQLEXPRESS" });
   });

   it("leaves a value that does not parse for the server to name", () => {
      expect(typedConnectionConfig("sqlserver", { port: "14x3" })).toEqual({
         port: "14x3",
      });
   });

   it("leaves out a field the form did not collect", () => {
      // An edit that leaves encrypt untouched must not send `encrypt: false`.
      expect(typedConnectionConfig("sqlserver", { server: "x" })).toEqual({
         server: "x",
      });
   });

   it("passes a type with no typed fields through unchanged", () => {
      const config = { host: "h", port: "3306" };
      expect(typedConnectionConfig("mysql", config)).toBe(config);
   });
});
