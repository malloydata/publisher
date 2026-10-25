// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// `createIfNotExists: false` is for an orchestrator that creates a catalog only
// through the bootstrap route. Asserted on the ATTACH actually issued, against a
// recording stub: every read-write attach but the bootstrap's must carry
// DuckLake's CREATE_IF_NOT_EXISTS false, and an unset field must issue exactly
// what it did before.
import { describe, expect, it } from "bun:test";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../api";
import {
   attachDuckLakeForBootstrap,
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
} from "./connection";

type DucklakeConnection = components["schemas"]["DucklakeConnection"];

function recorder(): { conn: DuckDBConnection; attach: () => string } {
   const sql: string[] = [];
   const conn = {
      runSQL: async (q: string) => {
         sql.push(q);
         return { rows: [], totalRows: 0 };
      },
   } as unknown as DuckDBConnection;
   const attach = () => {
      const found = sql.filter((q) => q.includes("'ducklake:postgres:"));
      expect(found.length).toBe(1);
      return found[0];
   };
   return { conn, attach };
}

const lake = (over: Partial<DucklakeConnection> = {}): DucklakeConnection =>
   ({
      catalog: {
         postgresConnection: {
            host: "catalog",
            port: 5432,
            databaseName: "ducklake",
            userName: "u",
            password: "p",
         },
      },
      storage: { bucketUrl: "/tmp/lake" },
      ...over,
   }) as DucklakeConnection;

const REFUSE = "CREATE_IF_NOT_EXISTS false";

describe("createIfNotExists on a DuckLake attach", () => {
   it("issues the pre-existing attach when unset or true", async () => {
      for (const cfg of [lake(), lake({ createIfNotExists: true })]) {
         const r = recorder();
         await attachDuckLakeReadWrite(r.conn, "lake", cfg);
         expect(r.attach()).not.toContain("CREATE_IF_NOT_EXISTS");
      }
   });

   it("refuses a missing catalog on a build or load attach when false", async () => {
      const r = recorder();
      await attachDuckLakeReadWrite(
         r.conn,
         "lake",
         lake({ createIfNotExists: false }),
      );
      expect(r.attach()).toContain(REFUSE);
   });

   it("still creates on the bootstrap, the one attach meant to", async () => {
      const r = recorder();
      await attachDuckLakeForBootstrap(
         r.conn,
         "lake",
         lake({ createIfNotExists: false }),
         { encrypted: true },
      );
      expect(r.attach()).not.toContain("CREATE_IF_NOT_EXISTS");
      expect(r.attach()).toContain("ENCRYPTED true");
   });

   it("adds nothing to a read-only attach, which never creates", async () => {
      const r = recorder();
      await attachDuckLakeReadOnly(
         r.conn,
         "lake",
         lake({ createIfNotExists: false }),
      );
      expect(r.attach()).toContain("READ_ONLY true");
      expect(r.attach()).not.toContain("CREATE_IF_NOT_EXISTS");
   });

   it("refuses on a read-write connection attach when false", async () => {
      const r = recorder();
      await attachDuckLakeReadOnly(
         r.conn,
         "lake",
         lake({ readOnly: false, createIfNotExists: false }),
      );
      expect(r.attach()).toContain(REFUSE);
   });
});
