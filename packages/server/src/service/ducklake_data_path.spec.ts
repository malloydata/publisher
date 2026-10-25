// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A DuckLake config with no data path is for a catalog created elsewhere (the
// admin API's POST /catalogs, or another tool): every attach uses the path the
// catalog recorded, and none may create a catalog. Asserted on the ATTACH
// actually issued, against a recording stub: a configured path must issue
// exactly what it did before.
import { describe, expect, it } from "bun:test";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../api";
import {
   attachDuckLakeForBootstrap,
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
   dataPathClauses,
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

const catalog = {
   postgresConnection: {
      host: "catalog",
      port: 5432,
      databaseName: "ducklake",
      userName: "u",
      password: "p",
   },
};
const withPath = (over: Partial<DucklakeConnection> = {}): DucklakeConnection =>
   ({
      catalog,
      storage: { bucketUrl: "/tmp/lake" },
      ...over,
   }) as DucklakeConnection;
const withoutPath = (
   over: Partial<DucklakeConnection> = {},
): DucklakeConnection => ({ catalog, ...over }) as DucklakeConnection;

const REFUSE = "CREATE_IF_NOT_EXISTS false";

describe("a DuckLake attach's data path", () => {
   it("is the pre-existing attach when a path is configured", async () => {
      const r = recorder();
      await attachDuckLakeReadWrite(r.conn, "lake", withPath());
      expect(r.attach()).toContain(
         "(DATA_PATH '/tmp/lake', OVERRIDE_DATA_PATH true)",
      );
      expect(r.attach()).not.toContain("CREATE_IF_NOT_EXISTS");
   });

   it("uses the recorded path and refuses a missing catalog on a build or load attach", async () => {
      const r = recorder();
      await attachDuckLakeReadWrite(r.conn, "lake", withoutPath());
      expect(r.attach()).not.toContain("DATA_PATH");
      expect(r.attach()).toContain(`(${REFUSE})`);
   });

   it("refuses on a read-write connection attach too", async () => {
      const r = recorder();
      await attachDuckLakeReadOnly(
         r.conn,
         "lake",
         withoutPath({ readOnly: false }),
      );
      expect(r.attach()).toContain(REFUSE);
   });

   it("adds nothing to a read-only attach, which never creates", async () => {
      const r = recorder();
      await attachDuckLakeReadOnly(r.conn, "lake", withoutPath());
      expect(r.attach()).toContain("(READ_ONLY true)");
      expect(r.attach()).not.toContain("DATA_PATH");
      expect(r.attach()).not.toContain("CREATE_IF_NOT_EXISTS");
   });

   it("creates on a bootstrap, which always names its path", async () => {
      const r = recorder();
      await attachDuckLakeForBootstrap(r.conn, "lake", withPath(), {
         encrypted: true,
      });
      expect(r.attach()).toContain("DATA_PATH '/tmp/lake'");
      expect(r.attach()).toContain("ENCRYPTED true");
      expect(r.attach()).not.toContain("CREATE_IF_NOT_EXISTS");
   });

   it("escapes a configured path", () => {
      expect(dataPathClauses("/tmp/o'brien", false)).toBe(
         "DATA_PATH '/tmp/o''brien', OVERRIDE_DATA_PATH true",
      );
   });
});
