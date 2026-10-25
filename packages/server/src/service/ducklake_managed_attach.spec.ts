// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBInstance } from "@duckdb/node-api";
import { afterAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { managedSearchPathSql } from "./connection";

/**
 * A managed connection's `<name>.table('t')` compiles to the bare table name,
 * and DuckDB never searches an attached catalog for one. The search path set
 * after a managed attach is what makes the bare name resolve. Proven on a plain
 * attached database: the rule is DuckDB's catalog search, not DuckLake's.
 */
describe("managed attach search path", () => {
   const dir = mkdtempSync(join(tmpdir(), "managed-search-path-"));
   afterAll(() => rmSync(dir, { recursive: true, force: true }));

   it("quotes the alias as an identifier inside the setting's string literal", () => {
      expect(managedSearchPathSql("sales")).toBe(`SET search_path = '"sales".main'`);
      expect(managedSearchPathSql(`we"ird'name`)).toBe(
         `SET search_path = '"we""ird''name".main'`,
      );
   });

   it("makes a bare table name resolve inside the attached catalog", async () => {
      const instance = await DuckDBInstance.create(":memory:");
      const conn = await instance.connect();
      await conn.run(`ATTACH '${join(dir, "store.duckdb")}' AS sales`);
      await conn.run("CREATE TABLE sales.main.orders AS SELECT 42 AS n");

      await expect(conn.run("SELECT n FROM orders")).rejects.toThrow();

      await conn.run(managedSearchPathSql("sales"));
      const reader = await conn.runAndReadAll("SELECT n FROM orders");
      expect(reader.getRows()).toEqual([[42]]);
      conn.closeSync();
   });
});
