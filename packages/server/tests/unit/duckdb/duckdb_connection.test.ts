/// <reference types="bun-types" />

// Direct unit tests for the storage-layer DuckDBConnection -- the embedded
// metadata DAO (publisher.db) that was migrated from the legacy `duckdb`
// callback binding to `@duckdb/node-api` (the "neo" promise API). The
// repositories depend on this class's public surface, so these assert it
// behaves identically across the migration: lifecycle, parameterized CRUD with
// `?` placeholders, row-shape parity, and error wrapping.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";

describe("DuckDBConnection (storage DAO over @duckdb/node-api)", () => {
   let dbDir: string;
   let conn: DuckDBConnection;

   beforeEach(async () => {
      // Unique dir per test so file handles never collide across tests
      // (Windows runners are sensitive to reopening the same DuckDB file).
      dbDir = await fs.mkdtemp(path.join(os.tmpdir(), "duckdb-conn-"));
      conn = new DuckDBConnection(path.join(dbDir, "test.db"));
      await conn.initialize();
   });

   afterEach(async () => {
      try {
         await conn.close();
      } catch {
         // already closed / never opened
      }
      await fs.rm(dbDir, { recursive: true, force: true });
   });

   describe("lifecycle", () => {
      it("initializes and reports a usable connection", async () => {
         const rows = await conn.all<{ answer: number }>("SELECT 42 AS answer");
         expect(rows[0].answer).toBe(42);
      });

      it("isInitialized is false before the schema exists", async () => {
         // Fresh DB with no `environments` table yet.
         expect(await conn.isInitialized()).toBe(false);
      });

      it("isInitialized becomes true once the environments table exists", async () => {
         await conn.run("CREATE TABLE environments (id VARCHAR PRIMARY KEY)");
         expect(await conn.isInitialized()).toBe(true);
      });

      it("close() is idempotent and a second close does not throw", async () => {
         await conn.close();
         await expect(conn.close()).resolves.toBeUndefined();
      });
   });

   describe("run / all / get with ? placeholders", () => {
      beforeEach(async () => {
         await conn.run(`
            CREATE TABLE t (
               id VARCHAR PRIMARY KEY,
               name VARCHAR,
               flag BOOLEAN,
               metadata JSON,
               created_at TIMESTAMP
            )
         `);
      });

      it("binds positional ? parameters on INSERT and reads them back", async () => {
         const now = new Date().toISOString();
         await conn.run(
            "INSERT INTO t (id, name, flag, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
            ["a", "alpha", true, JSON.stringify({ k: 1 }), now],
         );

         const row = await conn.get<{
            id: string;
            name: string;
            flag: boolean;
            metadata: string;
            created_at: unknown;
         }>("SELECT * FROM t WHERE id = ?", ["a"]);

         expect(row).not.toBeNull();
         expect(row!.id).toBe("a");
         expect(row!.name).toBe("alpha");
         // BOOLEAN round-trips as a JS boolean.
         expect(row!.flag).toBe(true);
         // JSON is stored/returned as a string the repositories JSON.parse().
         expect(typeof row!.metadata).toBe("string");
         expect(JSON.parse(row!.metadata)).toEqual({ k: 1 });
         // TIMESTAMP is consumable by `new Date(...)` (repositories do exactly this).
         expect(new Date(row!.created_at as string).toISOString()).toBe(now);
      });

      it("all() returns every matching row", async () => {
         await conn.run("INSERT INTO t (id, name) VALUES (?, ?)", ["a", "x"]);
         await conn.run("INSERT INTO t (id, name) VALUES (?, ?)", ["b", "y"]);

         const rows = await conn.all<{ id: string }>(
            "SELECT id FROM t ORDER BY id",
         );
         expect(rows.map((r) => r.id)).toEqual(["a", "b"]);
      });

      it("get() returns null when no row matches", async () => {
         const row = await conn.get("SELECT * FROM t WHERE id = ?", [
            "missing",
         ]);
         expect(row).toBeNull();
      });

      it("all() returns an empty array when no row matches", async () => {
         const rows = await conn.all("SELECT * FROM t WHERE id = ?", ["nope"]);
         expect(rows).toEqual([]);
      });

      it("supports statements with no parameters", async () => {
         await conn.run("INSERT INTO t (id) VALUES ('solo')");
         const rows = await conn.all<{ n: number | bigint }>(
            "SELECT count(*) AS n FROM t",
         );
         expect(Number(rows[0].n)).toBe(1);
      });

      it("UPDATE and DELETE bind ? parameters", async () => {
         await conn.run("INSERT INTO t (id, name) VALUES (?, ?)", ["a", "old"]);
         await conn.run("UPDATE t SET name = ? WHERE id = ?", ["new", "a"]);
         expect(
            (await conn.get<{ name: string }>(
               "SELECT name FROM t WHERE id = ?",
               ["a"],
            ))!.name,
         ).toBe("new");

         await conn.run("DELETE FROM t WHERE id = ?", ["a"]);
         expect(
            await conn.get("SELECT * FROM t WHERE id = ?", ["a"]),
         ).toBeNull();
      });
   });

   describe("error handling", () => {
      it("run() wraps a failing query with the query text", async () => {
         await expect(
            conn.run("INSERT INTO does_not_exist VALUES (1)"),
         ).rejects.toThrow(/Query execution failed[\s\S]*does_not_exist/);
      });

      it("all() wraps a failing query with the query text", async () => {
         await expect(conn.all("SELECT * FROM does_not_exist")).rejects.toThrow(
            /Query execution failed[\s\S]*does_not_exist/,
         );
      });

      it("run() throws when the connection is not initialized", async () => {
         const fresh = new DuckDBConnection(path.join(dbDir, "uninit.db"));
         await expect(fresh.run("SELECT 1")).rejects.toThrow(/not initialized/);
      });
   });

   describe("durability", () => {
      it("persists data to the file across reopen", async () => {
         const dbPath = path.join(dbDir, "persist.db");
         const first = new DuckDBConnection(dbPath);
         await first.initialize();
         await first.run("CREATE TABLE k (v VARCHAR)");
         await first.run("INSERT INTO k VALUES (?)", ["kept"]);
         await first.close();

         const second = new DuckDBConnection(dbPath);
         await second.initialize();
         const rows = await second.all<{ v: string }>("SELECT v FROM k");
         await second.close();
         expect(rows.map((r) => r.v)).toEqual(["kept"]);
      });
   });
});
