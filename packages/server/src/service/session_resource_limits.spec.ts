// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The resource bound that keeps N DuckDB instances in one process from each
// claiming most of the container. Asserted on the SQL actually issued, against a
// recording stub, so an unset value proving to be a no-op is a real assertion
// rather than an absence of one.
import { afterEach, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { applySessionResourceLimits } from "./connection";

function recorder(): { conn: DuckDBConnection; sql: string[] } {
   const sql: string[] = [];
   const conn = {
      runSQL: async (q: string) => {
         sql.push(q);
         return { rows: [], totalRows: 0 };
      },
   } as unknown as DuckDBConnection;
   return { conn, sql };
}

describe("applySessionResourceLimits", () => {
   afterEach(() => {
      delete process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT;
      delete process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY;
   });

   it("issues nothing when neither is configured", async () => {
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn);
      expect(sql).toEqual([]);
   });

   it("sets a flat memory limit verbatim", async () => {
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "1GB";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn);
      expect(sql).toEqual(["SET memory_limit = '1GB'"]);
   });

   it("treats `off` as unset, so a large single-session host can opt out", async () => {
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "off";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn);
      expect(sql).toEqual([]);
   });

   it("sets the configured temp directory", async () => {
      process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = "/var/spill";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn);
      expect(sql).toEqual(["SET temp_directory = '/var/spill'"]);
   });

   it("prefers a session-owned directory over the configured default", async () => {
      // A build session's disposable workDir: unique per build, removed with it.
      process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = "/var/spill";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn, { tempDirectory: "/tmp/build-1" });
      expect(sql).toEqual(["SET temp_directory = '/tmp/build-1'"]);
   });

   it("sets temp_directory BEFORE memory_limit", async () => {
      // Order matters: a limit low enough to force spill must never be in effect
      // while temp_directory still points at DuckDB's default.
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "512MB";
      process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = "/var/spill";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn);
      expect(sql).toEqual([
         "SET temp_directory = '/var/spill'",
         "SET memory_limit = '512MB'",
      ]);
   });

   it("applies once per connection even when the funnel is reached twice", async () => {
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "1GB";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn);
      await applySessionResourceLimits(conn);
      expect(sql).toEqual(["SET memory_limit = '1GB'"]);
   });

   it("escapes a quote in a configured value rather than breaking the statement", async () => {
      process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = "/var/it's";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn);
      expect(sql).toEqual(["SET temp_directory = '/var/it''s'"]);
   });

   it("propagates a rejected value instead of opening an unbounded session", async () => {
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "not-a-size";
      const conn = {
         runSQL: async () => {
            throw new Error("Parser Error: unrecognized memory size");
         },
      } as unknown as DuckDBConnection;
      await expect(applySessionResourceLimits(conn)).rejects.toThrow(
         /unrecognized memory size/,
      );
   });
});
