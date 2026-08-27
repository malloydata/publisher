// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The resource bound that keeps N DuckDB instances in one process from each
// claiming most of the container. Asserted on the SQL actually issued, against a
// recording stub, so an unset value proving to be a no-op is a real assertion
// rather than an absence of one.
import { afterEach, describe, expect, it } from "bun:test";
import { chmodSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { applySessionResourceLimits } from "./connection";
import { createIsolatedBuildSession } from "./materialization_build_session";
import { applyExtensionSessionSettings } from "./connection";
import { DuckDBConnection as MetadataStoreConnection } from "../storage/duckdb/DuckDBConnection";
import { assertDuckDBResourceConfig } from "../config";

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

   it("lets a session-owned directory re-point one already set from the default", async () => {
      // The order these are reached in is not visible from inside: a destination
      // attach carries a build session through the extension funnel, which knows
      // nothing of the build's own directory. Latching the first value silently
      // cost the build its directory on the destination type that reaches
      // production, so spill went to the shared path, outlived `dispose`, and
      // could collide between concurrent builds.
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "512MB";
      process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = "/var/spill";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn); // the funnel
      await applySessionResourceLimits(conn, { tempDirectory: "/tmp/build-1" });
      expect(sql).toEqual([
         "SET temp_directory = '/var/spill'",
         "SET memory_limit = '512MB'",
         "SET temp_directory = '/tmp/build-1'",
      ]);
   });

   it("does not re-issue when the session-owned directory is already in effect", async () => {
      process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = "/var/spill";
      const { conn, sql } = recorder();
      await applySessionResourceLimits(conn, { tempDirectory: "/tmp/build-1" });
      await applySessionResourceLimits(conn, { tempDirectory: "/tmp/build-1" });
      await applySessionResourceLimits(conn);
      expect(sql).toEqual(["SET temp_directory = '/tmp/build-1'"]);
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

   it("takes effect on a REAL build session, read back from DuckDB", async () => {
      // Against a real instance, not a stub. Every other case here asserts the
      // SQL a stub was handed, which cannot tell whether the statements reached
      // an engine or whether DuckDB accepted them — and it is what let an
      // ordering bug ship with nine green tests. `488.2 MiB` rather than `512MB`
      // is DuckDB reporting its own rounding, which is itself the evidence the
      // value was parsed rather than merely issued.
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "512MB";
      const { session, dispose, workDir } =
         createIsolatedBuildSession("limits_test");
      try {
         await applySessionResourceLimits(session, { tempDirectory: workDir });
         const settings = await session.runSQL(
            "SELECT current_setting('memory_limit') AS m, " +
               "current_setting('temp_directory') AS t",
         );
         const row = (
            settings as unknown as { rows: { m: string; t: string }[] }
         ).rows[0];
         expect(row.m).toBe("488.2 MiB");
         expect(row.t).toBe(workDir);
      } finally {
         await dispose();
      }
   }, 30_000);

   // WIRING. Everything above proves the applier behaves; these prove it is
   // actually reached. Deleting a call site left the whole suite green, so the
   // "every session is bounded" claim rested on reading the code.

   it("is reached through the extension-settings funnel", async () => {
      // The funnel is what covers the serve/package/attach paths — the limits sit
      // ahead of its autoinstall early-return precisely so they do not inherit
      // its conditions, and nothing held that.
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "512MB";
      const conn = new DuckDBConnection("funnel_wiring", ":memory:");
      try {
         await applyExtensionSessionSettings(conn);
         const r = await conn.runSQL(
            "SELECT current_setting('memory_limit') AS m",
         );
         expect((r as unknown as { rows: { m: string }[] }).rows[0].m).toBe(
            "488.2 MiB",
         );
      } finally {
         await conn.close();
      }
   }, 30_000);

   it("is reached by the metadata store, which is a different DuckDB class", async () => {
      // `publisher.db` is not Malloy's connection, so the funnel never sees it and
      // it takes its bound as instance config. It is also held open for the whole
      // server lifetime, which makes it one of the budgets that has to fit.
      process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "512MB";
      const store = new MetadataStoreConnection(":memory:");
      try {
         await store.initialize();
         const rows = await store.all<{ m: string }>(
            "SELECT current_setting('memory_limit') AS m",
         );
         expect(rows[0].m).toBe("488.2 MiB");
      } finally {
         await store.close();
      }
   }, 30_000);

   describe("assertDuckDBResourceConfig", () => {
      it("rejects trailing garbage that DuckDB would reject later", () => {
         process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "1GBB";
         expect(() => assertDuckDBResourceConfig()).toThrow(
            /PUBLISHER_DUCKDB_MEMORY_LIMIT/,
         );
      });

      it("rejects a bare byte count, which DuckDB has no unit for", () => {
         process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "1073741824";
         expect(() => assertDuckDBResourceConfig()).toThrow(
            /PUBLISHER_DUCKDB_MEMORY_LIMIT/,
         );
      });

      it("accepts the spellings DuckDB accepts", () => {
         for (const value of ["1GB", "1gb", "1 GB", "1GiB", "1.5GB", "512MB"]) {
            process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = value;
            expect(() => assertDuckDBResourceConfig()).not.toThrow();
         }
      });

      it("accepts `off`, which is the documented opt-out", () => {
         process.env.PUBLISHER_DUCKDB_MEMORY_LIMIT = "off";
         expect(() => assertDuckDBResourceConfig()).not.toThrow();
      });

      it("fails on an existing directory it cannot write to", () => {
         // The common Kubernetes shape: a volume mounted with the wrong
         // ownership. `mkdirSync(…, { recursive: true })` returns silently for an
         // existing directory whatever its mode, so without the explicit access
         // check this surfaced only at the first spill.
         const dir = mkdtempSync(path.join(os.tmpdir(), "unwritable-"));
         chmodSync(dir, 0o500);
         process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = dir;
         try {
            expect(() => assertDuckDBResourceConfig()).toThrow(
               /PUBLISHER_DUCKDB_TEMP_DIRECTORY/,
            );
         } finally {
            chmodSync(dir, 0o700);
            rmSync(dir, { recursive: true, force: true });
         }
      });

      it("creates a missing spill directory rather than failing at first spill", () => {
         const base = mkdtempSync(path.join(os.tmpdir(), "spillbase-"));
         const dir = path.join(base, "nested", "spill");
         process.env.PUBLISHER_DUCKDB_TEMP_DIRECTORY = dir;
         try {
            expect(() => assertDuckDBResourceConfig()).not.toThrow();
            expect(existsSync(dir)).toBe(true);
         } finally {
            rmSync(base, { recursive: true, force: true });
         }
      });
   });
});
