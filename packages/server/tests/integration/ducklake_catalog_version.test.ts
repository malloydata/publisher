/// <reference types="bun-types" />

// Contract test between SUPPORTED_CATALOG_VERSIONS and the DuckLake extension
// that the runtime engine actually bakes and loads.
//
// The unit spec in src/ducklake_version.spec.ts only checks the constant
// against itself, so it stays green even when the constant drifts away from
// what the engine does on ATTACH -- which is exactly how the list ended up
// claiming the 0.3 line while the baked engine had moved to the "1.0" catalog
// format. These tests drive a real @malloydata/db-duckdb connection (the same
// engine the attach paths use) against local-file DuckLake catalogs so the
// constant is pinned to observable engine behavior, with no cloud credentials
// required.
//
// Each test uses a single connection for its whole flow. DuckDB on Windows
// holds an exclusive OS lock on the catalog file until the owning connection's
// handle is released, so a second connection re-attaching the same file before
// the first is fully closed fails with a sharing-violation. Reusing one
// connection (with DETACH between attach modes) and reading the recorded
// version via the metadata catalog the ducklake ATTACH exposes
// (__ducklake_metadata_<alias>) avoids that.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { isCatalogVersionSupported } from "../../src/ducklake_version";

describe("DuckLake catalog version contract (real engine)", () => {
   let dir: string;
   let metaPath: string;
   let dataPath: string;
   let conn: DuckDBConnection;

   beforeEach(async () => {
      dir = await fs.mkdtemp(path.join(os.tmpdir(), "ducklake-version-"));
      metaPath = path.join(dir, "catalog.ducklake");
      dataPath = path.join(dir, "data");
      conn = new DuckDBConnection("ducklake_contract");
      await conn.runSQL("INSTALL ducklake");
      await conn.runSQL("LOAD ducklake");
   });

   afterEach(async () => {
      try {
         await conn.close();
      } catch {
         // already closed
      }
      await fs.rm(dir, { recursive: true, force: true });
   });

   // Create the catalog as ducklake and leave it detached so the same
   // connection can re-attach it (as ducklake or as a plain database) next.
   const createCatalog = async (): Promise<void> => {
      await conn.runSQL(
         `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}')`,
      );
      await conn.runSQL("CREATE TABLE dl.t AS SELECT 1 AS x");
      await conn.runSQL("DETACH dl");
   };

   // Read/modify the recorded version by attaching the catalog file as a plain
   // database (not as ducklake), then detaching so later attaches are clean.
   const readRecordedVersion = async (): Promise<string> => {
      await conn.runSQL(`ATTACH '${metaPath}' AS meta`);
      const res = await conn.runSQL(
         `SELECT value FROM meta.ducklake_metadata WHERE key = 'version'`,
      );
      await conn.runSQL("DETACH meta");
      return String((res.rows[0] as { value: string }).value);
   };

   const setRecordedVersion = async (version: string): Promise<void> => {
      await conn.runSQL(`ATTACH '${metaPath}' AS meta`);
      await conn.runSQL(
         `UPDATE meta.ducklake_metadata SET value = '${version}' WHERE key = 'version'`,
      );
      await conn.runSQL("DETACH meta");
   };

   const attachCatalog = async (): Promise<void> => {
      await conn.runSQL(
         `ATTACH 'ducklake:${metaPath}' AS attached (DATA_PATH '${dataPath}')`,
      );
   };

   it("a freshly created catalog records a version that the preflight accepts", async () => {
      await createCatalog();
      const version = await readRecordedVersion();
      expect(isCatalogVersionSupported(version)).toBe(true);
   });

   it("plain ATTACH succeeds for the version the engine writes", async () => {
      await createCatalog();
      // No throw == the version the engine recorded is attachable as-is, which
      // is the property the preflight promises when it returns true.
      await attachCatalog();
   });

   it("plain ATTACH rejects a 0.3-line catalog the preflight also rejects", async () => {
      await createCatalog();
      await setRecordedVersion("0.3");

      // The preflight must agree with the engine: 0.3 is not attachable here
      // (the 1.5.x extension requires migration), so the constant must reject it.
      expect(isCatalogVersionSupported("0.3")).toBe(false);

      let attachFailed = false;
      try {
         await attachCatalog();
      } catch {
         attachFailed = true;
      }
      expect(attachFailed).toBe(true);
   });
});
