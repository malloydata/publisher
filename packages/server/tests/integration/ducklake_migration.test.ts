/// <reference types="bun-types" />

// Migration matrix for DuckLake catalogs across historical catalog formats.
//
// The catalog format is set by the DuckDB engine that wrote it, and it has
// changed over time:
//
//     DuckDB 1.4.1 .. 1.4.4   -> catalog format "0.3"
//     DuckDB 1.5.0 .. 1.5.1   -> catalog format "0.4"
//     DuckDB 1.5.2 .. 1.5.3   -> catalog format "1.0"
//
// The baked engine (resolved from the repo's single source of truth) can only
// attach an older-format catalog after migrating it forward with
// AUTOMATIC_MIGRATION. This suite confirms that, for every historical format,
// a catalog written in that format migrates to the current format on attach
// with its data intact -- and prints a migration report.
//
// This does not contradict the preflight in src/ducklake_version.ts, whose
// SUPPORTED_CATALOG_VERSIONS gates plain attach-as-is (currently only the
// current format). Older formats are intentionally not attach-as-is; this
// suite documents that they remain recoverable through an explicit migration.
//
// The runner engine only writes the newest format, so the older-format
// catalogs are committed as fixtures under tests/fixtures/ducklake, one per
// distinct format, each seeded with the SAME rows. Regenerate them with
// scripts/generate-ducklake-fixtures.sh when a new format appears.
//
// Migration footguns this exercises (all load-bearing for any future decision
// to migrate on a user's behalf):
//   - Migration is one-way and in-place; it rewrites the user's catalog file.
//   - The attach DATA_PATH must match the path recorded in the catalog, or the
//     attach is refused unless OVERRIDE_DATA_PATH is set. Publisher computes
//     absolute data paths while these fixtures record a relative "data/", so
//     OVERRIDE_DATA_PATH is required here.
//   - OVERRIDE_DATA_PATH only suppresses the mismatch error; it does not
//     persist the passed path.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { resolveDuckDBVersion } from "../../../../scripts/duckdb-version.js";

const FIXTURE_ROOT = path.join(process.cwd(), "tests/fixtures/ducklake");

// The rows every fixture is seeded with (see generate-ducklake-fixtures.sh).
const EXPECTED_ROWS = [
   { id: 1, label: "alpha" },
   { id: 2, label: "beta" },
   { id: 3, label: "gamma" },
];

interface Fixture {
   // Catalog format the fixture is written in, e.g. "0.3", parsed from the
   // directory name "v<format>_from_duckdb_<cli>".
   format: string;
   // DuckDB CLI version that wrote it, for the report.
   writtenBy: string;
   dir: string;
}

interface ReportRow {
   sourceFormat: string;
   writtenBy: string;
   migratedTo: string;
   rows: number;
   status: "ok" | "FAILED";
   detail?: string;
}

const FIXTURE_DIR_PATTERN = /^v(.+)_from_duckdb_(.+)$/;

async function discoverFixtures(): Promise<Fixture[]> {
   const entries = await fs.readdir(FIXTURE_ROOT, { withFileTypes: true });
   const fixtures: Fixture[] = [];
   for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const match = entry.name.match(FIXTURE_DIR_PATTERN);
      if (!match) continue;
      fixtures.push({
         format: match[1],
         writtenBy: match[2],
         dir: path.join(FIXTURE_ROOT, entry.name),
      });
   }
   fixtures.sort((a, b) => a.format.localeCompare(b.format));
   return fixtures;
}

// Empirically determine the catalog format the current baked engine writes, by
// creating a throwaway catalog and reading back its recorded version. Avoids
// hard-coding a format that drifts when the engine is bumped.
async function currentCatalogFormat(): Promise<string> {
   const dir = await fs.mkdtemp(path.join(os.tmpdir(), "ducklake-current-"));
   try {
      const metaPath = path.join(dir, "catalog.ducklake");
      const dataPath = path.join(dir, "data");
      const writer = new DuckDBConnection("current_writer");
      await writer.runSQL("LOAD ducklake");
      await writer.runSQL(
         `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}')`,
      );
      await writer.runSQL("CREATE TABLE dl.probe AS SELECT 1 AS x");
      await writer.close();

      const reader = new DuckDBConnection("current_reader");
      await reader.runSQL(`ATTACH '${metaPath}' AS meta`);
      const res = await reader.runSQL(
         "SELECT value FROM meta.ducklake_metadata WHERE key = 'version'",
      );
      await reader.close();
      return String((res.rows[0] as { value: string }).value);
   } finally {
      await fs.rm(dir, { recursive: true, force: true });
   }
}

describe("DuckLake catalog migration matrix", () => {
   let fixtures: Fixture[];
   let currentFormat: string;
   const report: ReportRow[] = [];

   beforeAll(async () => {
      fixtures = await discoverFixtures();
      currentFormat = await currentCatalogFormat();
   });

   afterAll(() => {
      const engineVersion = resolveDuckDBVersion();
      const lines = [
         "",
         `DuckLake migration report (engine duckdb@${engineVersion}, current catalog format ${currentFormat})`,
         "-".repeat(78),
         [
            "source format".padEnd(15),
            "written by".padEnd(14),
            "migrated to".padEnd(13),
            "rows".padEnd(6),
            "status",
         ].join(""),
      ];
      for (const r of report) {
         lines.push(
            [
               r.sourceFormat.padEnd(15),
               `duckdb ${r.writtenBy}`.padEnd(14),
               r.migratedTo.padEnd(13),
               String(r.rows).padEnd(6),
               r.status === "ok" ? "ok" : `FAILED: ${r.detail ?? ""}`,
            ].join(""),
         );
      }
      lines.push("-".repeat(78), "");
      console.log(lines.join("\n"));
   });

   it("ships a fixture for the format the current engine writes", () => {
      // Guards against silent coverage loss: when the engine is bumped to a
      // format with no fixture, this fails until a fixture is added (via
      // scripts/generate-ducklake-fixtures.sh).
      const formats = fixtures.map((f) => f.format);
      expect(formats).toContain(currentFormat);
   });

   it("discovers at least one fixture", () => {
      expect(fixtures.length).toBeGreaterThan(0);
   });

   it("migrates every fixture format forward with data intact", async () => {
      expect(fixtures.length).toBeGreaterThan(0);

      for (const fixture of fixtures) {
         const workDir = await fs.mkdtemp(
            path.join(os.tmpdir(), `ducklake-migrate-${fixture.format}-`),
         );
         try {
            await fs.cp(fixture.dir, workDir, { recursive: true });
            const metaPath = path.join(workDir, "catalog.ducklake");
            const dataPath = path.join(workDir, "data");

            const conn = new DuckDBConnection(`migrate_${fixture.format}`);
            try {
               await conn.runSQL("LOAD ducklake");
               // AUTOMATIC_MIGRATION upgrades the catalog to the current format.
               // OVERRIDE_DATA_PATH is required because the fixture records a
               // relative data path while this attach uses an absolute one.
               await conn.runSQL(
                  `ATTACH 'ducklake:${metaPath}' AS dl (DATA_PATH '${dataPath}', AUTOMATIC_MIGRATION TRUE, OVERRIDE_DATA_PATH TRUE)`,
               );
               const rows = await conn.runSQL(
                  "SELECT id, label FROM dl.items ORDER BY id",
               );
               await conn.close();

               const migratedTo = await (async () => {
                  const reader = new DuckDBConnection(
                     `verify_${fixture.format}`,
                  );
                  await reader.runSQL(`ATTACH '${metaPath}' AS meta`);
                  const v = await reader.runSQL(
                     "SELECT value FROM meta.ducklake_metadata WHERE key = 'version'",
                  );
                  await reader.close();
                  return String((v.rows[0] as { value: string }).value);
               })();

               expect(rows.rows).toEqual(EXPECTED_ROWS);
               // A fixture already at the current format is a no-op migration;
               // an older one must end up at the current format.
               expect(migratedTo).toBe(currentFormat);

               report.push({
                  sourceFormat: fixture.format,
                  writtenBy: fixture.writtenBy,
                  migratedTo,
                  rows: rows.rows.length,
                  status: "ok",
               });
            } catch (e) {
               try {
                  await conn.close();
               } catch {
                  // ignore
               }
               report.push({
                  sourceFormat: fixture.format,
                  writtenBy: fixture.writtenBy,
                  migratedTo: "-",
                  rows: 0,
                  status: "FAILED",
                  detail: String(e).split("\n")[0],
               });
               throw e;
            }
         } finally {
            await fs.rm(workDir, { recursive: true, force: true });
         }
      }
   });
});
