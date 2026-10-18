// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which `# colspan` findings the load and a package-scope compile report.
 *
 * The renderer validates each view on its own and calls a top-level
 * `# colspan` unknown. A grid report reads that tag off the view each tile
 * names, so on a tile it is not a finding, wherever the view is defined. On a
 * view no grid report names it is: nothing reads it.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Environment } from "./environment";

const UNKNOWN_COLSPAN = "Unknown render tag 'colspan' on field 'root'";

const FLIGHTS = `source: flights is duckdb.sql("SELECT 'AA' AS carrier, 'SFO' AS origin, 'JFK' AS dest") extend {
  measure: flight_count is count()
  # bar_chart
  view: by_origin is { group_by: origin; aggregate: flight_count }
  # colspan=2
  view: by_carrier is { group_by: carrier; aggregate: flight_count }
  # colspan=2
  view: by_dest is { group_by: dest; aggregate: flight_count }
  # colspan=2
  view: unused is { group_by: dest; aggregate: flight_count }
}
`;

// The shape the report builder saves: the grid width beside the artifact tag,
// and a tile's width on a view in an extension of the imported source.
const GRID_REPORT = `## artifact { layout=grid title="Flights" tiles=["flights -> by_carrier", "flights_tiles -> by_origin_tile", "top_origins"] } dashboard { columns=3 }
import { flights } from 'flights.malloy'

source: flights_tiles is flights extend {
  # colspan=1
  view: by_origin_tile is by_origin
  # colspan=2
  view: helper is by_origin
}

# colspan=2
query: top_origins is flights -> by_origin
`;

// A linear report is one column, so a width on its tile does nothing.
const LINEAR_REPORT = `## artifact { layout=linear title="Notes" tiles=["flights -> by_dest"] }
import { flights } from 'flights.malloy'
`;

describe("render-tag findings for a tile's layout tags", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-tilelayout-"),
      );
      const envPath = path.join(rootDir, "env");
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture" }),
      );
      await fs.writeFile(path.join(dir, "flights.malloy"), FLIGHTS);
      await fs.writeFile(path.join(dir, "grid.malloy"), GRID_REPORT);
      await fs.writeFile(path.join(dir, "notes.malloy"), LINEAR_REPORT);
      env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   // `model: subject` for every unknown-colspan finding, sorted. A view is
   // checked in every file that has its source in scope, so one tag can be
   // reported from more than one file.
   const colspanSubjects = (
      warnings:
         | readonly { model?: string; subject?: string; message?: string }[]
         | undefined,
   ): string[] =>
      (warnings ?? [])
         .filter((w) => w.message === UNKNOWN_COLSPAN)
         .map((w) => `${w.model}: ${w.subject}`)
         .sort();

   // Absent: `by_carrier`, a grid tile defined in the imported file, in every
   // file that sees it, `flights_tiles -> by_carrier` (the copy the extension
   // inherits) included; `flights_tiles -> by_origin_tile`, a grid tile
   // defined in the report; and `top_origins`, a query tile.
   const EXPECTED = [
      // A tile of a linear report only, which is one column.
      "flights.malloy: flights -> by_dest",
      // No report names it.
      "flights.malloy: flights -> unused",
      "grid.malloy: flights -> by_dest",
      "grid.malloy: flights -> unused",
      "grid.malloy: flights_tiles -> by_dest",
      // In the grid report's own file, but not one of its tiles.
      "grid.malloy: flights_tiles -> helper",
      "grid.malloy: flights_tiles -> unused",
      "notes.malloy: flights -> by_dest",
      "notes.malloy: flights -> unused",
   ];

   it("drops it on a grid tile and keeps it everywhere else, at load", async () => {
      const pkg = await env.getPackage("pkg", false);
      expect(colspanSubjects(pkg.getPackageMetadata().warnings)).toEqual(
         EXPECTED,
      );
   });

   it("reports the same at package-scope compile", async () => {
      const { problems } = await env.compileSource(
         "pkg",
         "grid.malloy",
         undefined,
         false,
         undefined,
         "package",
      );
      // A compile problem leads its message with the subject.
      const found = problems
         .filter((p) => p.message.endsWith(`: ${UNKNOWN_COLSPAN}`))
         .map(
            (p) =>
               `${(p as { model?: string }).model}: ${p.message.slice(0, -`: ${UNKNOWN_COLSPAN}`.length)}`,
         )
         .sort();
      expect(found).toEqual(EXPECTED);
   });
});
