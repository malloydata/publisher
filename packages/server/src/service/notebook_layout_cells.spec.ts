// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A layout notebook (`## artifact { tiles=[…] }`) is served on the notebook GET
 * with the same cell fields as one written as cells.
 */
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Package } from "./package";

const BASE = `source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: hv is { aggregate: c }
}
`;

const INDEX = `source: customers is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
`;

const LAYOUT = `## artifact { kind=notebook tiles=["customers -> v", "helper -> hv"] }
import "../base.malloy"
import "../surface.malloy"
`;

async function cellsOf(curated: boolean) {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "layout-cells-"));
   const { MalloyConfig, FixedConnectionMap } = await import(
      "@malloydata/malloy"
   );
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(
      () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
   );
   try {
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({
            name: "pkg",
            ...(curated && { explores: ["surface.malloy"] }),
         }),
      );
      fs.mkdirSync(path.join(dir, "notebooks"));
      fs.writeFileSync(path.join(dir, "base.malloy"), BASE);
      fs.writeFileSync(path.join(dir, "surface.malloy"), INDEX);
      fs.writeFileSync(path.join(dir, "notebooks/layout.malloy"), LAYOUT);
      const pkg = await Package.create("env", "pkg", dir, malloyConfig);
      const raw = await pkg.getModel("notebooks/layout.malloy")?.getNotebook();
      return (raw?.notebookCells ?? []).filter((c) => c.kind === "query");
   } finally {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

describe("a layout notebook's tile cells", () => {
   it("carry queryInfo and proseLines like a notebook written as cells", async () => {
      const cells = await cellsOf(false);
      expect(cells).toHaveLength(2);
      for (const cell of cells) {
         expect(cell.proseLines).toEqual([]);
         const info = JSON.parse(cell.queryInfo ?? "null") as {
            name: string;
            schema: { fields: { name: string }[] };
         };
         expect(info.schema.fields.map((f) => f.name)).toEqual(["c"]);
      }
   });

   it("withhold queryInfo from a tile the surface refuses, and keep it on one it publishes", async () => {
      const cells = await cellsOf(true);
      expect(cells.map((c) => c.queryInfo !== undefined)).toEqual([
         true,
         false,
      ]);
      // A withheld cell still carries its prose lines, which are author text.
      expect(cells.map((c) => c.proseLines)).toEqual([[], []]);
   });
});
