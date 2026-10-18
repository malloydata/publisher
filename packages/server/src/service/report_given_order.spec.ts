// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The order of a report's control row as the reader gets it from the manifest:
 * tile by tile, and within a tile what its source reads before what the tile's
 * own `where:` binds. The builder's live row (`previewGivens` in the SDK)
 * mirrors this rule from the same case table (`reportGivenOrder.json`), fed the
 * per-tile `givenNames` pinned here, so a change to either side shows up as a
 * disagreement.
 */
import { describe, expect, it, setDefaultTimeout } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Package } from "./package";

setDefaultTimeout(30_000);

const { model, cases } = JSON.parse(
   fs.readFileSync(
      path.join(
         import.meta.dir,
         "../../../sdk/src/components/ReportBuilder/testing/reportGivenOrder.json",
      ),
      "utf8",
   ),
) as {
   model: string;
   cases: {
      name: string;
      report: string;
      tiles: Record<string, string[]>;
      row: string[];
   }[];
};

async function manifestOf(report: string) {
   const { MalloyConfig, FixedConnectionMap } = await import(
      "@malloydata/malloy"
   );
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(
      () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
   );
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "report-given-order-"));
   try {
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
      fs.writeFileSync(path.join(dir, "recalls.malloy"), model);
      fs.writeFileSync(path.join(dir, "r.malloy"), report);
      const pkg = await Package.create("env", "pkg", dir, malloyConfig);
      return await pkg.getReport("r.malloy");
   } finally {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   }
}

describe("a report's control row order", () => {
   for (const { name, report, tiles, row } of cases)
      it(name, async () => {
         const manifest = await manifestOf(report);
         expect(
            Object.fromEntries(
               (manifest?.tiles ?? []).flatMap((tile) =>
                  tile.kind === "query" && tile.query
                     ? [[tile.query, tile.givenNames]]
                     : [],
               ),
            ),
         ).toEqual(tiles);
         expect(manifest?.givens?.map((given) => given.name)).toEqual(row);
      });
});
