// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { addTileToDocument, duplicateTile } from "./addTileToDocument";
import { applyMapping, mappingOf } from "./controls";
import { edited, openDocument, queryTile } from "./testing/fixtures";

/**
 * The report a builder writes declares no source: a tile names a served view,
 * and a tile with settings of its own is a `query:` of the report's own. Every
 * case writes through the real writer, as the editor does (`edited`), and
 * asserts the whole file, then reads it back.
 */

const HEAD = `##! experimental.givens
## artifact { title="T" tiles=[TILES] }
import { flights } from "../m.malloy"

# label="Carrier" control=select
given: CARRIER :: filter<string> is f''
`;
const file = (tiles: string, rest = "") => HEAD.replace("TILES", tiles) + rest;
const PLAIN = file('"flights -> by_carrier", "flights -> kpis"');

describe("a report the builder writes", () => {
   it("adds a tile as an entry naming the served view, and nothing else", async () => {
      const out = await edited(PLAIN, (d) =>
         addTileToDocument(
            d,
            { base: "flights", modelPath: "m.malloy", view: "by_origin" },
            { modelPath: "dashboards/x.malloy", textHeld: false, widths: true },
         ),
      );
      expect(out).toBe(
         file(
            '"flights -> by_carrier", "flights -> kpis", "flights -> by_origin"',
         ),
      );
   });

   it("makes a tile with a chart a query of the report's own, written with viz, and takes it back", async () => {
      const charted = await edited(PLAIN, (d) => {
         queryTile(d, 0).chart = "line_chart";
      });
      expect(charted).toBe(
         file(
            '"by_carrier_tile", "flights -> kpis"',
            "\n# viz=line\nquery: by_carrier_tile is flights -> by_carrier\n",
         ),
      );
      expect(queryTile(await openDocument(charted), 0)).toEqual({
         name: "by_carrier_tile",
         source: "flights",
         declaration: { kind: "named", from: "by_carrier" },
         chart: "line_chart",
      });
      const back = await edited(charted, (d) => {
         queryTile(d, 0).chart = "default";
      });
      expect(back).toBe(PLAIN);
   });

   it("writes a chart, a colour and a filter on the one query, its width on the entry, and drops the query when all are gone", async () => {
      const set = await edited(file('"flights -> by_carrier"', ""), (d) => {
         const tile = queryTile(d, 0);
         tile.chart = "line_chart";
         tile.colspan = 2;
         tile.color = "#7f9862";
         tile.filters = [{ field: "carrier_code", given: "CARRIER" }];
      });
      expect(set).toBe(
         file(
            '"by_carrier_tile" { colspan=2 }',
            "\n# viz=line\n" +
               '# theme.palette.series=["#7f9862"]\n' +
               "query: by_carrier_tile is flights -> by_carrier + { where: carrier_code ~ $CARRIER }\n",
         ),
      );
      const colourOff = await edited(set, (d) => {
         delete queryTile(d, 0).color;
      });
      expect(colourOff).not.toContain("theme.palette.series");
      const cleared = await edited(set, (d) => {
         const tile = queryTile(d, 0);
         tile.chart = "default";
         delete tile.colspan;
         delete tile.color;
         delete tile.filters;
      });
      expect(cleared).toBe(file('"flights -> by_carrier"'));
   });

   it("binds a control on one tile, then on every tile, and unbinds it", async () => {
      const one = await edited(PLAIN, (d) => {
         queryTile(d, 1).filters = [
            { field: "carrier_code", given: "CARRIER" },
         ];
      });
      expect(one).toBe(
         file(
            '"flights -> by_carrier", "kpis_tile"',
            "\nquery: kpis_tile is flights -> kpis + { where: carrier_code ~ $CARRIER }\n",
         ),
      );
      const all = await edited(one, (d) => {
         const rows = mappingOf(d, {
            name: "CARRIER",
            field: "carrier_code",
            type: "filter<string>",
         }).map((row) => ({ ...row, include: true }));
         applyMapping(d, "CARRIER", rows);
      });
      expect(all).toBe(
         file(
            '"by_carrier_tile", "kpis_tile"',
            "\nquery: kpis_tile is flights -> kpis + { where: carrier_code ~ $CARRIER }\n" +
               "\nquery: by_carrier_tile is flights -> by_carrier + { where: carrier_code ~ $CARRIER }\n",
         ),
      );
      const none = await edited(all, (d) => {
         const rows = mappingOf(d, { name: "CARRIER" }).map((row) => ({
            ...row,
            include: false,
         }));
         applyMapping(d, "CARRIER", rows);
      });
      expect(none).toBe(file('"flights -> by_carrier", "flights -> kpis"'));
   });

   it("duplicates a tile as a query of its own right after it, and deletes one with its query", async () => {
      const copied = await edited(PLAIN, (d) => {
         duplicateTile(d, 0);
      });
      expect(copied).toBe(
         file(
            '"flights -> by_carrier", "by_carrier_tile", "flights -> kpis"',
            "\nquery: by_carrier_tile is flights -> by_carrier\n",
         ),
      );
      const removed = await edited(copied, (d) => {
         d.tiles.splice(1, 1);
      });
      expect(removed).toBe(PLAIN);
   });

   it("keeps a query that says more than the builder writes, and edits it in place", async () => {
      const own = file(
         '"top_carriers"',
         "\n# currency\nquery: top_carriers is flights -> by_carrier + { limit: 5 }\n",
      );
      const tile = queryTile(await openDocument(own), 0);
      expect(tile.declaration).toEqual({ kind: "named", from: "by_carrier" });
      expect(tile.unmodelled).toBe(true);
      const charted = await edited(own, (d) => {
         queryTile(d, 0).chart = "bar_chart";
      });
      expect(charted).toBe(
         file(
            '"top_carriers"',
            "\n# currency\n# viz=bar\nquery: top_carriers is flights -> by_carrier + { limit: 5 }\n",
         ),
      );
      const back = await edited(charted, (d) => {
         queryTile(d, 0).chart = "default";
      });
      expect(back).toBe(own);
   });

   // A report whose tile names a query the author wrote, `source -> view`:
   // edited where it is, under its own name, and never taken out of the file.
   it("edits a query the author named in place, and keeps it when its settings are gone", async () => {
      const authored = file(
         '"by_carrier"',
         "\nquery: by_carrier is flights -> by_carrier\n",
      );
      const wide = await edited(authored, (d) => {
         queryTile(d, 0).colspan = 2;
         queryTile(d, 0).label = "Carriers";
      });
      expect(wide).toBe(
         file(
            '"by_carrier" { colspan=2 }',
            '\n# label="Carriers"\nquery: by_carrier is flights -> by_carrier\n',
         ),
      );
      const back = await edited(wide, (d) => {
         delete queryTile(d, 0).colspan;
         delete queryTile(d, 0).label;
      });
      expect(back).toBe(authored);
   });

   // An author may filter the source rather than refine the view, the form an
   // inline multi-stage query needs (malloydata/publisher#723). The builder
   // reads it as the same tile and edits the filters where they are.
   it("reads filters on the query's source, and edits them there", async () => {
      const scoped = file(
         '"carrier_tile"',
         "\nquery: carrier_tile is flights extend { where: carrier_code ~ $CARRIER } -> by_carrier\n",
      );
      expect(queryTile(await openDocument(scoped), 0)).toEqual({
         name: "carrier_tile",
         source: "flights",
         declaration: { kind: "named", from: "by_carrier" },
         filters: [{ field: "carrier_code", given: "CARRIER" }],
      });
      const two = await edited(scoped, (d) => {
         queryTile(d, 0).filters?.push({
            field: "origin_code",
            given: "ORIGIN",
         });
      });
      expect(two).toBe(
         scoped.replace(
            "{ where: carrier_code ~ $CARRIER }",
            "{ where: carrier_code ~ $CARRIER, origin_code ~ $ORIGIN }",
         ),
      );
      const none = await edited(two, (d) => {
         delete queryTile(d, 0).filters;
      });
      expect(none).toBe(
         file(
            '"carrier_tile"',
            "\nquery: carrier_tile is flights -> by_carrier\n",
         ),
      );
      // A block with more in it than filters is the author's.
      const own = scoped.replace(
         "{ where: carrier_code ~ $CARRIER }",
         "{ where: carrier_code ~ $CARRIER; dimension: one is 1 }",
      );
      expect(queryTile(await openDocument(own), 0).unmodelled).toBe(true);
      await expect(
         edited(own, (d) => {
            delete queryTile(d, 0).filters;
         }),
      ).rejects.toThrow("extended with more than the builder writes");
   });

   it("reads a query of another shape as one it only shows", async () => {
      const other = file(
         '"by_month"',
         "\nquery: by_month is flights -> { group_by: dep_month }\n",
      );
      expect(queryTile(await openDocument(other), 0).declaration).toEqual({
         kind: "query",
      });
   });

   it("leaves a tile in a report-local extension where it is, and writes its chart as viz", async () => {
      const legacy = file(
         '"flights_tiles -> by_carrier_tile", "flights_tiles -> kpis_tile"',
         "\nsource: flights_tiles is flights extend {\n" +
            "  # -line_chart -big_value -viz bar_chart\n" +
            "  view: by_carrier_tile is by_carrier\n\n" +
            "  view: kpis_tile is kpis\n}\n",
      );
      // The legacy line's negations read as the tile's reset; a chart edit
      // sets the reset with the pick, as the tile menu does.
      expect(queryTile(await openDocument(legacy), 0).chartReset).toEqual([
         "line_chart",
         "big_value",
         "viz",
      ]);
      const out = await edited(legacy, (d) => {
         queryTile(d, 0).chart = "line_chart";
         delete queryTile(d, 0).chartReset;
      });
      expect(out).toBe(
         legacy.replace(
            "# -line_chart -big_value -viz bar_chart",
            "# viz=line",
         ),
      );
   });
});
