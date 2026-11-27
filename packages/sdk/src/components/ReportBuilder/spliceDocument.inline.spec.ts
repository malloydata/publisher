// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { duplicateTile } from "./addTileToDocument";
import { edited, openDocument, queryTile } from "./testing/fixtures";

/**
 * A tile whose query is an inline stage, `query: x is src -> { … }`, the form a
 * notebook conversion writes: as editable as one over a served view. Its
 * settings go in tags above the query, its filters in a `+ { where: … }` after
 * the stage, its layout on the entry, and the stage itself is never rewritten.
 */

const STAGE = `{
    group_by: carrier   // the author's spacing and comment stay
    aggregate: flight_count
  }`;
const HEAD = `##! experimental.givens
## artifact { title="T" tiles=[TILES] }
import { flights } from "../m.malloy"

# label="Carrier" control=select
given: CARRIER :: filter<string> is f''
`;
const file = (tiles: string, rest = "") => HEAD.replace("TILES", tiles) + rest;
const INLINE = file(
   '"by_carrier"',
   `\nquery: by_carrier is flights -> ${STAGE}\n`,
);

describe("an inline-stage query tile", () => {
   it("reads as the report's own query over its stage, exactly as written", async () => {
      const tile = queryTile(await openDocument(INLINE), 0);
      expect(tile.declaration).toEqual({ kind: "named", from: STAGE });
      expect(tile.source).toBe("flights");
      expect(tile.unmodelled).toBeUndefined();
   });

   it("writes chart, colour, title, subtitle and border as tags above the query, and reads them back", async () => {
      const out = await edited(INLINE, (d) => {
         const tile = queryTile(d, 0);
         tile.chart = "bar_chart";
         tile.color = "#7f9862";
         tile.label = "By airline";
         tile.subtitle = "Flights per carrier";
         tile.borderless = true;
      });
      // The tags, then the stage exactly as written.
      expect(out).toContain('tiles=["by_carrier"]');
      for (const line of [
         '# label="By airline"',
         '# subtitle="Flights per carrier"',
         "# borderless",
         "# viz=bar",
         '# theme.palette.series=["#7f9862"]',
      ])
         expect(out).toContain(`${line}\n`);
      expect(out.endsWith(`query: by_carrier is flights -> ${STAGE}\n`)).toBe(
         true,
      );
      const back = queryTile(await openDocument(out), 0);
      expect(back).toMatchObject({
         label: "By airline",
         subtitle: "Flights per carrier",
         borderless: true,
         chart: "bar_chart",
         color: "#7f9862",
         declaration: { kind: "named", from: STAGE },
      });
   });

   it("binds a filter as + { where } after the stage, and takes it away again", async () => {
      const filtered = await edited(INLINE, (d) => {
         queryTile(d, 0).filters = [{ field: "carrier", given: "CARRIER" }];
      });
      expect(filtered).toBe(
         file(
            '"by_carrier"',
            `\nquery: by_carrier is flights -> ${STAGE} + { where: carrier ~ $CARRIER }\n`,
         ),
      );
      expect(queryTile(await openDocument(filtered), 0).filters).toEqual([
         { field: "carrier", given: "CARRIER" },
      ]);
      const cleared = await edited(filtered, (d) => {
         delete queryTile(d, 0).filters;
      });
      expect(cleared).toBe(INLINE);
   });

   it("merges a second filter into the refinement already there", async () => {
      const filtered = file(
         '"by_carrier"',
         `\nquery: by_carrier is flights -> ${STAGE} + { where: carrier ~ $CARRIER }\n`,
      ).replace(
         "given: CARRIER",
         "given: ORIGIN :: filter<string> is f''\n# label=\"Carrier\" control=select\ngiven: CARRIER",
      );
      const out = await edited(filtered, (d) => {
         queryTile(d, 0).filters = [
            { field: "carrier", given: "CARRIER" },
            { field: "origin", given: "ORIGIN" },
         ];
      });
      expect(out).toContain(
         `flights -> ${STAGE} + { where: carrier ~ $CARRIER, where: origin ~ $ORIGIN }`,
      );
   });

   it("puts a layout change on the entry and leaves the query alone", async () => {
      const out = await edited(INLINE, (d) => {
         queryTile(d, 0).colspan = 6;
      });
      expect(out).toBe(
         file(
            '"by_carrier" { colspan=6 }',
            `\nquery: by_carrier is flights -> ${STAGE}\n`,
         ),
      );
   });

   it("stays a query when every setting is cleared: there is no view to name instead", async () => {
      const charted = await edited(INLINE, (d) => {
         queryTile(d, 0).chart = "bar_chart";
      });
      const back = await edited(charted, (d) => {
         queryTile(d, 0).chart = "default";
      });
      expect(back).toBe(INLINE);
   });

   it("duplicates as a query of its own over the same stage, named after it", async () => {
      const out = await edited(INLINE, (d) => {
         duplicateTile(d, 0);
      });
      expect(out).toContain('tiles=["by_carrier", "by_carrier_tile"]');
      expect(out).toContain(`query: by_carrier_tile is flights -> ${STAGE}`);
   });

   it("stays read-only for a shape it cannot parse, and says what that shape is", async () => {
      const piped = file(
         '"by_carrier"',
         "\nquery: by_carrier is flights -> { group_by: carrier } -> { select: carrier }\n",
      );
      expect(queryTile(await openDocument(piped), 0).declaration).toEqual({
         kind: "query",
      });
   });
});
