// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { isTextTile } from "./document";
import { openDocument, queryTile, spliced } from "./testing/fixtures";

/**
 * A tile's layout lives on its entry in `tiles=[…]`: the place this report
 * puts it. The reader takes the entry over the view's own tags, as the server
 * does, and the writer puts a changed layout there and takes the view's
 * `# colspan` and `# break` off, so the layout has one home.
 */
const TAGGED = `## artifact { layout=grid title="T" tiles=["a -> kpis", "a -> trend"] } dashboard { columns=12 }
import "../m.malloy"

source: a is one extend {
  # colspan=6
  # break
  # label="KPIs"
  view: kpis is vk

  view: trend is vt
}`;

describe("entry layout: reading", () => {
   it("takes the entry's colspan, break and height over the view's tags", async () => {
      const d = await openDocument(
         TAGGED.replace(
            '"a -> kpis", "a -> trend"',
            '"a -> kpis" { colspan=4 break=false height=3 }, "a -> trend" { break }',
         ),
      );
      expect(queryTile(d, 0)).toMatchObject({ colspan: 4, height: 3 });
      expect(queryTile(d, 0).break).toBeUndefined();
      expect(queryTile(d, 1)).toMatchObject({ break: true });
   });

   it("still reads a view's own tags when the entry carries none", async () => {
      const d = await openDocument(TAGGED);
      expect(queryTile(d, 0)).toMatchObject({ colspan: 6, break: true });
      expect(queryTile(d, 0).height).toBeUndefined();
   });

   it("reads a text tile's height", async () => {
      const d = await openDocument(
         `## artifact { layout=grid title="T" tiles=[intro { kind=text height=2 }] }\n##|(markdown) intro\nHi\n|##\n`,
      );
      const tile = d.tiles[0];
      expect(isTextTile(tile) && tile.height).toBe(2);
   });
});

describe("entry layout: writing", () => {
   it("moves a changed width to the entry, taking the view's layout tags off", async () => {
      const out = await spliced(TAGGED, (d) => {
         d.tiles[0].colspan = 8;
      });
      expect(out).toContain(
         'tiles=["a -> kpis" { colspan=8 break }, "a -> trend"]',
      );
      expect(out).toContain('  # label="KPIs"\n  view: kpis is vk');
      expect(out).not.toContain("# colspan");
      expect(out).not.toContain("# break");
      const back = await openDocument(out);
      expect(queryTile(back, 0)).toMatchObject({ colspan: 8, break: true });
   });

   it("leaves a view's layout tags alone when only its title changes", async () => {
      const out = await spliced(TAGGED, (d) => {
         queryTile(d, 0).label = "Totals";
      });
      expect(out).toContain('tiles=["a -> kpis", "a -> trend"]');
      expect(out).toContain(
         '  # colspan=6\n  # break\n  # label="Totals"\n  view: kpis is vk',
      );
   });

   it("writes a height on a query tile and a text tile", async () => {
      const source = TAGGED.replace(
         '"a -> kpis", "a -> trend"',
         'intro { kind=text }, "a -> kpis", "a -> trend"',
      ).replace("import", "##|(markdown) intro\nHi\n|##\nimport");
      const out = await spliced(source, (d) => {
         d.tiles[0].height = 2;
         d.tiles[2].height = 5;
      });
      expect(out).toContain(
         'tiles=[intro { kind=text height=2 }, "a -> kpis", "a -> trend" { height=5 }]',
      );
   });

   it("drops the braces when an entry's last layout property goes", async () => {
      const source = TAGGED.replace(
         '"a -> trend"',
         '"a -> trend" { height=4 }',
      );
      const out = await spliced(source, (d) => {
         delete d.tiles[1].height;
      });
      expect(out).toContain('tiles=["a -> kpis", "a -> trend"]');
   });

   it("keeps an entry's spelling and other properties when its layout changes", async () => {
      const source = TAGGED.replace(
         '"a -> trend"',
         '"a->trend" { height=4 kind=query }',
      );
      const out = await spliced(source, (d) => {
         d.tiles[1].height = 6;
      });
      expect(out).toContain('"a->trend" { kind=query height=6 }');
   });

   it("rewrites an entry that carries layout as written when only the order changes", async () => {
      const source = TAGGED.replace(
         '"a -> trend"',
         '"a -> trend" { height=4 }',
      );
      const out = await spliced(source, (d) => {
         d.tiles = [d.tiles[1], d.tiles[0]];
         // The pattern of breaks is the grid's, not the tiles': see layout.ts.
         delete d.tiles[1].break;
         d.tiles[0].break = undefined;
      });
      expect(out).toContain('tiles=["a -> trend" { height=4 }, "a -> kpis"');
   });
});
