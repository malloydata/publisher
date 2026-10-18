// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A tile's click actions live on its entry in `tiles=[…]`, beside its layout:
// read back as `tile.drill`, written as `drill { … }`, and left as written on
// every tile the edit did not touch.
import { describe, expect, it } from "bun:test";
import { queryTile } from "./testing/fixtures";
import { edited, openDocument, spliced } from "./testing/fixtures";

const IMPORT = "import { orders } from '../orders.malloy'";

describe("a tile entry's drill", () => {
   it("is read off the entry, beside its layout", async () => {
      const doc = await openDocument(
         `## artifact { title="T" tiles=["orders -> by_brand" { colspan=6 drill { brand { to=self given=BRAND } region { to=["sales/detail", self] } } }] }\n${IMPORT}`,
      );
      expect(doc.tiles[0]).toMatchObject({
         colspan: 6,
         drill: {
            brand: { to: ["self"], given: "BRAND" },
            region: { to: ["sales/detail", "self"] },
         },
      });
   });

   it("is written on the entry, and the tile still names the view", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand", "orders -> by_month"] }\n${IMPORT}`;
      const out = await spliced(source, (d) => {
         queryTile(d, 0).drill = { brand: { to: ["self"], given: "BRAND" } };
      });
      expect(out).toBe(
         `## artifact { title="T" tiles=["orders -> by_brand" { drill { brand { to=self given=BRAND } } }, "orders -> by_month"] }\n${IMPORT}`,
      );
      const back = await openDocument(out);
      expect(back.tiles[0]).toMatchObject({
         drill: { brand: { to: ["self"], given: "BRAND" } },
      });
      expect(back.tiles[1]).not.toHaveProperty("drill");
   });

   it("writes a report path quoted, several as a list, and to=[] to turn the model's tag off", async () => {
      const out = await spliced(
         `## artifact { title="T" tiles=["orders -> by_brand"] }\n${IMPORT}`,
         (d) => {
            queryTile(d, 0).drill = {
               region: { to: ["self", "sales/detail"], given: "REGION" },
               brand: { to: [] },
            };
         },
      );
      expect(out).toContain(
         `"orders -> by_brand" { drill { brand { to=[] } region { to=[self, "sales/detail"] given=REGION } } }`,
      );
      const back = await openDocument(out);
      expect(back.tiles[0]).toMatchObject({
         drill: {
            brand: { to: [] },
            region: { to: ["self", "sales/detail"], given: "REGION" },
         },
      });
   });

   it("replaces the entry's drill and keeps its layout when the drill changes", async () => {
      const out = await spliced(
         `## artifact { title="T" tiles=["orders -> by_brand" { colspan=6 drill { brand { to=self } } }] } dashboard { columns=12 }\n${IMPORT}`,
         (d) => {
            queryTile(d, 0).drill = { brand: { to: ["sales/detail"] } };
         },
      );
      expect(out).toContain(
         `"orders -> by_brand" { colspan=6 drill { brand { to="sales/detail" } } }`,
      );
   });

   it("drops the drill, and the braces with it, when every setting is removed", async () => {
      const out = await spliced(
         `## artifact { title="T" tiles=["orders -> by_brand" { drill { brand { to=self } } }] }\n${IMPORT}`,
         (d) => {
            delete queryTile(d, 0).drill;
         },
      );
      expect(out).toContain(`tiles=["orders -> by_brand"]`);
   });

   it("leaves a tile's drill as written when another tile moves", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand" { drill { brand { to=self } } }, "orders -> by_month"] } dashboard { columns=12 }\n${IMPORT}`;
      const out = await edited(source, (d) => {
         d.tiles[1].colspan = 4;
      });
      expect(out).toContain(`"orders -> by_brand" { drill { brand { to=self } } }`);
   });
});
