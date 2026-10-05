// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { openDocument, refused, spliced } from "./testing/fixtures";

const SOURCE = `## artifact { title="Probe" tiles=["a -> by_cat"] } dashboard { columns=12 }
import "../givens.malloy"
import { scoped_orders, products } from "../data_app.malloy"

source: a is scoped_orders extend {
  view: by_cat is by_category
}
`;

const reread = async (source: string) => (await openDocument(source)).imports;

describe("spliceDashboardDocument: imports", () => {
   it("adds a source to the statement for its file", async () => {
      const out = await spliced(SOURCE, (d) => {
         const names = d.imports[1];
         if (names.kind === "names") names.names.push("regions");
      });
      expect(out).toContain(
         'import { scoped_orders, products, regions } from "../data_app.malloy"',
      );
      expect(out).toContain('import "../givens.malloy"\n');
      expect(await reread(out)).toEqual([
         { kind: "all", from: "../givens.malloy" },
         {
            kind: "names",
            names: ["scoped_orders", "products", "regions"],
            from: "../data_app.malloy",
         },
      ]);
   });

   it("adds a source from a new file as a new statement after the last import", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.imports.push({
            kind: "names",
            names: ["events"],
            from: "../ev.malloy",
         });
      });
      expect(out).toContain(
         'import { scoped_orders, products } from "../data_app.malloy"\nimport { events } from "../ev.malloy"\n\nsource: a',
      );
      expect((await reread(out)).at(-1)).toEqual({
         kind: "names",
         names: ["events"],
         from: "../ev.malloy",
      });
   });

   it("takes a name off the statement", async () => {
      const out = await spliced(SOURCE, (d) => {
         const names = d.imports[1];
         if (names.kind === "names") names.names = ["scoped_orders"];
      });
      expect(out).toContain(
         'import { scoped_orders } from "../data_app.malloy"',
      );
      expect(await reread(out)).toEqual([
         { kind: "all", from: "../givens.malloy" },
         {
            kind: "names",
            names: ["scoped_orders"],
            from: "../data_app.malloy",
         },
      ]);
   });

   const WITH_SPARE = SOURCE.replace(
      "{ scoped_orders, products }",
      "{ scoped_orders }",
   ).replace(
      "\n\nsource: a",
      '\nimport { spare } from "../spare.malloy"\n\nsource: a',
   );
   const withoutSpare = (d: Awaited<ReturnType<typeof openDocument>>) => {
      d.imports = d.imports.filter(
         (i) => !(i.kind === "names" && i.names.includes("spare")),
      );
   };

   it("removes a statement whose last name nothing reads", async () => {
      const out = await spliced(WITH_SPARE, withoutSpare);
      expect(out).not.toContain("spare.malloy");
      expect(out).toContain(
         'import { scoped_orders } from "../data_app.malloy"\n\nsource: a',
      );
   });

   it("replaces the last import and adds another in one edit", async () => {
      const out = await spliced(WITH_SPARE, (d) => {
         withoutSpare(d);
         d.imports.push({
            kind: "names",
            names: ["events"],
            from: "../ev.malloy",
         });
      });
      expect(out).not.toContain("spare.malloy");
      expect(out).toContain(
         'import { scoped_orders } from "../data_app.malloy"\nimport { events } from "../ev.malloy"\n\nsource: a',
      );
      expect((await reread(out)).at(-1)).toEqual({
         kind: "names",
         names: ["events"],
         from: "../ev.malloy",
      });
   });

   it("refuses to remove a source a tile still reads", async () => {
      const reason = await refused(SOURCE, (d) => {
         const names = d.imports[1];
         if (names.kind === "names") names.names = ["products"];
      });
      expect(reason).toContain("`scoped_orders` is still read by a tile");
   });

   it("lets a tile go on a source added in the same edit", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.imports.push({
            kind: "names",
            names: ["events"],
            from: "../ev.malloy",
         });
         d.sources.push({ name: "events_tiles", base: "events" });
         d.tiles.push({
            name: "by_day_tile",
            source: "events_tiles",
            declaration: { kind: "reference", from: "by_day" },
         });
      });
      expect(out).toContain('import { events } from "../ev.malloy"');
      expect(out).toContain("source: events_tiles is events extend {");
   });

   it("never edits a whole-file import", async () => {
      const reason = await refused(SOURCE, (d) => {
         d.imports[0] = { kind: "all", from: "../other.malloy" };
      });
      expect(reason).toContain("whole-file");
   });

   it("refuses to rewrite an import with a comment inside it", async () => {
      const source = SOURCE.replace(
         "{ scoped_orders, products }",
         "{ scoped_orders, // the fact table\n  products }",
      );
      const reason = await refused(source, (d) => {
         const names = d.imports[1];
         if (names.kind === "names") names.names = ["scoped_orders"];
      });
      expect(reason).toContain("the fact table");
   });

   it("has nowhere to put a first import", async () => {
      const source = `## artifact { kind=notebook title="T" tiles=[intro { kind=text }] }\n\n##|(markdown) intro\nHi\n|##\n`;
      const reason = await refused(source, (d) => {
         d.imports.push({ kind: "names", names: ["x"], from: "../m.malloy" });
      });
      expect(reason).toContain("no `import`");
   });
});
