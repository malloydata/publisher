// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { blockAbove, readDashboardDocument } from "./readDocument";

const REPO = path.resolve(import.meta.dir, "../../../../..");

const read = async (source: string) => {
   const result = await readDashboardDocument(source);
   if (!result.ok) throw new Error(`expected a document: ${result.reason}`);
   return result.document;
};

const SIMPLE = `##! experimental.givens

##" A narrative header.
##" Second paragraph.
## artifact { title="Probe" tiles=["a -> by_cat"] givens { CATEGORY="Jeans" } } dashboard { columns=12 }
import "../data_app.malloy"
import { products, regions } from "../storefront.malloy"

source: a is scoped_orders extend {
  # colspan=6
  # break
  # label="By category"
  view: by_cat is by_category
}`;

describe("blockAbove", () => {
   // The boundary is a blank line, because that is the author's own separator.
   // Validated against the bundled dashboard, where the block above
   // `revenue_trend` is fourteen lines of comment plus three tags.
   it("collects tags and comments up to the blank line", () => {
      const lines = [
         "  }",
         "",
         "  // why this row is six and six",
         "  # colspan=6",
         '  # label="Revenue"',
         "  view: revenue is sales",
      ];
      const block = blockAbove(lines, 5);
      expect(block.start).toBe(2);
      expect(block.tags).toEqual(["# colspan=6", '# label="Revenue"']);
   });

   it("stops at the blank line rather than running into the tile above", () => {
      const lines = ["  # colspan=12", "  view: a is x", "", "  view: b is y"];
      expect(blockAbove(lines, 3).start).toBe(3);
      expect(blockAbove(lines, 3).tags).toEqual([]);
   });

   // `##` is a MODEL annotation and never belongs to a declaration below it.
   it("ignores model-level annotations", () => {
      const lines = ['## artifact { title="T" }', "source: a is b extend {"];
      expect(blockAbove(lines, 1).tags).toEqual([]);
   });
});

describe("readDashboardDocument", () => {
   it("reads the whole shape", async () => {
      const doc = await read(SIMPLE);
      expect(doc.title).toBe("Probe");
      expect(doc.description).toBe("A narrative header.\nSecond paragraph.");
      expect(doc.columns).toBe(12);
      expect(doc.startingGivens).toEqual({ CATEGORY: "Jeans" });
      expect(doc.sources).toEqual([{ name: "a", base: "scoped_orders" }]);
      expect(doc.tiles).toEqual([
         {
            name: "by_cat",
            source: "a",
            declaration: { kind: "reference", from: "by_category" },
            label: "By category",
            colspan: 6,
            break: true,
         },
      ]);
   });

   // Both forms, and they are not interchangeable: a bare import is not
   // transitive, so a `suggest { source=products }` needs `products` named.
   it("reads both import forms", async () => {
      const doc = await read(SIMPLE);
      expect(doc.imports).toEqual([
         { kind: "all", from: "../data_app.malloy" },
         {
            kind: "names",
            names: ["products", "regions"],
            from: "../storefront.malloy",
         },
      ]);
   });

   // The reason the document holds a LIST of sources: a composite exists to
   // combine queries no single Malloy result can span.
   it("reads a dashboard spanning two sources", async () => {
      const doc =
         await read(`## artifact { title="T" tiles=["a -> x", "b -> y"] }
import "../m.malloy"

source: a is one extend {
  view: x is vx
}

source: b is two extend {
  view: y is vy
}`);
      expect(doc.sources.map((s) => s.name)).toEqual(["a", "b"]);
      expect(doc.tiles.map((t) => `${t.source}.${t.name}`)).toEqual([
         "a.x",
         "b.y",
      ]);
   });

   it("reads a filter binding written as a refinement", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is vx + { where: products.brand ~ $BRAND }
}`);
      expect(doc.tiles[0].declaration).toEqual({
         kind: "reference",
         from: "vx",
      });
      expect(doc.tiles[0].filters).toEqual([
         { field: "products.brand", given: "BRAND" },
      ]);
   });

   // Drill is authorable in the dashboard file, so the document carries it.
   it("reads a drill dimension", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  # drill { to=self given=CATEGORY }
  dimension: cat is products.category

  view: x is vx
}`);
      expect(doc.drills).toEqual([
         {
            source: "a",
            name: "cat",
            expression: "products.category",
            to: ["self"],
            given: "CATEGORY",
         },
      ]);
   });

   // Givens are the one declaration the parser's symbol tree does not cover.
   it("reads a dashboard-local given", async () => {
      const doc = await read(`##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

given:
  # label="Local" control=select
  LOCAL_X :: filter<string> is f'Jeans'

source: a is one extend {
  view: x is vx
}`);
      expect(doc.localGivens).toEqual([
         {
            name: "LOCAL_X",
            type: "filter<string>",
            default: "f'Jeans'",
            label: "Local",
            control: "select",
         },
      ]);
   });
});

describe("readDashboardDocument: what it refuses", () => {
   // All-or-nothing, and every refusal names what it could not understand.
   // "Cannot open this dashboard" with no reason reads as a bug.
   it("refuses a file with no artifact tag", async () => {
      const r = await readDashboardDocument("source: a is b extend { }");
      expect(r.ok).toBe(false);
      if (!r.ok) expect(r.reason).toContain("composite dashboard");
   });

   it("refuses a tile that is not a source -> view expression", async () => {
      const r = await readDashboardDocument(
         `## artifact { title="T" tiles=["just_a_name"] }\nsource: a is b extend { view: x is y }`,
      );
      expect(r.ok).toBe(false);
      if (!r.ok) expect(r.reason).toContain("source -> view");
   });

   // NOT a refusal, and the tripwire is what taught us so: a dashboard whose
   // only tile reads a view on an IMPORTED source declares nothing at all, and
   // is a complete dashboard. It opens, and that tile is read-only because its
   // tags live on the model's view.
   it("opens a tile whose view belongs to an imported source", async () => {
      const doc = await read(
         `## artifact { title="T" tiles=["orders -> by_brand"] }\nimport { orders } from '../orders.malloy'`,
      );
      expect(doc.sources).toEqual([]);
      expect(doc.tiles).toEqual([
         {
            name: "by_brand",
            source: "orders",
            declaration: { kind: "inherited" },
         },
      ]);
   });

   // Same principle for an inline body: the tags are editable, the query is not.
   it("opens a tile declared with an inline body", async () => {
      const doc = await read(
         `## artifact { title="T" tiles=["a -> x"] }\nimport "../m.malloy"\nsource: a is one extend {\n  # colspan=6\n  view: x is { aggregate: n }\n}`,
      );
      expect(doc.tiles[0].declaration).toEqual({ kind: "inline" });
      expect(doc.tiles[0].colspan).toBe(6);
   });
});

/**
 * The tripwire. All-or-nothing refusal is only reasonable while real dashboards
 * open, so this asserts that every composite dashboard in the repository does.
 * If that stops being true, this fails in CI rather than in a support thread,
 * and it is the signal to reconsider opening files partially.
 */
describe("every composite dashboard in the repository opens", () => {
   const roots = ["examples", "packages/server/tests/fixtures"];
   const found: string[] = [];
   const walk = (dir: string) => {
      if (!fs.existsSync(dir)) return;
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
         const full = path.join(dir, entry.name);
         if (entry.isDirectory()) walk(full);
         else if (entry.name.endsWith(".malloy") && full.includes("dashboards"))
            found.push(full);
      }
   };
   for (const root of roots) walk(path.join(REPO, root));

   const composites = found.filter((f) =>
      /##\s*artifact[\s\S]*tiles\s*=/.test(fs.readFileSync(f, "utf8")),
   );

   it("finds composites to check", () => {
      expect(composites.length).toBeGreaterThan(0);
   });

   for (const file of composites) {
      const name = path.relative(REPO, file);
      // The lint fixtures exist to BE broken; they are exercised by the refusal
      // tests above rather than expected to open.
      const expectBroken = name.includes("dashboards-lint");
      it(`${expectBroken ? "refuses" : "opens"} ${name}`, async () => {
         const result = await readDashboardDocument(
            fs.readFileSync(file, "utf8"),
         );
         if (expectBroken) return; // either outcome is acceptable for these
         if (!result.ok) throw new Error(result.reason);
         // Tiles, always. Sources NOT always: a dashboard whose tiles all read
         // imported sources declares no extension at all, which the tripwire
         // taught us on its first run.
         expect(result.document.tiles.length).toBeGreaterThan(0);
      });
   }
});

/**
 * The bundle guard. `@malloydata/malloy` is ~440 KB gzipped, and it is only
 * worth adding because it loads when the builder does and not before. One
 * STATIC import anywhere eagerly reachable hoists it into the entry chunk and
 * every Console user pays for it — silently, visible only in a bundle diff.
 *
 * So the rule is enforced rather than reviewed: the reader reaches the compiler
 * through `await import(…)` and nothing else.
 */
describe("the compiler stays lazy", () => {
   it("is never imported statically", () => {
      const source = fs.readFileSync(
         path.join(import.meta.dir, "readDocument.ts"),
         "utf8",
      );
      expect(source).not.toMatch(/^\s*import\s[^(]*@malloydata\/malloy/m);
      expect(source).toContain('await import("@malloydata/malloy")');
   });
});
