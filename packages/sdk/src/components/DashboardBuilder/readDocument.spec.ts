// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { openDocument } from "./testing/fixtures";
import * as fs from "fs";
import * as path from "path";
import { blockAbove, readDashboardDocument, readFailed } from "./readDocument";

const REPO = path.resolve(import.meta.dir, "../../../../..");

const read = openDocument;

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
      expect(block.tags.map((t) => t.text)).toEqual([
         "# colspan=6",
         '# label="Revenue"',
      ]);
      // The writer patches a tag in place, so the line number is load-bearing.
      expect(block.tags.map((t) => t.line)).toEqual([3, 4]);
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
      // And the dimension itself, tagged or not, is where a drill can go.
      expect(doc.sources[0].dimensions).toEqual([
         { name: "cat", expression: "products.category" },
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

describe("readDashboardDocument: how a tile binds", () => {
   // A `filter<…>` binds with `~`; a plain `date` is a value and binds with a
   // comparison. The reader keeps `~` implicit so the common case stays small.
   it("reads the comparison a binding uses", async () => {
      const doc = await read(`##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is vx + { where: category ~ $CATEGORY, where: created_at >= $SINCE, limit: 5 }
}`);
      expect(doc.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
         { field: "created_at", given: "SINCE", op: ">=" },
      ]);
   });
});

describe("readDashboardDocument: inline body filters", () => {
   // A binding on an inline body is a depth-1 `where:` statement in the body's
   // own first stage, not a `+ { … }` refinement — the writer's job is to
   // locate the same statements, so both sides read the shape the same way.
   it("reads a depth-1 where: line as the tile's filter", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    where: category ~ $CATEGORY
    group_by: category
    aggregate: n is count()
  }
}`);
      expect(doc.tiles[0].declaration).toEqual({ kind: "inline" });
      expect(doc.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
   });

   it("reads it at the end, or between two other statements, the same way", async () => {
      const last = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    group_by: category
    aggregate: n is count()
    where: category ~ $CATEGORY
  }
}`);
      expect(last.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);

      const middle = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    group_by: category
    where: category ~ $CATEGORY
    aggregate: n is count()
  }
}`);
      expect(middle.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
   });

   // A `nest:`'s own `where:` is depth 2, one level inside the nest's own
   // brace, not depth 1 of the tile's body — it is that nested view's filter,
   // not this tile's, and must not be reported as one.
   it("does not read a nest's own where: as the tile's filter", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    group_by: category
    nest: by_period is {
      where: period ~ $PERIOD
      aggregate: n is count()
    }
  }
}`);
      expect(doc.tiles[0].filters).toBeUndefined();
   });

   // "Whose ENTIRE text is one or more binding clauses" — `a ~ $A and c = 1`
   // matches BINDING_CLAUSE once, for `a ~ $A` alone, and the ` and c = 1`
   // left over means the line is not READ as a binding at all.
   it("does not read a compound predicate as a binding", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    where: category ~ $CATEGORY and status = 'open'
    aggregate: n is count()
  }
}`);
      expect(doc.tiles[0].filters).toBeUndefined();
   });

   // `cleanBindingClauses` accepts a following statement keyword as a clean
   // clause's own boundary — the rule a one-liner's binding needs to share a
   // line with its query — so two clean clauses either side of a real
   // statement can each pass in isolation while the statement between them
   // goes unnoticed. The whole-line check has to reject this shape: read as
   // binding-only, a splice that owns the line would drop `aggregate:` along
   // with the bindings.
   // Two `where:` statements with a measure between them. Each clause has its
   // own span, so both are ordinary bindings -- the writer removes one without
   // touching the measure, which is what the old "not ours" rule stood in for.
   it("reads two bindings with a statement between them", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    where: a ~ $A, aggregate: n is count(), where: b ~ $B
  }
}`);
      expect(doc.tiles[0].filters).toEqual([
         { field: "a", given: "A" },
         { field: "b", given: "B" },
      ]);
   });

   // Two bindings alone on one line, comma-joined, are still binding-only —
   // the gap between them is nothing but the separator, so the tightening
   // above must not catch this shape too.
   it("still reads two bindings on one line as binding-only", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    where: a ~ $A, where: b ~ $B
  }
}`);
      expect(doc.tiles[0].filters).toEqual([
         { field: "a", given: "A" },
         { field: "b", given: "B" },
      ]);
   });

   // A second stage does not stop the FIRST stage's own binding from being
   // read; only the WRITER refuses to touch a body shaped like this.
   it("still reads a first-stage binding when a second stage follows", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    where: category ~ $CATEGORY
    group_by: category
    aggregate: n is count()
  } -> {
    where: n > 10
    select: category, n
  }
}`);
      expect(doc.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
   });

   it("reads a one-line body's binding alongside its query", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is { aggregate: n is count() where: category ~ $CATEGORY }
}`);
      expect(doc.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
   });

   // The boundary after a where: clause is the next statement keyword, not
   // only the next binding clause or the end of the content — a one-line
   // body's binding is followed by comma-joined query text, not nothing.
   it("reads a one-line body's where: even when a comma joins it to more query text", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is { where: category ~ $CATEGORY, aggregate: n is count() }
}`);
      expect(doc.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
   });

   // A where: sharing the line that opens the body's brace, or the line that
   // closes it, is still read as the tile's own binding.
   it("reads a where: sharing a line with the body's opening or closing brace", async () => {
      const opening = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is { where: category ~ $CATEGORY
    aggregate: n is count()
  }
}`);
      expect(opening.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);

      const closing = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    aggregate: n is count()
    where: category ~ $CATEGORY }
}`);
      expect(closing.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
   });

   // A source-level `where:` sits outside every view's extent; it is read as
   // part of no tile's filters, the same as it always was.
   it("never attributes a source-level where: to a tile", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  where: brand_name ~ $BRAND

  view: x is { aggregate: n is count() }
}`);
      expect(doc.tiles[0].filters).toBeUndefined();
   });
});

describe("readDashboardDocument: the dashboard's own givens", () => {
   // The spelling `givens.malloy` and the docs use, and the one the builder
   // writes: one declaration per `given:` line, its control contract above it.
   it("reads one-line givens with their whole control contract", async () => {
      const doc = await read(`##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import { one, products } from "../m.malloy"

# description="Narrow to one category" label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''

# label="Minimum line total" range_min=0 range_max=250
given: MIN_SALE :: filter<number> is f''

source: a is one extend {
  view: x is vx + { where: category ~ $CATEGORY }
}`);
      expect(doc.localGivens).toEqual([
         {
            name: "CATEGORY",
            type: "filter<string>",
            default: "f''",
            label: "Category",
            description: "Narrow to one category",
            control: "select",
            suggest: { source: "products", dimension: "category" },
         },
         {
            name: "MIN_SALE",
            type: "filter<number>",
            default: "f''",
            label: "Minimum line total",
            rangeMin: 0,
            rangeMax: 250,
         },
      ]);
   });

   it("reads both spellings from one file, in file order", async () => {
      const doc = await read(`##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

given: FIRST :: date is @2023-01-01

given:
  # control=multiselect suggest { query=brand_suggest dimension=brand }
  BRAND :: filter<string> is f''

source: a is one extend {
  view: x is vx
}`);
      expect(doc.localGivens?.map((g) => g.name)).toEqual(["FIRST", "BRAND"]);
      expect(doc.localGivens?.[1].suggest).toEqual({
         query: "brand_suggest",
         dimension: "brand",
      });
   });
});

describe("readDashboardDocument: what it refuses", () => {
   // All-or-nothing, and every refusal names what it could not understand.
   // "Cannot open this dashboard" with no reason reads as a bug.
   it("refuses a file with no artifact tag", async () => {
      const r = await readDashboardDocument("source: a is b extend { }");
      expect(r.ok).toBe(false);
      if (readFailed(r)) expect(r.reason).toContain("composite dashboard");
   });

   it("refuses a tile that is not a source -> view expression", async () => {
      const r = await readDashboardDocument(
         `## artifact { title="T" tiles=["just_a_name"] }\nsource: a is b extend { view: x is y }`,
      );
      expect(r.ok).toBe(false);
      if (readFailed(r)) expect(r.reason).toContain("source -> view");
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
         if (readFailed(result)) throw new Error(result.reason);
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
         path.join(import.meta.dir, "malloyTree.ts"),
         "utf8",
      );
      expect(source).not.toMatch(/^\s*import\s[^(]*@malloydata\/malloy/m);
      expect(source).toContain('await import("@malloydata/malloy")');
   });
});
