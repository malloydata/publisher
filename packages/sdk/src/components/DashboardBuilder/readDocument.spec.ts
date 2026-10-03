// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { openDocument } from "./testing/fixtures";
import * as fs from "fs";
import * as path from "path";
import { blockAbove, readDashboardDocument, readFailed } from "./readDocument";
import { parseMalloy, parseRefused } from "./malloyTree";
import { queryTile } from "./testing/fixtures";
import { tileKey } from "./document";
import { newNotebookSource } from "../DocumentCreate/newNotebook";

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
   // Real sources, parsed: where the block STARTS comes from the lexer's own
   // comments now, and a synthetic array of lines could not exercise that.
   const block = async (source: string, declLine: number) => {
      const parse = await parseMalloy(source);
      if (parseRefused(parse)) throw new Error(parse.reason);
      return blockAbove(parse.parsed, source.split("\n"), declLine);
   };

   // The boundary is a blank line, because that is the author's own separator.
   // Validated against the bundled dashboard, where the block above
   // `revenue_trend` is fourteen lines of comment plus three tags.
   it("collects tags and comments up to the blank line", async () => {
      const at = await block(
         [
            "source: s is a extend {",
            "",
            "  // why this row is six and six",
            "  # colspan=6",
            '  # label="Revenue"',
            "  view: revenue is sales",
            "}",
         ].join("\n"),
         5,
      );
      expect(at.start).toBe(2);
      expect(at.tags.map((t) => t.text)).toEqual([
         "# colspan=6",
         '# label="Revenue"',
      ]);
      // The writer patches a tag in place, so the line number is load-bearing.
      expect(at.tags.map((t) => t.line)).toEqual([3, 4]);
   });

   it("stops at the blank line rather than running into the tile above", async () => {
      const at = await block(
         [
            "source: s is a extend {",
            "  # colspan=12",
            "  view: a is x",
            "",
            "  view: b is y",
            "}",
         ].join("\n"),
         4,
      );
      expect(at.start).toBe(4);
      expect(at.tags).toEqual([]);
   });

   // `##` is a MODEL annotation and never belongs to a declaration below it.
   it("ignores model-level annotations", async () => {
      const at = await block(
         '## artifact { title="T" }\nsource: a is b extend {\n  view: v is x\n}',
         1,
      );
      expect(at.tags).toEqual([]);
   });

   it("leaves an attached markdown block and line out of the tags, whatever its body starts with", async () => {
      const source = [
         "source: s is a extend {",
         "",
         "  #|(markdown)",
         "  # Lead tile",
         "  |#",
         "  #(markdown) a note",
         "  # colspan=6",
         "  view: revenue is sales",
         "}",
      ].join("\n");
      const at = await block(source, 7);
      expect(at.start).toBe(2);
      expect(at.tags.map((t) => t.text)).toEqual(["# colspan=6"]);
      expect(at.tags.map((t) => t.line)).toEqual([6]);
      expect(at.prose).toEqual([2, 3, 4, 5]);
   });

   it("starts a markdown block at its own opener, not at a body line that begins `#|`", async () => {
      const source = [
         "source: s is a extend {",
         "  dimension: d is 1",
         "  #|(markdown)",
         "  Notes",
         "",
         "  #| a body line",
         "  |#",
         "  # colspan=6",
         "  view: v is x",
         "}",
      ].join("\n");
      const at = await block(source, 8);
      expect(at.start).toBe(2);
      expect(at.prose).toEqual([2, 3, 4, 5, 6]);
   });

   it("does not call `#(markdown)` text inside a block comment prose", async () => {
      const source = [
         "source: s is a extend {",
         "",
         "  /*",
         "  #(markdown) only a comment",
         "  */",
         "  # colspan=6",
         "  view: v is x",
         "}",
      ].join("\n");
      const at = await block(source, 6);
      expect(at.start).toBe(2);
      expect(at.prose).toEqual([]);
   });

   it("does not collect a `#` line written inside a block comment", async () => {
      const source = [
         "source: s is a extend {",
         "",
         "  /* Notes:",
         "     # colspan is chosen below",
         "  */",
         "  # colspan=6",
         "  view: revenue is sales",
         "}",
      ].join("\n");
      const at = await block(source, 6);
      expect(at.start).toBe(2);
      expect(at.tags.map((t) => t.text)).toEqual(["# colspan=6"]);
   });

   // Malloy spells a comment three ways. Each of the two this used to read as
   // code stopped the walk early and hid every tag above it from the WRITER,
   // while the reader took its tags from the parser and saw them -- so a retag
   // wrote a second copy below the comment and the read-back gate passed.
   for (const [kind, comment] of [
      ["a `--` line", "  -- six across, to sit beside the trend"],
      ["a `/* ... */` line", "  /* six across, to sit beside the trend */"],
      [
         "a block comment over several lines",
         "  /* six across,\n     to sit beside the trend */",
      ],
   ] as const) {
      it(`reads ${kind} as part of the block, not the end of it`, async () => {
         const source = [
            "source: s is a extend {",
            "",
            "  # colspan=6",
            comment,
            "  view: revenue is sales",
            "}",
         ].join("\n");
         const declLine = source
            .split("\n")
            .indexOf("  view: revenue is sales");
         const at = await block(source, declLine);
         expect(at.start).toBe(2);
         expect(at.tags.map((t) => t.text)).toEqual(["# colspan=6"]);
      });
   }
});

describe("readDashboardDocument", () => {
   it("reads a width spelled only as the dashboard_columns alias", async () => {
      const doc = await read(
         SIMPLE.replace(
            '"a -> by_cat"] givens { CATEGORY="Jeans" } } dashboard { columns=12 }',
            '"a -> by_cat"] givens { CATEGORY="Jeans" } dashboard_columns=8 }',
         ),
      );
      expect(doc.columns).toBe(8);
   });

   describe("a width written badly", () => {
      const withTag = (tagLine: string) =>
         SIMPLE.replace(/^## artifact.*$/m, tagLine);

      it("lets a written columns win even when it is not a width", async () => {
         const doc = await read(
            withTag(
               '## artifact { tiles=["a -> by_cat"] dashboard_columns=8 } dashboard { columns=1.5 }',
            ),
         );
         expect(doc.columns).toBeUndefined();
      });

      it("reads the alias, spaced round its =, when no columns is written", async () => {
         const doc = await read(
            withTag(
               '## artifact { tiles=["a -> by_cat"] dashboard_columns = 8 }',
            ),
         );
         expect(doc.columns).toBe(8);
      });

      it("takes text that only begins with digits as no width", async () => {
         const doc = await read(
            withTag(
               '## artifact { tiles=["a -> by_cat"] dashboard_columns=8px }',
            ),
         );
         expect(doc.columns).toBeUndefined();
      });
   });

   describe("the description, by the server's rule", () => {
      const rest = SIMPLE.split("\n").slice(5).join("\n");
      const ARTIFACT = '## artifact { title="Probe" tiles=["a -> by_cat"] }';

      it("reads the notes above the tag and ignores those below", async () => {
         const doc = await read(`##" Above\n${ARTIFACT}\n##" Below\n${rest}`);
         expect(doc.description).toBe("Above");
      });

      it("falls back to the notes below the tag when nothing above has prose", async () => {
         const doc = await read(
            `##"\n${ARTIFACT}\n##" Legacy\n##" text\n${rest}`,
         );
         expect(doc.description).toBe("Legacy\ntext");
      });

      it("finds the tag past a note whose prose says artifact", async () => {
         const doc = await read(
            `##" This artifact shows revenue\n${ARTIFACT}\n${rest}`,
         );
         expect(doc.description).toBe("This artifact shows revenue");
         expect(doc.title).toBe("Probe");
      });

      it('reads a ##|" block above the tag, not the note below it', async () => {
         const doc = await read(
            `##|"\nBlock prose\n|##\n${ARTIFACT}\n##" Legacy\n${rest}`,
         );
         expect(doc.description).toBe("Block prose");
      });

      it('does not count an empty ##|" block above the tag as prose', async () => {
         const doc = await read(`##|"\n|##\n${ARTIFACT}\n##" Legacy\n${rest}`);
         expect(doc.description).toBe("Legacy");
      });

      it("does not take an artifact line inside a block for the tag", async () => {
         const doc = await read(
            `##|(text) intro\n## artifact { title="Fake" tiles=["x -> y"] }\n|##\n##" Above\n${ARTIFACT}\n${rest}`,
         );
         expect(doc.title).toBe("Probe");
         expect(doc.description).toBe("Above");
      });

      it("ends a block only at a closer in the opener's column", async () => {
         const doc = await read(
            `##|"\nBlock prose\n  |##\nstill inside\n|##\n${ARTIFACT}\n${rest}`,
         );
         expect(doc.description).toBe("Block prose\n|##\nstill inside");
      });

      it('takes a ##|" block below the tag as the fallback description, like the server', async () => {
         const doc = await read(`${ARTIFACT}\n##|"\nBelow block\n|##\n${rest}`);
         expect(doc.description).toBe("Below block");
      });

      it('ignores a ##" line inside a block', async () => {
         const doc = await read(
            `##|(text) intro\n##" not a note\n|##\n${ARTIFACT}\n${rest}`,
         );
         expect(doc.description).toBeUndefined();
      });

      it("reads neither a ##(markdown) line nor a ##|(markdown) block as the description or the tag", async () => {
         const doc = await read(
            `##(markdown) not a note\n##|(markdown) intro\n## How to read\n##" also not a note\n|##\n##" Above\n${ARTIFACT}\n##(markdown) nor this\n${rest}`,
         );
         expect(doc.description).toBe("Above");
         expect(doc.title).toBe("Probe");
      });

      it("falls back to a note below the tag past a ##(markdown) line", async () => {
         const doc = await read(
            `##(markdown) skip\n${ARTIFACT}\n##(markdown) skip too\n##" Legacy\n${rest}`,
         );
         expect(doc.description).toBe("Legacy");
      });

      it("has no description when the only note is a malformed route", async () => {
         const doc = await read(`##"word\n${ARTIFACT}\n${rest}`);
         expect(doc.description).toBeUndefined();
      });
   });

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
      expect(doc.tiles.map(tileKey)).toEqual(["a.x", "b.y"]);
   });

   it("reads a filter binding written as a refinement", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is vx + { where: products.brand ~ $BRAND }
}`);
      expect(queryTile(doc, 0).declaration).toEqual({
         kind: "reference",
         from: "vx",
      });
      expect(queryTile(doc, 0).filters).toEqual([
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

   it("reads the givens an extension's own where: reads, compound or not", async () => {
      const doc =
         await read(`## artifact { title="T" tiles=["a -> x", "b -> y"] }
import "../m.malloy"

source: a is one extend {
  where: region ~ $REGION, (brand ~ $BRAND or brand = null)
  view: x is vx + { where: cat ~ $CATEGORY }
}

source: b is two extend {
  view: y is vy
}`);
      expect(doc.sources.map((source) => source.scopedBy)).toEqual([
         ["REGION", "BRAND"],
         undefined,
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
      expect(queryTile(doc, 0).filters).toEqual([
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
      expect(queryTile(doc, 0).declaration).toEqual({ kind: "inline" });
      expect(queryTile(doc, 0).filters).toEqual([
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
      expect(queryTile(last, 0).filters).toEqual([
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
      expect(queryTile(middle, 0).filters).toEqual([
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
      expect(queryTile(doc, 0).filters).toBeUndefined();
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
      expect(queryTile(doc, 0).filters).toBeUndefined();
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
      expect(queryTile(doc, 0).filters).toEqual([
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
      expect(queryTile(doc, 0).filters).toEqual([
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
      expect(queryTile(doc, 0).filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
   });

   it("reads a one-line body's binding alongside its query", async () => {
      const doc = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is { aggregate: n is count() where: category ~ $CATEGORY }
}`);
      expect(queryTile(doc, 0).filters).toEqual([
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
      expect(queryTile(doc, 0).filters).toEqual([
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
      expect(queryTile(opening, 0).filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);

      const closing = await read(`## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  view: x is {
    aggregate: n is count()
    where: category ~ $CATEGORY }
}`);
      expect(queryTile(closing, 0).filters).toEqual([
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
      expect(queryTile(doc, 0).filters).toBeUndefined();
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

describe("readDashboardDocument: line endings", () => {
   // A checkout with CRLF endings is the same document, so every field reads the same.
   const SOURCES: Record<string, string> = {
      "one-line tag, notes and tile tags": SIMPLE,
      "a block tag and a text tile": newNotebookSource({
         title: "Sales",
         modelPath: "m.malloy",
         source: "orders",
         view: "by_brand",
      }),
      "a block description and a given contract": `##! experimental.givens
##|"
Block prose
|##
##| artifact { title="T"
  tiles=["a -> x"]
}
|##
import { one } from "../m.malloy"

# label="Category" control=select
given: CATEGORY :: filter<string> is f''

source: a is one extend {
  # colspan=6
  view: x is vx + { where: category ~ $CATEGORY }
}`,
   };
   for (const [name, source] of Object.entries(SOURCES))
      it(`reads ${name} the same with CRLF`, async () => {
         const lf = await readDashboardDocument(source);
         if (readFailed(lf)) throw new Error(lf.reason);
         const crlf = await readDashboardDocument(
            source.replace(/\n/g, "\r\n"),
         );
         if (readFailed(crlf)) throw new Error(crlf.reason);
         expect(crlf.document).toEqual(lf.document);
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
      expect(queryTile(doc, 0).declaration).toEqual({ kind: "inline" });
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
      const expectBroken = /-lint[\\/]/.test(name);
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
