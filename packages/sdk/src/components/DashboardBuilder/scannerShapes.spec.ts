// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `malloyText.ts` is a hand-written scanner that is about to be deleted and
 * replaced by a real-parser adapter. Its own spec (`malloyText.spec.ts`) tests
 * internal functions that die with it. This file re-pins the same Malloy SHAPES
 * end to end, through the public read/splice API only, so a regression in the
 * replacement is caught rather than shipped.
 */

import { describe, expect, it } from "bun:test";
import { readDashboardDocument, readFailed } from "./readDocument";
import { openDocument, spliced } from "./testing/fixtures";

describe("declarations under a source", () => {
   // declarationsUnder / declarationLine / artifactLine: a source's own views
   // and dimensions are found in file order, across more than one extension.
   it("finds each source's own views and dimensions, across two extensions, in file order", async () => {
      const doc = await openDocument(`##! experimental.givens
## artifact { title="T" tiles=["a -> x", "a -> y", "b -> z"] }
import "../m.malloy"

source: a is one extend {
  dimension: cat is products.category
  view: x is vx
  view: y is { group_by: cat }
}

source: b is two extend {
  view: z is vz
}`);
      expect(doc.sources.map((s) => s.name)).toEqual(["a", "b"]);
      expect(doc.sources[0].dimensions).toEqual([
         { name: "cat", expression: "products.category" },
      ]);
      expect(doc.tiles.map((t) => t.name)).toEqual(["x", "y", "z"]);
   });
});

describe("a tile expression's steps", () => {
   // tileSteps: the plain and refined forms both resolve to a source and view.
   it("reads a plain tile step and a refined one the same way", async () => {
      const doc =
         await openDocument(`## artifact { title="T" tiles=["a -> x", "a -> y + { limit: 5 }"] }
import "../m.malloy"

source: a is one extend {
  view: x is vx
  view: y is vy
}`);
      expect(doc.tiles[0]).toMatchObject({ source: "a", name: "x" });
      expect(doc.tiles[1]).toMatchObject({ source: "a", name: "y" });
   });

   // tileSteps returns undefined for anything but exactly one `->`, so a
   // multi-stage pipeline is refused the same way a bare name is.
   it("refuses a tile expression with more than one -> step", async () => {
      const r = await readDashboardDocument(
         `## artifact { title="T" tiles=["{ group_by: x } -> y -> z"] }`,
      );
      expect(r.ok).toBe(false);
      if (readFailed(r)) expect(r.reason).toContain("source -> view");
   });
});

describe("givens in both spellings", () => {
   // givenDeclarations: one-per-line and the block form coexist in one file,
   // in file order, each readable regardless of which header it sits under.
   it("reads a one-per-line given and a block of givens in file order", async () => {
      const doc = await openDocument(`##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

given: CATEGORY :: filter<string> is f''

given:
  SINCE :: date is @2023-01-01
  MIN_SALE :: number is 0

source: a is one extend {
  view: x is vx
}`);
      expect(doc.localGivens?.map((g) => g.name)).toEqual([
         "CATEGORY",
         "SINCE",
         "MIN_SALE",
      ]);
      expect(doc.localGivens?.[1]).toMatchObject({
         type: "date",
         default: "@2023-01-01",
      });
      expect(doc.localGivens?.[2]).toMatchObject({
         type: "number",
         default: "0",
      });
   });
});

describe("a SQL literal that could be mistaken for the next declaration", () => {
   // declarationExtent: `file:///…` reaches for `<word>:` just as readily as a
   // real declaration does. Read as one, it would end the source's extent at
   // that line — an anchor inside the SQL text rather than after the block.
   it("does not read a URI inside a SQL literal as the next declaration", async () => {
      const source = `## artifact { title="T" tiles=["s -> v"] }
source: s is duckdb.sql("""
select * from read_csv(
file:///data/orders.csv
)
""") extend {
  view: v is { group_by: a }
}`;
      const out = await spliced(source, (d) => {
         d.tiles.push({
            name: "w",
            source: "s",
            declaration: { kind: "reference", from: "vw" },
         });
      });
      expect(out).toContain(
         '""") extend {\n  view: v is { group_by: a }\n\n  view: w is vw\n}',
      );
      // Never inside the literal, and never ahead of the source line.
      expect(out.indexOf("view: w")).toBeGreaterThan(
         out.indexOf('""") extend'),
      );
   });

   // declarationExtent: the block itself can open on a LATER line than the
   // `source:` line when the base is a multi-line SQL literal — an edit to
   // this view has to land inside the extension, past the literal.
   it("finds the block that opens after a multi-line SQL literal", async () => {
      const source = `## artifact { title="T" tiles=["regional -> by_region"] }
source: regional is duckdb.sql("""
  select region, sum(amount) as total from orders group by 1
""") extend {
  view: by_region is { group_by: region }
}`;
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "region", given: "REGION" }];
      });
      expect(out).toContain(
         "view: by_region is { group_by: region where: region ~ $REGION }",
      );
   });

   // A brace inside the `"""` span used to be unresolvable by a text scan, so
   // this refused outright. The lexer knows where the literal ends, so the
   // tile is now added inside the right block instead.
   it('adds a tile past a brace inside a """ span', async () => {
      const source = `## artifact { title="T" tiles=["bad -> x"] }
source: bad is duckdb.sql("""
  select {'region': 'West'} as s
""") extend {
  view: x is y
}`;
      const out = await spliced(source, (d) => {
         d.tiles.push({
            name: "w",
            source: "bad",
            declaration: { kind: "reference", from: "vw" },
         });
      });
      expect(out).toContain("  select {'region': 'West'} as s");
      expect(out).toContain("view: w is vw");
      expect(out.indexOf("view: w is vw")).toBeGreaterThan(
         out.indexOf('""") extend {'),
      );
   });
});

describe("a blockless declaration's own extent", () => {
   // declarationExtent: the LAST blockless view before the enclosing extend's
   // own closing brace must not swallow that brace as if it were its own.
   it("stops before the enclosing block's closing brace, not at it", async () => {
      const source = `## artifact { title="T" tiles=["a -> by_cat", "a -> by_brand"] }
source: a is one extend {
  view: by_cat is by_category
  view: by_brand is by_brand_view
}`;
      const out = await spliced(source, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "by_brand");
      });
      expect(out).toContain(
         "source: a is one extend {\n  view: by_cat is by_category\n}",
      );
      expect(out).not.toContain("by_brand");
      expect(out.match(/^}/gm)).toHaveLength(1);
   });

   // declarationExtent: a tag line between a blockless view and the next
   // declaration is skipped whole, not scanned for braces — deleting the
   // FIRST view must not take the tag that belongs to the one after it.
   it("does not count a tag line's brace between a blockless view and the next", async () => {
      const source = `## artifact { title="T" tiles=["a -> by_cat", "a -> by_brand"] }
source: a is one extend {
  view: by_cat is by_category
  # drill { to=self }
  view: by_brand is by_brand_view
}`;
      const out = await spliced(source, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "by_cat");
      });
      expect(out).toContain(
         "source: a is one extend {\n  # drill { to=self }\n  view: by_brand is by_brand_view\n}",
      );
      expect(out).not.toContain("by_cat");
   });

   // declarationExtent: the stop condition matches ANY `<word>:` line, so a
   // `join_one:` block right after a blockless view has to stop the same way
   // a `view:` would — and survive whole when that view is removed.
   it("stops at a join_one following a blockless view", async () => {
      const source = `## artifact { title="T" tiles=["a -> by_cat", "a -> by_brand"] }
source: a is one extend {
  view: by_cat is by_category
  join_one: other is other_source on other.id = id
  view: by_brand is by_brand_view
}`;
      const out = await spliced(source, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "by_cat");
      });
      expect(out).toContain(
         "  join_one: other is other_source on other.id = id\n  view: by_brand is by_brand_view\n}",
      );
      expect(out).not.toContain("by_cat");
   });
});

describe("text that masks structure inside a declaration", () => {
   // declarationExtent: a brace inside a trailing `//` comment is text, not
   // structure — retagging the view AFTER the commented one must land above
   // that view's own line, not somewhere inside the commented view's body.
   it("does not count a brace inside a trailing comment when finding where a view ends", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis", "a -> other"] }
source: a is scoped_orders extend {
  view: kpis is {
    group_by: cat // the { brace here is unbalanced
    aggregate: n is count()
  }

  view: other is { aggregate: m is count() }
}`;
      const out = await spliced(source, (d) => {
         d.tiles[1].colspan = 6;
      });
      expect(out).toContain(
         "  # colspan=6\n  view: other is { aggregate: m is count() }",
      );
      expect(out).not.toContain("colspan=6\n    group_by: cat");
   });

   // declarationExtent: `//` inside a quoted literal is not a comment, so a
   // brace that follows it on the same line is still real structure.
   it("still counts a brace that follows a // inside a quoted literal", async () => {
      const source = `## artifact { title="T" tiles=["a -> x", "a -> y"] }
source: a is one extend {
  view: x is { where: path ~ 'a//b' }
  view: y is vy
}`;
      const out = await spliced(source, (d) => {
         d.tiles[1].colspan = 4;
      });
      expect(out).toContain("  # colspan=4\n  view: y is vy");
      expect(out).not.toContain("colspan=4\n  view: x");
   });

   // declarationExtent: a brace inside a single-quoted filter literal is text,
   // not structure — same for one holding a stray `}`.
   it("does not count a brace inside a single-quoted literal", async () => {
      for (const literal of ["'a{b'", "'a}b'"]) {
         const source = `## artifact { title="T" tiles=["a -> kpis", "a -> other"] }
source: a is one extend {
  view: kpis is {
    where: path ~ ${literal}
    aggregate: n is count()
  }

  view: other is { aggregate: m is count() }
}`;
         const out = await spliced(source, (d) => {
            d.tiles[1].colspan = 4;
         });
         expect(out).toContain(
            "  # colspan=4\n  view: other is { aggregate: m is count() }",
         );
      }
   });

   // Same masking, double-quoted.
   it("does not count a brace inside a double-quoted literal", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis", "a -> other"] }
source: a is one extend {
  view: kpis is {
    where: path ~ "a{b"
    aggregate: n is count()
  }

  view: other is { aggregate: m is count() }
}`;
      const out = await spliced(source, (d) => {
         d.tiles[1].colspan = 4;
      });
      expect(out).toContain(
         "  # colspan=4\n  view: other is { aggregate: m is count() }",
      );
   });
});

describe("depth counting inside a view body", () => {
   // viewBodyStage1: the same masking, one level down — a brace inside a
   // trailing comment must not push a real depth-1 where: to depth 2, where
   // it would stop being read (or written) as the tile's own filter.
   it("does not let a brace inside a trailing comment inflate the depth count", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
source: a is one extend {
  view: kpis is {
    group_by: cat // the { brace here is unbalanced
    where: cat ~ $CATEGORY
    aggregate: n is count()
  }
}`;
      const before = await openDocument(source);
      expect(before.tiles[0].filters).toEqual([
         { field: "cat", given: "CATEGORY" },
      ]);
      const out = await spliced(source, (d) => {
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain(
         "    group_by: cat // the { brace here is unbalanced\n    where: cat ~ $CATEGORY\n",
      );
   });

   // viewBodyStage1: a brace inside a quoted literal on a depth-1 where: line
   // must not throw off the count that decides a LATER where: line's depth.
   it("does not let a brace inside a quoted literal inflate the depth count", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
source: a is one extend {
  view: kpis is {
    where: path ~ 'a{b'
    where: cat ~ $CATEGORY
    aggregate: n is count()
  }
}`;
      const before = await openDocument(source);
      // Both where: lines are seen at depth 1: the literal's phantom brace
      // would otherwise push the second one to depth 2 and drop it.
      expect(before.tiles[0].filters).toEqual([
         { field: "cat", given: "CATEGORY" },
      ]);
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "cat", given: "BRAND" }];
      });
      expect(out).toContain(
         "    where: path ~ 'a{b'\n    aggregate: n is count()\n    where: cat ~ $BRAND",
      );
   });
});

describe("a where: sharing a line with the body's own brace", () => {
   // viewBodyStage1: no line of its own to match a whole-line check against —
   // re-binding the same value must be a no-op, not a duplicate append.
   it("round-trips a where: that shares the opening line with the body's brace", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
source: a is one extend {
  view: kpis is { where: category ~ $CATEGORY
    aggregate: n is count()
  }
}`;
      const before = await openDocument(source);
      expect(before.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out).toBe(source);
      expect(out.match(/where:/g)).toHaveLength(1);
   });

   // Same, sharing the closing line — the old scan returned as soon as it saw
   // the brace close, before ever looking at the where: beside it.
   it("round-trips a where: that shares the closing line with the body's brace", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
source: a is one extend {
  view: kpis is {
    aggregate: n is count()
    where: category ~ $CATEGORY }
}`;
      const before = await openDocument(source);
      expect(before.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out).toBe(source);
      expect(out.match(/where:/g)).toHaveLength(1);
   });
});

describe("a one-liner body's where:", () => {
   // viewBodyStage1: body opens AND closes on the declaration line — its
   // where: is reported once, through the one-liner, never duplicated.
   it("does not double-collect a one-liner's where:", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
source: a is one extend {
  view: kpis is { aggregate: n is count(), where: cat ~ $CATEGORY }
}`;
      const before = await openDocument(source);
      expect(before.tiles[0].filters).toEqual([
         { field: "cat", given: "CATEGORY" },
      ]);
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "cat", given: "CATEGORY" }];
      });
      expect(out).toBe(source);
      expect(out.match(/where:/g)).toHaveLength(1);
   });
});

describe("a nested nest:'s own where:", () => {
   // viewBodyStage1: the scan tracks actual brace depth, not which line the
   // text sits on — a nest's where: stays excluded even sharing a line with
   // the outer body's own depth-1 where:, and must never be lifted to the
   // tile's own filters.
   it("excludes a nested nest's where: even sharing a line with the outer one", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
source: a is one extend {
  view: kpis is {
    group_by: category
    nest: by_period is {
      where: period ~ $PERIOD
      aggregate: n is count()
    }, where: cat ~ $CATEGORY
  }
}`;
      const before = await openDocument(source);
      expect(before.tiles[0].filters).toEqual([
         { field: "cat", given: "CATEGORY" },
      ]);
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "cat", given: "BRAND" }];
      });
      // The nest's own binding is untouched, not rewritten alongside the outer one.
      expect(out).toContain(
         "      where: period ~ $PERIOD\n      aggregate: n is count()\n    }\n    where: cat ~ $BRAND",
      );
   });
});

describe("a where: inside a multi-line string span", () => {
   // viewBodyStage1: the naive scan did not carry a `"""` span's open state
   // across lines, so a depth-1 line whose TEXT reads exactly like a real
   // binding — `where: cat ~ $CATEGORY` — must still not be read as one when
   // it sits inside a `"""` string opened by an earlier line: it is string
   // content, not a statement, and BINDING_CLAUSE would otherwise match it.
   it('does not read a where: from inside a multi-line """ span as a binding', async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
source: a is one extend {
  view: kpis is {
    where: note ~ f"""
where: cat ~ $CATEGORY
"""
    aggregate: n is count()
  }
}`;
      const doc = await openDocument(source);
      expect(doc.tiles[0].filters).toBeUndefined();
      // An unrelated edit must leave the string exactly as written.
      const out = await spliced(source, (d) => {
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain(
         '    where: note ~ f"""\nwhere: cat ~ $CATEGORY\n"""\n    aggregate: n is count()',
      );
   });
});

describe("splitting a trailing comment off a line", () => {
   // splitTrailingComment: a line with no `//` is left whole.
   it("finds no comment in a plain declaration line", async () => {
      const doc = await openDocument(`## artifact { title="T" tiles=["a -> x"] }
source: a is one extend {
  view: x is vx
}`);
      expect(doc.tiles[0].name).toBe("x");
   });

   // splitTrailingComment: the gap before `//` is kept with the code, so an
   // edit to an UNRELATED tile leaves this tile's line, gap and all, byte for
   // byte the same.
   it("keeps the gap with the code so an unchanged line stays unchanged", async () => {
      const source = `## artifact { title="T" tiles=["a -> x", "a -> y"] }
source: a is one extend {
  view: x is vx   // note
  view: y is vy
}`;
      const out = await spliced(source, (d) => {
         d.tiles[1].colspan = 6;
      });
      expect(out).toContain("  view: x is vx   // note\n");
   });

   // splitTrailingComment: `//` inside a quoted default is not a comment, so
   // the given's default text survives whole rather than being truncated.
   it("does not split inside a quoted literal", async () => {
      const doc = await openDocument(`##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
given: PATH :: filter<string> is f'a//b'

source: a is one extend {
  view: x is vx
}`);
      expect(doc.localGivens?.[0].default).toBe("f'a//b'");
   });

   // splitTrailingComment: a real comment after the literal has closed is
   // still split off normally.
   it("splits after a literal has closed", async () => {
      const source = `## artifact { title="T" tiles=["a -> x", "a -> y"] }
source: a is one extend {
  view: x is vx + { where: path ~ 'a//b' } // why
  view: y is vy
}`;
      const out = await spliced(source, (d) => {
         d.tiles[1].colspan = 6;
      });
      expect(out).toContain(
         "  view: x is vx + { where: path ~ 'a//b' } // why\n",
      );
   });

   // splitTrailingComment: a backslash-escaped quote is read as text, not a
   // string close, so the comment after it is found in the right place —
   // and the tag survives an unrelated edit exactly as written.
   it("reads an escaped quote as text rather than a close", async () => {
      const source = `## artifact { title="T" tiles=["a -> x", "a -> y"] }
source: a is one extend {
  # label="Say \\"hi\\"" // why
  view: x is vx
  view: y is vy
}`;
      const out = await spliced(source, (d) => {
         d.tiles[1].colspan = 6;
      });
      expect(out).toContain('  # label="Say \\"hi\\"" // why\n  view: x is vx');
   });
});
