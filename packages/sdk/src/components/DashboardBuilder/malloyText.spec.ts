// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   artifactLine,
   declarationExtent,
   declarationLine,
   declarationsUnder,
   givenDeclarations,
   splitTrailingComment,
   tileSteps,
   viewBodyStage1,
} from "./malloyText";

const LINES = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

given: CATEGORY :: filter<string> is f''
given:
  SINCE :: date is @2023-01-01
  MIN :: number is 0

source: a is one extend {
  # drill { to=self }
  dimension: cat is products.category
  view: x is vx + { limit: 5, where: cat ~ $CATEGORY }
  view: y is {
    group_by: cat
  }
}

source: b is two extend {
  view: z is vz
}`.split("\n");

describe("the text a dashboard file is read as", () => {
   it("finds each source's own declarations and where a body ends", () => {
      expect([...declarationsUnder(LINES, "a", "view")]).toEqual([
         ["x", { line: 12, rest: "vx + { limit: 5, where: cat ~ $CATEGORY }" }],
         ["y", { line: 13, rest: "{" }],
      ]);
      expect([...declarationsUnder(LINES, "a", "dimension")]).toEqual([
         ["cat", { line: 11, rest: "products.category" }],
      ]);
      expect([...declarationsUnder(LINES, "b", "view").keys()]).toEqual(["z"]);
      expect(declarationLine(LINES, "source", "b")).toBe(18);
      expect(declarationLine(LINES, "view", "missing")).toBe(-1);
      expect(artifactLine(LINES)).toBe(1);
   });

   it("reads givens in both spellings, remembering a block's header", () => {
      expect([...givenDeclarations(LINES)]).toEqual([
         [
            "CATEGORY",
            { line: 4, declaration: "CATEGORY :: filter<string> is f''" },
         ],
         [
            "SINCE",
            {
               line: 6,
               blockHeader: 5,
               declaration: "SINCE :: date is @2023-01-01",
            },
         ],
         [
            "MIN",
            { line: 7, blockHeader: 5, declaration: "MIN :: number is 0" },
         ],
      ]);
   });

   it("splits a tile expression into its steps", () => {
      expect(tileSteps("orders -> by_brand")).toEqual({
         source: "orders",
         view: "by_brand",
      });
      expect(tileSteps("orders->by_brand + { limit: 2 }")).toEqual({
         source: "orders",
         view: "by_brand",
         refinement: "+ { limit: 2 }",
      });
      expect(tileSteps("{ group_by: x } -> y -> z")).toBeUndefined();
      expect(tileSteps(undefined)).toBeUndefined();
   });
});

describe("declarationExtent", () => {
   // Inside a `"""` span every line is string content, so nothing in it starts
   // the next declaration. SQL reaches for `<word>:` readily enough that this
   // is not hypothetical: a `read_csv(file:///data/orders.csv)` ended the
   // source at its first line, and a tile added to it was refused.
   it("does not read a URI inside a SQL literal as the next declaration", () => {
      const lines = `source: s is duckdb.sql("""
select * from read_csv(
file:///data/orders.csv
)
""") extend {
  view: v is { group_by: a }
}`.split("\n");
      expect(declarationExtent(lines, 0)).toEqual({ end: 6, opened: true });
   });

   // The defect this scan exists to fix: a source whose block opens on a LATER
   // line, because the base is a multi-line `"""` SQL literal. The naive read
   // — no `{` on the start line means the declaration is blockless — reports
   // the extent as the `source:` line itself, which anchors an insertion
   // inside the SQL text and attributes the source's own view to the line
   // above it.
   const MULTILINE_SOURCE = `source: regional is duckdb.sql("""
  select region, sum(amount) as total from orders group by 1
""") extend {
  view: by_region is { group_by: region }
}`.split("\n");

   it("finds the block that opens after a multi-line SQL literal", () => {
      expect(declarationExtent(MULTILINE_SOURCE, 0)).toEqual({
         end: 4,
         opened: true,
      });
   });

   // The corruption case the round-trip gate cannot catch: a `view:` spliced
   // into this literal would read back as a `view:` under the source above it,
   // so the scan has to refuse outright rather than guess.
   it('refuses when a brace inside a """ span could be the block or could be SQL', () => {
      const lines = `source: bad is duckdb.sql("""
  select {'region': 'West'} as s
""") extend {
  view: x is y
}`.split("\n");
      const extent = declarationExtent(lines, 0);
      expect("unreadable" in extent).toBe(true);
      if ("unreadable" in extent) expect(extent.unreadable).toContain("line 2");
   });

   // A blockless declaration as the LAST one before the enclosing extend's own
   // closing brace: nothing of this declaration's own ever opens a block, so
   // the scan has to recognize the enclosing `}` as out of scope rather than
   // counting it as this declaration's close — which would delete the source's
   // own closing brace along with the view.
   it("stops before the enclosing block's closing brace, not at it", () => {
      const lines = `source: a is one extend {
  view: by_cat is by_category
  view: by_brand is by_brand_view
}`.split("\n");
      expect(declarationExtent(lines, 2)).toEqual({ end: 2, opened: false });
   });

   // A `# drill { … }` tag sits between a blockless view and the next
   // declaration. Its brace is not this declaration's, and skipping the whole
   // line — rather than scanning it for braces — is what keeps a deletion from
   // swallowing a tag that belongs to the view after it.
   it("does not count a tag line's brace between a blockless view and the next", () => {
      const lines = `source: a is one extend {
  view: by_cat is by_category
  # drill { to=self }
  view: by_brand is by_brand_view
}`.split("\n");
      expect(declarationExtent(lines, 1)).toEqual({ end: 1, opened: false });
   });

   // The stop condition matches ANY `<word>:` line, not a fixed list of
   // keywords — `join_one:` is one this scan has never been asked about
   // before, and it has to stop there the same way it stops at `view:`.
   it("stops at a join_one following a blockless view", () => {
      const lines = `source: a is one extend {
  view: by_cat is by_category
  join_one: b is other_source on b.id = id extend {
    view: y is z
  }
}`.split("\n");
      expect(declarationExtent(lines, 1)).toEqual({ end: 1, opened: false });
   });

   // A brace inside a trailing `//` comment is text, not structure. Counted as
   // structure it made `kpis`'s own extent run through `other`, an unrelated
   // view, and out to the enclosing source's own closing brace — a corruption
   // the round-trip gate cannot catch, since reader and writer share the same
   // scan.
   it("does not count a brace inside a trailing comment", () => {
      const lines = `source: a is scoped_orders extend {
  view: kpis is {
    group_by: cat // the { brace here is unbalanced
    aggregate: n is count()
  }

  view: other is { aggregate: m is count() }
}`.split("\n");
      expect(declarationExtent(lines, 1)).toEqual({ end: 4, opened: true });
   });

   // `//` inside a quoted literal is not a comment, so the brace that follows
   // it on the same line is still structure.
   it("still counts a brace that follows a // inside a quoted literal", () => {
      const lines = `source: a is one extend {
  view: x is { where: path ~ 'a//b' }
}`.split("\n");
      expect(declarationExtent(lines, 1)).toEqual({ end: 1, opened: true });
   });
});

describe("viewBodyStage1", () => {
   // The same defect as declarationExtent's, one level down: a stray `{` in a
   // trailing comment inflated the depth count, which pushed every line after
   // it one level too deep — deep enough that a real `where:` line, at what
   // should be depth 1, stopped looking like the tile's own binding.
   it("does not let a brace inside a trailing comment inflate the depth count", () => {
      const lines = `  view: kpis is {
    group_by: cat // the { brace here is unbalanced
    where: cat ~ $CATEGORY
    aggregate: n is count()
  }`.split("\n");
      const stage1 = viewBodyStage1(lines, 0, 4);
      expect(stage1.whereLines).toEqual([
         { line: 2, code: "where: cat ~ $CATEGORY" },
      ]);
      expect(stage1.end).toBe(4);
   });
});

describe("splitTrailingComment", () => {
   it("finds no comment in a plain line", () => {
      expect(splitTrailingComment("  view: x is y")).toEqual({
         code: "  view: x is y",
         comment: "",
      });
   });

   it("keeps the gap with the code so an unchanged line stays unchanged", () => {
      const { code, comment } = splitTrailingComment("  view: x is y  // note");
      expect(code).toBe("  view: x is y  ");
      expect(comment).toBe("// note");
      expect(code + comment).toBe("  view: x is y  // note");
   });

   // Both spellings a dashboard reaches for: a filter literal is single-quoted,
   // and a SQL block is triple-double-quoted and spans lines.
   it("does not split inside a quoted literal", () => {
      expect(splitTrailingComment("where: path ~ 'a//b'").comment).toBe("");
      expect(splitTrailingComment('label="http://x"').comment).toBe("");
      expect(splitTrailingComment('sql("""select // not a comment')).toEqual({
         code: 'sql("""select // not a comment',
         comment: "",
      });
   });

   it("splits after a literal has closed", () => {
      expect(splitTrailingComment("where: path ~ 'a//b' // why")).toEqual({
         code: "where: path ~ 'a//b' ",
         comment: "// why",
      });
   });

   it("reads an escaped quote as text rather than a close", () => {
      expect(splitTrailingComment('label="a\\"b" // why').comment).toBe(
         "// why",
      );
   });
});
