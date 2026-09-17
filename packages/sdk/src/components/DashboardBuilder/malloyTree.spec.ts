// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   parseMalloy,
   parseRefused,
   type ParsedMalloy,
   type TreeStage,
} from "./malloyTree";

async function parsed(source: string): Promise<ParsedMalloy> {
   const result = await parseMalloy(source);
   if (parseRefused(result))
      throw new Error(`expected a parse: ${result.reason}`);
   return result.parsed;
}

async function refusal(source: string): Promise<string> {
   const result = await parseMalloy(source);
   if (!parseRefused(result)) throw new Error("expected a refusal");
   return result.reason;
}

const text = (p: ParsedMalloy, span: { start: number; end: number }) =>
   p.text.slice(span.start, span.end);

const view = async (body: string, source = "a") => {
   const src = `source: s is ${source} extend {\n  view: v is ${body}\n}\n`;
   const p = await parsed(src);
   return { p, v: p.sources[0].views[0] };
};

describe("locating a view's body", () => {
   it("reads an inline body as its own stage", async () => {
      const { p, v } = await view("{\n    aggregate: n is count()\n  }");
      expect(v.body.kind).toBe("inline");
      if (v.body.kind !== "inline") throw new Error("kind");
      expect(text(p, v.body.stage.span)).toBe(
         "{\n    aggregate: n is count()\n  }",
      );
   });

   it("reads a bare reference", async () => {
      const { v } = await view("by_brand");
      expect(v.body).toMatchObject({ kind: "reference", from: "by_brand" });
   });

   // The shape that DELETES the refinement at head: `vx + vy + { … }` nests
   // left, so the base is everything left of the RIGHTMOST operand.
   it("keeps the whole base of a chained refinement", async () => {
      const { p, v } = await view("vx + vy + { where: a ~ $A }");
      expect(v.body.kind).toBe("reference");
      if (v.body.kind !== "reference") throw new Error("kind");
      expect(v.body.from).toBe("vx + vy");
      expect(text(p, v.body.refinement!.span)).toBe("{ where: a ~ $A }");
   });

   it("refuses to treat a `->` pipeline as editable", async () => {
      const { v } = await view("x -> { aggregate: n is count() }");
      expect(v.body).toMatchObject({ kind: "unsupported" });
   });
});

describe("locating filters", () => {
   const wheresOf = (stage: TreeStage) => stage.wheres;

   it("reads field, operator and given off the tree", async () => {
      const { v } = await view("{\n    where: products.cat = $CAT\n  }");
      if (v.body.kind !== "inline") throw new Error("kind");
      expect(wheresOf(v.body.stage)[0].clauses[0].binding).toEqual({
         field: "products.cat",
         op: "=",
         given: "CAT",
      });
   });

   // A live corrupting bug at head: the `::` defeated the boundary scan, so a
   // second `where:` was written and the tile filtered twice.
   it("reads a cast on the left of a binding", async () => {
      const { v } = await view("{\n    where: created_at::date >= $SINCE\n  }");
      if (v.body.kind !== "inline") throw new Error("kind");
      expect(wheresOf(v.body.stage)[0].clauses[0].binding).toEqual({
         field: "created_at::date",
         op: ">=",
         given: "SINCE",
      });
   });

   it("reads a backtick field name, apostrophe and all", async () => {
      const { v } = await view("{\n    where: `it's odd` = $ODD\n  }");
      if (v.body.kind !== "inline") throw new Error("kind");
      expect(wheresOf(v.body.stage)[0].clauses[0].binding?.field).toBe(
         "`it's odd`",
      );
   });

   // Structure, not an isolation heuristic: an `and` is simply not a
   // comparison, so it can never be read as a binding to remove.
   it("does not read a compound predicate as a binding", async () => {
      const { v } = await view("{\n    where: a ~ $A and c = 1\n  }");
      if (v.body.kind !== "inline") throw new Error("kind");
      const clauses = wheresOf(v.body.stage)[0].clauses;
      expect(clauses).toHaveLength(1);
      expect(clauses[0].binding).toBeUndefined();
   });

   it("locates each clause of a list separately", async () => {
      const { p, v } = await view("{\n    where: x ~ $X, y ~ $Y\n  }");
      if (v.body.kind !== "inline") throw new Error("kind");
      const clauses = wheresOf(v.body.stage)[0].clauses;
      expect(clauses.map((c) => text(p, c.span))).toEqual(["x ~ $X", "y ~ $Y"]);
      expect(clauses.map((c) => c.binding?.given)).toEqual(["X", "Y"]);
   });

   // Depth is structure. A nested `nest:`'s filter is that block's, so it
   // cannot be lifted to tile level however the braces happen to be written.
   it("does not surface a nested nest's where as the body's own", async () => {
      const { v } = await view(
         "{\n    where: mine ~ $M\n    nest: sub is { where: theirs ~ $T }\n  }",
      );
      if (v.body.kind !== "inline") throw new Error("kind");
      expect(
         wheresOf(v.body.stage).map((w) => w.clauses[0].binding?.given),
      ).toEqual(["M"]);
   });

   // A filtered measure's `where:` is the measure's, not the source's.
   it("keeps a filtered measure's where out of the source's own", async () => {
      const p = await parsed(
         "source: s is a extend {\n  where: src ~ $S\n  measure: m is count() { where: fm ~ $F }\n}\n",
      );
      expect(
         p.sources[0].wheres.map((w) => w.clauses[0].binding?.given),
      ).toEqual(["S"]);
   });

   it("does not read a where: out of a multi-line string literal", async () => {
      const p = await parsed(
         'source: s is a extend {\n  view: v is {\n    select: q is """\n      where: fake ~ $FAKE\n    """\n  }\n}\n',
      );
      const body = p.sources[0].views[0].body;
      if (body.kind !== "inline") throw new Error("kind");
      expect(body.stage.wheres).toHaveLength(0);
   });
});

describe("locating declarations", () => {
   it("reads a source's base through backticks", async () => {
      const p = await parsed(
         "source: s is `odd name` extend {\n  where: a = 1\n}\n",
      );
      expect(p.sources[0]).toMatchObject({ name: "s", base: "`odd name`" });
   });

   it("carries a view's own tag block, and not the model's", async () => {
      const p = await parsed(
         '##! experimental.givens\nsource: s is a extend {\n  # colspan=6\n  # label="Orders"\n  view: v is x\n}\n',
      );
      expect(p.sources[0].views[0].tags.map((t) => t.text)).toEqual([
         "# colspan=6",
         '# label="Orders"',
      ]);
   });

   it("counts the siblings of a comma-separated view list", async () => {
      const p = await parsed(
         "source: s is a extend {\n  view: x is p, y is q\n}\n",
      );
      expect(p.sources[0].views.map((v) => [v.name, v.siblings])).toEqual([
         ["x", 2],
         ["y", 2],
      ]);
   });

   it("reads givens in both spellings", async () => {
      const p = await parsed(
         "##! experimental.givens\ngiven: A :: filter<string> is f''\ngiven:\n  B :: date is @2023-01-01\n",
      );
      expect(p.givens.map((g) => g.name)).toEqual(["A", "B"]);
      expect(p.givens[1].declaration).toBe("B :: date is @2023-01-01");
   });

   it("reads both import spellings", async () => {
      const p = await parsed(
         "import { a, b } from '../m.malloy'\nimport \"../all.malloy\"\n",
      );
      expect(p.imports).toMatchObject([
         { from: "../m.malloy", names: ["a", "b"] },
         { from: "../all.malloy" },
      ]);
   });
});

describe("comments", () => {
   it("finds a trailing comment but not a // inside a literal", async () => {
      const p = await parsed(
         "source: s is a extend {\n  view: v is x // note\n  view: w is y + { where: u ~ 'http://z' }\n}\n",
      );
      expect(text(p, p.trailingComment(1)!)).toBe("// note");
      expect(p.trailingComment(2)).toBeUndefined();
   });

   it("takes the comment block above a declaration up to a blank line", async () => {
      const source =
         "source: s is a extend {\n\n  // why this tile leads\n  # colspan=6\n  view: v is x\n}\n";
      const p = await parsed(source);
      expect(p.blockStart(4)).toBe(2);
   });
});

describe("refusing rather than guessing", () => {
   // ANTLR recovers from a syntax error by inventing structure — a phantom
   // view, a backwards range, every later declaration dropped — and reports
   // none of it. An edit located in that tree deletes real code.
   it("refuses a file with a syntax error", async () => {
      const reason = await refusal("source: s is a extend {\n  view: v is {\n");
      expect(reason).toContain("syntax error");
   });

   it("names the version when the tree is not what it expects", async () => {
      // The assertion's own message, which is the only protection a host that
      // resolved a different peer version ever gets.
      const reason = await refusal("source: s is a extend {\n  view: v is }\n");
      expect(reason.length).toBeGreaterThan(0);
   });
});

describe("offsets are UTF-16, not code points", () => {
   // The parser counts code points. Slicing a JS string by its numbers puts a
   // clause two units early and cuts an emoji into a lone surrogate.
   it("locates a clause after an astral character exactly", async () => {
      const { p, v } = await view(
         "{\n    where: label = '\u{1F600}\u{1F600}', b ~ $B\n  }",
      );
      if (v.body.kind !== "inline") throw new Error("kind");
      const clauses = v.body.stage.wheres[0].clauses;
      expect(text(p, clauses[1].span)).toBe("b ~ $B");
      expect(text(p, clauses[0].span)).toBe("label = '\u{1F600}\u{1F600}'");
   });
});
