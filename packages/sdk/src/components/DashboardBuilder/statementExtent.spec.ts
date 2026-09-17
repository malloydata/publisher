// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { openDocument, refused, spliced } from "./testing/fixtures";
import { syntaxErrors } from "./spliceDocument";

/**
 * Malloy's unit is the statement; every scan in this directory works a line at
 * a time. Where the two disagree -- a clause list or a predicate carried onto
 * the next line -- the reader used to report the first line as a whole binding
 * and the writer would rewrite it, stranding the remainder as a statement of
 * its own. The result was invalid Malloy that the round-trip gate accepted,
 * because the orphaned text is not a filter and so nothing the gate compares
 * ever looked at it.
 *
 * Each case here asserts the SURVIVING TEXT, never that the splice merely
 * returned ok: the unfixed path returned ok too.
 */
describe("a where: that spans lines is never the builder's to rewrite", () => {
   const body = (stage: string) => `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
${stage}
    aggregate: n is count()
  }
}`;

   const CONTINUED_LIST = body("    where: a ~ $A,\n      b ~ $B");
   const CONTINUED_PREDICATE = body("    where: a ~ $A\n      and c = 1");

   // A clause list carried onto a second line: the parser gives both clauses
   // their exact spans, so both are ordinary bindings rather than a shape the
   // builder has to declare off limits.
   it("reads every clause of a continued list", async () => {
      const d = await openDocument(CONTINUED_LIST);
      expect(d.tiles[0].filters).toEqual([
         { field: "a", given: "A" },
         { field: "b", given: "B" },
      ]);
   });

   it("does not read a continued predicate as a binding", async () => {
      const d = await openDocument(CONTINUED_PREDICATE);
      expect(d.tiles[0].filters).toBeUndefined();
   });

   // Unticking used to delete the first line and leave `b ~ $B` behind as a
   // statement Malloy cannot parse. Now the whole statement goes, and the
   // aggregate beside it does not.
   it("removes a continued clause list whole when the tile is unbound", async () => {
      const out = await spliced(CONTINUED_LIST, (d) => {
         delete d.tiles[0].filters;
         d.tiles[0].colspan = 4;
      });
      expect(out).not.toContain("where:");
      expect(out).not.toContain("b ~ $B");
      expect(out).toContain("    aggregate: n is count()");
      expect(out).toContain("# colspan=4");
   });

   // Dropping ONE clause of the pair leaves the other exactly, separator and
   // all -- the half-read that stranded text on `main`.
   it("drops one clause of a continued list and keeps the other", async () => {
      const out = await spliced(CONTINUED_LIST, (d) => {
         d.tiles[0].filters = [{ field: "a", given: "A" }];
      });
      expect(out).toContain("    where: a ~ $A\n    aggregate: n is count()");
      expect(out).not.toContain("b ~ $B");
   });

   // The mirror risk, and the one that fails quietly: Malloy takes a comma as a
   // statement separator, so a trailing comma does NOT mean the clause list
   // carries on. Reading it as a continuation would leave an ordinary binding
   // unmanaged, and the control would silently stop filtering.
   it("still reads a binding whose line ends in a separator comma", async () => {
      const d = await openDocument(body("    where: a ~ $A,"));
      expect(d.tiles[0].filters).toEqual([{ field: "a", given: "A" }]);
   });

   it("still reads one where the comma is the last thing in the stage", async () => {
      const trailing = `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
    aggregate: n is count()
    where: a ~ $A,
  }
}`;
      const d = await openDocument(trailing);
      expect(d.tiles[0].filters).toEqual([{ field: "a", given: "A" }]);
   });

   it("leaves a continued predicate whole when the tile changes", async () => {
      const out = await spliced(CONTINUED_PREDICATE, (d) => {
         delete d.tiles[0].filters;
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain("    where: a ~ $A\n      and c = 1");
      expect(out).toContain("# colspan=4");
   });
});

/**
 * The same disagreement on the reference path, where it predates this change:
 * `filtersOf` matched binding clauses anywhere in a `+ { … }` refinement with
 * no isolation check, so `where: a ~ $A and c = 1` read as a binding on $A --
 * and the writer, removing what the reader reported, cut the clause out with a
 * global regex and stranded ` and c = 1`.
 */
describe("a compound predicate in a refinement is unmodeled Malloy", () => {
   const COMPOUND = `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is vx + { where: a ~ $A and c = 1 }
}`;

   it("does not read it as a binding", async () => {
      const d = await openDocument(COMPOUND);
      expect(d.tiles[0].filters).toBeUndefined();
   });

   it("survives an unrelated change to the tile", async () => {
      const out = await spliced(COMPOUND, (d) => {
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain("vx + { where: a ~ $A and c = 1 }");
   });

   // $A is already filtering from text the builder cannot rewrite, so adding a
   // managed clause for it would filter on $A twice.
   it("refuses to bind a given it already filters on", async () => {
      const reason = await refused(COMPOUND, (d) => {
         d.tiles[0].filters = [{ field: "a2", given: "A" }];
      });
      expect(reason).toContain("already filters on `$A`");
   });

   // A DIFFERENT given is fine: the compound is kept and spaced off the new
   // clause, because Malloy rejects a comma after some statement forms.
   it("keeps the compound when a different given is bound", async () => {
      const out = await spliced(COMPOUND, (d) => {
         d.tiles[0].filters = [{ field: "b", given: "B" }];
      });
      expect(out).toContain("vx + { where: a ~ $A and c = 1 where: b ~ $B }");
   });
});

/**
 * A binding is a statement of the view the builder owns. A `where:` inside a
 * `nest:`, or inside a filtered measure's own `count() { … }`, belongs to that
 * inner block -- reading it as the tile's filter means a later edit deletes or
 * relocates someone else's predicate. Both gates are blind to it: the result
 * still parses, and the projection still matches.
 */
describe("a where: inside an inner block is not the tile's binding", () => {
   const doc = (declaration: string) => `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
${declaration}
}`;

   const MEASURE = doc(
      "  view: kpis is { aggregate: big is count() { where: amt > $MIN } }",
   );
   const NESTED = doc("  view: kpis is vx + { nest: y is { where: b ~ $B } }");

   it("does not read a filtered measure's predicate as a tile filter", async () => {
      const d = await openDocument(MEASURE);
      expect(d.tiles[0].filters).toBeUndefined();
   });

   it("does not delete that predicate when the tile is edited", async () => {
      const out = await spliced(MEASURE, (d) => {
         delete d.tiles[0].filters;
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain("aggregate: big is count() { where: amt > $MIN }");
   });

   it("does not read a nest's own where: as a tile filter", async () => {
      const d = await openDocument(NESTED);
      expect(d.tiles[0].filters).toBeUndefined();
   });

   // This one used to LIFT the nest's filter out to tile level on an edit that
   // had nothing to do with filters at all.
   it("leaves a nest's where: inside the nest on an unrelated change", async () => {
      const out = await spliced(NESTED, (d) => {
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain("vx + { nest: y is { where: b ~ $B } }");
   });
});

describe("the duplicate-given guard sees text it does not own", () => {
   const CONTINUED = `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
    where: a ~ $A,
      b ~ $B
    aggregate: n is count()
  }
}`;

   // $B is on the CONTINUATION line, which a line-at-a-time guard never saw --
   // so a second $B clause was written and the tile filtered on one control
   // twice. Both clauses are the builder's now, so rebinding $B REPLACES it
   // rather than doubling it, which is the outcome the refusal stood in for.
   it("rebinds a given used only on a continuation line, without doubling it", async () => {
      const out = await spliced(CONTINUED, (d) => {
         d.tiles[0].filters = [{ field: "b2", given: "B" }];
      });
      expect(out.match(/\$B\b/g)).toHaveLength(1);
      expect(out).toContain("    where: b2 ~ $B");
      expect(out).toContain("    aggregate: n is count()");
   });

   // And it does not fire on a given's name sitting inside a string literal.
   it("does not mistake a given named in a literal for a use of it", async () => {
      const out = await spliced(
         `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is vx + { where: label = 'costs $B' }
}`,
         (d) => {
            d.tiles[0].filters = [{ field: "b", given: "B" }];
         },
      );
      expect(out).toContain("where: label = 'costs $B'");
      expect(out).toContain("where: b ~ $B");
   });
});

describe("syntaxErrors", () => {
   it("is quiet on a document whose imports are unresolved", async () => {
      expect(
         await syntaxErrors(`##! experimental.givens
import "../m.malloy"

source: a is one extend {
  view: v is { where: c ~ $C
    aggregate: n is count()
  }
}`),
      ).toEqual([]);
   });

   it("reports a statement Malloy cannot parse", async () => {
      const errs = await syntaxErrors(`source: a is one extend {
  view: v is {
      b ~ $B
    aggregate: n is count()
  }
}`);
      expect(errs.length).toBeGreaterThan(0);
   });
});

/**
 * The duplicate-given guard decides by reading text the builder does not own,
 * so what counts as "text" and what counts as "a filter" both matter. It used
 * to scan the raw first stage for `$NAME` anywhere, which broke in both
 * directions at once.
 */
describe("the duplicate-given guard reads only what actually filters", () => {
   const body = (stage: string) => `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
${stage}
    aggregate: n is count()
  }
}`;

   // FAIL-OPEN, the worst of the three: an apostrophe in prose opened a quote
   // that masked the rest of the stage, so the guard saw nothing and wrote a
   // second $B clause under the compound one it was meant to protect.
   it("is not disarmed by an apostrophe in a comment", async () => {
      const reason = await refused(
         body("    // don't remove\n    where: b ~ $B and c = 1"),
         (d) => {
            d.tiles[0].filters = [{ field: "b2", given: "B" }];
         },
      );
      expect(reason).toContain("already filters on `$B`");
   });

   it("does not treat a given named in a comment as a filter", async () => {
      const out = await spliced(
         body("    // TODO: maybe filter on $B later\n    group_by: c"),
         (d) => {
            d.tiles[0].filters = [{ field: "b", given: "B" }];
         },
      );
      expect(out).toContain("where: b ~ $B");
      expect(out).toContain("// TODO: maybe filter on $B later");
   });

   // One control both filtering a tile and feeding a derived column is an
   // ordinary shape. Refusing it made a file the builder itself wrote
   // uneditable through the builder.
   it("does not treat a given in a group_by expression as a filter", async () => {
      const out = await spliced(
         body("    where: amt > $MIN\n    group_by: big is amt > $MIN"),
         (d) => {
            d.tiles[0].filters = [{ field: "amt", given: "MIN", op: ">=" }];
         },
      );
      expect(out).toContain("where: amt >= $MIN");
      expect(out).toContain("group_by: big is amt > $MIN");
   });
});

/**
 * A chained `+ { … } + { … }` refinement is valid Malloy the builder cannot
 * manage: one greedy unwrap spans both blocks. An unmatched `}` mid-text used
 * to drive the mask below depth 0, which made the second block look top-level
 * -- so the reader reported only that block's binding and dropped the first
 * from the projection entirely.
 */
describe("a chained refinement is left alone rather than half-read", () => {
   const CHAINED = `##! experimental.givens
## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is vx + { where: a ~ $A } + { limit: 5 }
}`;

   it("does not report a partial filter set", async () => {
      const d = await openDocument(CHAINED);
      expect(d.tiles[0].filters).toBeUndefined();
   });

   // Declared HERE, in a body with no one block a binding belongs in. Saying
   // "declared on its source" about it -- which is what the menu used to say --
   // is simply untrue, and it hid the fact that its tags ARE editable.
   it("reads as declared here, with a reason, and keeps its tags", async () => {
      const d = await openDocument(
         CHAINED.replace("  view: kpis", "  # colspan=6\n  view: kpis"),
      );
      expect(d.tiles[0].declaration).toEqual({
         kind: "opaque",
         why: "a chained refinement",
      });
      expect(d.tiles[0].colspan).toBe(6);
   });

   it("survives an unrelated change untouched", async () => {
      const out = await spliced(CHAINED, (d) => {
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain("vx + { where: a ~ $A } + { limit: 5 }");
   });

   // Two refinement blocks: no one of them is where a binding belongs, and a
   // given already filtered on in the other would be bound a second time.
   it("refuses a filter change rather than writing into one block", async () => {
      const reason = await refused(CHAINED, (d) => {
         d.tiles[0].filters = [{ field: "n", given: "N" }];
      });
      expect(reason).toContain("chained refinement");
      // The view is declared right here; blaming the model would send whoever
      // reads this to the wrong file.
      expect(reason).not.toContain("declared on its source");
   });
});
