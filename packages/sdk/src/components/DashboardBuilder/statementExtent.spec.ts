// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { openDocument, refused, spliced } from "./testing/fixtures";

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

   it("does not read a continued clause list as a binding", async () => {
      const d = await openDocument(CONTINUED_LIST);
      expect(d.tiles[0].filters).toBeUndefined();
   });

   it("does not read a continued predicate as a binding", async () => {
      const d = await openDocument(CONTINUED_PREDICATE);
      expect(d.tiles[0].filters).toBeUndefined();
   });

   // Unticking the control the first line appears to carry used to delete that
   // line and leave `b ~ $B` behind as a statement Malloy cannot parse.
   it("leaves a continued clause list whole when the tile changes", async () => {
      const out = await spliced(CONTINUED_LIST, (d) => {
         delete d.tiles[0].filters;
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain("    where: a ~ $A,\n      b ~ $B");
      expect(out).toContain("# colspan=4");
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
