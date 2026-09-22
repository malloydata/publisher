// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { DashboardDocument } from "./document";
import { readDashboardDocument, readFailed } from "./readDocument";
import {
   spliceDashboardDocument,
   spliceFailed,
   syntaxErrors,
} from "./spliceDocument";
import { openDocument } from "./testing/fixtures";
import {
   NOTHING_MOVED,
   whatMoved,
   type InventoryDiff,
} from "./__test__/inventory";

/**
 * ONE INVARIANT for the writer, in place of one spec per corruption: an edit
 * changes the declaration it names, and nothing else.
 *
 * The sibling specs pin SHAPES -- this list, this refinement, this brace on
 * this line -- and each of them was written after a defect shipped. Three of
 * those defects returned `ok`, which is what makes the shape-by-shape approach
 * lose: the two gates in `spliceDocument` cannot see a comment or an unmodelled
 * tag go, so the next sibling of the same shape ships too. This file asks the
 * question once, of a fresh parse of the output, through `__test__/inventory`.
 *
 * EVERY ROW DECLARES ITS OUTCOME. A reasoned refusal is a legitimate answer on
 * a real file and the corpus sweep below treats it as one; here it is not, and
 * a row that is meant to write and refuses fails exactly as loudly as one that
 * is meant to refuse and writes. Without that the gate is hollow: reverting the
 * two-line `given:` fix turns a success into a reasoned refusal, and a sweep
 * that accepted refusals would stay green over the very defect it exists for.
 */

/**
 * One file carrying every shape at once, because each `it` re-reads it and the
 * only real constraint is that the shapes coexist syntactically.
 *
 * What is in here, and why each is load-bearing:
 *  - FOUR declared tiles: one is removed, so it cannot also be the one under a
 *    filter edit, and the reader needs one left over.
 *  - `kpis`: a `where:` whose every clause is the builder's, with a BLOCK
 *    comment inside its span -- the shape that deleted a comment and reported
 *    success, in the spelling the comment index could not see.
 *  - `kpis` and `counts`: `# colspan` above a `--` line and above a multi-line
 *    `/* ... *\/`, both of which are comments to Malloy. The reader takes tags
 *    from the parser and sees the tag; a writer that reads the block as text
 *    does not, and writes a second copy below.
 *  - `trend`: a refinement that collapses, with a comment beside it, and above
 *    it a dimension that SHARES ITS LINE with a block comment. A locator that
 *    asks "does this line hold a comment" rather than "does it hold code" walks
 *    straight past that dimension and takes its `# label` for the tile's.
 *  - `counts`: a trailing comment on the LAST statement of the body, which is
 *    where an appended binding slides underneath one, and a `#` line written
 *    inside its block comment that WOULD be a modelled tag anywhere else,
 *    which here is prose: read as a tag it becomes a label the tile does not
 *    have, and the writer then writes that label out for real.
 *  - `spare`: a tile to remove that carries a comment and a tag, and shares its
 *    line with a view that is not a tile of this page.
 *  - `SINCE`: a `given:` declared across two lines.
 *  - `run:`: a top-level statement the document does not model at all.
 */
const PROBE = `##! experimental.givens

##" The probe dashboard.
## artifact { title="Probe" tiles=["a -> kpis", "a -> trend", "a -> counts", "a -> spare"] } dashboard { columns=12 }
import "../m.malloy"

# label="Category"
given: CATEGORY :: filter<string> is f''

# label="Brand"
given: BRAND :: filter<string> is f''

given: SINCE :: filter<string> is
  f''

source: a is one extend {
  view: base is { aggregate: n is count() }

  // Why the KPI strip leads: revenue is the number people ask about first.
  # colspan=6
  -- six across, so it sits beside the trend
  # big_value
  view: kpis is {
    aggregate: revenue is count()
    where: cat ~ $CATEGORY,
      /* both halves of the same question */
      brand ~ $BRAND
  }

  # label="A helper, not a tile"
  /* a helper, not a tile */ dimension: helper is 1
  # colspan=6
  view: trend is base // the trend, on the right
    + { where: sold_on ~ $SINCE }

  # colspan=12
  /* twelve across: the breakdown reads as a table.
     It was tagged
     # label="Counts"
     until somebody decided the title said it twice. */
  view: counts is {
    group_by: cat
    aggregate: n is count() // keep with measure
  }

  // A note the author wrote about the spare.
  # colspan=12
  view: spare is base  view: sidekick is base
}

run: a -> kpis
`;

const tile = (d: DashboardDocument, name: string) =>
   d.tiles.find((t) => t.name === name)!;

interface Row {
   what: string;
   edit: (d: DashboardDocument) => void;
   /**
    * What the writer must do. A mismatch EITHER WAY fails: see the file's
    * header for why a refusal cannot be allowed to stand in for a write.
    */
   outcome: "succeeds" | "refuses";
   /** A substring of the refusal, matched loosely so wording stays editable. */
   because?: string;
   /** Everything the inventory may report as moved. Absent fields must be empty. */
   moved?: Partial<InventoryDiff>;
}

const ROWS: Row[] = [
   {
      what: "asks for nothing",
      edit: () => {},
      outcome: "succeeds",
   },
   {
      what: "reorders the tiles",
      edit: (d) => {
         d.tiles = [d.tiles[3], d.tiles[0], d.tiles[1], d.tiles[2]];
      },
      outcome: "succeeds",
   },
   {
      // The `#` tags this rewrites sit above a `--` line, so a writer that
      // cannot see them adds a second `# colspan` rather than patching the one
      // that is there -- and the reader, which takes tags from the parser,
      // reads the lower one back and the round-trip gate is satisfied.
      what: "retags a tile whose tag block holds a `--` comment",
      edit: (d) => {
         tile(d, "kpis").colspan = 3;
      },
      outcome: "succeeds",
      moved: { tagText: ["~view:a.kpis"] },
   },
   {
      // The same shape as the row above, in the spelling that also hid a tag
      // from a walk that had just learned about `--`: a `/* ... */`, and one
      // that runs over more than one line. That comment holds a `# label=` line,
      // which here is prose: walking further up to find the real tag must not
      // turn a line inside a comment into an annotation -- to rewrite in place
      // on the writer's side, or to report as a tag on the reader's.
      what: "retags a tile whose tag block holds a block comment",
      edit: (d) => {
         tile(d, "counts").colspan = 6;
      },
      outcome: "succeeds",
      moved: { tagText: ["~view:a.counts"] },
   },
   {
      // A line that is a comment AND a declaration at once. Read as wholly
      // comment, it is walked straight past, and the `# label` above it -- the
      // dimension's -- is taken for part of the tile's tag block and rewritten
      // to match a tile that never had a label. Nothing downstream can tell:
      // the reader takes tags from the parser and never attributed it to the
      // tile in the first place.
      what: "retags a tile below a declaration sharing its line with a comment",
      edit: (d) => {
         tile(d, "trend").colspan = 4;
      },
      outcome: "succeeds",
      moved: { tagText: ["~view:a.trend"] },
   },
   {
      what: "relabels a control",
      edit: (d) => {
         d.localGivens!.find((g) => g.name === "CATEGORY")!.label = "Product";
      },
      outcome: "succeeds",
      moved: { tagText: ["~given:CATEGORY"] },
   },
   {
      // The anchor is the last statement's end and the comment is past it, so
      // an insertion lands between them unless the anchor is widened. The
      // comment then explains a `where:` its author never wrote, and stays in
      // the file, so nothing downstream can tell.
      what: "binds a control onto a body whose last statement carries a comment",
      edit: (d) => {
         tile(d, "counts").filters = [{ field: "cat", given: "CATEGORY" }];
      },
      outcome: "succeeds",
      moved: { declarations: ["~view:a.counts"] },
   },
   {
      what: "adds a binding to a refinement a comment sits beside",
      edit: (d) => {
         tile(d, "trend").filters = [
            { field: "sold_on", given: "SINCE" },
            { field: "brand", given: "BRAND" },
         ];
      },
      outcome: "succeeds",
      moved: { declarations: ["~view:a.trend"] },
   },
   {
      // The comment above the removed tile is LEFT WHERE IT WAS, which is the
      // documented rule: the file cannot say whether it belonged to the tile,
      // the row or the page. With the tile gone it reads as the block above the
      // view that shared its line, and that is the whole of what moved.
      what: "removes a tile that shares its line with a view that is not a tile",
      edit: (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "spare");
      },
      outcome: "succeeds",
      moved: {
         declarations: ["-view:a.spare"],
         block: ["~view:a.sidekick"],
      },
   },
   {
      what: "removes a control declared across two lines",
      edit: (d) => {
         d.localGivens = d.localGivens!.filter((g) => g.name !== "SINCE");
      },
      outcome: "succeeds",
      moved: { declarations: ["-given:SINCE"] },
   },
   {
      what: "is asked to unbind a tile whose whole `where:` holds a comment",
      edit: (d) => {
         delete tile(d, "kpis").filters;
      },
      outcome: "refuses",
      because: "/* both halves of the same question */",
   },
   {
      what: "is asked to drop one clause across a comment",
      edit: (d) => {
         tile(d, "kpis").filters = [{ field: "cat", given: "CATEGORY" }];
      },
      outcome: "refuses",
      because: "/* both halves of the same question */",
   },
   {
      what: "is asked to collapse a refinement a comment sits beside",
      edit: (d) => {
         delete tile(d, "trend").filters;
      },
      outcome: "refuses",
      because: "// the trend, on the right",
   },
];

describe("an edit changes the declaration it names, and nothing else", () => {
   for (const row of ROWS) {
      it(`${row.what} (${row.outcome})`, async () => {
         const next = structuredClone(await openDocument(PROBE));
         row.edit(next);
         const result = await spliceDashboardDocument(PROBE, next);

         if (row.outcome === "refuses") {
            if (!spliceFailed(result))
               throw new Error(
                  "expected a refusal; the writer wrote instead:\n" +
                     result.source,
               );
            expect(result.reason).toContain(row.because!);
            return;
         }
         if (spliceFailed(result))
            throw new Error(`expected a write; refused: ${result.reason}`);

         expect(await syntaxErrors(result.source)).toEqual([]);
         const reread = await readDashboardDocument(result.source);
         if (readFailed(reread)) throw new Error(reread.reason);
         expect(await whatMoved(PROBE, result.source)).toEqual({
            ...NOTHING_MOVED,
            ...row.moved,
         });
      });
   }
});
