// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import type { DashboardDocument } from "./document";
import { givenDeclarations } from "./malloyText";
import { blockAbove, readDashboardDocument, readFailed } from "./readDocument";
import { spliceDashboardDocument, spliceFailed } from "./spliceDocument";
import { openDocument, refused, splice, spliced } from "./testing/fixtures";

const REPO = path.resolve(import.meta.dir, "../../../../..");

const SOURCE = `##! experimental.givens

##" A narrative header.
## artifact { title="Probe" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  // Why this tile leads: revenue is the number people ask about first.
  # colspan=6
  # break
  # label="By category"
  view: by_cat is by_category

  # colspan=6
  view: by_brand is by_brand_view
}`;

describe("spliceDashboardDocument: what it preserves", () => {
   // The whole reason for splicing rather than regenerating. A generated file
   // could not carry this comment, and the previous design's answer was to
   // refuse to edit any file that had one.
   it("leaves a comment in the tag block exactly where it was", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain(
         "  // Why this tile leads: revenue is the number people ask about first.",
      );
      expect(out).toContain("  # colspan=4");
      expect(out).not.toContain(
         '# colspan=6\n  # break\n  # label="By category"\n  view: by_cat',
      );
   });

   // A render tag on the tile — `# big_value`, `# currency` — is the renderer's,
   // not the document's. Measured on the storefront overview before this was
   // pinned: unticking one filter on the KPI strip deleted its `# big_value`,
   // and the strip came back as a one-row table.
   it("keeps the tags it does not model when it rewrites the ones it does", async () => {
      const source = SOURCE.replace(
         '  # label="By category"\n',
         '  # label="By category"\n  # big_value\n  # currency\n',
      );
      const out = await spliced(source, (d) => {
         d.tiles[0].label = "Categories";
         d.tiles[0].filters = [{ field: "cat", given: "CATEGORY" }];
      });
      expect(out).toContain(
         '  # label="Categories"\n  # big_value\n  # currency\n',
      );
      expect(out).toContain(
         "view: by_cat is by_category + { where: cat ~ $CATEGORY }",
      );
   });

   // Byte-minimal: one property changed, one line different.
   it("changes only the line it had to", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles[0].label = "Categories";
      });
      const before = SOURCE.split("\n");
      const after = out.split("\n");
      const differing = after.filter((line, i) => line !== before[i]);
      expect(differing).toEqual(['  # label="Categories"']);
   });

   it("leaves the file untouched when nothing changed", async () => {
      const out = await spliced(SOURCE, () => {});
      expect(out).toBe(SOURCE);
   });
});

/**
 * Two edits that share a start offset: a tag line inserted above a declaration,
 * and a rewrite of that declaration. Applied in the wrong order the insertion
 * moves the text out from under the rewrite's end offset and the rewrite lands
 * inside what was just inserted, so the save is refused with nothing to tell
 * the person which of the two changes it could not make. In the browser this is
 * what "add a filter" hit: the width change was the tile's FIRST modelled tag.
 */
describe("spliceDashboardDocument: a new tag over a rewritten declaration", () => {
   const UNTAGGED = `##! experimental.givens
## artifact { title="Probe" tiles=["a -> by_cat"] }
import "../data_app.malloy"

source: a is scoped_orders extend {
  view: by_cat is by_category
}`;

   it("writes both when the tile had no tags at all", async () => {
      const out = await spliced(UNTAGGED, (d) => {
         d.tiles[0].colspan = 4;
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out).toContain(
         "  # colspan=4\n  view: by_cat is by_category + { where: category ~ $CATEGORY }",
      );
   });
});

/**
 * A trailing `// …` on the declaration line. The refinement goes at the END of
 * that line, so appended blind it lands inside the comment. Driven in a browser
 * against a real draft package, that is what made "add a filter" refuse on a
 * tile the builder could plainly write: the file was rewritten, Malloy saw no
 * binding, and the readback gate refused the save with no way to tell why.
 *
 * With a refinement already on the line it is worse than a refusal: the reader's
 * greedy match finds the binding inside the comment, so the gate passes a file
 * whose filter Malloy never applies.
 */
describe("spliceDashboardDocument: a comment at the end of the declaration", () => {
   const commented = (declaration: string) => `##! experimental.givens
## artifact { title="Probe" tiles=["a -> by_cat"] }
import "../data_app.malloy"

source: a is scoped_orders extend {
${declaration}
}`;

   const bind = (d: DashboardDocument) => {
      d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
   };

   it("puts the binding before the comment, not inside it", async () => {
      const out = await spliced(
         commented("  view: by_cat is by_category // the lead tile"),
         bind,
      );
      expect(out).toContain(
         "  view: by_cat is by_category + { where: category ~ $CATEGORY } // the lead tile",
      );
   });

   it("keeps an existing refinement ahead of the binding", async () => {
      const out = await spliced(
         commented("  view: by_cat is by_category + { limit: 5 } // top five"),
         bind,
      );
      expect(out).toContain(
         "  view: by_cat is by_category + { limit: 5, where: category ~ $CATEGORY } // top five",
      );
   });

   it("takes the binding off without disturbing the comment", async () => {
      const source = commented(
         "  view: by_cat is by_category + { where: category ~ $CATEGORY } // the lead tile",
      );
      const out = await spliced(source, (d) => {
         delete d.tiles[0].filters;
         d.tiles[0].colspan = 4;
      });
      expect(out).toContain("  view: by_cat is by_category // the lead tile");
   });

   // `//` inside a filter literal is text, not the start of a comment.
   it("does not mistake a slash pair inside a literal for a comment", async () => {
      const out = await spliced(
         commented("  view: by_cat is by_category + { where: path ~ 'a//b' }"),
         bind,
      );
      expect(out).toContain(
         "  view: by_cat is by_category + { where: path ~ 'a//b', where: category ~ $CATEGORY }",
      );
   });
});

describe("spliceDashboardDocument: what it writes", () => {
   it("adds a tag that was not there", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles[1].label = "By brand";
      });
      expect(out).toContain('  # label="By brand"\n  view: by_brand');
      expect(await openDocument(out)).toMatchObject({
         tiles: [{ name: "by_cat" }, { name: "by_brand", label: "By brand" }],
      });
   });

   it("removes a tag that is no longer wanted", async () => {
      const out = await spliced(SOURCE, (d) => {
         delete d.tiles[0].break;
      });
      expect(out).not.toContain("# break");
      expect((await openDocument(out)).tiles[0].break).toBeUndefined();
   });

   // The filter binding lives in the declaration, as a refinement.
   it("writes and clears a filter binding", async () => {
      const bound = await spliced(SOURCE, (d) => {
         d.tiles[0].filters = [{ field: "products.brand", given: "BRAND" }];
      });
      expect(bound).toContain(
         "view: by_cat is by_category + { where: products.brand ~ $BRAND }",
      );

      const cleared = await spliced(bound, (d) => {
         delete d.tiles[0].filters;
      });
      expect(cleared).toContain("view: by_cat is by_category\n");
      expect(cleared).not.toContain("where:");
   });
});

describe("spliceDashboardDocument: reordering", () => {
   // Order lives in the `tiles=[…]` array, not in where a view is declared, so
   // a reorder rewrites that array and moves nothing else.
   it("reorders by rewriting the artifact tag's tiles array", async () => {
      const r = await splice(SOURCE, (d) => {
         d.tiles.reverse();
      });
      expect(r.ok).toBe(true);
      if (!spliceFailed(r)) {
         expect(r.source).toContain('tiles=["a -> by_brand", "a -> by_cat"]');
         // The declarations stayed exactly where they were.
         expect(r.source.indexOf("view: by_cat")).toBeLessThan(
            r.source.indexOf("view: by_brand"),
         );
      }
   });

   it("leaves a tile's comment with the tile it was written above", async () => {
      const r = await splice(SOURCE, (d) => {
         d.tiles.reverse();
      });
      expect(r.ok).toBe(true);
      if (!spliceFailed(r)) {
         // The comment is the reason reordering used to be refused. It never
         // moves, because no declaration does.
         expect(r.source).toContain(
            "// Why this tile leads: revenue is the number people ask about first.\n  # colspan=6",
         );
      }
   });

   it("keeps every tile's own tags through a reorder", async () => {
      const r = await splice(SOURCE, (d) => {
         d.tiles.reverse();
      });
      expect(r.ok).toBe(true);
      if (!spliceFailed(r)) {
         const back = await openDocument(r.source);
         expect(back.tiles.map((t) => t.name)).toEqual(["by_brand", "by_cat"]);
         // The writer persists the flags the DOCUMENT carries and takes no
         // view of what they mean: this document still has `break` on
         // `by_cat`, so the file does too. Deciding that a row start stays
         // with the POSITION rather than the tile is the builder's rule
         // (`keepRowStructure`), applied before a document reaches here.
         expect(back.tiles.find((t) => t.name === "by_cat")?.break).toBe(true);
         expect(back.tiles.find((t) => t.name === "by_brand")?.break).toBe(
            undefined,
         );
      }
   });

   it("reorders and re-tags in one write", async () => {
      const r = await splice(SOURCE, (d) => {
         d.tiles.reverse();
         // Matched by identity, not position: this is the tile now FIRST.
         d.tiles[0].label = "Brands";
      });
      expect(r.ok).toBe(true);
      if (!spliceFailed(r)) {
         const back = await openDocument(r.source);
         expect(back.tiles.map((t) => t.name)).toEqual(["by_brand", "by_cat"]);
         expect(back.tiles[0].label).toBe("Brands");
         expect(back.tiles[1].label).toBe("By category");
      }
   });
});

describe("spliceDashboardDocument: bindings", () => {
   it("writes a binding with the comparison it was given", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles[0].filters = [
            { field: "created_at", given: "SINCE", op: ">=" },
         ];
      });
      expect(out).toContain(
         "view: by_cat is by_category + { where: created_at >= $SINCE }",
      );
   });

   // The refinement is not all ours. What the reader does not model stays,
   // ahead of the bindings, exactly as written.
   it("keeps an unmodelled clause in the refinement", async () => {
      const source = SOURCE.replace(
         "view: by_cat is by_category",
         "view: by_cat is by_category + { limit: 5, where: category ~ $CATEGORY }",
      );
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [
            { field: "category", given: "CATEGORY" },
            { field: "brand", given: "BRAND" },
         ];
      });
      expect(out).toContain(
         "view: by_cat is by_category + { limit: 5, where: category ~ $CATEGORY, where: brand ~ $BRAND }",
      );
      // And clearing the bindings leaves the clause that was never ours.
      const cleared = await spliced(source, (d) => {
         delete d.tiles[0].filters;
      });
      expect(cleared).toContain("view: by_cat is by_category + { limit: 5 }");
   });
});

/**
 * `canBind` used to exclude an inline tile outright, because a `+ { where: …
 * }` refinement written onto a query body would not read back. It does, once
 * the binding is written INSIDE the body as a depth-1 `where:` statement in
 * its first stage instead of a refinement after it — which is how the
 * bundled `tiled.malloy` fixture, and most real dashboards, actually write a
 * tile. These are that writer path.
 */
describe("spliceDashboardDocument: inline body bindings", () => {
   const MULTILINE = `## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
    group_by: category
    aggregate: n is count()
  }
}`;

   it("adds a binding as a new depth-1 where: line before the closing brace", async () => {
      const out = await spliced(MULTILINE, (d) => {
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out).toContain(
         "  view: kpis is {\n    group_by: category\n    aggregate: n is count()\n    where: category ~ $CATEGORY\n  }",
      );
   });

   it("rewrites an existing binding line in place", async () => {
      const source = MULTILINE.replace(
         "view: kpis is {\n    group_by: category",
         "view: kpis is {\n    where: category ~ $CATEGORY\n    group_by: category",
      );
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "brand_name", given: "CATEGORY" }];
      });
      expect(out).toContain(
         "  view: kpis is {\n    where: brand_name ~ $CATEGORY\n    group_by: category",
      );
      // Still one line, not a rewrite-in-place plus a stray append.
      expect(out.match(/where:/g)).toHaveLength(1);
   });

   it("drops a removed binding's line entirely, no blank line left behind", async () => {
      const source = MULTILINE.replace(
         "view: kpis is {\n    group_by: category",
         "view: kpis is {\n    where: category ~ $CATEGORY\n    group_by: category",
      );
      const out = await spliced(source, (d) => {
         delete d.tiles[0].filters;
      });
      expect(out).toContain(MULTILINE);
      expect(out).not.toContain("where:");
   });

   // A `nest:`'s own `where:` is depth 2, one level inside the nest's own
   // brace, so it is never in the set of lines this scan owns — it survives a
   // binding change to the OUTER tile exactly as written, and the new binding
   // goes after the nest's closing brace, not inside it.
   it("leaves a nest's own where: untouched and adds the new line after it", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
    group_by: category
    nest: by_month is {
      where: month ~ $MONTH
      aggregate: n is count()
    }
  }
}`;
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out).toContain("where: month ~ $MONTH");
      expect(out).toContain(
         "    nest: by_month is {\n      where: month ~ $MONTH\n      aggregate: n is count()\n    }\n    where: category ~ $CATEGORY\n  }",
      );
   });

   // The compound line is unmodelled Malloy — never in the set this writer
   // owns — so it survives a binding change exactly as written, and the new
   // binding is its own line, not folded into it.
   it("keeps a compound predicate untouched and adds the new binding beside it", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
    where: category ~ $CATEGORY and status = 'open'
    aggregate: n is count()
  }
}`;
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "region", given: "REGION" }];
      });
      expect(out).toContain("where: category ~ $CATEGORY and status = 'open'");
      expect(out).toContain(
         "    where: category ~ $CATEGORY and status = 'open'\n    aggregate: n is count()\n    where: region ~ $REGION\n  }",
      );
   });

   it("writes a one-line body's binding inline, before the closing brace", async () => {
      const oneLiner = `## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is { aggregate: n is count() }
}`;
      const out = await spliced(oneLiner, (d) => {
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out).toContain(
         "view: kpis is { aggregate: n is count(), where: category ~ $CATEGORY }",
      );
      // And unbinding restores the file exactly as it was.
      const restored = await spliced(out, (d) => {
         delete d.tiles[0].filters;
      });
      expect(restored).toBe(oneLiner);
   });

   // A multi-stage `->` pipeline has no single first stage to write the
   // binding into, and the reason names the shape, not the tile's kind.
   it("refuses a filter change on a multi-stage body", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is {
    group_by: category
    aggregate: n is count()
  } -> {
    where: n > 10
    select: category, n
  }
}`;
      expect(
         await refused(source, (d) => {
            d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
         }),
      ).toContain("multi-stage");
   });

   it("refuses a filter change on a compound { … } + { … } body", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis"] }
import "../m.malloy"

source: a is one extend {
  view: kpis is { aggregate: n is count() } + { limit: 5 }
}`;
      expect(
         await refused(source, (d) => {
            d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
         }),
      ).toContain("compound refinement");
   });

   // The source-level `where:` sits outside every view's extent. Changing an
   // unrelated tile's own binding must not touch it.
   it("never touches a source-level where: while binding a tile", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis", "a -> other"] }
import "../m.malloy"

source: a is one extend {
  where: brand_name ~ $BRAND

  view: kpis is { aggregate: n is count() }
  view: other is { aggregate: m is count() }
}`;
      const out = await spliced(source, (d) => {
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out).toContain("  where: brand_name ~ $BRAND\n");
      expect(out).toContain(
         "view: kpis is { aggregate: n is count(), where: category ~ $CATEGORY }",
      );
   });

   it("round-trips: bind, read back, unbind, read back to the original bytes", async () => {
      const bound = await spliced(MULTILINE, (d) => {
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      const reopened = await openDocument(bound);
      expect(reopened.tiles[0].filters).toEqual([
         { field: "category", given: "CATEGORY" },
      ]);
      const unbound = await spliced(bound, (d) => {
         delete d.tiles[0].filters;
      });
      expect(unbound).toBe(MULTILINE);
      const reread = await openDocument(unbound);
      expect(reread.tiles[0].filters).toBeUndefined();
   });
});

describe("spliceDashboardDocument: the dashboard's own givens", () => {
   // The convention: a filter the builder adds is a declaration in THIS file.
   // A file that had none gets the experiment switch too, since a `given:`
   // without it fails the package load.
   it("adds a first given after the imports, switching the experiment on", async () => {
      const source = `## artifact { title="T" tiles=["a -> x"] }
import { one, products } from "../m.malloy"

source: a is one extend {
  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         d.localGivens = [
            {
               name: "CATEGORY",
               type: "filter<string>",
               default: "f''",
               label: "Category",
               control: "select",
               suggest: { source: "products", dimension: "category" },
            },
         ];
         d.tiles[0].filters = [{ field: "category", given: "CATEGORY" }];
      });
      expect(out.startsWith("##! experimental.givens\n")).toBe(true);
      expect(out).toContain(
         `import { one, products } from "../m.malloy"

# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''
`,
      );
      expect(out).toContain("view: x is vx + { where: category ~ $CATEGORY }");
   });

   it("adds a given after the ones already declared", async () => {
      const source = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

# label="Category"
given: CATEGORY :: filter<string> is f''

source: a is one extend {
  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         d.localGivens = [
            ...(d.localGivens ?? []),
            {
               name: "SINCE",
               type: "date",
               default: "@2023-01-01",
               label: "Since",
            },
         ];
      });
      expect(out).toContain(`given: CATEGORY :: filter<string> is f''

# label="Since"
given: SINCE :: date is @2023-01-01
`);
      // Exactly one switch, not a second copy.
      expect(out.match(/##! experimental\.givens/g)).toHaveLength(1);
   });

   // Its tags are its control contract, so they go with it; a comment does not.
   it("removes a given with its tags and leaves the comment above", async () => {
      const source = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

// Why the page filters by category at all.
# label="Category" control=select
given: CATEGORY :: filter<string> is f''
# label="Since"
given: SINCE :: date is @2023-01-01

source: a is one extend {
  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         d.localGivens = (d.localGivens ?? []).filter(
            (g) => g.name !== "CATEGORY",
         );
      });
      expect(out).toContain(`// Why the page filters by category at all.
# label="Since"
given: SINCE :: date is @2023-01-01`);
      expect(out).not.toContain("CATEGORY");
   });

   it("removes a block's header when its last declaration goes", async () => {
      const source = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

given:
  # label="Local"
  LOCAL_X :: filter<string> is f'Jeans'

source: a is one extend {
  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         delete d.localGivens;
      });
      expect(out).not.toContain("given:");
      expect(out).not.toContain("LOCAL_X");
      expect(out).toContain(`import "../m.malloy"

source: a is one extend {`);
   });

   it("retags a given in place, keeping the declaration's own line", async () => {
      const source = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

# label="Category"
given: CATEGORY :: filter<string> is f''

source: a is one extend {
  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         d.localGivens![0].label = "Product category";
         d.localGivens![0].control = "multiselect";
      });
      expect(out).toContain(`# label="Product category" control=multiselect
given: CATEGORY :: filter<string> is f''`);
      expect(out).not.toContain(`label="Category"`);
   });
});

describe("spliceDashboardDocument: drills", () => {
   const WITH_DIMENSION = `## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  // The dimension a view groups by, declared here so it can be tagged here.
  # label="Category"
  dimension: cat is products.category

  # drill { to=self given=BRAND }
  dimension: brand is products.brand

  view: x is vx
}`;

   it("puts a drill tag on a dimension this file declares, under its other tags", async () => {
      const out = await spliced(WITH_DIMENSION, (d) => {
         d.drills = [
            ...(d.drills ?? []),
            {
               source: "a",
               name: "cat",
               expression: "products.category",
               to: ["self", "regions"],
               given: "CATEGORY",
            },
         ];
      });
      expect(out).toContain(
         `  // The dimension a view groups by, declared here so it can be tagged here.
  # label="Category"
  # drill { to=["self", "regions"] given=CATEGORY }
  dimension: cat is products.category`,
      );
      // And it reads back as written.
      const back = await splice(out, () => {});
      if (spliceFailed(back)) throw new Error(back.reason);
      const reread = await readDashboardDocument(out);
      if (readFailed(reread)) throw new Error(reread.reason);
      expect(reread.document.drills?.map((d) => d.name)).toEqual([
         "cat",
         "brand",
      ]);
   });

   it("rewrites and removes a drill tag, leaving the dimension", async () => {
      const retargeted = await spliced(WITH_DIMENSION, (d) => {
         d.drills = [{ ...(d.drills ?? [])[0], to: ["brands"] }];
      });
      expect(retargeted).toContain(
         "  # drill { to=brands given=BRAND }\n  dimension: brand is products.brand",
      );
      const removed = await spliced(WITH_DIMENSION, (d) => {
         delete d.drills;
      });
      expect(removed).not.toContain("# drill");
      expect(removed).toContain("  dimension: brand is products.brand");
   });

   it("refuses a drill on a dimension the model declares", async () => {
      const r = await splice(WITH_DIMENSION, (d) => {
         d.drills = [
            ...(d.drills ?? []),
            { source: "a", name: "category", expression: "", to: ["self"] },
         ];
      });
      expect(spliceFailed(r) && r.reason).toContain("not a dimension");
   });
});

describe("spliceDashboardDocument: the page's own settings", () => {
   it("retitles the page on its one-line tag", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.title = "Renamed";
      });
      expect(out).toContain(
         '## artifact { title="Renamed" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }',
      );
   });

   it("changes the grid width, and can take it away", async () => {
      const wider = await spliced(SOURCE, (d) => {
         d.columns = 24;
      });
      expect(wider).toContain("] } dashboard { columns=24 }");
      const flowed = await spliced(SOURCE, (d) => {
         delete d.columns;
      });
      expect(flowed).toContain('tiles=["a -> by_cat", "a -> by_brand"] }\n');
      expect(flowed).not.toContain("dashboard {");
   });

   it("adds and removes autorun and starting values inside the tag", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.autorun = false;
         d.startingGivens = { CATEGORY: "Jeans" };
      });
      expect(out).toContain(
         '## artifact { title="Probe" tiles=["a -> by_cat", "a -> by_brand"] autorun=false givens { CATEGORY="Jeans" } } dashboard { columns=12 }',
      );
      const back = await spliced(out, (d) => {
         delete d.autorun;
         delete d.startingGivens;
      });
      expect(back).toContain(
         '## artifact { title="Probe" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }',
      );
   });

   // The description is the run of `##"` lines; a blank paragraph is a bare one.
   it("rewrites the description in place, paragraphs and all", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.description = "First line.\n\nSecond paragraph, **bold**.";
      });
      expect(out).toContain(
         '##! experimental.givens\n\n##" First line.\n##"\n##" Second paragraph, **bold**.\n## artifact {',
      );
      const gone = await spliced(SOURCE, (d) => {
         delete d.description;
      });
      expect(gone).toContain("##! experimental.givens\n\n## artifact {");
   });

   it("adds a description to a page that had none", async () => {
      const source = `## artifact { title="T" tiles=["a -> x"] }\nimport "../m.malloy"\n\nsource: a is one extend {\n  view: x is vx\n}`;
      const out = await spliced(source, (d) => {
         d.description = "Now with prose.";
      });
      expect(
         out.startsWith('##" Now with prose.\n## artifact { title="T"'),
      ).toBe(true);
   });

   it("retitles and reorders in one write, on the same line", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.title = "Renamed";
         d.tiles.reverse();
      });
      expect(out).toContain(
         '## artifact { title="Renamed" tiles=["a -> by_brand", "a -> by_cat"] } dashboard { columns=12 }',
      );
   });
});

describe("spliceDashboardDocument: tiles added and removed", () => {
   // A removed tile takes its declaration and its `#` tags. The `//` comment
   // above it stays: the file cannot say whose it was, and a comment left is a
   // smaller wrong than one destroyed — the builder shows this diff first.
   it("removes a tile, its tags and its entry, and leaves the comment", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles.splice(0, 1);
      });
      expect(out).toContain('tiles=["a -> by_brand"]');
      expect(out).not.toContain("view: by_cat");
      expect(out).not.toContain('# label="By category"');
      expect(out).not.toContain("# break");
      // The comment stays where it was, and the blank line after the removed
      // tile stays with it: closing that gap would hand the comment to the
      // next tile, which is the guess the writer refuses to make.
      expect(out).toContain(
         "  // Why this tile leads: revenue is the number people ask about first.\n\n  # colspan=6\n  view: by_brand is by_brand_view",
      );
   });

   it("removes an inline tile with its whole body", async () => {
      const source = `## artifact { title="T" tiles=["a -> kpis", "a -> x"] }
import { one } from "../m.malloy"

source: a is one extend {
  # colspan=12
  view: kpis is {
    aggregate:
      total_sales
      order_count
  }

  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         d.tiles.splice(0, 1);
      });
      expect(out).toBe(`## artifact { title="T" tiles=["a -> x"] }
import { one } from "../m.malloy"

source: a is one extend {
  view: x is vx
}`);
   });

   it("adds a tile inside the extension of the source it reads", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles.push({
            name: "by_state_tile",
            source: "a",
            declaration: { kind: "reference", from: "sales_by_state" },
            colspan: 6,
            label: "By state",
            filters: [{ field: "category", given: "CATEGORY" }],
         });
      });
      expect(out).toContain(
         'tiles=["a -> by_cat", "a -> by_brand", "a -> by_state_tile"]',
      );
      expect(out).toContain(`  # colspan=6
  view: by_brand is by_brand_view

  # colspan=6
  # label="By state"
  view: by_state_tile is sales_by_state + { where: category ~ $CATEGORY }
}`);
   });

   // A source the file imports BY NAME can take a new extension; the builder
   // never adds an import, so that is the only kind that can.
   it("adds a tile on a named import by declaring a new extension", async () => {
      const source = `## artifact { title="T" tiles=["a -> x"] }
import { one, products } from "../m.malloy"

source: a is one extend {
  view: x is vx
}`;
      const out = await spliced(source, (d) => {
         d.sources.push({ name: "products_tiles", base: "products" });
         d.tiles.push({
            name: "by_brand_tile",
            source: "products_tiles",
            declaration: { kind: "reference", from: "by_brand" },
            colspan: 4,
         });
      });
      expect(out)
         .toBe(`## artifact { title="T" tiles=["a -> x", "products_tiles -> by_brand_tile"] }
import { one, products } from "../m.malloy"

source: a is one extend {
  view: x is vx
}

source: products_tiles is products extend {
  # colspan=4
  view: by_brand_tile is by_brand
}
`);
   });

   it("refuses a tile on a source the file does not import by name", async () => {
      const r = await splice(SOURCE, (d) => {
         d.sources.push({ name: "orders_tiles", base: "order_items" });
         d.tiles.push({
            name: "t",
            source: "orders_tiles",
            declaration: { kind: "reference", from: "by_category" },
         });
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("not imported by name");
   });
});

describe("spliceDashboardDocument: what it refuses", () => {
   // Adding or removing a tile inserts or deletes a declaration, which carries
   // the comment block above it, and no file says who that comment belongs to.
   it("refuses to change the page's imports", async () => {
      const r = await splice(SOURCE, (d) => {
         d.imports.push({ kind: "all", from: "../more.malloy" });
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("imports");
   });

   // An inherited tile's tags live on the model's view, and the builder never
   // writes model files. Saying so beats writing a tag onto the wrong object.
   it("refuses to restyle a tile declared on its source", async () => {
      const source = `## artifact { title="T" tiles=["orders -> by_brand"] }\nimport { orders } from '../orders.malloy'`;
      const r = await splice(source, (d) => {
         d.tiles[0].colspan = 6;
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("declared on its source");
   });

   it("refuses a new tile that is not a reference to a view", async () => {
      expect(
         await refused(SOURCE, (d) => {
            d.tiles.push({
               name: "adhoc",
               source: "a",
               declaration: { kind: "inline" },
            });
         }),
      ).toContain("inline query");
   });

   it("refuses to change or remove an extension", async () => {
      expect(
         await refused(SOURCE, (d) => {
            d.sources[0].base = "other_orders";
         }),
      ).toContain("cannot be changed or removed");
      expect(
         await refused(SOURCE, (d) => {
            d.sources = [];
         }),
      ).toContain("cannot be changed or removed");
   });

   it("refuses a new extension that no tile reads", async () => {
      expect(
         await refused(SOURCE, (d) => {
            d.sources.push({ name: "b", base: "scoped_orders" });
         }),
      ).toContain("needs a tile on it");
   });

   it("refuses a drill that names no destination", async () => {
      const withDimension = SOURCE.replace(
         "source: a is scoped_orders extend {",
         "source: a is scoped_orders extend {\n  dimension: cat is products.category",
      );
      expect(
         await refused(withDimension, (d) => {
            d.drills = [
               {
                  source: "a",
                  name: "cat",
                  expression: "products.category",
                  to: [],
               },
            ];
         }),
      ).toContain("names no destination");
   });

   it("refuses to edit a file that will not open", async () => {
      const r = await spliceDashboardDocument("not malloy at all", {
         title: "x",
         imports: [],
         sources: [],
         tiles: [],
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("will not open");
   });
});

/**
 * The round-trip gate, exercised against the real thing. Every composite
 * dashboard in the repository is opened, has one property changed, is spliced,
 * and is read back — which is the property the writer promises and the reason a
 * commented file no longer has to be read-only.
 */
/** The tag keys the writer models, across tiles, givens and drills. */
const MODELLED_TAG =
   /^#\s*(colspan|break|borderless|label|subtitle|description|control|suggest|range_min|range_max|drill)\b/;

/**
 * Every `#` line the document does not model, keyed by the declaration it sits
 * above. Comparing this before and after an edit catches what a file-wide
 * count cannot: a tag that survived but moved onto a different declaration,
 * which in Malloy is a change of meaning rather than a change of layout.
 */
function unmodelledTagsByDeclaration(text: string): Record<string, string[]> {
   const lines = text.split("\n");
   const out: Record<string, string[]> = {};
   const record = (key: string, line: number) => {
      const tags = blockAbove(lines, line)
         .tags.map((t) => t.text)
         .filter((t) => !MODELLED_TAG.test(t));
      if (tags.length > 0) out[key] = tags;
   };
   // Keyed by the enclosing source as well as the name: `view:x` repeats
   // across sources, and a colliding key would let a tag move from one to
   // another without the comparison noticing.
   let scope = "";
   lines.forEach((line, i) => {
      const m =
         /^\s*(source|view|dimension|measure):\s*([A-Za-z_][A-Za-z0-9_]*)\s+is\b/.exec(
            line,
         );
      if (!m) return;
      if (m[1] === "source") scope = m[2];
      record(`${scope}/${m[1]}:${m[2]}`, i);
   });
   for (const [name, at] of givenDeclarations(lines))
      record(`given:${name}`, at.line);
   return out;
}

describe("every composite dashboard survives an edit", () => {
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
   walk(path.join(REPO, "examples"));
   walk(path.join(REPO, "packages/server/tests/fixtures"));

   const editable = found.filter((f) => {
      const text = fs.readFileSync(f, "utf8");
      return (
         /##\s*artifact[\s\S]*tiles\s*=/.test(text) &&
         !f.includes("dashboards-lint")
      );
   });

   for (const file of editable) {
      const name = path.relative(REPO, file);
      it(`opens ${name}, and writes it back byte for byte when nothing changed`, async () => {
         const source = fs.readFileSync(file, "utf8");
         const doc = await openDocument(source);
         const result = await spliceDashboardDocument(source, doc);
         if (spliceFailed(result)) throw new Error(result.reason);
         expect(result.source).toBe(source);
      });

      it(`round-trips a colspan change in ${name}`, async () => {
         const source = fs.readFileSync(file, "utf8");
         const doc = await readDashboardDocument(source);
         if (readFailed(doc)) throw new Error(doc.reason);
         const target = doc.document.tiles.findIndex(
            (t) => t.declaration.kind !== "inherited",
         );
         // A file whose tiles are all declared on their sources has nothing
         // here to patch, which is a real shape rather than a gap.
         if (target < 0) return;

         const next = structuredClone(doc.document);
         next.tiles[target].colspan = 3;
         const result = await spliceDashboardDocument(source, next);
         if (spliceFailed(result)) throw new Error(result.reason);

         const reread = await readDashboardDocument(result.source);
         if (readFailed(reread)) throw new Error(reread.reason);
         expect(reread.document.tiles[target].colspan).toBe(3);
         // Everything the file said that the builder does not model is still
         // there: the comment count is the cheapest proof.
         const comments = (text: string) =>
            text.split("\n").filter((l) => l.trim().startsWith("//")).length;
         expect(comments(result.source)).toBe(comments(source));
         // And every `#` tag the builder does not model is still there, on the
         // same declaration. A file-wide count cannot see a tag that moved to
         // the declaration below, which is how one silently changes meaning.
         expect(unmodelledTagsByDeclaration(result.source)).toEqual(
            unmodelledTagsByDeclaration(source),
         );
      });

      it(`retags a control in ${name} and leaves every other tag where it was`, async () => {
         const source = fs.readFileSync(file, "utf8");
         const doc = await readDashboardDocument(source);
         if (readFailed(doc)) throw new Error(doc.reason);
         const given = doc.document.localGivens?.[0];
         // Most dashboards declare no givens of their own; that is a real
         // shape, not a gap.
         if (!given) return;

         const next = structuredClone(doc.document);
         next.localGivens![0].label = `${given.label ?? given.name} (edited)`;
         const result = await spliceDashboardDocument(source, next);
         if (spliceFailed(result)) throw new Error(result.reason);
         expect(unmodelledTagsByDeclaration(result.source)).toEqual(
            unmodelledTagsByDeclaration(source),
         );
      });
   }
});

/**
 * A given carries two kinds of `#` line: the control contract the builder
 * writes, and annotations routed elsewhere -- `#(secure)` marks a given as an
 * access control, and a host may add its own. Only the first kind is the
 * writer's. The projection never holds the second, so the gate cannot notice
 * it lost one, which is why these assert on the text.
 */
describe("spliceDashboardDocument: a given's annotations it does not own", () => {
   const FILE = (givens: string) => `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

${givens}

source: a is one extend {
  view: x is vx
}`;

   const SECURE_FIRST = FILE(`#(secure)
# label="Category"
given: CATEGORY :: filter<string> is f''`);

   it("keeps a routed marker above the control line, still on the declaration", async () => {
      const result = await spliced(SECURE_FIRST, (d) => {
         d.localGivens![0].label = "Product category";
      });
      expect(result).toContain(
         `#(secure)\n# label="Product category"\ngiven: CATEGORY :: filter<string> is f''`,
      );
   });

   it("rewrites only the control line", async () => {
      const result = await spliced(SECURE_FIRST, (d) => {
         d.localGivens![0].label = "Product category";
      });
      const changed = result
         .split("\n")
         .filter((line, i) => line !== SECURE_FIRST.split("\n")[i]);
      expect(changed).toEqual([`# label="Product category"`]);
   });

   it("keeps a routed marker written below the control line", async () => {
      const source = FILE(`# label="Category"
#(secure)
given: CATEGORY :: filter<string> is f''`);
      const result = await spliced(source, (d) => {
         d.localGivens![0].label = "Product category";
      });
      // The rebuilt contract lands immediately above the declaration, so the
      // marker ends up above it -- still one contiguous block, and Malloy
      // reads a declaration's annotations without regard to their order.
      expect(result).toContain(
         `#(secure)\n# label="Product category"\ngiven: CATEGORY :: filter<string> is f''`,
      );
      expect(result).not.toContain(`label="Category"`);
   });

   it("keeps a keyed tag it does not model", async () => {
      const source = FILE(`# audit_scope=finance
# label="Category"
given: CATEGORY :: filter<string> is f''`);
      const result = await spliced(source, (d) => {
         d.localGivens![0].control = "multiselect";
      });
      expect(result).toContain(
         `# audit_scope=finance\n# label="Category" control=multiselect\ngiven: CATEGORY`,
      );
   });

   it("keeps the marker when the contract spans two lines under a comment", async () => {
      const source = FILE(`// Why this is scoped: one category at a time.
#(secure)
# description="Narrow to one category"
# label="Category" control=select
given: CATEGORY :: filter<string> is f''`);
      const result = await spliced(source, (d) => {
         d.localGivens![0].label = "Product category";
      });
      expect(result).toContain(
         `// Why this is scoped: one category at a time.\n#(secure)\n` +
            `# label="Product category" description="Narrow to one category" control=select\n` +
            `given: CATEGORY :: filter<string> is f''`,
      );
   });

   it("keeps the marker through a redeclaration", async () => {
      const result = await spliced(SECURE_FIRST, (d) => {
         d.localGivens![0].default = "f'Jeans'";
      });
      expect(result).toContain(
         `#(secure)\n# label="Category"\ngiven: CATEGORY :: filter<string> is f'Jeans'`,
      );
   });

   it("keeps the marker on a declaration inside a given: block", async () => {
      const source = FILE(`given:
  #(secure)
  # label="Local"
  LOCAL_X :: filter<string> is f'Jeans'`);
      const result = await spliced(source, (d) => {
         d.localGivens![0].label = "Local X";
      });
      expect(result).toContain(
         `  #(secure)\n  # label="Local X"\n  LOCAL_X :: filter<string> is f'Jeans'`,
      );
   });

   /**
    * The exception, and the reason the filter is on the retag path only: a `#`
    * line whose declaration goes does not lapse, it attaches to whatever is
    * declared next. Leaving `#(secure)` behind would silently mark a
    * different given as an access control.
    */
   it("takes a routed marker with the declaration it annotates", async () => {
      const source = FILE(`#(secure)
# label="Category"
given: CATEGORY :: filter<string> is f''

# label="Since"
given: SINCE :: date is @2023-01-01`);
      const result = await spliced(source, (d) => {
         d.localGivens = d.localGivens!.filter((g) => g.name !== "CATEGORY");
      });
      expect(result).not.toContain("#(secure)");
      expect(result).toContain(
         `import "../m.malloy"\n\n# label="Since"\ngiven: SINCE :: date is @2023-01-01`,
      );
      const reread = await openDocument(result);
      expect(reread.localGivens?.map((g) => g.name)).toEqual(["SINCE"]);
   });
});

/**
 * Two ways a save can fail with nothing written, and they want different
 * answers: nothing was recognized at all, or something was written and it read
 * back as a different dashboard. Reported identically, the person who hits one
 * cannot say which -- which is how an add-filter refusal went a round trip
 * before anyone knew whether a planner had even run.
 */
describe("spliceDashboardDocument: an ask no planner could place", () => {
   // A `drill` sharing a line with a `label` is not the `# drill` line
   // `planDrills` looks for, so taking the drill off plans no edit at all.
   const UNPLACEABLE = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

source: a is one extend {
  # label="Category" drill { to=self given=BRAND }
  dimension: cat is category

  view: x is vx
}`;

   it("says nothing was recognized when no planner placed an edit", async () => {
      const reason = await refused(UNPLACEABLE, (d) => {
         delete d.drills;
      });
      expect(reason).toContain("No part of this edit was recognized");
   });

   // The same unplaceable ask, stacked on a title change that does plan an
   // edit: now something IS written, and it is the readback that refuses.
   it("says the readback differed when an edit was written", async () => {
      const reason = await refused(UNPLACEABLE, (d) => {
         d.title = "Renamed";
         delete d.drills;
      });
      expect(reason).toContain("did not read back as what was asked for");
   });
});

/**
 * A label is free text a person types, and it is written into a double-quoted
 * tag value. Unescaped, a quote or a trailing backslash closed the value early
 * and the rest of the line parsed as tag syntax -- which the gate caught, so
 * the symptom was not a corrupt file but a document that could never be saved
 * again. The title and a starting given's value already escape; these are the
 * remaining tag values the writer composes.
 */
describe("spliceDashboardDocument: labels are free text", () => {
   const FILE = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

# label="Cat"
given: CATEGORY :: filter<string> is f''

source: a is one extend {
  # label="X"
  view: x is vx
}`;

   it("keeps a quote in a tile label, and reads it back", async () => {
      const label = 'Revenue (the "good" kind)';
      const result = await spliced(FILE, (d) => {
         d.tiles[0].label = label;
      });
      const reread = await openDocument(result);
      expect(reread.tiles[0].label).toBe(label);
   });

   it("keeps a trailing backslash in a given's label", async () => {
      const label = "Category\\";
      const result = await spliced(FILE, (d) => {
         d.localGivens![0].label = label;
      });
      const reread = await openDocument(result);
      expect(reread.localGivens?.[0].label).toBe(label);
   });

   it("keeps a quote in a given's description", async () => {
      const description = 'Only the "current" quarter';
      const result = await spliced(FILE, (d) => {
         d.localGivens![0].description = description;
      });
      const reread = await openDocument(result);
      expect(reread.localGivens?.[0].description).toBe(description);
   });

   it("keeps a quote in a tile subtitle", async () => {
      const subtitle = 'by "region"';
      const result = await spliced(FILE, (d) => {
         d.tiles[0].subtitle = subtitle;
      });
      const reread = await openDocument(result);
      expect(reread.tiles[0].subtitle).toBe(subtitle);
   });
});

/**
 * Two behaviours this writer has that are worth pinning rather than
 * discovering: what the no-edit gate treats as the same document, and the one
 * place an unmodelled tag is still lost.
 */
describe("spliceDashboardDocument: the shapes of no change", () => {
   const FILE = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

# label="Cat" audit_scope=finance
given: CATEGORY :: filter<string> is f''

source: a is one extend {
  # label="X"
  view: x is vx
}`;

   it("reads an omitted collection and an empty one as the same document", async () => {
      // A host that normalises its shape asks for no change, and must not be
      // told its save did not produce what it asked for.
      for (const materialise of [
         (d: DashboardDocument) => {
            d.drills = [];
         },
         (d: DashboardDocument) => {
            d.localGivens = d.localGivens ?? [];
            d.tiles[0].filters = [];
         },
      ]) {
         expect(await spliced(FILE, materialise)).toBe(FILE);
      }
   });

   /**
    * KNOWN, and the same rule the tile path has had since `MODELLED_TAG_KEYS`:
    * ownership is decided by the FIRST key on a line, and the line is replaced
    * whole. An unmodelled key sharing a line with a modelled one goes with it.
    *
    * A routed annotation cannot be caught this way -- `#(secure)` has no key
    * for `tagKey` to match, so it is never the first key on a shared line --
    * which is why this is a wart rather than a hole in what the fix is for.
    */
   it("loses an unmodelled key that shares a line with a modelled one", async () => {
      const result = await spliced(FILE, (d) => {
         d.localGivens![0].label = "Cat2";
      });
      expect(result).toContain(`# label="Cat2"`);
      expect(result).not.toContain("audit_scope");
   });

   it("keeps a routed marker that shares its block with a mixed line", async () => {
      const source = FILE.replace(
         `# label="Cat" audit_scope=finance`,
         `#(secure)
# label="Cat" audit_scope=finance`,
      );
      const result = await spliced(source, (d) => {
         d.localGivens![0].label = "Cat2";
      });
      expect(result).toContain(
         `#(secure)\n# label="Cat2"\ngiven: CATEGORY :: filter<string> is f''`,
      );
   });
});

/**
 * A view name is unique within a source, not within a file. Two sources on one
 * dashboard may each declare the same view, and the writer has to patch the
 * one belonging to the tile it is editing.
 */
describe("spliceDashboardDocument: two sources, one view name", () => {
   const SHARED = `##! experimental.givens

## artifact { title="Shared" tiles=["a -> by_month", "b -> by_month"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  # colspan=6
  view: by_month is by_month_view
}

source: b is scoped_orders extend {
  # colspan=6
  view: by_month is by_month_view
}`;

   it("retags the second source's view without touching the first's", async () => {
      const out = await spliced(SHARED, (d) => {
         const tile = d.tiles.find((t) => t.source === "b");
         if (!tile) throw new Error("no tile on b");
         tile.label = "B by month";
      });
      // The label landed inside `source: b`, and `source: a` is byte-identical
      // to how it went in. Before the lookup was scoped to the source, the
      // file-wide search found `a`'s declaration first and retagged a tile the
      // author never touched.
      const a = out.slice(out.indexOf("source: a"), out.indexOf("source: b"));
      const b = out.slice(out.indexOf("source: b"));
      expect(a).not.toContain("B by month");
      expect(b).toContain('# label="B by month"');
      expect(a).toBe(
         SHARED.slice(SHARED.indexOf("source: a"), SHARED.indexOf("source: b")),
      );
   });

   it("edits the first source's view when that is the tile", async () => {
      const out = await spliced(SHARED, (d) => {
         const tile = d.tiles.find((t) => t.source === "a");
         if (!tile) throw new Error("no tile on a");
         tile.label = "A by month";
      });
      const a = out.slice(out.indexOf("source: a"), out.indexOf("source: b"));
      const b = out.slice(out.indexOf("source: b"));
      expect(a).toContain('# label="A by month"');
      expect(b).not.toContain("A by month");
   });
});

/**
 * A source whose block opens on a LATER line than its `source:` line, because
 * the base is a multi-line `duckdb.sql("""…""")` literal. Before
 * `declarationExtent` replaced the line-bound `declarationEnd`, every one of
 * these four call sites read this source's extent as the `source:` line
 * itself — a search that never leaves that one line, an anchor inside the SQL
 * text, and an insertion point BEFORE the source rather than after its close.
 */
describe("spliceDashboardDocument: a source whose block opens after multi-line SQL", () => {
   const MULTILINE = `## artifact { title="T" tiles=["regional -> by_region", "regional -> by_month"] }
import { other_source } from "../data_app.malloy"

source: regional is duckdb.sql("""
  select region, sum(amount) as total from orders group by 1
""") extend {
  view: by_region is region_view

  view: by_month is month_view
}`;

   // viewDeclarationLine: retagging a tile requires finding its `view:` line
   // WITHIN the source's own extent, which used to be just the `source:` line.
   it("finds and retags a view declared inside the source, not just its own line", async () => {
      const out = await spliced(MULTILINE, (d) => {
         d.tiles[0].colspan = 6;
      });
      expect(out).toContain("  # colspan=6\n  view: by_region is region_view");
   });

   // planRemovedTiles: the LAST view before the extend's own closing brace.
   // Nothing of `by_month`'s own declaration ever opens a block, so the scan
   // has to recognize the source's `}` as out of scope rather than deleting it
   // along with the view — over-deletion that would leave the source's
   // `extend {` unclosed.
   it("removes the last view before the extend's closing brace without touching it", async () => {
      const out = await spliced(MULTILINE, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "by_month");
      });
      expect(out).toContain('tiles=["regional -> by_region"]');
      expect(out).not.toContain("by_month");
      // The closing brace is exactly one, and it is still the extend's own:
      // over-deletion would either take it or leave the block unclosed.
      expect(out.match(/^}/gm)).toHaveLength(1);
      expect(out).toContain(
         'select region, sum(amount) as total from orders group by 1\n""") extend {\n  view: by_region is region_view',
      );
   });

   // planAddedTiles's `close`: a new view on THIS source is inserted before
   // its own closing brace, which is AFTER the SQL literal — never before the
   // `source:` line, where the old bug anchored every insertion.
   it("adds a tile inside the extension, after the SQL literal", async () => {
      const out = await spliced(MULTILINE, (d) => {
         d.tiles.push({
            name: "by_year",
            source: "regional",
            declaration: { kind: "reference", from: "year_view" },
         });
      });
      expect(out).toContain(`  view: by_month is month_view

  view: by_year is year_view
}`);
      // Never inside the literal, and never ahead of the `source:` line.
      expect(out.indexOf("view: by_year")).toBeGreaterThan(
         out.indexOf('""") extend'),
      );
   });

   // planAddedTiles's `lastExtensionEnd`: anchoring a brand NEW extension after
   // the LAST one in the file has to know where this one actually ends.
   it("anchors a new extension after this source's real close, not its source line", async () => {
      const out = await spliced(MULTILINE, (d) => {
         d.sources.push({ name: "other_tiles", base: "other_source" });
         d.tiles.push({
            name: "by_year",
            source: "other_tiles",
            declaration: { kind: "reference", from: "year_view" },
         });
      });
      const sourceEnd = out.indexOf('""") extend {\n');
      const newExtension = out.indexOf("source: other_tiles");
      expect(newExtension).toBeGreaterThan(sourceEnd);
      expect(out).toContain(`source: other_tiles is other_source extend {
  view: by_year is year_view
}`);
   });

   // A brace inside the `"""` span is the one case the round-trip gate cannot
   // catch: a spliced-in `view:` would read back as belonging to the source
   // above it either way. Refused outright rather than risked.
   it("refuses to add a tile when the SQL literal itself holds a brace", async () => {
      const source = MULTILINE.replace(
         "select region, sum(amount) as total from orders group by 1",
         "select {'region': 'West'} as region",
      );
      const r = await splice(source, (d) => {
         d.tiles.push({
            name: "by_year",
            source: "regional",
            declaration: { kind: "reference", from: "year_view" },
         });
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) {
         expect(r.reason).toContain("regional");
         expect(r.reason).toContain('brace inside a """ string');
         expect(r.reason).not.toContain("undefined");
      }
   });
});

/**
 * Both brace scanners in malloyText.ts used to count a `{` inside a trailing
 * `//` comment as structure. `kpis`'s own extent ran through `other`, an
 * unrelated view, out to the enclosing source's closing brace — so a filter
 * bound to `kpis` returned `ok: true` and wrote the `where:` refinement after
 * `other`, at the end of the source block, instead of inside `kpis`. The
 * round-trip gate cannot catch this: the written text still reads back as a
 * `view:` under the right source either way.
 */
describe("spliceDashboardDocument: a trailing comment holding an unbalanced brace", () => {
   const SOURCE = `## artifact { title="T" tiles=["a -> kpis", "a -> other"] }
import "../m.malloy"

source: a is scoped_orders extend {
  view: kpis is {
    group_by: cat // the { brace here is unbalanced
    aggregate: n is count()
  }

  view: other is { aggregate: m is count() }
}`;

   it("binds inside kpis's own body and never touches the unrelated view", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles[0].filters = [{ field: "cat", given: "CATEGORY" }];
      });
      expect(out).toContain(
         "  view: kpis is {\n    group_by: cat // the { brace here is unbalanced\n    aggregate: n is count()\n    where: cat ~ $CATEGORY\n  }",
      );
      expect(out).toContain("  view: other is { aggregate: m is count() }\n}");
      expect(out).not.toContain(
         "  view: other is { aggregate: m is count() }\n    where:",
      );
   });
});
