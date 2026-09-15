// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import type { DashboardDocument } from "./document";
import { readDashboardDocument, readFailed } from "./readDocument";
import { spliceDashboardDocument, spliceFailed } from "./spliceDocument";

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

const open = async (source: string) => {
   const r = await readDashboardDocument(source);
   if (readFailed(r)) throw new Error(r.reason);
   return r.document;
};

const splice = async (source: string, edit: (d: DashboardDocument) => void) => {
   const next = structuredClone(await open(source));
   edit(next);
   return spliceDashboardDocument(source, next);
};

const spliced = async (
   source: string,
   edit: (d: DashboardDocument) => void,
): Promise<string> => {
   const r = await splice(source, edit);
   if (spliceFailed(r)) throw new Error(r.reason);
   return r.source;
};

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

describe("spliceDashboardDocument: what it writes", () => {
   it("adds a tag that was not there", async () => {
      const out = await spliced(SOURCE, (d) => {
         d.tiles[1].label = "By brand";
      });
      expect(out).toContain('  # label="By brand"\n  view: by_brand');
      expect(await open(out)).toMatchObject({
         tiles: [{ name: "by_cat" }, { name: "by_brand", label: "By brand" }],
      });
   });

   it("removes a tag that is no longer wanted", async () => {
      const out = await spliced(SOURCE, (d) => {
         delete d.tiles[0].break;
      });
      expect(out).not.toContain("# break");
      expect((await open(out)).tiles[0].break).toBeUndefined();
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
         const back = await open(r.source);
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
         const back = await open(r.source);
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

describe("spliceDashboardDocument: what it refuses", () => {
   // Adding or removing a tile inserts or deletes a declaration, which carries
   // the comment block above it, and no file says who that comment belongs to.
   it("refuses to remove a tile", async () => {
      const r = await splice(SOURCE, (d) => {
         d.tiles.pop();
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("Adding or removing");
   });

   it("refuses to change the page's own settings", async () => {
      const r = await splice(SOURCE, (d) => {
         d.title = "Renamed";
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("settings");
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
      });
   }
});
