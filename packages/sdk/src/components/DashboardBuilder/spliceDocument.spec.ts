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

describe("spliceDashboardDocument: what it refuses", () => {
   // Structural edits move declarations and their comment blocks, and no file
   // says whether a comment belongs to the tile, the row or the page.
   it("refuses to reorder tiles", async () => {
      const r = await splice(SOURCE, (d) => {
         d.tiles.reverse();
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("reordering");
   });

   it("refuses to remove a tile", async () => {
      const r = await splice(SOURCE, (d) => {
         d.tiles.pop();
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("reordering");
   });

   it("refuses to change the page's own settings", async () => {
      const r = await splice(SOURCE, (d) => {
         d.title = "Renamed";
      });
      expect(r.ok).toBe(false);
      if (spliceFailed(r)) expect(r.reason).toContain("presentation");
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
