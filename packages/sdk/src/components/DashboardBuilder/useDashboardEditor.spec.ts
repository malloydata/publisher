// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { openDocument } from "./testing/fixtures";
import { useDashboardEditor } from "./useDashboardEditor";

const SOURCE = `## artifact { title="Probe" tiles=["a -> by_cat", "a -> by_brand"] } dashboard { columns=12 }
import "../data_app.malloy"

source: a is scoped_orders extend {
  // Kept, because a splice never rewrites what it did not change.
  # colspan=6
  # label="By category"
  view: by_cat is by_category

  # colspan=6
  view: by_brand is by_brand_view
}`;

const editor = async (
   onSave?: (source: string) => Promise<void> | void,
   source = SOURCE,
) => {
   const document = await openDocument(source);
   return renderHook(() =>
      useDashboardEditor({ source, document, ...(onSave ? { onSave } : {}) }),
   );
};

describe("useDashboardEditor: editing", () => {
   it("applies a change to a copy, leaving the previous state intact", async () => {
      const view = await editor();
      const before = view.result.current.document;

      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].label = "Categories";
         });
      });

      expect(view.result.current.document.tiles[0].label).toBe("Categories");
      // The history entry it replaced is untouched, which is what makes undo a
      // move rather than a reconstruction.
      expect(before.tiles[0].label).toBe("By category");
   });

   it("is dirty only once something actually changed", async () => {
      const view = await editor();
      expect(view.result.current.dirty).toBe(false);

      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].colspan = 4;
         });
      });
      expect(view.result.current.dirty).toBe(true);
   });

   // Otherwise a control that sets a value to what it already was would push a
   // history entry, and the next undo would appear to do nothing.
   it("records no history for a change that changes nothing", async () => {
      const view = await editor();
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].label = "By category";
         });
      });
      expect(view.result.current.canUndo).toBe(false);
      expect(view.result.current.dirty).toBe(false);
   });
});

describe("useDashboardEditor: history", () => {
   it("undoes and redoes a change", async () => {
      const view = await editor();
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].colspan = 4;
         });
      });
      expect(view.result.current.canUndo).toBe(true);
      expect(view.result.current.canRedo).toBe(false);

      act(() => view.result.current.undo());
      expect(view.result.current.document.tiles[0].colspan).toBe(6);
      expect(view.result.current.canRedo).toBe(true);

      act(() => view.result.current.redo());
      expect(view.result.current.document.tiles[0].colspan).toBe(4);
   });

   it("walks back through several changes in order", async () => {
      const view = await editor();
      for (const colspan of [5, 4, 3]) {
         act(() => {
            view.result.current.update((d) => {
               d.tiles[0].colspan = colspan;
            });
         });
      }
      act(() => view.result.current.undo());
      expect(view.result.current.document.tiles[0].colspan).toBe(4);
      act(() => view.result.current.undo());
      expect(view.result.current.document.tiles[0].colspan).toBe(5);
      act(() => view.result.current.undo());
      expect(view.result.current.document.tiles[0].colspan).toBe(6);
      expect(view.result.current.canUndo).toBe(false);
   });

   // What every editor does, and what a reader expects: the future you undid
   // is gone once you take a different path.
   it("discards the redo tail when you edit after undoing", async () => {
      const view = await editor();
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].colspan = 4;
         });
      });
      act(() => view.result.current.undo());
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].colspan = 2;
         });
      });
      expect(view.result.current.canRedo).toBe(false);
      expect(view.result.current.document.tiles[0].colspan).toBe(2);
   });

   it("does nothing at the ends of the history", async () => {
      const view = await editor();
      act(() => view.result.current.undo());
      act(() => view.result.current.redo());
      expect(view.result.current.document.tiles[0].colspan).toBe(6);
   });
});

describe("useDashboardEditor: saving", () => {
   it("splices the change into the file and keeps its comments", async () => {
      let written: string | undefined;
      const view = await editor((source) => {
         written = source;
      });

      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].label = "Categories";
         });
      });
      await act(async () => {
         await view.result.current.save();
      });

      expect(written).toContain('# label="Categories"');
      expect(written).toContain(
         "  // Kept, because a splice never rewrites what it did not change.",
      );
      await waitFor(() => expect(view.result.current.dirty).toBe(false));
   });

   // The second save has to patch what is now on disk, not the text the session
   // opened with, or it would splice against a stale file.
   it("saves twice, the second against the first result", async () => {
      const written: string[] = [];
      const view = await editor((source) => {
         written.push(source);
      });

      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].label = "One";
         });
      });
      await act(async () => {
         await view.result.current.save();
      });
      act(() => {
         view.result.current.update((d) => {
            d.tiles[1].label = "Two";
         });
      });
      await act(async () => {
         await view.result.current.save();
      });

      expect(written).toHaveLength(2);
      expect(written[1]).toContain('# label="One"');
      expect(written[1]).toContain('# label="Two"');
   });

   // The rule the whole writer rests on: a refused write keeps the work.
   it("keeps the edit when the writer refuses", async () => {
      let called = false;
      const view = await editor(() => {
         called = true;
      });

      act(() => {
         view.result.current.update((d) => {
            // An import is not the builder's to change, so this is refused.
            // (Tiles, settings and the page's own givens are NOT: the writer
            // handles those, and the builder shows a diff for the structural ones.)
            d.imports.push({ kind: "all", from: "../more.malloy" });
         });
      });
      await act(async () => {
         expect((await view.result.current.save()).ok).toBe(false);
      });

      expect(called).toBe(false);
      expect(view.result.current.error).toContain("imports");
      // Still there, still dirty. The reader can undo or try something else.
      expect(view.result.current.document.imports).toHaveLength(2);
      expect(view.result.current.dirty).toBe(true);
   });

   it("stays dirty when storage rejects the write", async () => {
      const view = await editor(() => {
         throw new Error("disk full");
      });
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].colspan = 4;
         });
      });
      await act(async () => {
         expect((await view.result.current.save()).ok).toBe(false);
      });
      expect(view.result.current.error).toContain("disk full");
      expect(view.result.current.dirty).toBe(true);
   });

   it("clears the error once an edit is made", async () => {
      const view = await editor(() => {
         throw new Error("disk full");
      });
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].colspan = 4;
         });
      });
      await act(async () => {
         await view.result.current.save();
      });
      expect(view.result.current.error).toBeDefined();

      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].colspan = 3;
         });
      });
      expect(view.result.current.error).toBeUndefined();
   });
});

/**
 * `structural` decides whether the builder shows the author a diff before it
 * saves, so it has to mean what the WRITER means by a changed tile — not what
 * the grid means.
 */
describe("useDashboardEditor: structural", () => {
   it("is false for presentation and layout edits", async () => {
      const view = await editor();
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].label = "Categories";
            d.tiles[0].colspan = 4;
         });
      });
      expect(view.result.current.dirty).toBe(true);
      expect(view.result.current.structural).toBe(false);
   });

   it("is false for a reorder, which moves no declaration", async () => {
      const view = await editor();
      act(() => {
         view.result.current.update((d) => {
            d.tiles.reverse();
         });
      });
      expect(view.result.current.structural).toBe(false);
   });

   it("is true when a tile is removed", async () => {
      const view = await editor();
      act(() => {
         view.result.current.update((d) => {
            d.tiles.pop();
         });
      });
      expect(view.result.current.structural).toBe(true);
   });

   it("is true when a tile is redeclared, though the grid's key is unchanged", async () => {
      // The regression: `document.tileKey` is `source.name`, which this edit
      // leaves alone, so keying `structural` on it reported a save that
      // rewrites the declaration as a plain presentation change — and the
      // builder skipped the diff on exactly the edit that most needs one.
      const view = await editor();
      const before = view.result.current.document.tiles[0];
      act(() => {
         view.result.current.update((d) => {
            d.tiles[0].declaration = { kind: "inline" };
         });
      });
      const after = view.result.current.document.tiles[0];
      expect([after.source, after.name]).toEqual([before.source, before.name]);
      expect(view.result.current.structural).toBe(true);
   });
});
