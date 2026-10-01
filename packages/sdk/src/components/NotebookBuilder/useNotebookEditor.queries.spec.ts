// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import {
   notebookSourceRefused,
   readNotebookSource,
} from "./readNotebookSource";
import { notebookDocumentOf } from "./spliceNotebook";
import { useNotebookEditor } from "./useNotebookEditor";

const DEF = 'source: a is duckdb.sql("select 1 as x")';
const RUN = "run: a -> { select: x }";
const TEXT = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Intro.\n\n// Why.\n# bar_chart\n${RUN}\n`;

async function open(reachableSources: string[] | null = ["a"]) {
   const read = await readNotebookSource(TEXT);
   if (notebookSourceRefused(read)) throw new Error(read.refused);
   const saves: string[] = [];
   const view = renderHook(() =>
      useNotebookEditor({
         source: TEXT,
         document: notebookDocumentOf(read.source),
         onSave: (s) => void saves.push(s),
         reachableSources: reachableSources ?? undefined,
      }),
   );
   return { view, saves };
}

const addQuery = (view: Awaited<ReturnType<typeof open>>["view"]) =>
   act(() =>
      view.result.current.update((d) => {
         d.cells.push({
            id: "added-q",
            kind: "query",
            added: true,
            chart: "bar_chart",
            run: { source: "a", view: "v", caption: "New" },
         });
      }),
   );

describe("useNotebookEditor: query cells", () => {
   it("adds a query, saves it, and edits its chart against the saved file", async () => {
      const { view, saves } = await open();
      addQuery(view);
      expect(view.result.current.structural).toBe(true);
      expect(view.result.current.clearsHistory).toBe(false);
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(saves[0]).toContain('#" New\n# -line_chart');
      expect(saves[0]).toContain("run: a -> v\n");

      act(() =>
         view.result.current.update((d) => {
            d.cells[d.cells.length - 1].chart = "line_chart";
         }),
      );
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(saves[1]).toContain(
         "# -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz line_chart\nrun: a -> v\n",
      );
      expect(saves[1].match(/run: a -> v/g)).toHaveLength(1);
      // The added cell's run is ignored once it is in the file.
      act(() =>
         view.result.current.update((d) => {
            d.cells[d.cells.length - 1].run = { source: "zzz", view: "x" };
         }),
      );
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(saves[2]).toBe(saves[1]);
   });

   it("refuses an added query when the model's sources are not known", async () => {
      const { view } = await open(null);
      addQuery(view);
      await act(async () => {
         const outcome = await view.result.current.save();
         expect(outcome.ok).toBe(false);
      });
   });

   it("undoes an added query's removal after a save, but clears undo when a read query is removed", async () => {
      const { view, saves } = await open();
      addQuery(view);
      await act(async () => void (await view.result.current.save()));
      act(() =>
         view.result.current.update((d) => {
            d.cells.pop();
         }),
      );
      expect(view.result.current.clearsHistory).toBe(false);
      await act(async () => void (await view.result.current.save()));
      expect(view.result.current.canUndo).toBe(true);
      act(() => view.result.current.undo());
      await act(async () => void (await view.result.current.save()));
      expect(saves[saves.length - 1]).toContain("run: a -> v");

      act(() =>
         view.result.current.update((d) => {
            const at = d.cells.findIndex((c) => c.kind === "query" && !c.added);
            d.cells.splice(at, 1);
         }),
      );
      expect(view.result.current.clearsHistory).toBe(true);
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(view.result.current.canUndo).toBe(false);
      expect(view.result.current.canRedo).toBe(false);
      expect(view.result.current.clearsHistory).toBe(false);
      expect(saves[saves.length - 1]).not.toContain("bar_chart\n" + RUN);
      expect(saves[saves.length - 1]).not.toContain("// Why.");
   });

   it("lists the comments a removed query takes with it", async () => {
      const { view } = await open();
      act(() =>
         view.result.current.update((d) => {
            const at = d.cells.findIndex((c) => c.kind === "query");
            d.cells.splice(at, 1);
         }),
      );
      let comments: string[] = [];
      await act(async () => {
         comments = await view.result.current.removedComments();
      });
      expect(comments).toEqual(["// Why."]);
   });

   it("disables moving an added query above a definition", async () => {
      const { view } = await open();
      addQuery(view);
      const last = view.result.current.document.cells.length - 1;
      expect(view.result.current.canMove(last, 0)).toBe(false);
      expect(view.result.current.canMove(last, 1)).toBe(true);
      expect(view.result.current.canMove(last, 2)).toBe(true);
   });
});
