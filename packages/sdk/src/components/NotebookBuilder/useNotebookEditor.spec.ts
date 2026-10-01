// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import {
   notebookSourceRefused,
   readNotebookSource,
} from "./readNotebookSource";
import { notebookDocumentOf, type NotebookDocument } from "./spliceNotebook";
import { takePlacement, useNotebookEditor } from "./useNotebookEditor";

const DEF = 'source: a is duckdb.sql("select 1 as x")';
const RUN = "run: a -> { select: x }";
const TEXT = `## artifact { kind=notebook }\n##(markdown) Intro.\n\n${DEF}\n\n##(markdown) Middle.\n\n${RUN}\n`;

async function docOf(text: string): Promise<NotebookDocument> {
   const read = await readNotebookSource(text);
   if (notebookSourceRefused(read)) throw new Error(read.refused);
   return notebookDocumentOf(read.source);
}

const move = (doc: NotebookDocument, from: number, to: number) => {
   const [cell] = doc.cells.splice(from, 1);
   doc.cells.splice(to, 0, cell);
};

async function open() {
   const document = await docOf(TEXT);
   const saves: string[] = [];
   const view = renderHook(() =>
      useNotebookEditor({
         source: TEXT,
         document,
         onSave: (s) => void saves.push(s),
      }),
   );
   return { view, saves };
}

describe("useNotebookEditor", () => {
   it("is structural only when a cell is added or removed", async () => {
      const { view } = await open();
      act(() => view.result.current.update((d) => move(d, 0, 2)));
      expect(view.result.current.structural).toBe(false);
      act(() =>
         view.result.current.update((d) => {
            d.cells.splice(0, 0, {
               id: "added-1",
               kind: "markdown",
               markdown: "New.",
               added: true,
            });
         }),
      );
      expect(view.result.current.structural).toBe(true);
   });

   // Ids are read indices, so without rebasing the second save would place cells by the first file's order.
   it("saves again after a reorder, against the file the first save wrote", async () => {
      const { view, saves } = await open();
      act(() => view.result.current.update((d) => move(d, 2, 3)));
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(saves[0]).toContain("##(markdown) Intro.\n\n" + DEF);
      expect(saves[0].indexOf(RUN)).toBeLessThan(
         saves[0].indexOf("##(markdown) Middle."),
      );

      act(() =>
         view.result.current.update((d) => {
            const intro = d.cells.find((c) => c.markdown === "Intro.");
            if (intro) intro.markdown = "Intro, edited.";
         }),
      );
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(saves[1]).toBe(saves[0].replace("Intro.", "Intro, edited."));
   });

   it("lets a query back above a definition it was opened above, after a save moved it below", async () => {
      const text = `## artifact { kind=notebook }\n${RUN.replace("a ->", "duckdb.sql('select 1 as x') ->")}\n\n${DEF}\n`;
      const document = await docOf(text);
      const view = renderHook(() =>
         useNotebookEditor({ source: text, document, onSave: () => {} }),
      );
      act(() => view.result.current.update((d) => move(d, 0, 1)));
      expect(view.result.current.canMove(1, 0)).toBe(true);
      await act(async () => {
         await view.result.current.save();
      });
      // The saved file has it below, but it compiled above the definition when opened, so it does not read it.
      expect(view.result.current.canMove(1, 0)).toBe(true);
   });

   it("saves an undo past a save that moved a query below a definition", async () => {
      const text = `## artifact { kind=notebook }\n${RUN.replace("a ->", "duckdb.sql('select 1 as x') ->")}\n\n${DEF}\n\n##(markdown) After.\n`;
      const document = await docOf(text);
      const saves: string[] = [];
      const view = renderHook(() =>
         useNotebookEditor({
            source: text,
            document,
            onSave: (s) => void saves.push(s),
         }),
      );
      act(() => view.result.current.update((d) => move(d, 0, 1)));
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      act(() => view.result.current.undo());
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(saves[1]).toBe(text);
   });

   it("still refuses a query above a definition it was opened below", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Middle.\n\n${RUN}\n`;
      const document = await docOf(text);
      const view = renderHook(() =>
         useNotebookEditor({ source: text, document, onSave: () => {} }),
      );
      act(() => view.result.current.update((d) => move(d, 2, 1)));
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(view.result.current.canMove(1, 0)).toBe(false);
   });
});

describe("useNotebookEditor: undoing past a saved removal", () => {
   const TWO = `## artifact { kind=notebook }\n// about A\n##(markdown) A.\n\n// about B\n##(markdown) B.\n\n${DEF}\n`;

   async function openTwo(text = TWO) {
      const document = await docOf(text);
      const saves: string[] = [];
      const view = renderHook(() =>
         useNotebookEditor({
            source: text,
            document,
            onSave: (s) => void saves.push(s),
         }),
      );
      const save = async () => {
         let outcome: unknown;
         await act(async () => {
            outcome = await view.result.current.save();
         });
         return outcome;
      };
      const remove = (markdown: string) =>
         act(() =>
            view.result.current.update((d) => {
               d.cells = d.cells.filter((c) => c.markdown !== markdown);
            }),
         );
      return { view, saves, save, remove };
   }

   it("saves the restored cell back in", async () => {
      const { view, saves, save, remove } = await openTwo();
      remove("A.");
      expect(await save()).toEqual({ ok: true });
      expect(saves[0]).not.toContain("A.");
      act(() => view.result.current.undo());
      expect(await save()).toEqual({ ok: true });
      expect(saves[1]).toContain("##(markdown) A.\n");
      expect(saves[1]).not.toContain("// about A");
      expect(saves[1].indexOf("A.")).toBeLessThan(saves[1].indexOf("B."));
   });

   it("saves every cell back after removing them all, saving, and undoing", async () => {
      // An all-text file saved empty has an empty placement, which is still a save that happened.
      const { view, saves, save } = await openTwo(
         "## artifact { kind=notebook }\n##(markdown) A.\n\n##(markdown) B.\n",
      );
      act(() =>
         view.result.current.update((d) => {
            d.cells = [];
         }),
      );
      expect(await save()).toEqual({ ok: true });
      expect(saves[0]).not.toContain("A.");
      act(() => view.result.current.undo());
      expect(await save()).toEqual({ ok: true });
      expect(saves[1]).toContain("##(markdown) A.");
      expect(saves[1]).toContain("##(markdown) B.");
   });

   it("removes the right cell, and its own comment, after a removal is undone", async () => {
      const { view, saves, save, remove } = await openTwo();
      remove("A.");
      await save();
      act(() => view.result.current.undo());
      remove("B.");
      let comments: string[] = [];
      await act(async () => {
         comments = await view.result.current.removedComments();
      });
      expect(comments).toEqual(["// about B"]);
      expect(await save()).toEqual({ ok: true });
      expect(saves[1]).toContain("##(markdown) A.\n");
      expect(saves[1]).not.toContain("B.");
      expect(saves[1]).not.toContain("// about B");
   });
});

describe("takePlacement", () => {
   it("refuses, before anything is written, a file no splice produced", () => {
      const pending = new Map([["written", new Map([["0", "0"]])]]);
      expect(() => takePlacement(pending, "other")).toThrow(
         "cannot tell which cells",
      );
      expect(takePlacement(pending, "written").get("0")).toBe("0");
   });
});
