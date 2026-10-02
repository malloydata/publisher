// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { type SpliceResult } from "./spliceDocument";
import { type SaveContext, useDocumentEditor } from "./useDocumentEditor";

interface Doc {
   items: string[];
}

const splice = async (source: string, doc: Doc): Promise<SpliceResult> =>
   doc.items.includes("bad")
      ? { ok: false, reason: "refused" }
      : { ok: true, source: doc.items.join(",") || source };

const structural = (saved: Doc, doc: Doc) =>
   saved.items.length !== doc.items.length;

const open = (
   extra: {
      onSave?: (s: string, context: SaveContext<Doc>) => Promise<void> | void;
   } = {},
) =>
   renderHook(() =>
      useDocumentEditor<Doc>({
         source: "a",
         document: { items: ["a"] },
         splice,
         structural,
         ...extra,
      }),
   );

describe("useDocumentEditor: clearing history on save", () => {
   it("keeps an edit typed while the write was in flight", async () => {
      let finish!: () => void;
      const view = renderHook(() =>
         useDocumentEditor<Doc>({
            source: "a",
            document: { items: ["a"] },
            splice,
            clearsHistory: () => true,
            onSave: () => new Promise<void>((resolve) => (finish = resolve)),
         }),
      );
      act(() => view.result.current.update((d) => void d.items.push("b")));
      let saving!: Promise<unknown>;
      act(() => {
         saving = view.result.current.save();
      });
      await act(async () => {
         await Promise.resolve();
      });
      act(() => view.result.current.update((d) => void d.items.push("typed")));
      await act(async () => {
         finish();
         await saving;
      });
      expect(view.result.current.document.items).toEqual(["a", "b", "typed"]);
      expect(view.result.current.canUndo).toBe(false);
      expect(view.result.current.dirty).toBe(true);
   });
});

describe("useDocumentEditor", () => {
   it("reports structural only through the supplied comparison", () => {
      const { result } = open();
      act(() => result.current.update((d) => void (d.items[0] = "b")));
      expect(result.current.dirty).toBe(true);
      expect(result.current.structural).toBe(false);
      act(() => result.current.update((d) => void d.items.push("c")));
      expect(result.current.structural).toBe(true);
   });

   it("is never structural without a comparison", () => {
      const { result } = renderHook(() =>
         useDocumentEditor<Doc>({
            source: "a",
            document: { items: ["a"] },
            splice,
         }),
      );
      act(() => result.current.update((d) => void d.items.push("c")));
      expect(result.current.structural).toBe(false);
   });

   it("surfaces a splice refusal and keeps the document", async () => {
      const saves: string[] = [];
      const { result } = open({ onSave: (s) => void saves.push(s) });
      act(() => result.current.update((d) => void d.items.push("bad")));
      let outcome: unknown;
      await act(async () => {
         outcome = await result.current.save();
      });
      expect(outcome).toEqual({ ok: false, reason: "refused" });
      expect(result.current.error).toBe("refused");
      expect(result.current.dirty).toBe(true);
      expect(saves).toEqual([]);
   });

   it("reports a splice that throws as a refusal, not an unhandled rejection", async () => {
      const saves: string[] = [];
      const { result } = renderHook(() =>
         useDocumentEditor<Doc>({
            source: "a",
            document: { items: ["a"] },
            splice: async () => {
               throw new Error("parser fell over");
            },
            onSave: (s) => void saves.push(s),
         }),
      );
      act(() => result.current.update((d) => void d.items.push("c")));
      let saved: unknown;
      let shown: unknown;
      await act(async () => {
         saved = await result.current.save();
         shown = await result.current.preview();
      });
      const reason = "Could not build the file: parser fell over";
      expect(saved).toEqual({ ok: false, reason });
      expect(shown).toEqual({ ok: false, reason });
      expect(result.current.error).toBe(reason);
      expect(result.current.dirty).toBe(true);
      expect(saves).toEqual([]);
   });

   it("previews through the supplied splice, writing nothing", async () => {
      const saves: string[] = [];
      const { result } = open({ onSave: (s) => void saves.push(s) });
      act(() => result.current.update((d) => void d.items.push("c")));
      let shown: unknown;
      await act(async () => {
         shown = await result.current.preview();
      });
      expect(shown).toEqual({ ok: true, source: "a,c" });
      act(() => result.current.update((d) => void d.items.push("bad")));
      await act(async () => {
         shown = await result.current.preview();
      });
      expect(shown).toEqual({ ok: false, reason: "refused" });
      expect(saves).toEqual([]);
      expect(result.current.dirty).toBe(true);
   });

   it("saves the spliced source and clears dirty", async () => {
      const saves: string[] = [];
      const { result } = open({ onSave: (s) => void saves.push(s) });
      act(() => result.current.update((d) => void d.items.push("c")));
      await act(async () => {
         await result.current.save();
      });
      expect(saves).toEqual(["a,c"]);
      expect(result.current.dirty).toBe(false);
      expect(result.current.source).toBe("a,c");
   });
});

describe("useDocumentEditor: a second save while one is in flight", () => {
   it("writes once and refuses the second", async () => {
      const writes: string[] = [];
      let finish!: () => void;
      const view = open({
         onSave: (source: string) => {
            writes.push(source);
            return new Promise<void>((resolve) => (finish = resolve));
         },
      });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      const save = view.result.current.save;
      let first!: Promise<unknown>;
      let second!: Promise<unknown>;
      act(() => {
         first = save();
         second = save();
      });
      expect(await second).toEqual({
         ok: false,
         reason: "A save is still being written.",
      });
      await act(async () => {
         while (writes.length === 0) await Promise.resolve();
         finish();
         await first;
      });
      expect(writes).toEqual(["a,b"]);
   });
});

describe("useDocumentEditor: undoing a save", () => {
   type Write = { source: string } & SaveContext<Doc>;
   const writer = () => {
      const writes: Write[] = [];
      const onSave = (source: string, context: SaveContext<Doc>) =>
         void writes.push({ source, ...context });
      return { writes, onSave };
   };
   const saveIt = (view: ReturnType<typeof open>) =>
      act(async () => {
         await view.result.current.save();
      });

   it("undoing a save that replaced a different file writes that file back, not the text the editor read", async () => {
      const { writes, onSave } = writer();
      const view = renderHook(() =>
         useDocumentEditor<Doc>({
            source: "D",
            replaces: "P",
            document: { items: ["a"] },
            splice,
            structural,
            onSave,
         }),
      );
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      expect(view.result.current.lastSave?.before).toBe("P");
      await act(async () => {
         expect(await view.result.current.undoSave()).toEqual({ ok: true });
      });
      expect(writes.map((w) => [w.source, w.purpose])).toEqual([
         ["a,b", "save"],
         ["P", "undo"],
      ]);
      expect(view.result.current.source).toBe("D");
      // A save after the undo still replaces the file the draft was opened over.
      await saveIt(view);
      expect(view.result.current.lastSave?.before).toBe("P");
   });

   it("offers to undo a save, and undoing writes the file back and restores the editor to just before Save", async () => {
      const { writes, onSave } = writer();
      const view = open({ onSave });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      act(() => view.result.current.update((d) => void d.items.push("c")));
      expect(view.result.current.canUndoSave).toBe(false);
      await saveIt(view);
      expect(view.result.current.canUndoSave).toBe(true);
      expect(view.result.current.lastSave).toEqual({
         before: "a",
         after: "a,b,c",
         structural: true,
         clearsHistory: false,
      });

      await act(async () => {
         expect(await view.result.current.undoSave()).toEqual({ ok: true });
      });
      expect(writes.map((w) => [w.source, w.purpose])).toEqual([
         ["a,b,c", "save"],
         ["a", "undo"],
      ]);
      expect(writes[0].document).toEqual({ items: ["a", "b", "c"] });
      // The undo write carries the document the restored file holds.
      expect(writes[1].document).toEqual({ items: ["a"] });
      const editor = view.result.current;
      expect(editor.source).toBe("a");
      expect(editor.saved).toEqual({ items: ["a"] });
      expect(editor.document.items).toEqual(["a", "b", "c"]);
      expect(editor.dirty).toBe(true);
      expect(editor.canUndoSave).toBe(false);
      expect(editor.lastSave).toBeUndefined();
      act(() => view.result.current.undo());
      expect(view.result.current.document.items).toEqual(["a", "b"]);
   });

   it("restores the undo stack a history-clearing save emptied", async () => {
      const { onSave } = writer();
      const view = renderHook(() =>
         useDocumentEditor<Doc>({
            source: "a",
            document: { items: ["a"] },
            splice,
            clearsHistory: () => true,
            onSave,
         }),
      );
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await act(async () => {
         await view.result.current.save();
      });
      expect(view.result.current.canUndo).toBe(false);
      expect(view.result.current.lastSave?.clearsHistory).toBe(true);
      await act(async () => {
         await view.result.current.undoSave();
      });
      expect(view.result.current.canUndo).toBe(true);
      act(() => view.result.current.undo());
      expect(view.result.current.document.items).toEqual(["a"]);
      expect(view.result.current.dirty).toBe(false);
   });

   it("withdraws the offer on the first edit after the save", async () => {
      const { writes, onSave } = writer();
      const view = open({ onSave });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      act(() => view.result.current.update((d) => void d.items.push("c")));
      expect(view.result.current.canUndoSave).toBe(false);
      expect(view.result.current.lastSave).toBeUndefined();
      await act(async () => {
         expect((await view.result.current.undoSave()).ok).toBe(false);
      });
      expect(writes).toHaveLength(1);
   });

   it("keeps the offer through an edit that changes nothing", async () => {
      const { onSave } = writer();
      const view = open({ onSave });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      act(() => view.result.current.update((d) => void (d.items[0] = "a")));
      expect(view.result.current.canUndoSave).toBe(true);
   });

   it("withdraws the offer on toolbar undo, and redo does not bring it back", async () => {
      const { onSave } = writer();
      const view = open({ onSave });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      act(() => view.result.current.undo());
      expect(view.result.current.canUndoSave).toBe(false);
      act(() => view.result.current.redo());
      expect(view.result.current.canUndoSave).toBe(false);
   });

   it("withdraws the offer on toolbar redo", async () => {
      const { onSave } = writer();
      const view = open({ onSave });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      act(() => view.result.current.update((d) => void d.items.push("c")));
      act(() => view.result.current.undo());
      await saveIt(view);
      expect(view.result.current.canUndoSave).toBe(true);
      act(() => view.result.current.redo());
      expect(view.result.current.canUndoSave).toBe(false);
   });

   it("replaces the offer with a later save's, whose undo goes back only that far", async () => {
      const { writes, onSave } = writer();
      const view = open({ onSave });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      act(() => view.result.current.update((d) => void d.items.push("c")));
      await saveIt(view);
      expect(view.result.current.lastSave?.before).toBe("a,b");
      await act(async () => {
         await view.result.current.undoSave();
      });
      expect(writes.at(-1)?.source).toBe("a,b");
      expect(view.result.current.saved).toEqual({ items: ["a", "b"] });
      expect(view.result.current.document.items).toEqual(["a", "b", "c"]);
   });

   it("makes no offer when an edit was typed while the write was in flight", async () => {
      let finish!: () => void;
      const view = open({
         onSave: () => new Promise<void>((resolve) => (finish = resolve)),
      });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      let saving!: Promise<unknown>;
      act(() => {
         saving = view.result.current.save();
      });
      await act(async () => {
         await Promise.resolve();
      });
      act(() => view.result.current.update((d) => void d.items.push("typed")));
      await act(async () => {
         finish();
         await saving;
      });
      expect(view.result.current.document.items).toEqual(["a", "b", "typed"]);
      expect(view.result.current.canUndoSave).toBe(false);
   });

   it("refuses to undo while a write is in flight", async () => {
      const writes: string[] = [];
      let finish!: () => void;
      const view = open({
         onSave: (source: string) => {
            writes.push(source);
            return new Promise<void>((resolve) => (finish = resolve));
         },
      });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      let saving!: Promise<unknown>;
      act(() => {
         saving = view.result.current.save();
      });
      await act(async () => {
         await Promise.resolve();
      });
      expect(view.result.current.canUndoSave).toBe(false);
      await act(async () => {
         finish();
         await saving;
      });
      let undoing!: Promise<unknown>;
      act(() => {
         undoing = view.result.current.undoSave();
      });
      expect(view.result.current.canUndoSave).toBe(false);
      await act(async () => {
         expect(await view.result.current.undoSave()).toEqual({
            ok: false,
            reason: "A save is still being written.",
         });
      });
      await act(async () => {
         finish();
         await undoing;
      });
      expect(writes).toEqual(["a,b", "a"]);
   });

   it("keeps the offer and the saved state when the undo write is refused", async () => {
      let refuse = false;
      const view = open({
         onSave: () => {
            if (refuse) throw new Error("the file changed since you opened it");
         },
      });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      refuse = true;
      await act(async () => {
         expect((await view.result.current.undoSave()).ok).toBe(false);
      });
      const editor = view.result.current;
      expect(editor.error).toContain("the file changed since you opened it");
      expect(editor.source).toBe("a,b");
      expect(editor.dirty).toBe(false);
      expect(editor.canUndoSave).toBe(true);
   });

   it("keeps an edit typed while the undo was writing, unsaved against the restored file", async () => {
      let finish: (() => void) | undefined;
      const view = open({
         onSave: (_s, context) =>
            context.purpose === "undo"
               ? new Promise<void>((resolve) => (finish = resolve))
               : undefined,
      });
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      let undoing!: Promise<unknown>;
      act(() => {
         undoing = view.result.current.undoSave();
      });
      act(() => view.result.current.update((d) => void d.items.push("typed")));
      await act(async () => {
         finish?.();
         await undoing;
      });
      expect(view.result.current.document.items).toEqual(["a", "b", "typed"]);
      expect(view.result.current.source).toBe("a");
      expect(view.result.current.dirty).toBe(true);
   });

   it("never offers an undo without a writer", async () => {
      const view = open();
      act(() => view.result.current.update((d) => void d.items.push("b")));
      await saveIt(view);
      expect(view.result.current.canUndoSave).toBe(false);
   });
});

describe("useDocumentEditor: opening unsaved", () => {
   const conversion = () => {
      const writes: Array<[string, string]> = [];
      const view = renderHook(() =>
         useDocumentEditor<Doc>({
            source: "legacy",
            document: { items: ["a"] },
            splice: async (_source, doc) => ({
               ok: true,
               source: doc.items.join(","),
            }),
            opensDirty: true,
            onSave: (source, context) =>
               void writes.push([source, context.purpose]),
         }),
      );
      return { view, writes };
   };

   it("is dirty before any edit, yet reports no edits", () => {
      const { view } = conversion();
      expect(view.result.current.dirty).toBe(true);
      expect(view.result.current.edited).toBe(false);
      expect(view.result.current.pendingOpen).toBe(true);
      act(() => view.result.current.update((d) => void d.items.push("b")));
      expect(view.result.current.edited).toBe(true);
   });

   it("saves the untouched document, and is clean once written", async () => {
      const { view, writes } = conversion();
      await act(async () => {
         expect(await view.result.current.save()).toEqual({ ok: true });
      });
      expect(writes).toEqual([["a", "save"]]);
      expect(view.result.current.dirty).toBe(false);
      expect(view.result.current.pendingOpen).toBe(false);
   });

   it("returns to unsaved when the save is undone, and writes the file back", async () => {
      const { view, writes } = conversion();
      await act(async () => {
         await view.result.current.save();
      });
      await act(async () => {
         await view.result.current.undoSave();
      });
      expect(writes).toEqual([
         ["a", "save"],
         ["legacy", "undo"],
      ]);
      expect(view.result.current.source).toBe("legacy");
      expect(view.result.current.pendingOpen).toBe(true);
      expect(view.result.current.dirty).toBe(true);
      expect(view.result.current.edited).toBe(false);
   });
});
