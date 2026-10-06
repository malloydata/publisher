// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import {
   useBuilderSession,
   type BuilderSessionOptions,
   type SessionEditor,
} from "./useBuilderSession";
import type { SaveOutcome } from "./useDocumentEditor";

type Doc = { n: number };

const makeEditor = (
   over: Partial<SessionEditor<Doc>> = {},
): SessionEditor<Doc> => ({
   document: { n: 1 },
   saved: { n: 0 },
   dirty: true,
   structural: false,
   canUndo: true,
   canRedo: false,
   undo: mock(() => {}),
   redo: mock(() => {}),
   save: mock(async (): Promise<SaveOutcome> => ({ ok: true })),
   canUndoSave: false,
   undoSave: mock(async (): Promise<SaveOutcome> => ({ ok: true })),
   ...over,
});

const mount = (
   over: Partial<BuilderSessionOptions<Doc>> = {},
   editor: SessionEditor<Doc> = makeEditor(),
) => {
   const saved = mock((_info: unknown) => {});
   const refused = mock((_reason: string) => {});
   const undone = mock((_info: unknown) => {});
   const undoRefused = mock((_reason: string) => {});
   const onSave = mock(async (_source: string) => {});
   const shortcuts = { escape: () => {} };
   const view = renderHook(
      (props: { editor: SessionEditor<Doc> }) =>
         useBuilderSession<Doc>({
            editor: props.editor,
            onSave,
            shortcuts,
            unit: { name: "tile", count: (document) => document.n },
            report: { size: 3, saved, refused, undone, undoRefused },
            ...over,
         }),
      { initialProps: { editor } },
   );
   return { ...view, editor, saved, refused, undone, undoRefused, onSave };
};

describe("useBuilderSession save", () => {
   it("writes at once and reports the size and structure it saved", async () => {
      const { result, saved, editor } = mount(
         {},
         makeEditor({ structural: false }),
      );
      await act(async () => {
         await result.current.save();
      });
      expect(editor.save).toHaveBeenCalledTimes(1);
      expect(saved).toHaveBeenCalledTimes(1);
      const info = saved.mock.calls[0][0] as {
         size: number;
         structural: boolean;
         durationMs: number;
      };
      expect(info.size).toBe(3);
      expect(info.structural).toBe(false);
      expect(typeof info.durationMs).toBe("number");
   });

   it("reports a refusal and not a save", async () => {
      const editor = makeEditor({
         save: mock(
            async (): Promise<SaveOutcome> => ({ ok: false, reason: "nope" }),
         ),
      });
      const { result, saved, refused } = mount({}, editor);
      await act(async () => {
         await result.current.save();
      });
      expect(refused).toHaveBeenCalledWith("nope");
      expect(saved).not.toHaveBeenCalled();
   });

   it("does nothing when clean, without a writer, or while saving", async () => {
      const clean = mount({}, makeEditor({ dirty: false }));
      await act(async () => {
         await clean.result.current.save();
      });
      expect(clean.editor.save).not.toHaveBeenCalled();

      const none = mount({ onSave: undefined });
      await act(async () => {
         await none.result.current.save();
      });
      expect(none.editor.save).not.toHaveBeenCalled();
      expect(none.result.current.toolbarProps.onSave).toBeUndefined();

      let release = () => {};
      const slow = makeEditor({
         save: mock(
            () =>
               new Promise<SaveOutcome>((resolve) => {
                  release = () => resolve({ ok: true });
               }),
         ),
      });
      const busy = mount({}, slow);
      let first: Promise<void> | void;
      act(() => {
         first = busy.result.current.save();
      });
      expect(busy.result.current.saving).toBe(true);
      await act(async () => {
         await busy.result.current.save();
      });
      expect(slow.save).toHaveBeenCalledTimes(1);
      await act(async () => {
         release();
         await first;
      });
      expect(busy.result.current.saving).toBe(false);
   });

   it("runs every entry point through prepare", async () => {
      const prepare = mock((run: () => Promise<void> | void) => run());
      const { result, editor } = mount({ prepare });
      await act(async () => {
         await result.current.save();
      });
      expect(prepare).toHaveBeenCalledTimes(1);
      expect(editor.save).toHaveBeenCalledTimes(1);
   });
});

describe("useBuilderSession reports", () => {
   it("reports dirty from the editor or the extra state, and clean on unmount", () => {
      const onDirtyChange = mock((_dirty: boolean) => {});
      const clean = makeEditor({ dirty: false });
      const view = mount({ onDirtyChange, extraDirty: true }, clean);
      expect(onDirtyChange).toHaveBeenLastCalledWith(true);
      expect(view.result.current.toolbarProps.dirty).toBe(true);
      view.unmount();
      expect(onDirtyChange).toHaveBeenLastCalledWith(false);
   });

   it("reports the document on mount and when it changes", () => {
      const onChange = mock((_doc: Doc) => {});
      const view = mount({ onChange });
      expect(onChange).toHaveBeenLastCalledWith({ n: 1 });
      view.rerender({ editor: makeEditor({ document: { n: 2 } }) });
      expect(onChange).toHaveBeenLastCalledWith({ n: 2 });
   });
});

describe("useBuilderSession undo save", () => {
   const LAST = {
      before: "a",
      after: "a,b",
      structural: true,
      clearsHistory: false,
   };
   const offering = (over: Partial<SessionEditor<Doc>> = {}) =>
      makeEditor({ dirty: false, canUndoSave: true, lastSave: LAST, ...over });

   it("undoes through the editor and reports it like a save", async () => {
      const { result, editor, undone, saved } = mount({}, offering());
      expect(result.current.canUndoSave).toBe(true);
      expect(result.current.lastSave).toEqual(LAST);
      await act(async () => {
         await result.current.undoSave();
      });
      expect(editor.undoSave).toHaveBeenCalledTimes(1);
      expect(saved).not.toHaveBeenCalled();
      const info = undone.mock.calls[0][0] as {
         size: number;
         structural: boolean;
         durationMs: number;
      };
      expect(info.size).toBe(3);
      expect(info.structural).toBe(true);
      expect(typeof info.durationMs).toBe("number");
   });

   it("reports a refused undo with its reason", async () => {
      const editor = offering({
         undoSave: mock(
            async (): Promise<SaveOutcome> => ({
               ok: false,
               reason: "changed since",
            }),
         ),
      });
      const { result, undone, undoRefused } = mount({}, editor);
      await act(async () => {
         await result.current.undoSave();
      });
      expect(undone).not.toHaveBeenCalled();
      expect(undoRefused).toHaveBeenCalledWith("changed since");
   });

   it("is saving while the undo writes, so Save and a second Undo do nothing", async () => {
      let finish!: (outcome: SaveOutcome) => void;
      const editor = offering({
         dirty: true,
         undoSave: mock(
            () => new Promise<SaveOutcome>((resolve) => (finish = resolve)),
         ),
      });
      const { result } = mount({}, editor);
      let undoing!: Promise<void> | void;
      act(() => {
         undoing = result.current.undoSave();
         void result.current.undoSave();
      });
      expect(result.current.saving).toBe(true);
      expect(result.current.canUndoSave).toBe(false);
      await act(async () => {
         await result.current.save();
      });
      expect(editor.save).not.toHaveBeenCalled();
      await act(async () => {
         finish({ ok: true });
         await undoing;
      });
      expect(editor.undoSave).toHaveBeenCalledTimes(1);
      expect(result.current.saving).toBe(false);
   });

   it("does nothing without a writer or an offer", async () => {
      const none = mount({}, makeEditor());
      await act(async () => {
         await none.result.current.undoSave();
      });
      expect(none.editor.undoSave).not.toHaveBeenCalled();
      const readOnly = mount({ onSave: undefined }, offering());
      expect(readOnly.result.current.canUndoSave).toBe(false);
      await act(async () => {
         await readOnly.result.current.undoSave();
      });
      expect(readOnly.editor.undoSave).not.toHaveBeenCalled();
   });

   it("tells the host whether a save can still be undone, and that it cannot once unmounted", () => {
      const onSaveNoticeChange = mock((_showing: boolean) => {});
      const view = mount({ onSaveNoticeChange }, makeEditor());
      expect(onSaveNoticeChange).toHaveBeenLastCalledWith(false);
      view.rerender({ editor: offering() });
      expect(onSaveNoticeChange).toHaveBeenLastCalledWith(true);
      view.unmount();
      expect(onSaveNoticeChange).toHaveBeenLastCalledWith(false);
   });

   it("withdraws the notice when the writer goes away while it stands", () => {
      const onSaveNoticeChange = mock((_showing: boolean) => {});
      const over: Partial<BuilderSessionOptions<Doc>> = { onSaveNoticeChange };
      const view = mount(over, offering());
      expect(onSaveNoticeChange).toHaveBeenLastCalledWith(true);
      over.onSave = undefined;
      view.rerender({ editor: offering() });
      expect(onSaveNoticeChange).toHaveBeenLastCalledWith(false);
   });
});
