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
type Review = { after: string };

const makeEditor = (
   over: Partial<SessionEditor<Doc>> = {},
): SessionEditor<Doc> => ({
   document: { n: 1 },
   dirty: true,
   structural: false,
   canUndo: true,
   canRedo: false,
   undo: mock(() => {}),
   redo: mock(() => {}),
   save: mock(async (): Promise<SaveOutcome> => ({ ok: true })),
   ...over,
});

const mount = (
   over: Partial<BuilderSessionOptions<Doc, Review>> = {},
   editor: SessionEditor<Doc> = makeEditor(),
) => {
   const saved = mock((_info: unknown) => {});
   const refused = mock((_reason: string) => {});
   const onSave = mock(async (_source: string) => {});
   const shortcuts = { escape: () => {} };
   const view = renderHook(
      (props: { editor: SessionEditor<Doc> }) =>
         useBuilderSession<Doc, Review>({
            editor: props.editor,
            onSave,
            shortcuts,
            report: { size: 3, saved, refused },
            ...over,
         }),
      { initialProps: { editor } },
   );
   return { ...view, editor, saved, refused, onSave };
};

const press = (key: string) =>
   window.dispatchEvent(
      new KeyboardEvent("keydown", {
         key,
         metaKey: true,
         ctrlKey: true,
         bubbles: true,
      }),
   );

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

describe("useBuilderSession review", () => {
   const reviewing = (editor: SessionEditor<Doc>) =>
      mount(
         {
            review: () =>
               Promise.resolve({ ok: true as const, review: { after: "x" } }),
         },
         editor,
      );

   it("holds a reviewed save until it is confirmed", async () => {
      const { result, editor } = reviewing(makeEditor({ structural: true }));
      await act(async () => {
         await result.current.save();
      });
      expect(result.current.pendingSave).toEqual({ after: "x" });
      expect(editor.save).not.toHaveBeenCalled();
      await act(async () => {
         await result.current.confirmSave();
      });
      expect(editor.save).toHaveBeenCalledTimes(1);
      expect(result.current.pendingSave).toBeUndefined();
   });

   it("drops the review without saving", async () => {
      const { result, editor } = reviewing(makeEditor());
      await act(async () => {
         await result.current.save();
      });
      act(() => result.current.dismissReview());
      expect(result.current.pendingSave).toBeUndefined();
      expect(editor.save).not.toHaveBeenCalled();
   });

   it("saves straight away when the review cannot be built, so the refusal surfaces", async () => {
      const { result, editor } = mount({
         review: () => Promise.resolve({ ok: false as const }),
      });
      await act(async () => {
         await result.current.save();
      });
      expect(editor.save).toHaveBeenCalledTimes(1);
      expect(result.current.pendingSave).toBeUndefined();
   });

   it("pauses undo while a review is open", async () => {
      const { result, editor } = reviewing(makeEditor());
      press("z");
      expect(editor.undo).toHaveBeenCalledTimes(1);
      await act(async () => {
         await result.current.save();
      });
      press("z");
      expect(editor.undo).toHaveBeenCalledTimes(1);
      act(() => result.current.dismissReview());
      press("z");
      expect(editor.undo).toHaveBeenCalledTimes(2);
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

describe("useBuilderSession exit", () => {
   it("offers Done only when the host can leave, and leaves at once when clean", () => {
      expect(mount().result.current.toolbarProps.onExit).toBeUndefined();
      const onExit = mock(() => {});
      const { result } = mount({ onExit }, makeEditor({ dirty: false }));
      act(() => result.current.toolbarProps.onExit?.());
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("asks before leaving unsaved edits", () => {
      const onExit = mock(() => {});
      const { result } = mount({ onExit });
      act(() => result.current.toolbarProps.onExit?.());
      expect(onExit).not.toHaveBeenCalled();
      expect(result.current.exitGuard.dialog.open).toBe(true);
   });
});
