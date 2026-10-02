// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, mock } from "bun:test";
import { useExitGuard } from "./useExitGuard";

interface Props {
   dirty: boolean;
   saving: boolean;
   reviewing: boolean;
   canSave: boolean;
}

/** A save the test settles by hand: it flips `saving` the way a builder's does. */
const mount = (initial: Partial<Props> = {}) => {
   const onExit = mock(() => {});
   let current: Props = {
      dirty: false,
      saving: false,
      reviewing: false,
      canSave: true,
      ...initial,
   };
   let resolve = () => {};
   let throwOnSave = false;
   const save = mock(() => {
      if (throwOnSave) throw new Error("no");
      return new Promise<void>((r) => {
         resolve = r;
      });
   });
   const view = renderHook(
      (props: Props) => useExitGuard({ ...props, save, onExit }),
      { initialProps: current },
   );
   const set = (next: Partial<Props>) => {
      current = { ...current, ...next };
      view.rerender(current);
   };
   /** The builder's save settling: `saving` falls, `dirty` is what it left, then the promise resolves. */
   const settle = async (next: Partial<Props>) => {
      await act(async () => {
         set({ saving: false, ...next });
         resolve();
      });
   };
   return {
      view,
      onExit,
      save,
      set,
      settle,
      failing: () => {
         throwOnSave = true;
      },
   };
};

const saveAndExit = async (view: ReturnType<typeof mount>["view"]) => {
   act(() => view.result.current.requestExit());
   await act(async () => view.result.current.dialog.onSaveAndExit());
};

describe("useExitGuard", () => {
   it("exits at once when nothing is unsaved", () => {
      const { view, onExit } = mount();
      act(() => view.result.current.requestExit());
      expect(onExit).toHaveBeenCalledTimes(1);
      expect(view.result.current.dialog.open).toBe(false);
   });

   it("asks first when dirty, and Keep editing closes without exiting", () => {
      const { view, onExit } = mount({ dirty: true });
      act(() => view.result.current.requestExit());
      expect(view.result.current.dialog.open).toBe(true);
      expect(onExit).not.toHaveBeenCalled();
      act(() => view.result.current.dialog.onKeepEditing());
      expect(view.result.current.dialog.open).toBe(false);
      expect(onExit).not.toHaveBeenCalled();
   });

   it("Discard exits without saving", () => {
      const { view, onExit, save } = mount({ dirty: true });
      act(() => view.result.current.requestExit());
      act(() => view.result.current.dialog.onDiscard());
      expect(onExit).toHaveBeenCalledTimes(1);
      expect(save).not.toHaveBeenCalled();
   });

   it("offers no save when the builder cannot save", () => {
      const { view } = mount({ dirty: true, canSave: false });
      act(() => view.result.current.requestExit());
      expect(view.result.current.dialog.canSave).toBe(false);
   });

   it("Save and exit closes the dialog, saves, and exits once the save lands clean", async () => {
      const { view, onExit, save, set, settle } = mount({ dirty: true });
      await saveAndExit(view);
      expect(view.result.current.dialog.open).toBe(false);
      expect(save).toHaveBeenCalledTimes(1);
      act(() => set({ saving: true }));
      expect(onExit).not.toHaveBeenCalled();
      await settle({ dirty: false });
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("stays when the save settles still dirty, and a later clean state is not that save", async () => {
      const { view, onExit, set, settle } = mount({ dirty: true });
      await saveAndExit(view);
      act(() => set({ saving: true }));
      await settle({});
      expect(onExit).not.toHaveBeenCalled();
      act(() => set({ dirty: false }));
      expect(onExit).not.toHaveBeenCalled();
   });

   it("holds while the review dialog is open and clears when it is dismissed", async () => {
      const { view, onExit, set, settle } = mount({ dirty: true });
      await saveAndExit(view);
      act(() => set({ reviewing: true }));
      await settle({});
      expect(onExit).not.toHaveBeenCalled();
      act(() => set({ reviewing: false }));
      act(() => set({ dirty: false }));
      expect(onExit).not.toHaveBeenCalled();
   });

   it("exits after the review is confirmed and the save lands", async () => {
      const { view, onExit, set, settle } = mount({ dirty: true });
      await saveAndExit(view);
      act(() => set({ reviewing: true }));
      await settle({});
      act(() => set({ reviewing: false, saving: true }));
      expect(onExit).not.toHaveBeenCalled();
      act(() => set({ saving: false, dirty: false }));
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("waits out a save already in flight, then saves what is still dirty", async () => {
      const { view, onExit, save, set, settle } = mount({
         dirty: true,
         saving: true,
      });
      await saveAndExit(view);
      expect(save).not.toHaveBeenCalled();
      await act(async () => set({ saving: false, dirty: true }));
      expect(save).toHaveBeenCalledTimes(1);
      act(() => set({ saving: true }));
      await settle({ dirty: false });
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("exits when the in-flight save leaves nothing dirty", async () => {
      const { view, onExit, save, set } = mount({ dirty: true, saving: true });
      await saveAndExit(view);
      await act(async () => set({ saving: false, dirty: false }));
      expect(save).not.toHaveBeenCalled();
      expect(onExit).toHaveBeenCalledTimes(1);
   });

   it("clears the request when the save throws", async () => {
      const { view, onExit, set, failing } = mount({ dirty: true });
      failing();
      await saveAndExit(view);
      act(() => set({ dirty: false }));
      expect(onExit).not.toHaveBeenCalled();
      // The request is over, so a fresh Done asks again rather than being swallowed.
      set({ dirty: true });
      act(() => view.result.current.requestExit());
      expect(view.result.current.dialog.open).toBe(true);
   });

   it("ignores the dialog's buttons once it is closed", () => {
      const { view, onExit, save } = mount({ dirty: true });
      act(() => view.result.current.requestExit());
      act(() => view.result.current.dialog.onKeepEditing());
      act(() => view.result.current.dialog.onDiscard());
      act(() => view.result.current.dialog.onSaveAndExit());
      expect(onExit).not.toHaveBeenCalled();
      expect(save).not.toHaveBeenCalled();
   });
});
