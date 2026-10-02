// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { type SpliceResult } from "./spliceDocument";
import { useDocumentEditor } from "./useDocumentEditor";

interface Doc {
   items: string[];
}

const splice = async (source: string, doc: Doc): Promise<SpliceResult> =>
   doc.items.includes("bad")
      ? { ok: false, reason: "refused" }
      : { ok: true, source: doc.items.join(",") || source };

const structural = (saved: Doc, doc: Doc) =>
   saved.items.length !== doc.items.length;

const open = (extra: { onSave?: (s: string) => void } = {}) =>
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
