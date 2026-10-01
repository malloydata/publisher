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
