// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { useGivenUrlParams } from "./useGivenUrlParams";

function mount(initialEntry: string) {
   const wrapper = ({ children }: { children: ReactNode }) => (
      <MemoryRouter initialEntries={[initialEntry]}>{children}</MemoryRouter>
   );
   const view = renderHook(
      () => ({ hook: useGivenUrlParams(), location: useLocation() }),
      { wrapper },
   );
   return {
      get params() {
         return view.result.current.hook.params;
      },
      get search() {
         return new URLSearchParams(view.result.current.location.search);
      },
      get hash() {
         return view.result.current.location.hash;
      },
      change(next: Record<string, string>, managed: string[]) {
         act(() => view.result.current.hook.onGivensChange(next, managed));
      },
   };
}

describe("useGivenUrlParams", () => {
   it("reads the query string as a flat record", () => {
      const host = mount("/d?REGION=West&utm=x");
      expect(host.params).toEqual({ REGION: "West", utm: "x" });
   });

   it("merges into the query string, leaving parameters that are not ours", () => {
      const host = mount("/d?utm=x");
      host.change({ REGION: "West" }, ["REGION"]);
      expect(host.search.get("REGION")).toBe("West");
      expect(host.search.get("utm")).toBe("x");
   });

   it("removes a managed name that was cleared, and only that", () => {
      const host = mount("/d?REGION=West&STATUS=open&utm=x");
      host.change({ STATUS: "open" }, ["REGION", "STATUS"]);
      expect(host.search.has("REGION")).toBe(false);
      expect(host.search.get("STATUS")).toBe("open");
      expect(host.search.get("utm")).toBe("x");
   });

   it("remembers what it wrote, so a given the model stopped declaring is still cleaned up", () => {
      const host = mount("/d");
      host.change({ REGION: "West" }, ["REGION"]);
      // The model reloads without REGION; the report no longer manages it.
      host.change({}, []);
      expect(host.search.has("REGION")).toBe(false);
   });

   it("keeps the fragment the reader arrived on", () => {
      const host = mount("/d?utm=x#trend");
      host.change({ REGION: "West" }, ["REGION"]);
      expect(host.hash).toBe("#trend");
   });

   it("is not fooled by a given named like an Object.prototype member", () => {
      const host = mount("/d?constructor=1");
      host.change({}, ["constructor"]);
      expect(host.search.has("constructor")).toBe(false);
   });
});
