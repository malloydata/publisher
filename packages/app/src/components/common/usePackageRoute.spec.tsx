// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a package's files may be written, as the route reads it: only once
 * its version listing has said it has none. A path that is not
 * version-shaped is read the same way, so a failed listing never offers Edit.
 */
import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import type { ReactNode } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";

let listing: {
   versions: { id: string }[];
   isPending: boolean;
   isError: boolean;
   refetch: () => void;
};

const sdk = await import("@malloy-publisher/sdk");
mock.module("@malloy-publisher/sdk", () => ({
   ...sdk,
   usePackageVersions: () => listing,
}));

const { usePackageRoute } = await import("./usePackageRoute");

const routeAt = (url: string) => {
   const wrapper = ({ children }: { children: ReactNode }) => (
      <MemoryRouter initialEntries={[url]}>
         <Routes>
            <Route path="/:environmentName/:packageName/*" element={children} />
         </Routes>
      </MemoryRouter>
   );
   return renderHook(() => usePackageRoute({ always: true }), { wrapper })
      .result.current;
};

beforeEach(() => {
   listing = {
      versions: [],
      isPending: false,
      isError: false,
      refetch: () => {},
   };
});

describe("usePackageRoute writable", () => {
   it("is writable once the listing has said there are no versions", () => {
      expect(routeAt("/env/pkg/sales/q3").writable).toBe(true);
   });

   it("is not writable while the listing loads, or when it failed, on any path", () => {
      listing = { ...listing, isPending: true };
      expect(routeAt("/env/pkg/sales/q3").writable).toBe(false);
      listing = { ...listing, isPending: false, isError: true };
      expect(routeAt("/env/pkg/sales/q3").writable).toBe(false);
      expect(routeAt("/env/pkg/1.0.0/q3").writable).toBe(false);
   });

   it("is never writable for a versioned package", () => {
      listing = { ...listing, versions: [{ id: "1.0.0" }] };
      expect(routeAt("/env/pkg/1.0.0/q3").writable).toBe(false);
      expect(routeAt("/env/pkg/sales/q3").writable).toBe(false);
   });
});
