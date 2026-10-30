// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A path inside a package, decided by whether the package has published
 * versions. A versioned package's path with no version segment (an old link,
 * a typed URL) moves to its `latest` version's, keeping the query string, as
 * the bare package route does; a package with no versions keeps its paths.
 * The page opens at once and is never mounted twice for the move, so nothing
 * it asked for is asked again.
 */
import { render, screen } from "@testing-library/react";
import { beforeEach, expect, it, mock } from "bun:test";
import { Suspense, useEffect } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

let listing: { versions: { id: string }[]; isPending: boolean };
let modelMounts = 0;

const sdk = await import("@malloy-publisher/sdk");
mock.module("@malloy-publisher/sdk", () => ({
   ...sdk,
   usePackageVersions: () => listing,
}));
function Model() {
   useEffect(() => {
      modelMounts++;
   }, []);
   return <div data-testid="page">model</div>;
}
mock.module("./ModelPage/ModelPage", () => ({ default: Model }));
mock.module("./PackagePage/PackagePage", () => ({
   default: () => <div data-testid="page">package</div>,
}));

const { default: PackageSplatRoute } = await import("./PackageSplatRoute");

function Where() {
   const { pathname, search } = useLocation();
   return <div data-testid="where">{`${pathname}${search}`}</div>;
}

const tree = (url: string) => (
   <MemoryRouter initialEntries={[url]}>
      <Routes>
         <Route
            path="/:environmentName/:packageName/*"
            element={
               <Suspense fallback={null}>
                  <PackageSplatRoute />
                  <Where />
               </Suspense>
            }
         />
      </Routes>
   </MemoryRouter>
);

const VERSIONED = { versions: [{ id: "1.0.0" }], isPending: false };
const UNVERSIONED = { versions: [], isPending: false };
const PENDING = { versions: [], isPending: true };

beforeEach(() => {
   listing = VERSIONED;
   modelMounts = 0;
});

it("moves a versioned package's path with no version to latest's", async () => {
   render(tree("/demo/auto_recalls/index.malloy/sources/recalls?tab=data"));
   expect((await screen.findByTestId("page")).textContent).toBe("model");
   expect(screen.getByTestId("where").textContent).toBe(
      "/demo/auto_recalls/latest/index.malloy/sources/recalls?tab=data",
   );
   expect(modelMounts).toBe(1);
});

it("opens the page at once, and moves it without a remount once the listing says it is versioned", async () => {
   listing = PENDING;
   const url = "/demo/auto_recalls/index.malloy";
   const { rerender } = render(tree(url));
   expect((await screen.findByTestId("page")).textContent).toBe("model");
   expect(screen.getByTestId("where").textContent).toBe(url);
   listing = VERSIONED;
   rerender(tree(url));
   expect(
      (await screen.findByText("/demo/auto_recalls/latest/index.malloy"))
         .textContent,
   ).toBe("/demo/auto_recalls/latest/index.malloy");
   expect(modelMounts).toBe(1);
});

it.each([
   ["latest", "/demo/auto_recalls/latest/index.malloy"],
   ["a pinned version", "/demo/auto_recalls/1.0.0/index.malloy"],
])("keeps a path that names %s", async (_, url) => {
   render(tree(url));
   expect((await screen.findByTestId("page")).textContent).toBe("model");
   expect(screen.getByTestId("where").textContent).toBe(url);
});

it("opens a bare version as the package page", async () => {
   render(tree("/demo/auto_recalls/1.0.0"));
   expect((await screen.findByTestId("page")).textContent).toBe("package");
});

it("keeps the paths of a package with no versions", async () => {
   listing = UNVERSIONED;
   render(tree("/demo/plain/index.malloy/sources/recalls"));
   expect((await screen.findByTestId("page")).textContent).toBe("model");
   expect(screen.getByTestId("where").textContent).toBe(
      "/demo/plain/index.malloy/sources/recalls",
   );
});

it("waits on a version-shaped first segment until the listing says what it is", () => {
   listing = PENDING;
   render(tree("/demo/auto_recalls/1.0.0/index.malloy"));
   expect(screen.queryByTestId("page")).toBeNull();
});
