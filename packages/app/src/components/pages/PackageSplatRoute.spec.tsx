// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A path inside a package, decided by whether the package has published
 * versions. A versioned package's path with no version segment (an old link,
 * a typed URL) moves to its `latest` version's, keeping the query string, as
 * the bare package route does; a package with no versions keeps its paths.
 */
import { render, screen } from "@testing-library/react";
import { beforeEach, expect, it, mock } from "bun:test";
import { Suspense } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

let listing: { versions: { id: string }[]; isPending: boolean };

const sdk = await import("@malloy-publisher/sdk");
mock.module("@malloy-publisher/sdk", () => ({
   ...sdk,
   usePackageVersions: () => listing,
}));
mock.module("./ModelPage/ModelPage", () => ({
   default: () => <div data-testid="page">model</div>,
}));
mock.module("./PackagePage/PackagePage", () => ({
   default: () => <div data-testid="page">package</div>,
}));

const { default: PackageSplatRoute } = await import("./PackageSplatRoute");

function Where() {
   const { pathname, search } = useLocation();
   return <div data-testid="where">{`${pathname}${search}`}</div>;
}

function open(url: string) {
   render(
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
      </MemoryRouter>,
   );
}

const VERSIONED = { versions: [{ id: "1.0.0" }], isPending: false };
const UNVERSIONED = { versions: [], isPending: false };

beforeEach(() => {
   listing = VERSIONED;
});

it("moves a versioned package's path with no version to latest's", async () => {
   open("/demo/auto_recalls/index.malloy/sources/recalls?tab=data");
   expect((await screen.findByTestId("where")).textContent).toBe(
      "/demo/auto_recalls/latest/index.malloy/sources/recalls?tab=data",
   );
   expect((await screen.findByTestId("page")).textContent).toBe("model");
});

it.each([
   ["latest", "/demo/auto_recalls/latest/index.malloy"],
   ["a pinned version", "/demo/auto_recalls/1.0.0/index.malloy"],
])("keeps a path that names %s", async (_, url) => {
   open(url);
   expect((await screen.findByTestId("page")).textContent).toBe("model");
   expect(screen.getByTestId("where").textContent).toBe(url);
});

it("opens a bare version as the package page", async () => {
   open("/demo/auto_recalls/1.0.0");
   expect((await screen.findByTestId("page")).textContent).toBe("package");
});

it("keeps the paths of a package with no versions", async () => {
   listing = UNVERSIONED;
   open("/demo/plain/index.malloy/sources/recalls");
   expect((await screen.findByTestId("page")).textContent).toBe("model");
   expect(screen.getByTestId("where").textContent).toBe(
      "/demo/plain/index.malloy/sources/recalls",
   );
});

it("draws nothing until the listing says whether the package has versions", () => {
   listing = { versions: [], isPending: true };
   open("/demo/auto_recalls/index.malloy");
   expect(screen.queryByTestId("page")).toBeNull();
   expect(screen.getByTestId("where").textContent).toBe(
      "/demo/auto_recalls/index.malloy",
   );
});
