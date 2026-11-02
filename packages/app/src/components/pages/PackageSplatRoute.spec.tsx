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
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, expect, it, mock } from "bun:test";
import { Suspense, useEffect } from "react";
import {
   MemoryRouter,
   Route,
   Routes,
   useLocation,
   useNavigate,
} from "react-router-dom";

let listing: {
   versions: { id: string }[];
   isPending: boolean;
   isError?: boolean;
   refetch?: () => void;
};
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
   const navigate = useNavigate();
   return (
      <>
         <div data-testid="where">{`${pathname}${search}`}</div>
         <button onClick={() => navigate(-1)}>back</button>
      </>
   );
}

const tree = (url: string, before: string[] = []) => (
   <MemoryRouter initialEntries={[...before, url]}>
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
         {/* Anywhere outside a package, to say where Back went. */}
         <Route path="*" element={<Where />} />
      </Routes>
   </MemoryRouter>
);

const VERSIONED = { versions: [{ id: "1.0.0" }], isPending: false };
const UNVERSIONED = { versions: [], isPending: false };
const PENDING = { versions: [], isPending: true };
const refetch = mock(() => {});
const FAILED = { versions: [], isPending: false, isError: true, refetch };

beforeEach(() => {
   listing = VERSIONED;
   modelMounts = 0;
   refetch.mockClear();
});

it("moves a versioned package's path with no version to latest's", async () => {
   render(tree("/demo/auto_recalls/index.malloy/sources/recalls?tab=data"));
   expect((await screen.findByTestId("page")).textContent).toBe("model");
   // The page opens first and moves in an effect, so wait for the move.
   expect(
      (
         await screen.findByText(
            "/demo/auto_recalls/latest/index.malloy/sources/recalls?tab=data",
         )
      ).textContent,
   ).toBe("/demo/auto_recalls/latest/index.malloy/sources/recalls?tab=data");
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

it("replaces the versionless address on the move to latest, so Back leaves the package", async () => {
   render(tree("/demo/auto_recalls/index.malloy", ["/elsewhere"]));
   expect(
      (await screen.findByText("/demo/auto_recalls/latest/index.malloy"))
         .textContent,
   ).toBe("/demo/auto_recalls/latest/index.malloy");
   fireEvent.click(screen.getByRole("button", { name: "back" }));
   expect((await screen.findByText("/elsewhere")).textContent).toBe(
      "/elsewhere",
   );
});

it.each([
   ["latest", "/demo/plain/latest/q.malloy"],
   ["a version", "/demo/plain/1.0.0/r.malloy"],
])(
   "opens a folder named like %s in a package with no versions by its path",
   async (_, url) => {
      listing = UNVERSIONED;
      render(tree(url));
      expect((await screen.findByTestId("page")).textContent).toBe("model");
      expect(screen.getByTestId("where").textContent).toBe(url);
   },
);

it("opens nothing at a version-shaped address when the listing failed, and offers a retry", async () => {
   listing = FAILED;
   render(tree("/demo/auto_recalls/1.0.0/top.malloy"));
   expect(
      await screen.findByText(/Could not read this package's versions/),
   ).toBeDefined();
   // Neither the model at `1.0.0/top.malloy` nor the package page.
   expect(screen.queryByTestId("page")).toBeNull();
   fireEvent.click(screen.getByRole("button", { name: "Retry" }));
   expect(refetch).toHaveBeenCalledTimes(1);
});
