// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a report route draws, decided by the package's listing: the report
 * page, the builder, a redirect from a deprecated by-name alias, the list of
 * reports an ambiguous alias names, a draft the host keeps, a redirect from
 * the legacy `<path>/edit` route to `<path>?edit`, or nothing.
 */
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { useEffect } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

type Listed = { path: string; layout: string; editable?: boolean };

let listing: {
   reports: Listed[];
   settled: boolean;
   isFetching: boolean;
   isError: boolean;
   refetch: () => void;
};
let draft: { layout: "grid" | "linear" | undefined; settled: boolean };
const draftCalls: unknown[][] = [];
const refetch = mock(() => {});

const sdk = await import("@malloy-publisher/sdk");
mock.module("@malloy-publisher/sdk", () => ({
   ...sdk,
   useReportListing: () => listing,
   useDraftLayout: (...args: unknown[]) => {
      draftCalls.push(args);
      return draft;
   },
}));
let editorMounts = 0;
/** Stands in for the builder: counts mounts, so losing its unsaved state is visible. */
function Editor(props: { path: string; layout: string }) {
   useEffect(() => {
      editorMounts++;
   }, []);
   return (
      <div data-testid="editor">{`edit ${props.path} as ${props.layout}`}</div>
   );
}
mock.module("../DashboardEditPage/DashboardEditPage", () => ({
   default: Editor,
}));
mock.module("../ReportPage/ReportPage", () => ({
   default: (props: { path: string; editable: boolean }) => (
      <div data-testid="report">{`read ${props.path} editable=${props.editable}`}</div>
   ),
}));

const { default: RoutedDocument } = await import("./RoutedDocument");

let frameMounts = 0;
/** Stands in for a data app's frame: counts mounts, so a reload is visible. */
function Frame() {
   useEffect(() => {
      frameMounts++;
   }, []);
   return <div data-testid="not-found">nothing</div>;
}

function Where() {
   const location = useLocation();
   return (
      <div data-testid="where">{`${location.pathname}${location.search}`}</div>
   );
}

function mount(
   routePath: string,
   edit = false,
   search = "",
   notFoundUntilListed = false,
) {
   const at = `/env/pkg/${routePath}${search || (edit ? "?edit" : "")}`;
   return render(tree(at, routePath, edit, notFoundUntilListed));
}

function tree(
   at: string,
   routePath: string,
   edit: boolean,
   notFoundUntilListed = false,
) {
   return (
      <MemoryRouter initialEntries={[at]}>
         <Routes>
            <Route
               path="/env/pkg/*"
               element={
                  <>
                     <Where />
                     <RoutedDocument
                        environmentName="env"
                        packageName="pkg"
                        routePath={routePath}
                        edit={edit}
                        notFound={<Frame />}
                        notFoundUntilListed={notFoundUntilListed}
                     />
                  </>
               }
            />
         </Routes>
      </MemoryRouter>
   );
}

beforeEach(() => {
   listing = {
      reports: [],
      settled: true,
      isFetching: false,
      isError: false,
      refetch,
   };
   draft = { layout: undefined, settled: true };
   draftCalls.length = 0;
   frameMounts = 0;
   editorMounts = 0;
   refetch.mockClear();
});

describe("RoutedDocument", () => {
   it("opens a listed path as the report page", () => {
      listing.reports = [
         { path: "sales/q3.malloy", layout: "grid", editable: false },
      ];
      mount("sales/q3");
      expect(screen.getByTestId("report").textContent).toBe(
         "read sales/q3.malloy editable=false",
      );
   });

   it("opens a listed path's ?edit in the builder with the listed layout", () => {
      listing.reports = [{ path: "story.malloy", layout: "linear" }];
      mount("story", true);
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit story.malloy as linear",
      );
      // A listed report is not looked up as a draft.
      expect(draftCalls.at(-1)).toEqual(["env", "pkg", "story.malloy", false]);
   });

   it("waits for the listing before deciding", () => {
      listing.settled = false;
      listing.reports = [{ path: "story.malloy", layout: "linear" }];
      mount("story");
      expect(screen.queryByTestId("report")).toBeNull();
      expect(screen.queryByTestId("not-found")).toBeNull();
   });

   it("opens a just-created linear report as linear, not as an unlisted grid draft, while the cached listing is refetched", () => {
      // The create invalidated the listing and navigated here: the cached
      // copy predates the new file and a refetch is in flight.
      listing.reports = [{ path: "old.malloy", layout: "grid" }];
      listing.isFetching = true;
      const view = mount("by-category", true);
      expect(screen.queryByTestId("editor")).toBeNull();
      expect(draftCalls.every((call) => call[3] === false)).toBe(true);
      listing = {
         ...listing,
         isFetching: false,
         reports: [
            ...listing.reports,
            { path: "by-category.malloy", layout: "linear" },
         ],
      };
      view.rerender(tree("/env/pkg/by-category?edit", "by-category", true));
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit by-category.malloy as linear",
      );
   });

   it("keeps an open unlisted editor through a later background refetch", () => {
      const view = mount("drafts/new", true);
      expect(screen.getByTestId("editor")).toBeDefined();
      listing = { ...listing, isFetching: true };
      view.rerender(tree("/env/pkg/drafts/new?edit", "drafts/new", true));
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit drafts/new.malloy as grid",
      );
   });

   // React Query keeps the last listing when a background refetch fails and
   // sets isError beside it: the open editor must not be swapped for the
   // "could not list" alert, which would drop its unsaved edits and undo.
   it("keeps an open editor mounted when a background refetch of the listing fails", () => {
      listing.reports = [{ path: "story.malloy", layout: "linear" }];
      const view = mount("story", true);
      expect(screen.getByTestId("editor")).toBeDefined();
      listing = { ...listing, isFetching: true };
      view.rerender(tree("/env/pkg/story?edit", "story", true));
      listing = { ...listing, isFetching: false, isError: true };
      view.rerender(tree("/env/pkg/story?edit", "story", true));
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit story.malloy as linear",
      );
      expect(screen.queryByRole("alert")).toBeNull();
      expect(editorMounts).toBe(1);
   });

   it("keeps an open unlisted draft editor mounted when a background refetch fails", () => {
      const view = mount("drafts/new", true);
      expect(screen.getByTestId("editor")).toBeDefined();
      listing = { ...listing, isError: true };
      view.rerender(tree("/env/pkg/drafts/new?edit", "drafts/new", true));
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit drafts/new.malloy as grid",
      );
      expect(screen.queryByRole("alert")).toBeNull();
      expect(editorMounts).toBe(1);
   });

   it("keeps an open report when a background refetch fails", () => {
      listing.reports = [{ path: "story.malloy", layout: "linear" }];
      const view = mount("story");
      listing = { ...listing, isError: true };
      view.rerender(tree("/env/pkg/story", "story", false));
      expect(screen.getByTestId("report")).toBeDefined();
      expect(screen.queryByRole("alert")).toBeNull();
   });

   it("opens a listed report at once, whatever a refetch is doing", () => {
      listing.reports = [{ path: "story.malloy", layout: "linear" }];
      listing.isFetching = true;
      mount("story");
      expect(screen.getByTestId("report")).toBeDefined();
   });

   it("draws a data app at once while the listing is unread, and keeps it when the listing has no report there", () => {
      listing = { ...listing, settled: false, isFetching: true };
      const view = mount("data-apps/app.html", false, "", true);
      expect(screen.getByTestId("not-found")).toBeDefined();
      listing = { ...listing, settled: true, isFetching: false };
      view.rerender(
         tree("/env/pkg/data-apps/app.html", "data-apps/app.html", false, true),
      );
      expect(screen.getByTestId("not-found")).toBeDefined();
      // The same frame throughout: the app is not reloaded when the listing lands.
      expect(frameMounts).toBe(1);
   });

   it("decides a data app from a cached listing at once, and gives way to a report the listing names", () => {
      listing.reports = [{ path: "data-apps/board.malloy", layout: "grid" }];
      listing.isFetching = true;
      mount("data-apps/board", false, "", true);
      expect(screen.getByTestId("report").textContent).toBe(
         "read data-apps/board.malloy editable=true",
      );
   });

   it("switches a data app to the report a later listing names", () => {
      listing = { ...listing, settled: false, isFetching: true };
      const view = mount("data-apps/board", false, "", true);
      expect(screen.getByTestId("not-found")).toBeDefined();
      listing = {
         ...listing,
         settled: true,
         isFetching: false,
         reports: [{ path: "data-apps/board.malloy", layout: "grid" }],
      };
      view.rerender(
         tree("/env/pkg/data-apps/board", "data-apps/board", false, true),
      );
      expect(screen.getByTestId("report")).toBeDefined();
      expect(screen.queryByTestId("not-found")).toBeNull();
   });

   it("draws a data app when the listing cannot be read", () => {
      listing.isError = true;
      mount("data-apps/app.html", false, "", true);
      expect(screen.getByTestId("not-found")).toBeDefined();
      expect(screen.queryByRole("alert")).toBeNull();
   });

   it("says a failed listing cannot tell, and retries on request", () => {
      listing.isError = true;
      mount("sales/q3");
      expect(screen.getByRole("alert").textContent).toContain(
         "Could not list this package's reports, so this page cannot tell what is at sales/q3.",
      );
      fireEvent.click(screen.getByRole("button", { name: "Retry" }));
      expect(refetch).toHaveBeenCalledTimes(1);
      expect(screen.queryByTestId("not-found")).toBeNull();
   });

   it("redirects a deprecated dashboards/<name> alias to the grid report's path, keeping the query", () => {
      listing.reports = [
         { path: "notebooks/overview.malloy", layout: "linear" },
         { path: "team/overview.malloy", layout: "grid" },
      ];
      mount("dashboards/overview", false, "?region=west");
      expect(screen.getByTestId("where").textContent).toBe(
         "/env/pkg/team/overview?region=west",
      );
   });

   it("redirects a deprecated notebooks/<name> alias in edit mode to the linear report's ?edit route", () => {
      listing.reports = [
         { path: "team/overview.malloy", layout: "grid" },
         { path: "notes/overview.malloy", layout: "linear" },
      ];
      mount("notebooks/overview", true);
      expect(screen.getByTestId("where").textContent).toBe(
         "/env/pkg/notes/overview?edit",
      );
   });

   it("redirects the legacy <path>/edit route to <path>?edit, keeping the query", () => {
      listing.reports = [{ path: "sales/q3.malloy", layout: "grid" }];
      mount("sales/q3/edit", false, "?region=west");
      expect(screen.getByTestId("where").textContent).toBe(
         "/env/pkg/sales/q3?edit&region=west",
      );
   });

   it("opens a report at <path>/edit.malloy rather than reading the segment as the legacy edit route", () => {
      listing.reports = [
         { path: "sales/q3/edit.malloy", layout: "grid", editable: true },
      ];
      mount("sales/q3/edit");
      expect(screen.getByTestId("report").textContent).toBe(
         "read sales/q3/edit.malloy editable=true",
      );
   });

   it("lists the reports an ambiguous alias names rather than picking one", () => {
      listing.reports = [
         { path: "b/overview.malloy", layout: "grid" },
         { path: "a/overview.malloy", layout: "grid" },
      ];
      mount("dashboards/overview");
      expect(screen.getByText("More than one report is named overview"));
      expect(
         screen.getAllByRole("link").map((a) => a.getAttribute("href")),
      ).toEqual(["/env/pkg/a/overview", "/env/pkg/b/overview"]);
   });

   it("draws not-found for an alias nothing has, and for an unlisted path", () => {
      mount("dashboards/absent");
      expect(screen.getByTestId("not-found"));
   });

   it("opens an unlisted ?edit as the host's draft, with the layout its store recorded", () => {
      draft = { layout: "linear", settled: true };
      mount("drafts/new", true);
      expect(draftCalls.at(-1)).toEqual([
         "env",
         "pkg",
         "drafts/new.malloy",
         true,
      ]);
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit drafts/new.malloy as linear",
      );
   });

   it("opens an unlisted ?edit with no draft as a grid report", () => {
      mount("drafts/new", true);
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit drafts/new.malloy as grid",
      );
   });

   it("waits for the host's answer before opening an unlisted /edit", () => {
      draft = { layout: undefined, settled: false };
      mount("drafts/new", true);
      expect(screen.queryByTestId("editor")).toBeNull();
   });

   it("does not open an unlisted path for reading", () => {
      draft = { layout: "linear", settled: true };
      mount("drafts/new");
      expect(screen.getByTestId("not-found"));
      expect(screen.queryByTestId("editor")).toBeNull();
   });
});
