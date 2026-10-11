// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a report route draws, decided by the package's listing: the report
 * page, the builder, a redirect from a deprecated by-name alias, the list of
 * reports an ambiguous alias names, a draft the host keeps, or nothing.
 */
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

type Listed = { path: string; layout: string; editable?: boolean };

let listing: {
   reports: Listed[];
   settled: boolean;
   isError: boolean;
   refetch: () => void;
};
let draft: { type: "dashboard" | "notebook" | undefined; settled: boolean };
const draftCalls: unknown[][] = [];
const refetch = mock(() => {});

const sdk = await import("@malloy-publisher/sdk");
mock.module("@malloy-publisher/sdk", () => ({
   ...sdk,
   useReportListing: () => listing,
   useDraftType: (...args: unknown[]) => {
      draftCalls.push(args);
      return draft;
   },
}));
mock.module("../DashboardEditPage/DashboardEditPage", () => ({
   default: (props: { path: string; kind: string }) => (
      <div data-testid="editor">{`edit ${props.path} as ${props.kind}`}</div>
   ),
}));
mock.module("../ReportPage/ReportPage", () => ({
   default: (props: { path: string; editable: boolean }) => (
      <div data-testid="report">{`read ${props.path} editable=${props.editable}`}</div>
   ),
}));

const { default: RoutedDocument } = await import("./RoutedDocument");

function Where() {
   const location = useLocation();
   return (
      <div data-testid="where">{`${location.pathname}${location.search}`}</div>
   );
}

function mount(routePath: string, edit = false, search = "") {
   const at = `/env/pkg/${routePath}${edit ? "/edit" : ""}${search}`;
   return render(
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
                        notFound={<div data-testid="not-found">nothing</div>}
                     />
                  </>
               }
            />
         </Routes>
      </MemoryRouter>,
   );
}

beforeEach(() => {
   listing = { reports: [], settled: true, isError: false, refetch };
   draft = { type: undefined, settled: true };
   draftCalls.length = 0;
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

   it("opens a listed path's /edit in the builder with the listed layout's kind", () => {
      listing.reports = [{ path: "story.malloy", layout: "linear" }];
      mount("story", true);
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit story.malloy as notebook",
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

   it("redirects a deprecated notebooks/<name>/edit alias to the linear report's edit route", () => {
      listing.reports = [
         { path: "team/overview.malloy", layout: "grid" },
         { path: "notes/overview.malloy", layout: "linear" },
      ];
      mount("notebooks/overview", true);
      expect(screen.getByTestId("where").textContent).toBe(
         "/env/pkg/notes/overview/edit",
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

   it("opens an unlisted /edit as the host's draft, under the type it was filed as", () => {
      draft = { type: "notebook", settled: true };
      mount("drafts/new", true);
      expect(draftCalls.at(-1)).toEqual([
         "env",
         "pkg",
         "drafts/new.malloy",
         true,
      ]);
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit drafts/new.malloy as notebook",
      );
   });

   it("opens an unlisted /edit with no draft as a grid report", () => {
      mount("drafts/new", true);
      expect(screen.getByTestId("editor").textContent).toBe(
         "edit drafts/new.malloy as dashboard",
      );
   });

   it("waits for the host's answer before opening an unlisted /edit", () => {
      draft = { type: undefined, settled: false };
      mount("drafts/new", true);
      expect(screen.queryByTestId("editor")).toBeNull();
   });

   it("does not open an unlisted path for reading", () => {
      draft = { type: "notebook", settled: true };
      mount("drafts/new");
      expect(screen.getByTestId("not-found"));
      expect(screen.queryByTestId("editor")).toBeNull();
   });
});
