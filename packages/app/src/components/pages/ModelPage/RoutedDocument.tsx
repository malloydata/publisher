// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Loading,
   LOADING_COPY,
   locateReport,
   type ReportLayout,
   useDraftLayout,
   useReportListing,
} from "@malloy-publisher/sdk";
import { Alert, Box, Button, type SxProps, type Theme } from "@mui/material";
import { useEffect, useState } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { legacyEditBase, withEdit } from "../../common/documentRoutes";
import DashboardEditPage from "../DashboardEditPage/DashboardEditPage";
import ReportPage from "../ReportPage/ReportPage";
import LegacyAliasRoute from "./LegacyAliasRoute";
import OlderNotebookNotice from "./OlderNotebookNotice";
import { PAGE_FRAME_SX } from "../pageFrame";

/**
 * A route with no model extension, opened as the report at that path: the
 * route is the report's package path without `.malloy`, so `sales/q3` is
 * `sales/q3.malloy`. The package's listing decides; no directory name does.
 *
 * `?edit` on the route opens the report in the builder. A legacy route ending
 * in `/edit` (`sales/q3/edit`) is checked as a path FIRST: when
 * `sales/q3/edit.malloy` is a report it opens, and only when it is not does
 * the route redirect to `sales/q3?edit`.
 *
 * When no report is at that path, an old by-name link is redirected by
 * `LegacyAliasRoute`, and otherwise `?edit` opens the host's draft at the path.
 *
 * With `notFoundUntilListed`, a route that is something else unless a report
 * claims it (a data app) draws `notFound` at once rather than waiting on the
 * listing, and switches only when the listing names a report at its path.
 */
export default function RoutedDocument({
   environmentName,
   packageName,
   routePath,
   edit,
   notFound,
   notFoundUntilListed = false,
}: {
   environmentName: string;
   packageName: string;
   /** The route's path under the package, as the URL spells it (no `?edit`). */
   routePath: string;
   /** The route carries `?edit`. */
   edit: boolean;
   /** What to draw when nothing is at this route. */
   notFound: React.ReactNode;
   /**
    * Draw `notFound` while the listing is unread, refreshing or unreadable,
    * rather than a spinner or a retry: for a route that is not a report unless
    * the listing says so. Not for `?edit`, which opens the builder.
    */
   notFoundUntilListed?: boolean;
}) {
   const { reports, settled, isFetching, isError, refetch } = useReportListing(
      environmentName,
      packageName,
   );
   const { search } = useLocation();
   const path = `${routePath}.malloy`;
   // A route with no `.malloy` report may name an older `.malloynb` of the same
   // name: `/q3` opens `q3.malloynb` when there is no `q3.malloy`.
   const report = settled
      ? (locateReport(path, reports) ??
        locateReport(`${routePath}.malloynb`, reports))
      : undefined;
   // "No report at this path" is read only from a listing no request is
   // refreshing: right after a create invalidates it, the cached copy predates
   // the file this route was opened for. Once a current listing has been seen
   // for the path the decision holds, so a later background refetch never
   // swaps an open editor for a spinner, nor for the error below when it
   // fails (React Query keeps the last listing beside the error).
   const current = settled && !isFetching && !isError;
   const [seenCurrent, setSeenCurrent] = useState<string>();
   useEffect(() => {
      if (current) setSeenCurrent(path);
   }, [current, path]);
   const decided = report !== undefined || current || seenCurrent === path;
   // An unlisted report opened for editing is the host's draft, or nothing
   // yet: the layout its store recorded is the one the listing cannot say.
   const draft = useDraftLayout(
      environmentName,
      packageName,
      path,
      edit && decided && !report,
   );
   // Drawn in the same place as the unlisted branch's `notFound` below, so a
   // listing that answers with nothing here keeps the page mounted (a data
   // app's frame does not reload).
   const unlisted = (children: React.ReactNode) => (
      <LegacyAliasRoute
         environmentName={environmentName}
         packageName={packageName}
         routePath={routePath}
         edit={edit}
         reports={reports}
      >
         {children}
      </LegacyAliasRoute>
   );
   const early = notFoundUntilListed && !edit && !report;
   if (!decided && early) return unlisted(notFound);
   // A listing that failed, with no earlier answer for this path to decide
   // from, says nothing about what is at this route, so it is not "nothing
   // here": say so, and offer to ask again.
   if (!decided && isError && !isFetching)
      return (
         <Box sx={PAGE_FRAME_SX}>
            <Alert
               severity="warning"
               action={
                  <Button
                     color="inherit"
                     size="small"
                     onClick={() => void refetch()}
                  >
                     Retry
                  </Button>
               }
            >
               Could not list this package&apos;s reports, so this page cannot
               tell what is at {routePath}.
            </Alert>
         </Box>
      );
   if (!decided) return <Loading text={LOADING_COPY.opening()} />;

   if (report && edit && report.path.endsWith(".malloynb"))
      return (
         <OlderNotebookNotice
            environmentName={environmentName}
            packageName={packageName}
            path={report.path}
         />
      );
   if (report)
      return (
         <ReadAndEdit
            environmentName={environmentName}
            packageName={packageName}
            path={report.path}
            editable={report.editable}
            layout={report.layout}
            edit={edit}
         />
      );

   // The legacy edit route, `<path>/edit`: no report has this path (checked
   // above), so it is a link to edit `<path>`.
   const legacyEdit = edit ? undefined : legacyEditBase(routePath);
   if (legacyEdit !== undefined)
      return (
         <Navigate
            replace
            to={`/${environmentName}/${packageName}/${legacyEdit
               .split("/")
               .map(encodeURIComponent)
               .join("/")}${withEdit(search)}`}
         />
      );

   // Not listed, but asked for in the builder: a draft the host keeps, or a
   // file the package has not served yet. The editor says which, opened with
   // the layout the draft recorded (grid when there is no draft).
   return unlisted(
      edit ? (
         draft.settled ? (
            <DashboardEditPage
               environmentName={environmentName}
               packageName={packageName}
               path={path}
               layout={draft.layout ?? "grid"}
            />
         ) : (
            <Loading text={LOADING_COPY.opening()} />
         )
      ) : (
         notFound
      ),
   );
}

/**
 * A listed report, read or edited, as one page rather than two routes, so
 * switching between them never blanks it.
 *
 * A switch is a handoff. The page being switched to renders out of sight over
 * the one on screen until it is drawn (open, with the tiles in view showing
 * their results), then the outgoing page goes, in the same frame. Two routes
 * did the opposite: each switch replaced the page outright, and every result
 * on it was drawn again from nothing, blank in between, two or three times
 * over on the way into the builder. Only during a switch are both mounted.
 */
function ReadAndEdit({
   environmentName,
   packageName,
   path,
   editable,
   layout,
   edit,
}: {
   environmentName: string;
   packageName: string;
   path: string;
   editable: boolean | undefined;
   layout: ReportLayout | undefined;
   edit: boolean;
}) {
   // The page on screen; the route says which one is wanted.
   const [shown, setShown] = useState<"reader" | "editor">(
      edit ? "editor" : "reader",
   );
   const wanted = edit ? "editor" : "reader";
   const reader = wanted === "reader" || shown === "reader";
   const editor = wanted === "editor" || shown === "editor";
   // Out of sight: drawn underneath, not yet the page on screen.
   const offstage: SxProps<Theme> = {
      position: "absolute",
      inset: 0,
      visibility: "hidden",
      pointerEvents: "none",
      overflow: "hidden",
   };
   return (
      // Marked while both pages are mounted, for anything that has to wait
      // for the switch to finish (a test reading the page, say).
      <Box
         sx={{ position: "relative" }}
         data-switching={(reader && editor) || undefined}
      >
         {reader && (
            <Box
               key="reader"
               sx={shown === "reader" ? undefined : offstage}
               aria-hidden={shown !== "reader" || undefined}
            >
               <ReportPage
                  environmentName={environmentName}
                  packageName={packageName}
                  path={path}
                  {...(editable !== undefined ? { editable } : {})}
                  onReady={() => {
                     if (!edit) setShown("reader");
                  }}
               />
            </Box>
         )}
         {editor && (
            <Box
               key="editor"
               sx={shown === "editor" ? undefined : offstage}
               aria-hidden={shown !== "editor" || undefined}
            >
               <DashboardEditPage
                  environmentName={environmentName}
                  packageName={packageName}
                  path={path}
                  {...(layout ? { layout } : {})}
                  // On a direct ?edit link there is no reader under it, so it
                  // draws the report while it loads, as it always has.
                  quietLoading={reader}
                  // Its controls go into the header with it, not before.
                  offstage={wanted !== "editor" || shown !== "editor"}
                  onReady={() => {
                     if (edit) setShown("editor");
                  }}
               />
            </Box>
         )}
      </Box>
   );
}
