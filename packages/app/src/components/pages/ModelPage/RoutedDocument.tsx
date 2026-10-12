// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Loading,
   LOADING_COPY,
   locateReport,
   useDraftLayout,
   useReportListing,
} from "@malloy-publisher/sdk";
import { Alert, Box, Button } from "@mui/material";
import { useEffect, useState } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { legacyEditBase, withEdit } from "../../common/documentRoutes";
import DashboardEditPage from "../DashboardEditPage/DashboardEditPage";
import ReportPage from "../ReportPage/ReportPage";
import LegacyAliasRoute from "./LegacyAliasRoute";

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
   const report = settled ? locateReport(path, reports) : undefined;
   // "No report at this path" is read only from a listing no request is
   // refreshing: right after a create invalidates it, the cached copy predates
   // the file this route was opened for. Once a current listing has been seen
   // for the path the decision holds, so a later background refetch never
   // swaps an open editor for a spinner.
   const current = settled && !isFetching;
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
   if (!decided)
      return early ? (
         unlisted(notFound)
      ) : (
         <Loading text={LOADING_COPY.opening()} />
      );
   if (isError && early) return unlisted(notFound);
   // A listing that failed says nothing about what is at this route, so it
   // is not "nothing here": say so, and offer to ask again.
   if (isError)
      return (
         <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
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

   if (report) {
      if (edit)
         return (
            <DashboardEditPage
               environmentName={environmentName}
               packageName={packageName}
               path={report.path}
               layout={report.layout}
            />
         );
      return (
         <ReportPage
            environmentName={environmentName}
            packageName={packageName}
            path={report.path}
            editable={report.editable}
         />
      );
   }

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
