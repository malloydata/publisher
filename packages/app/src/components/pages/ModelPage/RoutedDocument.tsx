// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Loading,
   LOADING_COPY,
   locateReport,
   locateReportByName,
   MONO_FONT_FAMILY,
   reportRoute,
   useDraftType,
   useReportListing,
} from "@malloy-publisher/sdk";
import { Alert, Box, Button, Typography } from "@mui/material";
import { Link as RouterLink, Navigate, useLocation } from "react-router-dom";
import { legacyEditBase, withEdit } from "../../common/documentRoutes";
import DashboardEditPage from "../DashboardEditPage/DashboardEditPage";
import ReportPage from "../ReportPage/ReportPage";

/** `dashboards/<name>` or `notebooks/<name>`: the deprecated by-name routes, and the layout each preferred. */
const ALIAS = /^(dashboards|notebooks)\/([^/]+)$/;

/**
 * A route with no model extension, opened as the report at that path: the
 * route is the report's package path without `.malloy`, so `sales/q3` is
 * `sales/q3.malloy`. The package's listing decides; no directory name does.
 *
 * `?edit` on the route opens the report in the builder. A legacy route ending
 * in `/edit` (`sales/q3/edit`, the old edit route) is checked as a path FIRST:
 * when `sales/q3/edit.malloy` is a report it opens, and only when it is not
 * does the route redirect to `sales/q3?edit`.
 *
 * When no report is at that path and the route is exactly `dashboards/<x>` or
 * `notebooks/<x>`, it is an old link: the basename `<x>` is looked up (a grid
 * report preferred for `dashboards/`, a linear one for `notebooks/`) and the
 * reader redirected to the report's own route. That is a compatibility alias
 * only, and nothing new generates it. An ambiguous name says which reports
 * share it rather than picking one.
 */
export default function RoutedDocument({
   environmentName,
   packageName,
   routePath,
   edit,
   notFound,
   notFoundOnError = false,
}: {
   environmentName: string;
   packageName: string;
   /** The route's path under the package, as the URL spells it (no `?edit`). */
   routePath: string;
   /** The route carries `?edit`. */
   edit: boolean;
   /** What to draw when nothing is at this route. */
   notFound: React.ReactNode;
   /** Draw `notFound` rather than a retry when the listing cannot be read. */
   notFoundOnError?: boolean;
}) {
   const { reports, settled, isError, refetch } = useReportListing(
      environmentName,
      packageName,
   );
   const { search } = useLocation();
   const path = `${routePath}.malloy`;
   const report = settled ? locateReport(path, reports) : undefined;
   // An unlisted report opened for editing is the host's draft, or nothing
   // yet: the type the host filed the draft under is its layout, which the
   // listing cannot say. The editor looks the draft up under that type.
   const draft = useDraftType(
      environmentName,
      packageName,
      path,
      edit && settled && !report,
   );
   if (!settled) return <Loading text={LOADING_COPY.opening()} />;
   if (isError && notFoundOnError) return <>{notFound}</>;
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
               kind={report.kind}
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

   // The old edit route, `<path>/edit`: no report has this path (checked
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

   const alias = ALIAS.exec(routePath);
   if (alias) {
      const lookup = locateReportByName(
         alias[2],
         reports,
         alias[1] === "dashboards" ? "grid" : "linear",
      );
      if ("found" in lookup)
         return (
            <Navigate
               replace
               to={`${reportRoute(environmentName, packageName, lookup.found.path)}${edit ? withEdit(search) : search}`}
            />
         );
      if ("ambiguous" in lookup)
         return (
            <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
               <Typography variant="h6" sx={{ fontWeight: "fontWeightMedium" }}>
                  More than one report is named {alias[2]}
               </Typography>
               <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ mt: 1 }}
               >
                  This is an old link, which names a report by its file name
                  alone. Open one by its path:
               </Typography>
               <Box component="ul" sx={{ mt: 1 }}>
                  {lookup.ambiguous.map((candidate) => (
                     <li key={candidate}>
                        <RouterLink
                           to={reportRoute(
                              environmentName,
                              packageName,
                              candidate,
                           )}
                           style={{ fontFamily: MONO_FONT_FAMILY }}
                        >
                           {candidate}
                        </RouterLink>
                     </li>
                  ))}
               </Box>
            </Box>
         );
   }

   // Not listed, but asked for in the builder: a draft the host keeps, or a
   // file the package has not served yet. The editor says which, opened with
   // the layout the draft was filed under (grid when there is no draft).
   if (edit) {
      if (!draft.settled) return <Loading text={LOADING_COPY.opening()} />;
      return (
         <DashboardEditPage
            environmentName={environmentName}
            packageName={packageName}
            path={path}
            kind={draft.type ?? "dashboard"}
         />
      );
   }
   return <>{notFound}</>;
}
