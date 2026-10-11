// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Loading,
   LOADING_COPY,
   locateReport,
   locateReportByName,
   MONO_FONT_FAMILY,
   reportRoute,
   useReportListing,
} from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";
import { Link as RouterLink, Navigate, useLocation } from "react-router-dom";
import DashboardEditPage from "../DashboardEditPage/DashboardEditPage";
import ReportPage from "../ReportPage/ReportPage";

/** `dashboards/<name>` or `notebooks/<name>`: the deprecated by-name routes, and the layout each preferred. */
const ALIAS = /^(dashboards|notebooks)\/([^/]+)$/;

/**
 * A route with no model extension, opened as the report at that path: the
 * route is the report's package path without `.malloy`, so `sales/q3` is
 * `sales/q3.malloy`. The package's listing decides; no directory name does.
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
}: {
   environmentName: string;
   packageName: string;
   /** The route's path under the package, without `/edit`. */
   routePath: string;
   edit: boolean;
   /** What to draw when nothing is at this route. */
   notFound: React.ReactNode;
}) {
   const { reports, settled } = useReportListing(environmentName, packageName);
   const { search } = useLocation();
   if (!settled) return <Loading text={LOADING_COPY.opening()} />;

   const path = `${routePath}.malloy`;
   const report = locateReport(path, reports);
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
               to={`${reportRoute(environmentName, packageName, lookup.found.path)}${edit ? "/edit" : ""}${search}`}
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
   // file the package has not served yet. The editor says which.
   if (edit)
      return (
         <DashboardEditPage
            environmentName={environmentName}
            packageName={packageName}
            path={path}
            kind="dashboard"
         />
      );
   return <>{notFound}</>;
}
