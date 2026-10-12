// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   legacyAliasOf,
   locateReportByName,
   MONO_FONT_FAMILY,
   reportRoute,
   type ListedReport,
} from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { Link as RouterLink, Navigate, useLocation } from "react-router-dom";
import { withEdit } from "../../common/documentRoutes";

/**
 * An old by-name link, `dashboards/<x>` or `notebooks/<x>`, from before
 * reports were addressed by their path: the basename `<x>` is looked up (a
 * grid report preferred for `dashboards/`, a linear one for `notebooks/`) and
 * the reader redirected to the report's own route. An ambiguous name says
 * which reports share it rather than picking one. Any other route, or a name
 * nothing has, draws `children`.
 *
 * A compatibility alias only: nothing new generates these links, and removing
 * it is deleting this file and its one mount in `RoutedDocument`.
 */
export default function LegacyAliasRoute({
   environmentName,
   packageName,
   routePath,
   edit,
   reports,
   children,
}: {
   environmentName: string;
   packageName: string;
   /** The route's path under the package, with no report at it. */
   routePath: string;
   /** The route carries `?edit`. */
   edit: boolean;
   reports: readonly ListedReport[];
   children: ReactNode;
}) {
   const { search } = useLocation();
   const alias = legacyAliasOf(routePath);
   if (!alias) return <>{children}</>;
   const lookup = locateReportByName(alias.name, reports, alias.prefer);
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
               More than one report is named {alias.name}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
               This is an old link, which names a report by its file name alone.
               Open one by its path:
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
   return <>{children}</>;
}
