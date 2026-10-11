// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   isDrillTargetPath,
   reportRoute,
   resolveDrillTarget,
   useReportListing,
   useRouterClickHandler,
   type DrillNavigation,
} from "@malloy-publisher/sdk";
import { useCallback } from "react";

/**
 * Route a `# drill` to another report, for the Console's URL scheme.
 *
 * The tag's `to=` is a package path from the root (`sales/overview` is
 * `sales/overview.malloy`, and a bare name is a file at the root), resolved
 * against the package's report listing by the server's rule. The click lands
 * on the report's path route. A target nothing resolves still navigates, to
 * the path as written, where the page says nothing is there. A target that is
 * not a plain package path (`../../x`, `/x`, `a//b`) goes nowhere: a route
 * built from it could leave the package, and the package's load already
 * reports it as an unresolved drill.
 *
 * A drill IS a navigation step, so unlike a control change it pushes history:
 * Back returns to the report the reader drilled from. `useRouterClickHandler`
 * also honours cmd/ctrl-click by opening the destination in a new tab.
 *
 * Lives in the app rather than the SDK because the route shape is the
 * Console's; a host with its own routes writes its own.
 */
export function useDrillNavigate(environmentName: string, packageName: string) {
   const navigate = useRouterClickHandler();
   // Shared with every other reader of the listing, so this is usually already cached.
   const { reports } = useReportListing(environmentName, packageName);
   return useCallback(
      (target: DrillNavigation, event?: MouseEvent) => {
         const written = target.dashboard;
         if (!isDrillTargetPath(written)) return;
         const resolved = resolveDrillTarget(written, reports);
         const route = reportRoute(
            environmentName,
            packageName,
            resolved.kind === "path" ? resolved.report.path : written,
         );
         const query = new URLSearchParams(target.givens).toString();
         navigate(route + (query ? `?${query}` : ""), event);
      },
      [navigate, environmentName, packageName, reports],
   );
}
