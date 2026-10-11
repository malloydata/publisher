// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   reportRoute,
   reportRoutePath,
   resolveDrillTarget,
   useReportListing,
   useRouterClickHandler,
   type DrillNavigation,
} from "@malloy-publisher/sdk";
import { useCallback } from "react";

/**
 * Route a `# drill` to another report, for the Console's URL scheme.
 *
 * The tag's `to=` is resolved against the package's report listing by the
 * server's rule: a package path from the root (`sales/overview` is
 * `sales/overview.malloy`), and only for a bare name with no report at that
 * path, the deprecated basename fallback, which prefers a grid report. The
 * click then lands on the report's path route. A target nothing resolves still
 * navigates, to the path as written, where the page says nothing is there.
 *
 * A drill IS a navigation step, so unlike a control change it pushes history:
 * Back returns to the report the reader drilled from. `useRouterClickHandler`
 * also honours cmd/ctrl-click by opening the destination in a new tab.
 *
 * Every segment is encoded: a target names files, and a character in one could
 * otherwise read as structure in a path or start the query string early.
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
         const query = new URLSearchParams(target.givens).toString();
         const env = encodeURIComponent(environmentName);
         const pkg = encodeURIComponent(packageName);
         const written = target.dashboard.replace(/^\.?\//, "");
         const resolved = resolveDrillTarget(written, reports);
         // Unresolved here (a listing not yet in, or nothing there): a bare
         // name goes by the deprecated `dashboards/<name>` alias, which resolves
         // it on arrival and says which reports share it when several do; a
         // path goes to itself.
         const route =
            resolved.kind !== "unresolved"
               ? reportRoute(env, pkg, resolved.report.path)
               : written.includes("/")
                 ? `/${env}/${pkg}/${reportRoutePath(written)}`
                 : `/${env}/${pkg}/dashboards/${encodeURIComponent(written)}`;
         navigate(route + (query ? `?${query}` : ""), event);
      },
      [navigate, environmentName, packageName, reports],
   );
}
