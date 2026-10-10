// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   useRouterClickHandler,
   type DrillNavigation,
} from "@malloy-publisher/sdk";
import { useCallback } from "react";

/**
 * Route a `# drill` to another dashboard, for the Console's URL scheme.
 *
 * A drill IS a navigation step, so unlike a control change it pushes history:
 * Back returns to the document the reader drilled from. `useRouterClickHandler`
 * also honours cmd/ctrl-click by opening the destination in a new tab.
 *
 * Every segment is encoded. The destination slug comes from a `# drill` tag
 * naming a filename, so it can hold a character that would read as structure in
 * a path or start the query string early; the server percent-encodes the same
 * name when it publishes a dashboard's own URL. The environment and package
 * names are validated on the way in and hold nothing worth encoding, but a
 * name from the URL is not something validated, so they are encoded on the
 * same principle.
 *
 * Lives in the app rather than the SDK because the route shape is the
 * Console's; a host with its own routes writes its own.
 */
export function useDrillNavigate(environmentName: string, packageName: string) {
   const navigate = useRouterClickHandler();
   return useCallback(
      (target: DrillNavigation, event?: MouseEvent) => {
         const query = new URLSearchParams(target.givens).toString();
         const env = encodeURIComponent(environmentName);
         const pkg = encodeURIComponent(packageName);
         const slug = encodeURIComponent(target.dashboard);
         navigate(
            `/${env}/${pkg}/dashboards/${slug}` + (query ? `?${query}` : ""),
            event,
         );
      },
      [navigate, environmentName, packageName],
   );
}
