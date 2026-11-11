// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   useRouterClickHandler,
   type NavigationClick,
} from "@malloy-publisher/sdk";
import { useCallback } from "react";
import { withVersionSegment } from "./packageRoute";

/**
 * Router navigation for links that point inside a package without a version
 * (the package page's rows, a report's prose links): each one gains the
 * version on screen, so a pinned version stays pinned from page to page. A
 * link elsewhere is followed as written. Honours cmd/ctrl-click as
 * `useRouterClickHandler` does.
 */
export function usePackageLinkNavigate(
   environmentName: string,
   packageName: string,
   versionSegment: string | undefined,
) {
   const navigate = useRouterClickHandler();
   return useCallback(
      (to: string, event?: NavigationClick) =>
         navigate(
            withVersionSegment(
               to,
               environmentName,
               packageName,
               versionSegment,
            ),
            event,
         ),
      [navigate, environmentName, packageName, versionSegment],
   );
}
