// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo, type MouseEvent } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { logReportEvent } from "../../../utils/consoleTelemetry";
import { documentPath } from "../../common/documentRoutes";
import {
   LATEST_SEGMENT,
   packagePath,
   withVersionSegment,
} from "../../common/packageRoute";
import { usePackageRoute } from "../../common/usePackageRoute";
import {
   encodeResourceUri,
   Package,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";

function PackagePage() {
   const {
      environmentName,
      packageName,
      versionSegment,
      versionId,
      versioned,
   } = usePackageRoute({ always: true });
   const { search } = useLocation();
   const routerNavigate = useRouterClickHandler();
   // Every link the package page builds points inside the package without a
   // version; here it learns the one on screen, so a pinned version stays
   // pinned from the page to its reports, models and apps.
   const navigate = useCallback(
      (to: string, event?: MouseEvent) =>
         routerNavigate(
            withVersionSegment(
               to,
               environmentName ?? "",
               packageName ?? "",
               versionSegment,
            ),
            event,
         ),
      [routerNavigate, environmentName, packageName, versionSegment],
   );
   // What the New report dialog created, or why it could not.
   const onEvent = useMemo(
      () =>
         logReportEvent({
            environmentName: environmentName ?? "",
            packageName: packageName ?? "",
         }),
      [environmentName, packageName],
   );
   if (!environmentName) {
      return (
         <div>
            <h2>Missing environment name</h2>
         </div>
      );
   } else if (!packageName) {
      return (
         <div>
            <h2>Missing package name</h2>
         </div>
      );
   }
   // A versioned package's bare route is its latest version's: say so in the
   // URL, as `/<env>/<pkg>/latest`. A replace, since canonicalizing an address
   // is not a step to go back to.
   if (versioned && versionSegment === undefined) {
      return (
         <Navigate
            to={`${packagePath(environmentName, packageName, LATEST_SEGMENT)}${search}`}
            replace
         />
      );
   }
   const resourceUri = encodeResourceUri({
      environmentName,
      packageName,
      versionId,
   });
   return (
      <Package
         onClickPackageFile={navigate}
         // The Console decides where reading and editing live: the
         // report's path route, and the builder at that route with `?edit`.
         onOpenDocument={({ path, mode }, event) =>
            navigate(
               documentPath(environmentName, packageName, path, mode),
               event,
            )
         }
         onSelectVersion={(id, event) =>
            routerNavigate(packagePath(environmentName, packageName, id), event)
         }
         resourceUri={resourceUri}
         onEvent={onEvent}
      />
   );
}
export default PackagePage;
