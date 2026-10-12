// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useMemo } from "react";
import { useParams } from "react-router-dom";
import { logReportEvent } from "../../../utils/consoleTelemetry";
import { documentPath } from "../../common/documentRoutes";
import {
   encodeResourceUri,
   Package,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";

function PackagePage() {
   const { environmentName, packageName } = useParams();
   const navigate = useRouterClickHandler();
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
   } else {
      const resourceUri = encodeResourceUri({
         environmentName,
         packageName,
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
            resourceUri={resourceUri}
            onEvent={onEvent}
         />
      );
   }
}
export default PackagePage;
