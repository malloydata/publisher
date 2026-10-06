// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useParams } from "react-router-dom";
import {
   documentRoute,
   encodeResourceUri,
   Package,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";

function PackagePage() {
   const { environmentName, packageName } = useParams();
   const navigate = useRouterClickHandler();
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
            // document's route, and the builder one segment under it.
            onOpenDocument={({ kind, slug, mode }, event) =>
               navigate(
                  `${documentRoute(environmentName, packageName, kind, slug)}${mode === "edit" ? "/edit" : ""}`,
                  event,
               )
            }
            resourceUri={resourceUri}
         />
      );
   }
}
export default PackagePage;
