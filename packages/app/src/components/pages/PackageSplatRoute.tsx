// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React from "react";
import { Navigate, useLocation } from "react-router-dom";
import { LATEST_SEGMENT, withVersionSegment } from "../common/packageRoute";
import { usePackageRoute } from "../common/usePackageRoute";

const ModelPage = React.lazy(() => import("./ModelPage/ModelPage"));
const PackagePage = React.lazy(() => import("./PackagePage/PackagePage"));

/**
 * `/:environmentName/:packageName/*`: a path inside a package, or, for a
 * package with published versions, a version (`/<env>/<pkg>/1.2.0`) and a
 * path inside it. A bare version is the package page at that version;
 * anything else opens as a path, as it always has.
 */
export default function PackageSplatRoute() {
   const {
      environmentName,
      packageName,
      versionSegment,
      path,
      versioned,
      deciding,
   } = usePackageRoute({ always: true });
   const { pathname, search, hash } = useLocation();
   // Waits for the listing that says whether this package has versions.
   if (deciding) return null;
   // A versioned package's path with no version (an old link, a typed URL) is
   // its latest version's: say so in the URL, as the bare package route does,
   // so the page reads as a published version and offers no edit.
   const latestPath =
      versioned && versionSegment === undefined
         ? withVersionSegment(
              pathname,
              environmentName ?? "",
              packageName ?? "",
              LATEST_SEGMENT,
           )
         : undefined;
   // The same path back means the segment could not be placed; never loop.
   if (latestPath !== undefined && latestPath !== pathname) {
      return <Navigate to={`${latestPath}${search}${hash}`} replace />;
   }
   if (
      versionSegment !== undefined &&
      (path === undefined || path === "" || path === "/")
   ) {
      return <PackagePage />;
   }
   return <ModelPage />;
}
