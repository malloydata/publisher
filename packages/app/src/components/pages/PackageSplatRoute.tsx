// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React, { useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
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
   const navigate = useNavigate();
   // A versioned package's path with no version (an old link, a typed URL) is
   // its latest version's: say so in the URL, as the bare package route does,
   // so the page reads as a published version and offers no edit. The page
   // opens at once and stays mounted while its address is replaced, so
   // nothing it asked for is asked again.
   const latestPath =
      versioned && versionSegment === undefined
         ? withVersionSegment(
              pathname,
              environmentName ?? "",
              packageName ?? "",
              LATEST_SEGMENT,
           )
         : undefined;
   useEffect(() => {
      // The same path back means the segment could not be placed; never loop.
      if (latestPath !== undefined && latestPath !== pathname) {
         navigate(`${latestPath}${search}${hash}`, { replace: true });
      }
   }, [latestPath, pathname, search, hash, navigate]);
   // Waits only on a version-shaped first segment, and only for the listing
   // that says whether this package has versions at all.
   if (deciding) return null;
   if (
      versionSegment !== undefined &&
      (path === undefined || path === "" || path === "/")
   ) {
      return <PackagePage />;
   }
   return <ModelPage />;
}
