// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React from "react";
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
   const { versionSegment, path, deciding } = usePackageRoute();
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
