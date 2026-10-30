// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { usePackageVersions } from "@malloy-publisher/sdk";
import { useParams } from "react-router-dom";
import { splitVersionSegment, versionIdOf } from "./packageRoute";

export interface PackageRoute {
   environmentName: string | undefined;
   packageName: string | undefined;
   /** The version segment the route carries (`latest` or an id), if any. */
   versionSegment: string | undefined;
   /** The `versionId` to send: undefined for `latest` and for no version. */
   versionId: string | undefined;
   /** The path inside the package, after any version segment. */
   path: string | undefined;
   /** Whether the package has published versions. */
   versioned: boolean;
   /**
    * True while the route waits on the version listing: to read a
    * version-shaped first segment, or, with `always`, to learn whether the
    * package has versions at all.
    */
   deciding: boolean;
}

/**
 * The package route under the cursor, read the way the Console routes
 * versions (`packageRoute.ts`). The first segment after the package is a
 * version only when it is version-shaped AND the package has versions; the
 * listing that decides is fetched only for such a segment, so an ordinary
 * report route costs no extra request.
 */
export function usePackageRoute(
   options: { always?: boolean } = {},
): PackageRoute {
   const params = useParams();
   const environmentName = params.environmentName;
   const packageName = params.packageName;
   const splat = params["*"];
   const candidate =
      splat === undefined ? undefined : splitVersionSegment(splat);
   const listed = options.always === true || candidate !== undefined;
   const { versions, isPending } = usePackageVersions(
      environmentName ?? "",
      packageName ?? "",
      { enabled: listed },
   );
   const versioned = versions.length > 0;
   const version = candidate && versioned ? candidate : undefined;
   return {
      environmentName,
      packageName,
      versionSegment: version?.segment,
      versionId: versionIdOf(version?.segment),
      path: version ? version.rest : splat,
      versioned,
      deciding: listed && isPending,
   };
}
