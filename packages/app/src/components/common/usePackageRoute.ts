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
   /** True while the route waits on the version listing to read its first segment. */
   deciding: boolean;
   /**
    * The listing that reads the first segment failed, so whether it names a
    * version is unknown. The route is neither opened as a version nor as a
    * path, which would read the wrong file; `retry` asks again.
    */
   undecided: boolean;
   /** Ask for the version listing again. */
   retry: () => void;
   /**
    * The listing has said the package has no versions, so its files may be
    * written. False for a versioned package, while the listing loads, and
    * when it failed. Read with `always`: without it, a path that is not
    * version-shaped never asks, and this is just `!versioned`.
    */
   writable: boolean;
}

/**
 * The package route under the cursor, read the way the Console routes
 * versions (`packageRoute.ts`). The first segment after the package is a
 * version only when it is version-shaped AND the package has versions. The
 * listing that decides is fetched for such a segment, and, with `always`,
 * for every route: the splat route needs it to move a versioned package's
 * path with no version onto `latest`, so every page under a package costs
 * one listing request, shared by every reader and cached for 30 s.
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
   const { versions, isPending, isError, refetch } = usePackageVersions(
      environmentName ?? "",
      packageName ?? "",
      { enabled: options.always === true || candidate !== undefined },
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
      deciding: candidate !== undefined && isPending,
      undecided: candidate !== undefined && isError,
      retry: refetch,
      writable: !versioned && !isPending && !isError,
   };
}
