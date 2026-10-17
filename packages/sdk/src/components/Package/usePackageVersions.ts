// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import type { PackageVersion } from "../../client";
import { useServer } from "../ServerProvider";

/** The query key of a package's version listing, shared by every reader. */
export const packageVersionsQueryKey = (
   environmentName: string,
   packageName: string,
) => ["packageVersions", environmentName, packageName] as const;

export interface PackageVersions {
   /** Highest first, archived ones included. Empty for a package with none. */
   versions: PackageVersion[];
   /** The version a request that names none is served from, if there is one. */
   latest: string | undefined;
   /** True until the listing has answered once. */
   isPending: boolean;
}

/**
 * A package's published versions.
 *
 * A package with none — versioning off, a package loaded from configuration —
 * a server that predates versions, and a listing that failed all read as no
 * versions, so the page draws exactly as it did before versions existed.
 */
export function usePackageVersions(
   environmentName: string,
   packageName: string,
   options: { enabled?: boolean } = {},
): PackageVersions {
   const { apiClients } = useServer();
   const enabled =
      (options.enabled ?? true) && environmentName !== "" && packageName !== "";
   const query = useQuery({
      queryKey: packageVersionsQueryKey(environmentName, packageName),
      queryFn: async () => {
         try {
            const response = await apiClients.versions.listPackageVersions(
               environmentName,
               packageName,
            );
            return response.data;
         } catch {
            return [] as PackageVersion[];
         }
      },
      enabled,
      staleTime: 30_000,
      retry: false,
   });
   const versions = query.data ?? [];
   return {
      versions,
      latest: versions.find((version) => version.latest)?.id,
      isPending: enabled && query.isPending,
   };
}

/**
 * Refresh everything drawn for one package: its version listing and every
 * query keyed `[kind, environment, package, …]`. Moving `latest` or archiving
 * a version changes what a request that names no version is served from, so
 * nothing cached for the package can be trusted afterwards.
 */
export function useInvalidatePackage(
   environmentName: string,
   packageName: string,
) {
   const queryClient = useQueryClient();
   return useCallback(
      () =>
         queryClient.invalidateQueries({
            predicate: (query) =>
               query.queryKey[1] === environmentName &&
               query.queryKey[2] === packageName,
         }),
      [queryClient, environmentName, packageName],
   );
}
