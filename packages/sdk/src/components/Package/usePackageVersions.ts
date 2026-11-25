// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";
import type { PackageVersion } from "../../client";
import { useServer } from "../ServerProvider";

/**
 * The query key of a package's version listing, shared by every reader. The
 * server comes last, as every other SDK query carries it, so two servers
 * with the same environment and package names never share a list.
 */
export const packageVersionsQueryKey = (
   environmentName: string,
   packageName: string,
   server: string,
) => ["packageVersions", environmentName, packageName, server] as const;

export interface PackageVersions {
   /** Highest first, archived ones included. Empty for a package with none. */
   versions: PackageVersion[];
   /** The version a request that names none is served from, if there is one. */
   latest: string | undefined;
   /** True until the listing has answered once. */
   isPending: boolean;
   /**
    * True when the listing failed, so whether the package has versions is
    * unknown. A package with none is not a failure.
    */
   isError: boolean;
   /** Ask for the listing again. */
   refetch: () => void;
}

/**
 * A package's versions, as the server lists them. A 404 (a package with no
 * versions, or a server that predates them) and a 501 (a server, or a proxy
 * in front of one, that does not serve the route) both mean no versions; any
 * other failure is thrown, so a reader can tell "none" from "unknown".
 */
async function listVersions(
   apiClients: ReturnType<typeof useServer>["apiClients"],
   environmentName: string,
   packageName: string,
): Promise<PackageVersion[]> {
   try {
      const response = await apiClients.versions.listPackageVersions(
         environmentName,
         packageName,
      );
      return response.data;
   } catch (error) {
      const status = (error as { response?: { status?: number } })?.response
         ?.status;
      if (status === 404 || status === 501) return [];
      throw error;
   }
}

/**
 * A package's published versions.
 *
 * A package with none — one in a watched environment, or any with versioning
 * off — and a server that predates versions read as no versions, so the page draws
 * exactly as it did before versions existed. A listing that failed reads as
 * no versions too, but says so with `isError`: until it answers, nothing may
 * assume the package can be edited.
 */
export function usePackageVersions(
   environmentName: string,
   packageName: string,
   options: { enabled?: boolean } = {},
): PackageVersions {
   const { apiClients, server } = useServer();
   const enabled =
      (options.enabled ?? true) && environmentName !== "" && packageName !== "";
   const query = useQuery({
      queryKey: packageVersionsQueryKey(environmentName, packageName, server),
      queryFn: () => listVersions(apiClients, environmentName, packageName),
      enabled,
      staleTime: 30_000,
      retry: false,
   });
   const { refetch: refetchQuery } = query;
   const refetch = useCallback(() => void refetchQuery(), [refetchQuery]);
   const versions = query.data ?? [];
   return {
      versions,
      latest: versions.find((version) => version.latest)?.id,
      isPending: enabled && query.isPending,
      isError: enabled && query.isError,
      refetch,
   };
}

/**
 * Read a package's versions now, never from the cache, and keep the answer
 * for every other reader: what a change to a version is decided against,
 * since another tab or client may have changed them since the page listed
 * them. Rejects when the listing fails.
 */
export function useFetchPackageVersions(
   environmentName: string,
   packageName: string,
) {
   const { apiClients, server } = useServer();
   const queryClient = useQueryClient();
   return useCallback(
      () =>
         queryClient.fetchQuery({
            queryKey: packageVersionsQueryKey(
               environmentName,
               packageName,
               server,
            ),
            queryFn: () =>
               listVersions(apiClients, environmentName, packageName),
            staleTime: 0,
            retry: false,
         }),
      [queryClient, apiClients, server, environmentName, packageName],
   );
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
