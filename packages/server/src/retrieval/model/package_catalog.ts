// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { EnvironmentStore } from "../../service/environment_store";
import { logger } from "../../logger";

/** One package as the catalog reports it. */
interface CatalogPackage {
   name: string;
   description?: string;
   /**
    * Set when the package is not queryable as its files say: with `stale`, it
    * serves the model compiled before its last failed reload; without, it
    * failed to load and is not served at all.
    */
   error?: string;
   stale?: true;
   /**
    * On its first load: registered, not serving yet. Listed only when the
    * catalog is read with `includeLoading`.
    */
   loading?: true;
}

/** One environment and its packages. */
export interface CatalogEnvironment {
   name: string;
   /** Its packages could not be listed: `packages` is empty because the read failed. */
   error?: string;
   packages: CatalogPackage[];
}

/**
 * Every environment this server serves and the packages in each, with the
 * health facts a plain listing hides: a package that failed to load (absent
 * otherwise) and one serving a stale model. `list_packages` returns it as is;
 * `get_context` lists from it when a call names no package.
 */
export async function readPackageCatalog(
   environmentStore: EnvironmentStore,
   /**
    * Also list packages on their first load, marked `loading`. `get_context`
    * asks for them, so "the only package" is never said while another is
    * still loading; `list_packages` does not, and its output is unchanged.
    */
   { includeLoading = false }: { includeLoading?: boolean } = {},
): Promise<CatalogEnvironment[]> {
   const environments = await environmentStore.listEnvironments();
   const results: CatalogEnvironment[] = [];
   for (const env of environments) {
      const name = env.name;
      if (!name) continue;
      try {
         const environment = await environmentStore.getEnvironment(name, false);
         const staleErrors = environment.getStaleCompileErrors();
         const packages: CatalogPackage[] = [];
         for (const pkg of await environment.listPackages({
            includeLoading,
         })) {
            if (!pkg.name) continue;
            // A first load: a reload is `loading` too, but its copy serves.
            if (pkg.status?.loading && pkg.status.serving === false) {
               packages.push({ name: pkg.name, loading: true });
               continue;
            }
            const stale = staleErrors.get(pkg.name);
            packages.push({
               name: pkg.name,
               ...(pkg.description ? { description: pkg.description } : {}),
               ...(stale ? { error: stale.message, stale: true as const } : {}),
            });
         }
         for (const [failed, message] of environment.getFailedPackages()) {
            packages.push({ name: failed, error: message });
         }
         results.push({ name, packages });
      } catch (error) {
         // One unreachable environment must not hide the others, but must be
         // reported too: an empty list would read as an empty environment.
         const message = error instanceof Error ? error.message : String(error);
         logger.warn("[MCP] listing an environment's packages failed", {
            environmentName: name,
            error: message,
         });
         results.push({ name, error: message, packages: [] });
      }
   }
   return results;
}

/** A package that serves queries: loaded, stale or not. One that failed or is on its first load does not. */
export function isServed(pkg: CatalogPackage): boolean {
   if (pkg.loading) return false;
   return !pkg.error || pkg.stale === true;
}
