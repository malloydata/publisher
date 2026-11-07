// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The levels above a package: a `get_context` call that names no package
 * lists environments, or one environment's packages, so an agent can find
 * data with this one tool. A level with a single choice is entered rather
 * than listed, with a warning, so a server serving one package answers a
 * call with no arguments with that package's sources.
 *
 * The shapes are the hosted retrieval API's (`listing`, `environments`,
 * `packages`, `package_info`), without its organizations and workspaces,
 * which a Publisher does not have. One difference: hosted can search every
 * package of an environment at once, but Publisher indexes each package on
 * its own, so a search needs a package, named in `scopes` or entered because
 * it is the only one.
 */

import { InvalidArgumentError } from "../../errors";
import { EnvironmentStore } from "../../service/environment_store";
import { FCJ_MAX_CHARS } from "../context/pages";
import type { GetContextParams, ScopedParams } from "./get_context_request";
import type { PackageInfo } from "./get_context_types";
import { oneLineSummary } from "./package_entities";
import {
   isServed,
   readPackageCatalog,
   type CatalogEnvironment,
} from "./package_catalog";

export type LevelOutcome =
   /** A listing of environments or packages, answered here. */
   | { kind: "listing"; environment?: string; payload: Record<string, unknown> }
   | { kind: "error"; environment?: string; error: InvalidArgumentError }
   /** A package is named (or entered): search or list it as before. */
   | {
        kind: "scoped";
        params: ScopedParams;
        warnings: string[];
        /**
         * The response carries `package_info`: a listing of one whole
         * package. Built from the package itself (packageInfoOf), so naming
         * one package never reads the rest of the server.
         */
        withPackageInfo?: true;
     };

/** Most names a refusal lists before saying how many more there are. */
const NAMES_IN_FIX = 20;

function names(list: readonly string[]): string {
   const shown = list.slice(0, NAMES_IN_FIX).join(", ");
   return list.length > NAMES_IN_FIX
      ? `${shown}, and ${list.length - NAMES_IN_FIX} more`
      : shown;
}

/**
 * `package_info` for a package, from its own description. One too long to
 * send beside its sources is cut to its one-line summary, and the warning
 * says where the rest is.
 */
export function packageInfoOf(
   environment: string,
   packageName: string,
   description: string | undefined,
): { info: PackageInfo; warning?: string } {
   const info: PackageInfo = { environment, package: packageName };
   if (!description) return { info };
   if (description.length > FCJ_MAX_CHARS / 2) {
      const line = oneLineSummary(description);
      return {
         info: { ...info, ...(line ? { one_line_summary: line } : {}) },
         warning: `The package description is ${description.length.toLocaleString("en-US")} characters, too long to send beside its sources, so package_info carries its one-line summary. list_packages returns it in full.`,
      };
   }
   return { info: { ...info, description } };
}

/** What makes the catalog an incomplete view of the server. */
function incompleteness(envs: readonly CatalogEnvironment[]): string[] {
   const out: string[] = [];
   const unreadable = envs.filter((e) => e.error).map((e) => e.name);
   if (unreadable.length > 0) {
      out.push(
         `${unreadable.length === 1 ? "environment" : "environments"} ${names(unreadable)} could not be listed`,
      );
   }
   const loading = envs.flatMap((env) =>
      env.packages.filter((p) => p.loading).map((p) => `${env.name}/${p.name}`),
   );
   if (loading.length > 0) {
      out.push(
         `${names(loading)} ${loading.length === 1 ? "is" : "are"} still loading`,
      );
   }
   return out;
}

/** Warnings for packages a listing leaves out or that serve an old model. */
function healthWarnings(
   envs: readonly CatalogEnvironment[],
   stale: boolean,
   /** `env/package` left out of the stale list: an entered package, whose own stale note the response already carries. */
   except?: string,
): string[] {
   const out: string[] = [];
   for (const env of envs) {
      if (env.error) {
         out.push(
            `Environment ${env.name}'s packages could not be listed: ${env.error}. Its packages are not missing; the read failed.`,
         );
      }
   }
   const failed = envs.flatMap((env) =>
      env.packages
         .filter((p) => !isServed(p) && !p.loading)
         .map((p) => `${env.name}/${p.name}`),
   );
   if (failed.length > 0) {
      out.push(
         `${failed.length === 1 ? "A package" : `${failed.length} packages`} failed to load and ${failed.length === 1 ? "is" : "are"} not listed: ${names(failed)}. list_packages and get_status say why.`,
      );
   }
   const loading = envs.flatMap((env) =>
      env.packages.filter((p) => p.loading).map((p) => `${env.name}/${p.name}`),
   );
   if (loading.length > 0) {
      out.push(
         `${names(loading)} ${loading.length === 1 ? "is" : "are"} still loading and not listed yet. Ask again in a moment.`,
      );
   }
   if (stale) {
      const old = envs.flatMap((env) =>
         env.packages
            .filter((p) => p.stale)
            .map((p) => `${env.name}/${p.name}`)
            .filter((name) => name !== except),
      );
      if (old.length > 0) {
         out.push(
            `${names(old)} ${old.length === 1 ? "serves" : "serve"} the model compiled before a reload that failed, not the files on disk. get_status says why.`,
         );
      }
   }
   return out;
}

/**
 * The longest prefix of `entries` whose JSON fits FCJ_MAX_CHARS, the budget
 * every get_context response pages by. A listing is not paged, so the rest
 * are named in a warning instead.
 */
function withinBudget<T>(entries: readonly T[]): T[] {
   let size = 0;
   const kept: T[] = [];
   for (const e of entries) {
      size += JSON.stringify(e).length + 1;
      if (size > FCJ_MAX_CHARS && kept.length > 0) break;
      kept.push(e);
   }
   return kept;
}

/** The targets a call with none lists: a source's fields, or a package's sources. */
function listingTargets(
   scope: NonNullable<GetContextParams["scopes"]>[number],
): ScopedParams["search_targets"] {
   if (scope.source || scope.entity_name) {
      return (["dimension", "measure", "view", "join"] as const).map(
         (target_type) => ({ target_type }),
      );
   }
   return [{ target_type: "source" as const }];
}

const isWholePackage = (
   scope: NonNullable<GetContextParams["scopes"]>[number],
) => !scope.source && !scope.model_path && !scope.entity_name;

/**
 * Decide what a call is about. A call that names a package goes on as
 * before (with default listing targets when it sends none), and never reads
 * the rest of the server; one that does not is answered with a listing, or
 * enters the only choice there is.
 */
export async function resolveLevel(
   params: GetContextParams,
   environmentStore: EnvironmentStore,
): Promise<LevelOutcome> {
   const environment = params.environment?.trim() || undefined;
   const scope = params.scopes?.[0];

   if (scope) {
      if (environment && environment !== scope.environment) {
         return {
            kind: "error",
            environment,
            error: new InvalidArgumentError(
               `Invalid environment: "${environment}" contradicts scopes[0].environment "${scope.environment}". Fix: drop \`environment\`, or make the two the same.`,
            ),
         };
      }
      return {
         kind: "scoped",
         params: {
            ...params,
            scopes: params.scopes!,
            search_targets: params.search_targets ?? listingTargets(scope),
         },
         warnings: [],
         ...(!params.search_targets && isWholePackage(scope)
            ? { withPackageInfo: true as const }
            : {}),
      };
   }

   const catalog = await readPackageCatalog(environmentStore, {
      includeLoading: true,
   });
   const served = (env: CatalogEnvironment) =>
      env.packages.filter(isServed).map((p) => p.name);
   const searching = (params.search_targets ?? []).length > 0;
   const ranked = (params.search_targets ?? []).some((t) =>
      Boolean(t.search_text?.trim()),
   );

   /**
    * Enter `env`'s lone package, as if the call had named it. `offset` is
    * kept: a listing of environments or packages is never paged, so an offset
    * can only have come from this package's own results (a page of a search,
    * or of its sources).
    *
    * A search is entered only when the catalog is complete: with an
    * environment that could not be listed, or a package still on its first
    * load, the lone package that loaded may not be the one meant, and a miss
    * there would read as "not modelled". A listing is entered either way,
    * since its warnings say what is missing.
    */
   const enter = (
      env: CatalogEnvironment,
      why: string,
      seen: readonly CatalogEnvironment[],
   ): LevelOutcome => {
      const pkgName = served(env)[0];
      const gaps = incompleteness(seen);
      if (ranked && gaps.length > 0) {
         return {
            kind: "error",
            ...(environment ? { environment } : {}),
            error: new InvalidArgumentError(
               `Only ${env.name}/${pkgName} has loaded, but ${gaps.join(" and ")}, so it may not be the package to search. Fix: name the package in scopes, e.g. scopes: [{"environment": "${env.name}", "package": "${pkgName}"}], or ask again once the server has finished loading.`,
            ),
         };
      }
      const what = ranked
         ? " and searched it"
         : searching
           ? ", and lists what the targets name in it"
           : ", and lists its sources";
      return {
         kind: "scoped",
         params: {
            ...params,
            scopes: [{ environment: env.name, package: pkgName }],
            search_targets: params.search_targets ?? [
               { target_type: "source" },
            ],
         },
         warnings: [
            `${why}, so this call entered ${env.name}/${pkgName}${what}. Name it in scopes from now on.`,
            // A package that failed or is loading would otherwise read as not
            // there: "the only package" is only true of the ones that loaded.
            ...healthWarnings(seen, true, `${env.name}/${pkgName}`),
         ],
         ...(!params.search_targets ? { withPackageInfo: true as const } : {}),
      };
   };

   const refuseOffset = (level: string): LevelOutcome | undefined =>
      params.offset
         ? {
              kind: "error",
              ...(environment ? { environment } : {}),
              error: new InvalidArgumentError(
                 `Invalid offset: a listing of ${level} is not paged; it is returned whole. Fix: drop \`offset\`.`,
              ),
           }
         : undefined;

   /**
    * List `env`'s packages. `report` is the environments whose failures the
    * warnings name: every environment when the call named none, so a failure
    * elsewhere is not hidden behind the one environment that has packages.
    */
   const listPackages = (
      env: CatalogEnvironment,
      report: readonly CatalogEnvironment[],
      entered?: string,
   ): LevelOutcome => {
      const refused = refuseOffset("packages");
      if (refused) return refused;
      const all = env.packages
         .filter(isServed)
         .map((p) => {
            const line = p.description
               ? oneLineSummary(p.description)
               : undefined;
            return {
               environment: env.name,
               package: p.name,
               ...(line ? { one_line_summary: line } : {}),
            };
         })
         .sort((a, b) => (a.package < b.package ? -1 : 1));
      const packages = withinBudget(all);
      const warnings = [
         ...(entered ? [entered] : []),
         ...healthWarnings(report, true),
         ...(packages.length < all.length
            ? [
                 `Returned ${packages.length} of ${all.length} packages: the rest do not fit in one response. Name a package in scopes to list its sources, or call list_packages for every package.`,
              ]
            : []),
      ];
      return {
         kind: "listing",
         environment: env.name,
         payload: {
            sources: [],
            listing: "packages",
            packages,
            total_available: all.length,
            returned: packages.length,
            ...(warnings.length > 0 ? { warnings } : {}),
         },
      };
   };

   if (environment) {
      const env = catalog.find((e) => e.name === environment);
      if (!env) {
         return {
            kind: "error",
            environment,
            error: new InvalidArgumentError(
               `Invalid environment: "${environment}" is not served here. Fix: use one of ${names(catalog.map((e) => e.name)) || "(none: this server serves no environment)"}, or omit environment to list them.`,
            ),
         };
      }
      const pkgs = served(env);
      if (pkgs.length === 1) {
         return enter(env, `${env.name} serves one package`, [env]);
      }
      if (searching) {
         const why = env.error
            ? `its packages could not be listed: ${env.error}`
            : env.packages.some((p) => p.loading)
              ? "its packages are still loading"
              : "it serves no package that loaded";
         return {
            kind: "error",
            environment,
            error: new InvalidArgumentError(
               pkgs.length > 1
                  ? `A search needs one package: Publisher indexes each package on its own. Fix: add scopes: [{"environment": "${env.name}", "package": "<one of: ${names(pkgs)}>"}].`
                  : `A search needs one package, and environment "${env.name}" has none to search: ${why}. Fix: call get_status or list_packages for load errors, or search another environment.`,
            ),
         };
      }
      return listPackages(env, [env]);
   }

   const withPackages = catalog.filter((e) => served(e).length > 0);
   const total = withPackages.reduce((n, e) => n + served(e).length, 0);
   if (total === 1) {
      return enter(withPackages[0], "This server serves one package", catalog);
   }
   if (searching) {
      const all = withPackages.flatMap((e) =>
         served(e).map((p) => `${e.name}/${p}`),
      );
      return {
         kind: "error",
         error: new InvalidArgumentError(
            all.length === 0
               ? "A search needs one package, and this server serves none. Fix: call get_status for load errors, or list_packages."
               : `A search needs one package: Publisher indexes each package on its own. Fix: add scopes: [{"environment": "<env>", "package": "<package>"}], one of ${names(all)}.`,
         ),
      };
   }
   if (withPackages.length === 1) {
      const env = withPackages[0];
      return listPackages(
         env,
         catalog,
         `Only environment ${env.name} has packages that loaded, so this lists its packages. Name it in environment from now on.`,
      );
   }

   const refused = refuseOffset("environments");
   if (refused) return refused;
   const environments = withPackages.map((e) => ({
      environment: e.name,
      packages: served(e).length,
   }));
   const warnings = healthWarnings(catalog, false);
   if (environments.length === 0 && warnings.length === 0) {
      warnings.push(
         "This server serves no packages. Call get_status: an emptyReason or loadErrors there says why.",
      );
   }
   return {
      kind: "listing",
      payload: {
         sources: [],
         listing: "environments",
         environments,
         total_available: environments.length,
         returned: environments.length,
         ...(warnings.length > 0 ? { warnings } : {}),
      },
   };
}
