// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Modules an operator asks the server to import before it does anything else.
 *
 * `@malloydata/malloy` keeps process-wide registries for connection types
 * (`registerConnectionType`) and dialects (`registerDialect`), and every
 * `@malloydata/db-*` package registers itself as a side effect of being
 * imported. A driver packaged outside this server plugs in the same way: it
 * has to be imported, in this process, before a package that names its
 * connection type loads. `PUBLISHER_PRELOAD_MODULES` is where an operator
 * names those imports.
 *
 * Registration is per JS realm. The server compiles packages in
 * worker_threads (package_load_worker.ts), each its own realm with its own
 * copy of the registries, so the same list is imported by every entrypoint
 * that compiles or runs Malloy: `server.ts` and the load worker both call
 * `preloadModulesFromEnv()` before they accept work. A module that only runs
 * in the main thread would register a dialect the compiler never sees.
 *
 * A preload runs after the built-in drivers have registered, so a module
 * that registers an existing type name replaces the built-in for that name;
 * both registries are last-writer-wins. That is deliberate and worth knowing.
 */

export const PRELOAD_MODULES_ENV = "PUBLISHER_PRELOAD_MODULES";

/**
 * Split the env value into module specifiers: comma separated, whitespace
 * trimmed, empties dropped, order kept. Duplicates are kept too — importing
 * a module twice is a no-op, and collapsing them would hide a list that was
 * assembled wrong.
 */
export function parsePreloadModules(value: string | undefined): string[] {
   if (!value) return [];
   return value
      .split(",")
      .map((spec) => spec.trim())
      .filter((spec) => spec.length > 0);
}

/**
 * A specifier is a package name (resolved from this server's node_modules)
 * or an absolute path. A module's own imports resolve from where it lives,
 * so a driver that imports @malloydata/malloy belongs in node_modules; an
 * absolute path serves a self-contained module. A relative path is refused:
 * it would resolve against this file, wherever the bundle happens to live,
 * not the operator's cwd, and a list that works in one layout and not
 * another is worse than a startup error.
 */
export function assertResolvablePreloadSpecifier(spec: string): void {
   if (spec.startsWith("./") || spec.startsWith("../") || spec === ".") {
      throw new Error(
         `${PRELOAD_MODULES_ENV}: "${spec}" is a relative path; name a package or an absolute path`,
      );
   }
}

export type ModuleImporter = (spec: string) => Promise<unknown>;

/**
 * Import each specifier in order, awaiting each before the next so a module
 * that depends on an earlier one's registration sees it. A failed import
 * throws with the specifier in the message: a driver that silently failed to
 * load surfaces later as "unknown connection type" on the first package that
 * needs it, which points nowhere near the cause.
 */
export async function preloadModules(
   specs: string[],
   importer: ModuleImporter = (spec) => import(spec),
): Promise<string[]> {
   const loaded: string[] = [];
   for (const spec of specs) {
      assertResolvablePreloadSpecifier(spec);
      try {
         await importer(spec);
      } catch (error) {
         throw new Error(
            `${PRELOAD_MODULES_ENV}: failed to import "${spec}": ${(error as Error).message}`,
            { cause: error },
         );
      }
      loaded.push(spec);
   }
   return loaded;
}

/** The entrypoint form: read the env, import what it names. */
export async function preloadModulesFromEnv(
   env: NodeJS.ProcessEnv = process.env,
   importer?: ModuleImporter,
): Promise<string[]> {
   return preloadModules(
      parsePreloadModules(env[PRELOAD_MODULES_ENV]),
      importer,
   );
}
