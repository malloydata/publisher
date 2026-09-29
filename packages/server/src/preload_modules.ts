// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Modules an operator asks the server to import before it accepts work.
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
 * in the main thread would register a dialect the compiler never sees. The
 * corollary: a module runs once per realm -- main, each worker, and again
 * whenever a worker is respawned -- so it should register and return, not
 * start a process-wide exporter or listener.
 *
 * The imports run after the server's own static module graph has evaluated
 * (ESM runs every dependency's top-level code first), so the built-in drivers
 * are already registered. A module that registers an existing type name
 * replaces the built-in for that name; both registries are last-writer-wins.
 * That is deliberate and worth knowing.
 *
 * Registration only lands in THIS server's `@malloydata/malloy`. The server
 * pins the compiler exactly (`^0.0.x` cannot cross the leftmost non-zero), so
 * a module that lists `@malloydata/malloy` as its own dependency at any other
 * version is given a nested copy with a registry nobody reads: the import
 * succeeds and the first package still fails with an unknown connection type.
 * Such a module has to declare the compiler a peerDependency. `preloadModules`
 * reports what each import registered so that mistake is one visible line
 * at boot rather than a package failure later.
 */

import { getRegisteredConnectionTypes } from "@malloydata/malloy";
import { getConnectionTypeDef } from "@malloydata/malloy/connection";
import * as path from "path";
import { pathToFileURL } from "url";

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
 * or an absolute path. A module's own imports resolve upward from where it
 * lives, so a module that imports @malloydata/malloy has to sit somewhere
 * beneath this server's install; an absolute path elsewhere serves a
 * self-contained module. A relative path is refused: it would resolve
 * against this file, wherever the bundle happens to live, not the operator's
 * cwd, and a list that works in one layout and not another is worse than a
 * startup error. An absolute path is returned as a file URL, which is what
 * `import()` needs on every platform (a bare Windows path is not a URL).
 */
export function resolvePreloadSpecifier(spec: string): string {
   const relative = spec === "." || spec === ".." || /^\.\.?[\\/]/.test(spec);
   if (relative) {
      throw new Error(
         `${PRELOAD_MODULES_ENV}: "${spec}" is a relative path; name a package or an absolute path`,
      );
   }
   return path.isAbsolute(spec) ? pathToFileURL(spec).href : spec;
}

export type ModuleImporter = (spec: string) => Promise<unknown>;

export interface PreloadedModule {
   /** The specifier as the operator wrote it. */
   spec: string;
   /** Connection type names that were not registered before this import. */
   addedConnectionTypes: string[];
   /** Connection type names whose definition this import replaced. */
   replacedConnectionTypes: string[];
   /** The same specifier appeared earlier in the list; this import was a no-op. */
   repeated: boolean;
}

/** A snapshot of this realm's connection-type registry, by definition identity. */
function connectionTypeSnapshot(): Map<string, unknown> {
   return new Map(
      getRegisteredConnectionTypes().map((name) => [
         name,
         getConnectionTypeDef(name),
      ]),
   );
}

/**
 * Import each specifier in order, awaiting each before the next so a module
 * that depends on an earlier one's registration sees it. A failed import
 * throws with the specifier in the message: a driver that silently failed to
 * load surfaces later as "unknown connection type" on the first package that
 * needs it, which points nowhere near the cause.
 *
 * Each result carries the connection types the import added or replaced,
 * read from this realm's registry. Dialects are not reported: the compiler
 * exports `registerDialect` but no way to list what is registered.
 */
export async function preloadModules(
   specs: string[],
   importer: ModuleImporter = (spec) => import(spec),
): Promise<PreloadedModule[]> {
   const loaded: PreloadedModule[] = [];
   const seen = new Set<string>();
   for (const spec of specs) {
      const resolved = resolvePreloadSpecifier(spec);
      const repeated = seen.has(resolved);
      seen.add(resolved);
      const before = connectionTypeSnapshot();
      try {
         await importer(resolved);
      } catch (error) {
         throw new Error(
            `${PRELOAD_MODULES_ENV}: failed to import "${spec}": ${(error as Error).message}`,
            { cause: error },
         );
      }
      const addedConnectionTypes: string[] = [];
      const replacedConnectionTypes: string[] = [];
      for (const [name, def] of connectionTypeSnapshot()) {
         if (!before.has(name)) addedConnectionTypes.push(name);
         else if (before.get(name) !== def) replacedConnectionTypes.push(name);
      }
      loaded.push({
         spec,
         addedConnectionTypes,
         replacedConnectionTypes,
         repeated,
      });
   }
   return loaded;
}

/** The entrypoint form: read the env, import what it names. */
export async function preloadModulesFromEnv(
   env: NodeJS.ProcessEnv = process.env,
   importer?: ModuleImporter,
): Promise<PreloadedModule[]> {
   return preloadModules(
      parsePreloadModules(env[PRELOAD_MODULES_ENV]),
      importer,
   );
}
