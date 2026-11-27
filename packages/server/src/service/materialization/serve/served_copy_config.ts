// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The connections a served copy compiles and runs with: the package's own,
 * plus each storage destination it reads, under a name only this process
 * knows.
 *
 * A served copy is the author's file, so anything it names resolves against
 * these connections. Storage destinations are deliberately kept out of the
 * package's namespace (Package.setServeDestinationConfig): an author must not
 * be able to read a destination by naming it. So a destination is reachable
 * here only through its alias, `__publisher_storage_<random>_<n>`, minted once
 * per process from a random token and never shown to a caller. The served copy
 * reaches it only through `virtual()` handles the server mapped. The real
 * destination name resolves exactly as it does for the author's model: to a
 * package connection of that name if one exists, and to nothing otherwise.
 */

import { randomBytes } from "crypto";
import { type Connection, MalloyConfig } from "@malloydata/malloy";
import { toMalloyConfig, type ModelConnectionInput } from "../../model_runtime";

const TOKEN = randomBytes(12).toString("hex");
const aliases = new Map<string, string>();
const destinationsByAlias = new Map<string, string>();

/** The alias a destination is reached by in a served copy, stable per process. */
export function servedCopyAlias(destinationName: string): string {
   let alias = aliases.get(destinationName);
   if (!alias) {
      alias = `__publisher_storage_${TOKEN}_${aliases.size}`;
      aliases.set(destinationName, alias);
      destinationsByAlias.set(alias, destinationName);
   }
   return alias;
}

/**
 * Whether `text` names a served-copy alias anywhere in it (a compiler message
 * about the served copy can: a join refused across connections names both
 * connections). Such text stays in the server log, never in a caller's view.
 */
export function namesServedCopyAlias(text: string): boolean {
   return text.includes(`__publisher_storage_${TOKEN}_`);
}

/**
 * `text` with each served-copy alias (quoted or not) said as `the storage
 * destination`, for a caller's view of a message that names one.
 */
export function withoutServedCopyAliases(text: string): string {
   return text.replace(
      new RegExp(`'?__publisher_storage_${TOKEN}_\\d+'?`, "g"),
      "the storage destination",
   );
}

/** The storage destination a served-copy alias stands for, or undefined for any other name. */
export function destinationOfAlias(name: string): string | undefined {
   return destinationsByAlias.get(name);
}

/**
 * A connection the served copy compiles with, in a caller's words: a
 * served-copy alias as the storage destination it stands for, by its
 * configured name (never the alias), any other name as a connection.
 */
export function servedConnectionLabel(name: string): string {
   const destination = destinationsByAlias.get(name);
   return destination !== undefined
      ? `storage destination '${destination}'`
      : `connection '${name}'`;
}

/** Each destination connection's view under an alias, so a lookup returns one object. */
const aliasedViews = new WeakMap<Connection, Map<string, Connection>>();

/**
 * `connection` as a served copy reads it under `alias`: its `name` is the
 * alias, and so is the connection of every table schema it fetches, so a
 * `<alias>.table(...)` source compiles on the alias, as a `virtual()` one
 * does, and its queries look the alias up again. Every other member is the
 * connection's own, called on it.
 */
function underAlias(connection: Connection, alias: string): Connection {
   let views = aliasedViews.get(connection);
   if (!views) {
      views = new Map();
      aliasedViews.set(connection, views);
   }
   const known = views.get(alias);
   if (known) return known;
   const view = new Proxy(connection, {
      get(target, prop) {
         if (prop === "name") return alias;
         if (prop === "fetchSchemaForTables") {
            return async (
               ...args: Parameters<Connection["fetchSchemaForTables"]>
            ) => {
               const fetched = await target.fetchSchemaForTables(...args);
               return {
                  ...fetched,
                  schemas: Object.fromEntries(
                     Object.entries(fetched.schemas).map(([key, def]) => [
                        key,
                        { ...def, connection: alias },
                     ]),
                  ),
               };
            };
         }
         const value = Reflect.get(target, prop, target);
         return typeof value === "function" ? value.bind(target) : value;
      },
   });
   views.set(alias, view);
   return view;
}

/**
 * The package's connections, plus `destinations` (those in `allowed`) under
 * their aliases. The default connection is the package's. A destination in
 * `tablesUnderAlias` is looked up as itself under its alias
 * ({@link underAlias}), for a copy that reads a connection's tables through
 * it (same_catalog_reads.ts); every other one as the connection itself.
 */
export function servedCopyConfig(
   packageConfig: ModelConnectionInput,
   destinations: MalloyConfig,
   allowed: ReadonlySet<string>,
   tablesUnderAlias: ReadonlySet<string> = new Set(),
): MalloyConfig {
   const base = toMalloyConfig(packageConfig);
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(() => ({
      lookupConnection: async (name?: string) => {
         if (name !== undefined) {
            const destination = destinationsByAlias.get(name);
            if (destination !== undefined) {
               if (!allowed.has(destination)) {
                  throw new Error(
                     `A storage destination is not available to this served copy`,
                  );
               }
               const connection =
                  await destinations.connections.lookupConnection(destination);
               return tablesUnderAlias.has(destination)
                  ? underAlias(connection, name)
                  : connection;
            }
         }
         return base.connections.lookupConnection(name);
      },
   }));
   return config;
}
