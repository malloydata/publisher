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
import { MalloyConfig } from "@malloydata/malloy";
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
 * The package's connections, plus `destinations` (those in `allowed`) under
 * their aliases. The default connection is the package's.
 */
export function servedCopyConfig(
   packageConfig: ModelConnectionInput,
   destinations: MalloyConfig,
   allowed: ReadonlySet<string>,
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
               return destinations.connections.lookupConnection(destination);
            }
         }
         return base.connections.lookupConnection(name);
      },
   }));
   return config;
}
