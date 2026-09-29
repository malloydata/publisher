// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Connections whose type is not one of the server's built-ins.
 *
 * The built-in types each have a typed config object on the API
 * (`postgresConnection`, `bigqueryConnection`, ...) and a hand-written branch
 * that turns it into a core config entry. A type a preloaded module
 * registered (PUBLISHER_PRELOAD_MODULES) has neither: what it has is the
 * property definitions it registered with `@malloydata/malloy`, which say
 * which fields exist, which are optional, and which are credentials. That
 * is enough to accept a config for it, forward it to its factory, and
 * withhold its secrets on the way back out -- which is what this module
 * does. The config travels in `pluginConnection`, a bag keyed by those
 * property names.
 *
 * Core itself passes a config entry to the factory without checking it
 * against the declared properties, so the checks live here: a key the type
 * never declared, a required property left out, or a value written for a
 * property the type only accepts from a host overlay, is a config error at
 * load rather than a driver error at first query.
 */

import type { ConnectionPropertyDefinition } from "@malloydata/malloy/connection";
import {
   getConnectionProperties,
   getRegisteredConnectionTypes,
} from "@malloydata/malloy";
import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

/** The types with a typed config object on the API and a branch of their own. */
export const BUILT_IN_CONNECTION_TYPES: ReadonlySet<string> = new Set([
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
]);

export function isBuiltInConnectionType(type: string | undefined): boolean {
   return type !== undefined && BUILT_IN_CONNECTION_TYPES.has(type);
}

/**
 * The declared properties of a registered non-built-in type, or undefined
 * when no such type is registered in this process. A built-in name answers
 * undefined too: its config never goes through the bag.
 */
export function pluginConnectionProperties(
   type: string | undefined,
): readonly ConnectionPropertyDefinition[] | undefined {
   if (!type || isBuiltInConnectionType(type)) return undefined;
   if (!getRegisteredConnectionTypes().includes(type)) return undefined;
   return getConnectionProperties(type) ?? [];
}

/** A property whose value must never come back out of the server. */
function isCredential(property: ConnectionPropertyDefinition): boolean {
   return (
      property.type === "password" ||
      property.type === "secret" ||
      property.type === "opaque"
   );
}

/**
 * Field names of the bag a response may carry, by the type's own
 * declarations. Absent when the type is not registered here, so a bag stored
 * for a type this boot did not load is withheld whole rather than published
 * on the guess that none of it is a credential.
 */
export function publicPluginConnectionFields(
   type: string | undefined,
): readonly string[] | undefined {
   const properties = pluginConnectionProperties(type);
   if (!properties) return undefined;
   return properties.filter((p) => !isCredential(p)).map((p) => p.name);
}

/**
 * Check the bag against the declarations and return the core config entry
 * for it: `is` names the type, every other key is a declared property.
 */
export function pluginConnectionEntry(
   connection: ApiConnection,
): { is: string } & Record<string, unknown> {
   const type = connection.type;
   const properties = pluginConnectionProperties(type);
   if (!type || !properties) {
      const registered = getRegisteredConnectionTypes()
         .filter((t) => !isBuiltInConnectionType(t))
         .sort();
      throw new Error(
         `Unsupported connection type: ${type}. ` +
            (registered.length > 0
               ? `Types registered by preloaded modules: ${registered.join(", ")}.`
               : "No preloaded module has registered a connection type (PUBLISHER_PRELOAD_MODULES)."),
      );
   }
   const bag = (connection.pluginConnection ?? {}) as Record<string, unknown>;
   const declared = new Map(properties.map((p) => [p.name, p]));
   for (const key of Object.keys(bag)) {
      const property = declared.get(key);
      if (!property) {
         throw new Error(
            `Connection '${connection.name}' sets pluginConnection.${key}, which type '${type}' does not declare. ` +
               `Declared: ${[...declared.keys()].join(", ") || "(none)"}.`,
         );
      }
      if (property.source === "overlay") {
         throw new Error(
            `Connection '${connection.name}' sets pluginConnection.${key}, which type '${type}' only accepts from a host overlay, not from config.`,
         );
      }
   }
   for (const property of properties) {
      const supplied =
         bag[property.name] !== undefined && bag[property.name] !== null;
      const hasDefault = property.default !== undefined;
      if (
         !supplied &&
         !property.optional &&
         !hasDefault &&
         property.source !== "overlay"
      ) {
         throw new Error(
            `Connection '${connection.name}' is missing pluginConnection.${property.name}, which type '${type}' requires.`,
         );
      }
   }
   return { is: type, ...bag };
}
