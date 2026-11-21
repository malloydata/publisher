// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `publisher` connection type's registry entry: a connection to another
 * Publisher's connection, whose dialect and attributes are resolved at
 * runtime, so it has no static attributes.
 */

import type { Connection } from "@malloydata/malloy";
import { settings } from "../../settings";
import type {
   ApiConnection,
   ApiSchema,
   ApiTable,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const PUBLISHER: ConnectionDialect = Object.freeze({
   type: "publisher",
   attributes: undefined,
   validateShape: validatePublisherShape,
   coreEntry: buildPublisherEntry,
   introspector: Object.freeze({
      schemas: getSchemasForPublisher,
      tables: (
         connection: ApiConnection,
         schemaName: string,
         _malloyConnection: Connection,
         tableNames?: string[],
      ) => listTablesForPublisher(connection, schemaName, tableNames),
   }),
});

function validatePublisherShape(connection: ApiConnection): void {
   // SSRF gate (default-deny, fail-closed). A `publisher` connection makes
   // THIS server issue outbound HTTP to a tenant-controlled `connectionUri`
   // (the query path in db-publisher and the introspection path in db_utils).
   // Intended for local `--watch-env` authoring, but an SSRF surface in a
   // hosted multi-tenant deployment, so it needs an explicit opt-in. Every
   // connection passes validateConnectionShape before it can be assembled,
   // queried, or introspected, so denying here shuts off all three.
   if (!settings.get("allow.proxyConnections")) {
      throw new Error(
         `Publisher proxy connection '${connection.name}' is disabled in this deployment. ` +
            `'publisher' connections make the server issue outbound requests to a configured connectionUri, ` +
            `which is only appropriate for local --watch-env authoring. ` +
            `Fix: set the environment variable PUBLISHER_ALLOW_PROXY_CONNECTIONS=true to enable them.`,
      );
   }
   const publisher = connection.publisherConnection;
   if (!publisher?.connectionUri) {
      throw new Error(
         `Invalid publisher connection '${connection.name}': missing connectionUri. ` +
            `Fix: { "name": "${connection.name}", "type": "publisher", ` +
            `"publisherConnection": { "connectionUri": "https://…/connections/${connection.name}", "accessToken": "<jwt>" } }`,
      );
   }
   // Reject a malformed connectionUri at config load: deep in the request
   // path the error can echo the raw value, leaking embedded credentials
   // (`redactUrlCredentials` returns an unparseable URI unchanged). Never
   // include the raw connectionUri in these messages; the scheme is safe.
   let parsedUri: URL;
   try {
      parsedUri = new URL(publisher.connectionUri);
   } catch {
      throw new Error(
         `Invalid publisher connection '${connection.name}': connectionUri is not a valid URL. ` +
            `Fix: set connectionUri to an absolute https URL like "https://…/connections/${connection.name}".`,
      );
   }
   if (parsedUri.protocol !== "http:" && parsedUri.protocol !== "https:") {
      throw new Error(
         `Invalid publisher connection '${connection.name}': connectionUri must use http or https (got '${parsedUri.protocol}'). ` +
            `Fix: set connectionUri to an absolute https URL like "https://…/connections/${connection.name}".`,
      );
   }
}

function buildPublisherEntry(connection: NamedConnection): CoreConnectionEntry {
   // connectionUri presence is validated by validateConnectionShape. The
   // proxied dataplane owns auth and read-only enforcement; PublisherConnection
   // itself does not reject writes. The dialect is the remote connection's,
   // resolved at runtime, so getStaticConnectionAttributes returns undefined.
   const publisher = connection.publisherConnection!;
   return {
      is: "publisher",
      connectionUri: publisher.connectionUri,
      accessToken: publisher.accessToken,
   };
}

/** Abort introspection requests that hang instead of holding the slot open. */
const PUBLISHER_INTROSPECTION_TIMEOUT_MS = 60_000;

/**
 * Strip userinfo (`user:pass@`) from a URL before it reaches a log or error
 * message. `connectionUri` comes from config and may embed credentials; the
 * bearer token travels only in the header.
 */
function redactUrlCredentials(url: string): string {
   try {
      const parsed = new URL(url);
      parsed.username = "";
      parsed.password = "";
      return parsed.toString();
   } catch {
      // Unparseable: nothing to redact.
      return url;
   }
}

/**
 * Publisher proxy connections introspect against the remote dataplane, which
 * serves the same connection API, so discovery is a GET passthrough returning
 * the same Schema/Table shapes verbatim.
 *
 * `connectionUri` must point at the connection resource itself
 * (`.../connections/<name>`). This path appends `pathSuffix` to it verbatim,
 * while db-publisher's `PublisherConnection` rebuilds it through its generated
 * client, so the remote must serve both forms for the same connection.
 */
async function fetchFromPublisherDataplane<T>(
   connection: ApiConnection,
   pathSuffix: string,
): Promise<T> {
   const publisher = connection.publisherConnection;
   // Type-narrowing guard only; the user-facing validation is
   // validateConnectionShape, which every connection passes first.
   if (!publisher?.connectionUri) {
      throw new Error(
         `Publisher connection "${connection.name}" is missing connectionUri`,
      );
   }
   const url = `${publisher.connectionUri.replace(/\/+$/, "")}${pathSuffix}`;
   const headers: Record<string, string> = {};
   if (publisher.accessToken) {
      headers["Authorization"] = `Bearer ${publisher.accessToken}`;
   }

   let response: Response;
   try {
      response = await fetch(url, {
         headers,
         signal: AbortSignal.timeout(PUBLISHER_INTROSPECTION_TIMEOUT_MS),
      });
   } catch (error) {
      // Network-level failure (no HTTP status). Never echo the token-bearing
      // header.
      const reason =
         (error as Error)?.name === "TimeoutError"
            ? `timed out after ${PUBLISHER_INTROSPECTION_TIMEOUT_MS}ms`
            : (error as Error).message;
      throw new Error(
         `Publisher dataplane request to ${redactUrlCredentials(url)} failed ` +
            `for connection "${connection.name}": ${reason}`,
         // Keeps fetch's socket code reachable, so a dataplane that refused
         // the connection answers 502 CONNECTION_FAILED rather than 500.
         { cause: error },
      );
   }

   if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(
         `Publisher dataplane request to ${redactUrlCredentials(url)} failed ` +
            `(${response.status}): ${body.slice(0, 200)}`,
      );
   }
   return (await response.json()) as T;
}

async function getSchemasForPublisher(
   connection: ApiConnection,
): Promise<ApiSchema[]> {
   return fetchFromPublisherDataplane<ApiSchema[]>(connection, "/schemas");
}

async function listTablesForPublisher(
   connection: ApiConnection,
   schemaName: string,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const tables = await fetchFromPublisherDataplane<ApiTable[]>(
      connection,
      `/schemas/${encodeURIComponent(schemaName)}/tables`,
   );
   if (!tableNames) {
      return tables;
   }
   // `tableNames` holds bare table names, but the remote qualifies `resource`
   // per its dialect, so match the last segment, the schema-stripped form, and
   // the full resource.
   const allowed = new Set(tableNames);
   const prefix = `${schemaName}.`;
   return tables.filter((table) => {
      const resource = table.resource ?? "";
      const schemaStripped = resource.startsWith(prefix)
         ? resource.slice(prefix.length)
         : resource;
      const bareName = resource.slice(resource.lastIndexOf(".") + 1);
      return (
         allowed.has(bareName) ||
         allowed.has(schemaStripped) ||
         allowed.has(resource)
      );
   });
}
