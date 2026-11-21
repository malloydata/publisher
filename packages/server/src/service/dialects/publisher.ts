// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `publisher` connection type's registry entry: a connection to another
 * Publisher's connection, whose dialect and attributes are resolved at
 * runtime, so it has no static attributes.
 */

import { settings } from "../../settings";
import type {
   ApiConnection,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const PUBLISHER: ConnectionDialect = Object.freeze({
   type: "publisher",
   attributes: undefined,
   validateShape: validatePublisherShape,
   coreEntry: buildPublisherEntry,
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
