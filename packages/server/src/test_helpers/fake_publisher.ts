// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A local stand-in for a remote Publisher's connection API, for specs and
 * benches that need a real `PublisherConnection` (the "publisher" connection
 * type) without a second server. It records the bearer token of every request
 * and, given a warehouse, serves its table schemas.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";

export interface FakePublisher {
   /** A `connectionUri` for a connection named `connectionName`. */
   connectionUri: (connectionName: string) => string;
   /** The `Authorization` header of every request so far, oldest first. */
   authorizations: string[];
   stop: () => void;
}

export function startFakePublisher(
   warehouse?: DuckDBConnection,
   projectName = "remote-project",
): FakePublisher {
   const authorizations: string[] = [];
   const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      // A warehouse failure answers 500, as a real remote does, rather than failing the running test.
      error: () => new Response("warehouse failed", { status: 500 }),
      async fetch(req) {
         authorizations.push(req.headers.get("authorization") ?? "");
         const url = new URL(req.url);
         const segments = url.pathname.split("/");
         const connectionName = segments[6] ?? "";
         const tail = segments.slice(7);
         if (req.method === "POST" && tail[0] === "sqlQuery") {
            return Response.json({
               data: JSON.stringify({ rows: [], totalRows: 0 }),
            });
         }
         if (tail[0] === "schemas" && tail[2] === "tables" && warehouse) {
            const tablePath = decodeURIComponent(tail[3]);
            const schema = await warehouse.fetchTableSchema(
               tablePath,
               tablePath,
            );
            return Response.json({ source: JSON.stringify(schema) });
         }
         if (req.method === "GET" && segments.length === 7) {
            return Response.json({
               name: connectionName,
               attributes: {
                  dialectName: "duckdb",
                  isPool: false,
                  canPersist: false,
                  canStream: false,
               },
            });
         }
         return Response.json({});
      },
   });
   return {
      connectionUri: (connectionName) =>
         `http://127.0.0.1:${server.port}/api/v0/projects/${projectName}/connections/${connectionName}`,
      authorizations,
      stop: () => void server.stop(true),
   };
}
