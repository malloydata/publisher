// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A model that joins sources on two origin connections is invalid Malloy
 * (`join-connection-mismatch`, malloydata/malloy#3032), and Publisher serves
 * it no differently, with the materialize switch on as off: the package fails
 * to load with Malloy's own diagnostic, at boot and at publish, and every
 * other package in its environment loads.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   spawnServer,
   type SpawnedServer,
   writeSourcePackage,
} from "../../harness/spawn_server";

const ENV = "xorigin";

/** `accounts` on `crm`, `usage` on `product`, joined: Malloy refuses it. */
const CROSS = `source: accounts is crm.sql("select 1 as id, 'acme' as account_name") extend {
  primary_key: id
}
source: usage is product.sql("select 1 as account_id, 5 as events")

source: account_usage is accounts extend {
  join_many: u is usage on u.account_id = id
  measure: total_events is u.events.sum()
}
`;

/** The same sources, each on its own connection, never joined. */
const SAME = `source: accounts is crm.sql("select 1 as id, 'acme' as account_name")
source: usage is product.sql("select 1 as account_id, 5 as events")
`;

/** An environment DuckDB connection of its own. */
const duck = (name: string) => ({
   name,
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
});

interface LoadError {
   environment: string;
   package?: string;
   message?: string;
}

describe("a package joining sources across origin connections", () => {
   let server: SpawnedServer;
   const dirs: { cleanup: () => void }[] = [];
   const source = (name: string, model: string) => {
      const pkg = writeSourcePackage(name, { "model.malloy": model });
      dirs.push(pkg);
      return pkg.dir;
   };
   const api = (sub: string) => `${server.baseUrl}/api/v0${sub}`;

   beforeAll(async () => {
      server = await spawnServer({
         config: {
            frozenConfig: false,
            environments: [
               {
                  name: ENV,
                  packages: [
                     { name: "cross", location: source("cross", CROSS) },
                     { name: "sibling", location: source("sibling", SAME) },
                  ],
                  connections: [duck("crm"), duck("product")],
               },
            ],
         },
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      });
      await server.waitUntilServing();
   }, 180_000);

   afterAll(async () => {
      await server?.stop();
      for (const d of dirs) d.cleanup();
   });

   it("fails to load with Malloy's diagnostic, and its sibling loads", async () => {
      const status = (await (await fetch(api("/status"))).json()) as {
         operationalState: string;
         environments: { name: string; packages?: { name: string }[] }[];
         loadErrors?: LoadError[];
      };
      expect(status.operationalState).toBe("serving");
      expect(
         status.environments
            .find((e) => e.name === ENV)
            ?.packages?.map((p) => p.name),
      ).toEqual(["sibling"]);
      expect(status.loadErrors).toEqual([
         expect.objectContaining({ environment: ENV, package: "cross" }),
      ]);
      expect(status.loadErrors?.[0].message).toMatch(
         /Cannot join 'u', which is on connection 'product', into a source on connection 'crm'/,
      );

      const sibling = await fetch(
         api(`/environments/${ENV}/packages/sibling/models/model.malloy/query`),
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
               query: "run: usage -> { aggregate: n is count() }",
               compactJson: true,
            }),
         },
      );
      expect(sibling.status).toBe(200);
      const body = (await sibling.json()) as { result: string };
      expect(JSON.parse(body.result)).toEqual([{ n: 1 }]);
   });

   it("refuses a publish of one with the same diagnostic", async () => {
      const published = await fetch(api(`/environments/${ENV}/packages`), {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: "cross2",
            location: source("cross2", CROSS),
         }),
      });
      // 424: the package it names does not compile, as any other model.
      expect(published.status).toBe(424);
      expect(await published.text()).toMatch(
         /Cannot join 'u', which is on connection 'product', into a source on connection 'crm'/,
      );
      const after = await fetch(api(`/environments/${ENV}/packages/sibling`));
      expect(after.status).toBe(200);
   });
});
