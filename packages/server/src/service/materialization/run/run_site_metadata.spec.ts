// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A run's own statements at a table's site (the ownership probe and mark, a
 * leftover view's drop) carry the run's query metadata, as its builds do, so
 * they group with the run in query history.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../test_helpers/settings_env";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { tableSiteContext } from "./instructed_build";
import { atTableSite } from "./table_site";

function recordingConnection(): {
   connection: MalloyConnection;
   options: unknown[];
} {
   const options: unknown[] = [];
   const connection = {
      name: "warehouse",
      runSQL: async (_sql: string, opts: unknown) => {
         options.push(opts);
         return { rows: [] };
      },
   } as unknown as MalloyConnection;
   return { connection, options };
}

const environment = {
   getApiConnection: () => ({}),
   getStorageDestination: () => {
      throw new Error("no destination");
   },
   getEnvironmentPath: () => "/env",
   getEnvironmentName: () => "env",
} as never;

describe("a run's statements at a table's site", () => {
   let restore: () => void;
   beforeAll(() => {
      restore = installSettings({
         env: { PUBLISHER_QUERY_METADATA_ENABLED: "on" },
      }).restore;
   });
   afterAll(() => restore());

   it("carry the run's id and class", async () => {
      const { connection, options } = recordingConnection();
      const context = tableSiteContext(
         {
            compiled: { connections: new Map() } as never,
            environment,
            buildMetadata: {
               packageMaterialization: null,
               context: {
                  queryClass: "materialize",
                  environment: "env",
                  package: "pkg",
                  runId: "run-1",
               },
            },
         },
         connection,
      );
      await atTableSite({ connectionName: "warehouse" }, context, (site) =>
         site.run("SELECT 1"),
      );
      expect(options).toHaveLength(1);
      const metadata = (
         options[0] as { queryMetadata?: Record<string, string> }
      ).queryMetadata;
      expect(metadata).toMatchObject({ run_id: "run-1" });
   });

   it("carry nothing a run did not give", async () => {
      const { connection, options } = recordingConnection();
      const context = tableSiteContext(
         { compiled: { connections: new Map() } as never, environment },
         connection,
      );
      await atTableSite({ connectionName: "warehouse" }, context, (site) =>
         site.run("SELECT 1"),
      );
      expect(
         (options[0] as { queryMetadata?: unknown } | undefined)?.queryMetadata,
      ).toBeUndefined();
   });
});
