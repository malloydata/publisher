// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { swapIntoPlace } from "./colocated_build_strategy";
import {
   colocatedNameViewSite,
   pointNameView,
   probeNameView,
   settleNameViewsBeforeDrop,
   swapDependentsFor,
   type NameViewIntent,
} from "./name_view";

/**
 * The `name=` view on a real Postgres, where a view binds to its table: a
 * repoint to a table of another shape, a rebuild of the very table the view
 * reads (the swap carries the view), and a drop of that table (the view is
 * settled first). Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a database the
 * test may create and drop the `name_view_test` schema in, e.g.
 * `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

describe.skipIf(!url)("the name= view on Postgres", () => {
   let pg: PostgresConnection;
   const run = (sql: string) => pg.runSQL(sql);
   const intent: NameViewIntent = {
      sourceEntityId: "addr",
      sourceName: "flights",
      viewName: "name_view_test.flights_all",
      physicalTableName: "name_view_test.flights_g1",
      connectionName: "pg",
   };
   // Malloy's Postgres runSQL returns each row's one `row` column.
   const answer = async () =>
      (
         await run(
            'SELECT row_to_json(v) AS "row" FROM name_view_test.flights_all v',
         )
      ).rows;

   beforeAll(async () => {
      pg = new PostgresConnection({ name: "pg", connectionString: url! });
      await run("DROP SCHEMA IF EXISTS name_view_test CASCADE");
      await run("CREATE SCHEMA name_view_test");
      await run("CREATE TABLE name_view_test.flights_g1 AS SELECT 1 AS x");
      await run(
         "CREATE TABLE name_view_test.flights_g2 AS SELECT 'two' AS x, 2 AS y",
      );
   });
   afterAll(async () => {
      await run("DROP SCHEMA IF EXISTS name_view_test CASCADE");
      await pg.close();
   });

   it("is made, and repointed to a table of another shape", async () => {
      const site = colocatedNameViewSite(pg);
      await pointNameView(site, intent.viewName, "name_view_test.flights_g1");
      expect(await answer()).toEqual([{ x: 1 }]);
      // CREATE OR REPLACE refuses a column of another type: the view is
      // dropped and made again in one transaction.
      await pointNameView(site, intent.viewName, "name_view_test.flights_g2");
      expect(await answer()).toEqual([{ x: "two", y: 2 }]);
      expect(await probeNameView(site, intent.viewName)).toEqual({
         state: "ours",
         target: "name_view_test.flights_g2",
      });
      await pointNameView(site, intent.viewName, "name_view_test.flights_g1");
   });

   it("a rebuild of the table the view reads keeps the view, through the swap", async () => {
      const site = colocatedNameViewSite(pg);
      const slot = await probeNameView(site, intent.viewName);
      const swapDependents = swapDependentsFor(intent, slot, "postgres");
      expect(swapDependents).toBeDefined();
      const swap = (staging: string, dependents?: typeof swapDependents) =>
         swapIntoPlace({
            connection: pg,
            runOptions: {},
            dialect: "postgres",
            physicalTableName: "name_view_test.flights_g1",
            stagingTableName: staging,
            backupTableName: "name_view_test.flights_g1_o",
            connectionName: "pg",
            swapDependents: dependents,
         });

      // Without the view carried, Postgres refuses the drop and the build.
      await run("CREATE TABLE name_view_test.flights_g1_s AS SELECT 10 AS x");
      await expect(swap("name_view_test.flights_g1_s")).rejects.toThrow(
         /depend/,
      );
      expect(await answer()).toEqual([{ x: 1 }]);

      await run("CREATE TABLE name_view_test.flights_g1_s AS SELECT 11 AS x");
      await swap("name_view_test.flights_g1_s", swapDependents);
      expect(await answer()).toEqual([{ x: 11 }]);
      expect(await probeNameView(site, intent.viewName)).toEqual({
         state: "ours",
         target: "name_view_test.flights_g1",
      });
   });

   it("before its table's drop the view is repointed, so the drop succeeds", async () => {
      const context = {
         connectionFor: () => pg,
         destinationFor: () => undefined,
         environmentPath: "",
      };
      await settleNameViewsBeforeDrop(
         intent,
         [{ ...intent, outcome: "created" }],
         () => ({
            ...intent,
            physicalTableName: "name_view_test.flights_g2",
            outcome: "repointed",
         }),
         context,
      );
      await run("DROP TABLE name_view_test.flights_g1");
      expect(await answer()).toEqual([{ x: "two", y: 2 }]);

      // And with no generation left, dropped.
      await settleNameViewsBeforeDrop(
         { ...intent, physicalTableName: "name_view_test.flights_g2" },
         [
            {
               ...intent,
               physicalTableName: "name_view_test.flights_g2",
               outcome: "repointed",
            },
         ],
         () => undefined,
         context,
      );
      await run("DROP TABLE name_view_test.flights_g2");
      expect(
         await probeNameView(colocatedNameViewSite(pg), intent.viewName),
      ).toEqual({ state: "empty" });
   });
});
