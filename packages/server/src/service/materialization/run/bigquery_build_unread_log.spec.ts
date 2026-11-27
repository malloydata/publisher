// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The warning a BigQuery `storage=` build logs when its catalog read did not
 * prove its strings byte-exact: one line per build, naming the source, its
 * connection, and where the read looked, with the bounded reason the read
 * reported (serve/collation/bigquery.spec.ts proves each reason is reported).
 */

import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../logger";
import type { BigQueryBuildUnreadReason } from "../domain/collation";
import { logBigQueryBuildUnread } from "./source_build";

const REASONS: BigQueryBuildUnreadReason[] = [
   "collate_in_sql",
   "no_job_project",
   "table_on_other_connection",
   "table_unplaceable",
   "table_types_unread",
   "table_not_listed",
   "project_unread",
   "project_collated",
   "dataset_table_not_listed",
   "read_failed",
];

const SOURCE = { name: "engaged_events", connectionName: "bq_connection" };

describe("a BigQuery storage= build's unproved catalog read, logged", () => {
   const spies: { mockRestore(): void }[] = [];
   afterEach(() => {
      for (const s of spies.splice(0)) s.mockRestore();
   });
   /** The fields of each warning logged while the spy is installed. */
   const logged = () => {
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      spies.push(warn);
      return () =>
         warn.mock.calls.map(
            (c) => c as unknown as [string, Record<string, unknown>],
         );
   };

   for (const reason of REASONS) {
      it(`logs ${reason} with the connection, project, location, dataset and table`, () => {
         const calls = logged();
         logBigQueryBuildUnread(SOURCE as never, {
            reason,
            project: "dev-project",
            location: "us",
            dataset: "openfga_logs",
            table: "stdout",
         });
         expect(calls()).toHaveLength(1);
         const [message, fields] = calls()[0];
         expect(message).toContain("compared none");
         expect(fields).toEqual({
            sourceName: "engaged_events",
            connectionName: "bq_connection",
            reason,
            project: "dev-project",
            location: "us",
            dataset: "openfga_logs",
            table: "stdout",
         });
      });
   }

   it("names the location per-dataset when the connection declares none, and leaves out what the read never reached", () => {
      const calls = logged();
      logBigQueryBuildUnread(SOURCE as never, {
         reason: "project_unread",
         project: "dev-project",
      });
      expect(calls()[0][1]).toEqual({
         sourceName: "engaged_events",
         connectionName: "bq_connection",
         reason: "project_unread",
         project: "dev-project",
         location: "per-dataset",
      });
   });

   it("carries a table read as other than a table by its catalog type", () => {
      const calls = logged();
      logBigQueryBuildUnread(SOURCE as never, {
         reason: "dataset_table_not_listed",
         project: "p",
         dataset: "d",
         table: "v",
         tableType: "VIEW",
      });
      expect(calls()[0][1]).toMatchObject({ tableType: "VIEW" });
   });
});
