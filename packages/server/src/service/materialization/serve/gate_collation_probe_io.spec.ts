// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The collation reads' I/O adapter (`probeIO` in gate_collation.ts): it alone
 * reads a BigQuery connection's driver duck-typed (its client's
 * `getDatasets`, its `projectId`, `billingProjectId` and `location`), so the
 * readers in `collation/` see only the answers.
 */

import type { Connection } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import fs from "fs";
import path from "path";
import { CATALOG_ROW_LIMIT } from "./collation/probe_io";
import { probeIO } from "./gate_collation";

function fake(fields: Record<string, unknown>): Connection {
   return {
      dialectName: "standardsql",
      runSQL: async () => ({ rows: undefined }),
      ...fields,
   } as unknown as Connection;
}

describe("the collation reads' I/O adapter", () => {
   it("lists a project's dataset locations through the driver's client, lower-cased and once each", async () => {
      const asked: unknown[] = [];
      const io = probeIO(
         fake({
            bigQuery: {
               getDatasets: async (options: unknown) => {
                  asked.push(options);
                  return [
                     [
                        { metadata: { location: "US" } },
                        { metadata: { location: "us" } },
                        { metadata: { location: "EU" } },
                     ],
                  ];
               },
            },
         }),
      );
      expect(await io.datasetLocations("p1")).toEqual(["us", "eu"]);
      expect(asked).toEqual([{ projectId: "p1", autoPaginate: true }]);
   });

   it("refuses a connection with no client, and a dataset with no location", async () => {
      await expect(probeIO(fake({})).datasetLocations("p")).rejects.toThrow(
         "no BigQuery client",
      );
      const noLocation = probeIO(
         fake({ bigQuery: { getDatasets: async () => [[{ metadata: {} }]] } }),
      );
      await expect(noLocation.datasetLocations("p")).rejects.toThrow(
         "no location",
      );
   });

   it("reads the job's projects and location off the driver", () => {
      expect(
         probeIO(
            fake({ projectId: "a", billingProjectId: "b", location: "US" }),
         ).jobContext(),
      ).toEqual({ projects: ["a", "b"], location: "us" });
      // One project named twice; an unresolved placeholder; no location.
      expect(
         probeIO(
            fake({ projectId: "a", billingProjectId: "a", location: "" }),
         ).jobContext(),
      ).toEqual({ projects: ["a"], location: undefined });
      expect(
         probeIO(fake({ projectId: "{{projectId}}" })).jobContext(),
      ).toEqual({ projects: [], location: undefined });
   });

   it("hands back the connection's dialect, and no rows as an empty list", async () => {
      const io = probeIO(fake({}));
      expect(io.dialectName).toBe("standardsql");
      expect(await io.rows("SELECT 1")).toEqual([]);
   });

   it("answers a complete read only when it is provably whole", async () => {
      const asked: unknown[] = [];
      const reading = (answer: Record<string, unknown>) =>
         probeIO(
            fake({
               runSQL: async (_sql: string, options: unknown) => {
                  asked.push(options);
                  return answer;
               },
            }),
         );
      const two = [{ a: 1 }, { a: 2 }];
      expect(
         await reading({ rows: two, totalRows: 2 }).completeRows("SELECT 1"),
      ).toEqual(two);
      expect(asked).toEqual([{ rowLimit: CATALOG_ROW_LIMIT }]);
      // One page of a longer answer, or a read that says nothing of its size.
      for (const answer of [
         { rows: two, totalRows: 11 },
         { rows: two, totalRows: null },
         { rows: two },
         {
            rows: Array.from({ length: CATALOG_ROW_LIMIT }, () => ({})),
            totalRows: CATALOG_ROW_LIMIT,
         },
      ]) {
         await expect(reading(answer).completeRows("SELECT 1")).rejects.toThrow(
            "may be incomplete",
         );
      }
   });

   it("is the one place in serve/ that reads the BigQuery driver", () => {
      const driverReads = /\.bigQuery\b|\bprojectId\b|\bbillingProjectId\b/;
      const offenders: string[] = [];
      const visit = (dir: string) => {
         for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            const file = path.join(dir, entry.name);
            if (entry.isDirectory()) visit(file);
            else if (
               /\.ts$/.test(entry.name) &&
               !/\.spec\.ts$/.test(entry.name) &&
               entry.name !== "gate_collation.ts" &&
               driverReads.test(fs.readFileSync(file, "utf8"))
            ) {
               offenders.push(path.relative(__dirname, file));
            }
         }
      };
      visit(__dirname);
      expect(offenders).toEqual([]);
   });

   it("leaves the BigQuery reads' scope to gate_collation.ts: the reader keeps no map of its own", () => {
      const reader = fs.readFileSync(
         path.join(__dirname, "collation/bigquery.ts"),
         "utf8",
      );
      // A top-level map would outlive the runtime and bind it was read under.
      expect(reader).not.toMatch(
         /^(const|let|var)\s+\w+\s*=\s*new (Weak)?Map\b/m,
      );
   });
});
