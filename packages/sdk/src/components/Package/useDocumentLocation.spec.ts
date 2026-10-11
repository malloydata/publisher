// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { ApiClients } from "../ServerProvider";
import { fetchReportListing } from "./useDocumentLocation";

const clientsAnswering = (answer: () => Promise<unknown>) =>
   ({ reports: { listReports: answer } }) as unknown as ApiClients;

describe("fetchReportListing", () => {
   it("returns the listing", async () => {
      const reports = [{ path: "a.malloy", layout: "grid" }];
      expect(
         await fetchReportListing(
            clientsAnswering(async () => ({ data: reports })),
            "env",
            "pkg",
         ),
      ).toEqual(reports as never);
   });

   it("reads a 404 as an older server with no reports to list", async () => {
      expect(
         await fetchReportListing(
            clientsAnswering(() =>
               Promise.reject({ response: { status: 404 } }),
            ),
            "env",
            "pkg",
         ),
      ).toEqual([]);
   });

   it("raises a failure with no answer, rather than reading it as no reports", async () => {
      // Every reader caches this listing; an empty one cached for a minute
      // would say "nothing here" about a package that has reports.
      await expect(
         fetchReportListing(
            clientsAnswering(() => Promise.reject(new Error("Network Error"))),
            "env",
            "pkg",
         ),
      ).rejects.toThrow("Network Error");
   });

   it("raises a server error", async () => {
      await expect(
         fetchReportListing(
            clientsAnswering(() =>
               Promise.reject({ response: { status: 500 } }),
            ),
            "env",
            "pkg",
         ),
      ).rejects.toEqual({ response: { status: 500 } });
   });
});
