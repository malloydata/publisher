// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { databaseTableUnreadWarnings } from "./package_warnings";
import type { BuildPlan } from "./domain/types";

/**
 * `database-table-unread`: one warning per bound database table the bind
 * could not read, named by the source a query names, saying why and whether
 * a retry is pending.
 */
describe("databaseTableUnreadWarnings", () => {
   const plan = {
      graphs: [],
      sources: {
         "orders@file:///m.malloy": {
            name: "orders",
            sourceEntityId: "addr1",
            modelPath: "m.malloy",
         },
         "orders__rollup__r@file:///m.malloy": {
            name: "orders__rollup__r",
            sourceEntityId: "addr2",
            modelPath: "m.malloy",
            preaggregate: { baseSourceName: "orders" },
         },
      },
   } as unknown as BuildPlan;

   it("names the table or rollup, its source or base, a fixed reason and the pending retry", () => {
      expect(
         databaseTableUnreadWarnings(
            [
               {
                  sourceEntityId: "addr1",
                  reason: "timed_out",
                  tableName: "s.orders_t",
               },
               {
                  sourceEntityId: "addr2",
                  reason: "not_found",
                  tableName: "s.r_t",
               },
            ],
            plan,
            true,
         ),
      ).toEqual([
         {
            model: "m.malloy",
            subject: "orders",
            code: "database-table-unread",
            reason: "timed_out",
            sourceEntityId: "addr1",
            sourceName: "orders",
            severity: "warn",
            message:
               "The table `s.orders_t` of `orders` could not be read in its database when it was bound (the catalog read timed out), so its queries are answered live. A retry is pending.",
         },
         {
            model: "m.malloy",
            subject: "orders",
            code: "database-table-unread",
            reason: "not_found",
            // The rollup's own table, though `subject` names its base.
            sourceEntityId: "addr2",
            sourceName: "orders__rollup__r",
            severity: "warn",
            message:
               "The rollup `orders__rollup__r` of `orders` could not be read in its database when it was bound (it was not found in its database's catalog), so its queries are answered live. A retry is pending.",
         },
      ]);
   });

   it("says the next bind reads it when no retry is left, and warns of nothing once read", () => {
      const [w] = databaseTableUnreadWarnings(
         [{ sourceEntityId: "addr1", reason: "connection_failed" }],
         plan,
         false,
      );
      expect(w.message).toContain(
         "It is read again at the next bind or manifest poll.",
      );
      expect(databaseTableUnreadWarnings([], plan, true)).toEqual([]);
   });

   it("names a storage= table's source by address and name, a rollup's included", () => {
      const warnings = databaseTableUnreadWarnings(
         [
            {
               sourceEntityId: "addr1",
               reason: "timed_out",
               destinationName: "lake",
            },
            {
               sourceEntityId: "addr2",
               reason: "not_found",
               destinationName: "lake",
            },
         ],
         plan,
         false,
         true,
      );
      expect(
         warnings.map((w) => [
            w.code,
            w.subject,
            w.sourceEntityId,
            w.sourceName,
         ]),
      ).toEqual([
         ["storage-table-unread", "orders", "addr1", "orders"],
         ["storage-table-unread", "orders", "addr2", "orders__rollup__r"],
      ]);
   });

   it("carries the address alone for a table the plan does not know", () => {
      const [w] = databaseTableUnreadWarnings(
         [{ sourceEntityId: "addr9", reason: "not_read" }],
         plan,
         false,
      );
      expect([w.subject, w.sourceEntityId, w.sourceName]).toEqual([
         undefined,
         "addr9",
         undefined,
      ]);
   });
});
