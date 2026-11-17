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

   it("names the source, or a rollup's base, the reason and the pending retry", () => {
      expect(
         databaseTableUnreadWarnings(
            [
               { sourceEntityId: "addr1", reason: "timed out" },
               { sourceEntityId: "addr2", reason: "not found" },
            ],
            plan,
            true,
         ),
      ).toEqual([
         {
            model: "m.malloy",
            subject: "orders",
            code: "database-table-unread",
            severity: "warn",
            message:
               "The table of `orders` in its database could not be read when it was bound (timed out), so its queries are answered live. A retry is pending.",
         },
         {
            model: "m.malloy",
            subject: "orders",
            code: "database-table-unread",
            severity: "warn",
            message:
               "The table of `orders` in its database could not be read when it was bound (not found), so its queries are answered live. A retry is pending.",
         },
      ]);
   });

   it("says the next bind reads it when no retry is left, and warns of nothing once read", () => {
      const [w] = databaseTableUnreadWarnings(
         [{ sourceEntityId: "addr1", reason: "x" }],
         plan,
         false,
      );
      expect(w.message).toContain(
         "It is read again at the next bind or manifest poll.",
      );
      expect(databaseTableUnreadWarnings([], plan, true)).toEqual([]);
   });
});
