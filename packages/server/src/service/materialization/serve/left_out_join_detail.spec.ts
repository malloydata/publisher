// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A query declined `CROSS_CONNECTION` (a join the served copy left out across
 * connections) says which join, of which source, and the two connections:
 * read off Malloy's own refusal, with a served-copy alias said as the
 * storage destination's configured name, never the alias.
 */

import { describe, expect, it } from "bun:test";
import { refusedJoinConnections } from "../text/cross_connection_text";
import { namesServedCopyAlias, servedCopyAlias } from "./served_copy_config";
import { leftOutJoinDetail } from "./served_copy_serve";
import { testFileURL } from "../../../test_helpers/test_file_url";

const alias = servedCopyAlias("credible");

describe("the CROSS_CONNECTION decline's detail", () => {
   it("reads both connections off Malloy's refusal", () => {
      expect(
         refusedJoinConnections(
            `Cannot join 'customer', which is on connection 'credible', into a source on connection '${alias}'`,
         ),
      ).toEqual({ joined: "credible", into: alias });
      expect(refusedJoinConnections("Reference to undefined object 'x'")).toBe(
         undefined,
      );
      expect(refusedJoinConnections(undefined)).toBe(undefined);
   });

   it("names the join, its source and both connections, the destination by name", () => {
      const detail = leftOutJoinDetail([
         {
            url: testFileURL("pkg", "m.malloy"),
            keyword: "join_one",
            name: "customer",
            declaration: "kept_orders",
            crossConnection: true,
            connections: { joined: "credible", into: alias },
         },
      ]);
      expect(detail).toBe(
         "The query uses the join `customer` of `kept_orders` (connection 'credible' into storage destination 'credible'), whose joined source is on another connection than the storage destination its table is read from, so it is answered live.",
      );
      expect(namesServedCopyAlias(detail)).toBe(false);
   });

   it("names a join whose source is left out whole by the join, and several joins", () => {
      expect(
         leftOutJoinDetail([
            {
               url: "u",
               keyword: "source",
               name: "kept_orders",
               crossConnection: true,
               join: "customer",
            },
            {
               url: "u",
               keyword: "join_many",
               name: "items",
               declaration: "kept_orders",
               crossConnection: true,
               connections: { joined: "warehouse", into: alias },
            },
         ]),
      ).toBe(
         "The query uses the joins `customer` of `kept_orders`, `items` of `kept_orders` (connection 'warehouse' into storage destination 'credible'), whose joined source is on another connection than the storage destination its table is read from, so it is answered live.",
      );
   });
});
