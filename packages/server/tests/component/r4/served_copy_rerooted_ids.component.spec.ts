// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The row-scope check leaves out the query of a joined source the served
 * copy reads from a stored table, read or not: each author source the copy
 * re-rooted, and each extension it left as written that reads its base's
 * table by name. It is handed exactly those, from the copy's compile, not
 * every `sourceID` a bound table lists: an extension with a binding of its
 * own the copy did not re-root (`c`, which reads `p`'s table) is left out
 * as `p`'s table, and a source the copy re-rooted under a binding listing
 * another is not widened to that other.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import { buildScenario, disposeScenarios, type R4Fixture } from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'east', 10.5), (2, 'west', 5.0), (3, 'y', 7.0)
    ) AS t(id, region, amt)`,
];

const model = `##! experimental { persistence }
#@ persist name="j_t" storage="lake"
source: j is duckdb.table('ev') -> { where: region != 'x'; select: * }

#@ persist name="p_t" storage="lake"
source: p is duckdb.table('ev') -> { where: region != 'y'; select: * }

source: c is p extend { measure: n is count() }

#@ persist name="k_t" storage="lake"
source: k is duckdb.table('ev') -> { select: id, amt } extend {
  join_one: j on id = j.id
  join_one: c on id = c.id
}
`;

describe("the served copy hands the row-scope check the sources it reads from a table", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({ files: { "m.malloy": model }, seedSql });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("names each re-rooted source and each extension reading its base's table, by author sourceID", async () => {
      const answer = await fixture
         .servedCopy()
         .loadQuery("run: k -> { aggregate: x is count() }");
      const names = (ids: Iterable<string>) =>
         [...ids].map((id) => id.split("@")[0]).sort();
      expect(names(answer.bindings.flatMap((b) => b.sourceIDs ?? []))).toEqual([
         "j",
         "k",
         "p",
      ]);
      expect(names(answer.rerootedSourceIDs)).toEqual(["c", "j", "k", "p"]);
   });

   for (const malloy of [
      "run: k -> { aggregate: x is count() }",
      "run: k -> { group_by: r is c.region; aggregate: x is count(); order_by: r }",
   ]) {
      it(`${malloy}: answers from storage, equal to live`, async () => {
         const r = await fixture.run({ name: malloy, malloy });
         expect(r.served).toEqual(r.live);
         expect(r.servedFrom).toBe("storage");
      });
   }
});
