// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which serve bindings are withheld because a caller-scoped join on them cannot
 * be reproduced on the shape. Compile-backed: what decides it is whether the
 * compiler reports a join as given-scoped, which a hand-built field list would
 * encode as our belief rather than the compiler's.
 */
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";
import {
   withholdUnreproducibleCallerScopedJoins,
   type ServeBinding,
} from "./materialization_serve_transform";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

const MODEL = `##! experimental { persistence givens access_modifiers }
given:
  ORG_ID :: number is 1
  USER_ID :: number is 7
source: raw is duckdb.sql("""SELECT * FROM (VALUES (1,10)) AS t(org_id, opp_id)""")
source: grants_raw is duckdb.sql("""SELECT * FROM (VALUES (1,7,10)) AS g(org_id, user_id, opp_id)""")
source: accounts_raw is duckdb.sql("""SELECT * FROM (VALUES (1,1)) AS a(org_id, account_id)""")

#@ persist name="grants" storage=lake
source: grants is grants_raw -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}

#@ persist name="opps" storage=lake
source: opps is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is g.opp_id is not null
}

#@ persist name="opp_notes" storage=lake
source: opp_notes is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: o is opps on opp_id = o.opp_id
}

#@ persist name="hidden_grants_opps" storage=lake
source: hidden_grants_opps is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is g.opp_id is not null
} include { private: g; public: * }

#@ persist name="accounts" storage=lake
source: accounts is accounts_raw -> { select: * } extend {
  where: org_id = $ORG_ID
}
`;

async function withheldFor(
   bound: string[],
   /** A destination per bound source; every source defaults to `lake`. */
   destinations: Record<string, string> = {},
): Promise<string[]> {
   const model = await loadTestModel(connections, MODEL).getModel();
   /* eslint-disable @typescript-eslint/no-explicit-any */
   const contents = (model as any)._modelDef.contents as Record<
      string,
      { fields?: unknown; sourceID?: unknown }
   >;
   /* eslint-enable @typescript-eslint/no-explicit-any */
   const sourceNameById = new Map<string, string>();
   for (const [name, def] of Object.entries(contents)) {
      if (typeof def?.sourceID === "string")
         sourceNameById.set(def.sourceID, name);
   }
   const bindings = bound.map(
      (sourceName) =>
         ({
            sourceName,
            destinationName: destinations[sourceName] ?? "lake",
         }) as unknown as ServeBinding,
   );
   return withholdUnreproducibleCallerScopedJoins(
      bindings,
      contents,
      sourceNameById,
   ).withheld;
}

describe("withholdUnreproducibleCallerScopedJoins", () => {
   it("withholds nothing while every caller-scoped join target is bound", async () => {
      expect(
         await withheldFor(["grants", "opps", "opp_notes", "accounts"]),
      ).toEqual([]);
   });

   it("withholds a joiner whose grant table is not bound, and leaves siblings", async () => {
      expect(await withheldFor(["opps", "accounts"])).toEqual(["opps"]);
   });

   it("withholds to a fixpoint: a joiner of a withheld joiner goes too", async () => {
      // `opp_notes` joins `opps`, which is itself caller-scoped through its grant
      // join. With `grants` unbound, `opps` is withheld — and then `opp_notes`
      // has a caller-scoped join to a source no longer on the shape.
      expect(await withheldFor(["opps", "opp_notes", "accounts"])).toEqual([
         "opps",
         "opp_notes",
      ]);
   });

   it("withholds a joiner whose caller-scoped join is access-restricted, though its target is bound", async () => {
      // A private join is never re-emitted onto the shape, while the public
      // `visible` dimension still reads through it — so the shape could not
      // compile with this binding in it, whatever `grants`'s state.
      expect(
         await withheldFor(["grants", "hidden_grants_opps", "accounts"]),
      ).toEqual(["hidden_grants_opps"]);
   });

   it("withholds a joiner whose grant table is bound on another destination", async () => {
      // One query cannot join across two connections, so a grant table bound
      // elsewhere is as unreproducible on this shape as an unbound one.
      expect(
         await withheldFor(["grants", "opps", "accounts"], { grants: "other" }),
      ).toEqual(["opps"]);
   });
});
