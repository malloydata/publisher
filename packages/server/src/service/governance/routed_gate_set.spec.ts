// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A query routed to a stored answer past a gate on its entry point (the
 * served copy, a persisted view) is re-proven in `govern()`: the routed
 * compile must enforce EXACTLY the gates the live entry point does, every
 * `#(authorize)` and `#(access_filter)`, own and inherited. Finding any one of
 * them is not enough: a routed form that kept the lock but lost the filter
 * would answer a caller rows live refuses. A routed answer whose live gate
 * set cannot be read is denied.
 */
import { describe, expect, it } from "bun:test";
import type {
   GivenValue,
   ModelMaterializer,
   QueryMaterializer,
} from "@malloydata/malloy";
import { AccessDeniedError } from "../../errors";
import {
   withAuthorizeModel,
   type LoadedAuthorizeModel,
} from "../../test_helpers/authorize_fixture";
import { governorOf } from "../../test_helpers/governor";
import type { GraftScope } from "../gate_classification";
import type { GovernRequest } from "./query_governor";

const MODEL = `##! experimental.givens
#(secure)
given: ORG :: string
#(secure)
given: ROLES :: string[]
#(secure)
given: REGION :: string

#(authorize) 'analyst' in $ROLES
#(access_filter) org_id = $ORG
source: two_gates is duckdb.table('accounts') extend {}

#(authorize) 'analyst' in $ROLES
#(access_filter) org_id = $ORG
source: two_gates_twin is duckdb.table('accounts') extend {}

#(authorize) 'analyst' in $ROLES
source: lock_only is duckdb.table('accounts') extend {}

#(access_filter) org_id = $ORG
source: filter_only is duckdb.table('accounts') extend {}

#(access_filter) region = $REGION
source: inherits_two is two_gates extend {}

#(access_filter) region = $REGION
source: east_only is duckdb.table('accounts') extend {}

#(authorize) 'analyst' in $ROLES
#(access_filter) org_id = $ORG
#(access_filter) region = $REGION
source: three_gates is duckdb.table('accounts') extend {}
`;

const ORG1: Record<string, GivenValue> = {
   ORG: "org1",
   ROLES: ["analyst"],
   REGION: "east",
};

/** The model's own compile of `run: <source> -> { select: id }`. */
function compile(loaded: LoadedAuthorizeModel, source: string) {
   const mm = (
      loaded.model as unknown as { modelMaterializer: ModelMaterializer }
   ).modelMaterializer;
   return mm.loadRestrictedQuery(
      `\nrun: ${source} -> { select: id; order_by: id }`,
   );
}

/**
 * Govern a query whose live compile runs `live` but whose routed compile runs
 * `routed`, as the router hands it over past an entry-point gate.
 */
async function governRouted(
   loaded: LoadedAuthorizeModel,
   routed: string,
   live: string | undefined,
   givens: Record<string, GivenValue> = ORG1,
): Promise<number[]> {
   const governor = governorOf(loaded.model);
   const graftScope = (
      governor as unknown as { defaultGraftScope(): GraftScope }
   ).defaultGraftScope();
   const routedText = `\nrun: ${routed} -> { select: id; order_by: id }`;
   const request: GovernRequest = {
      givens,
      recompile: (mm: ModelMaterializer) => mm.loadRestrictedQuery(routedText),
      graftScope,
      routedPastEntryPointGate: true,
   };
   const runnable: QueryMaterializer = compile(loaded, routed);
   const { runnable: governed } = await governor.govern(request, {
      runnable,
      live: live === undefined ? undefined : compile(loaded, live),
   });
   const result = await governed.run({ rowLimit: 1000, givens });
   return result.data.value.map((r) => Number(r.id));
}

async function expectDenied(promise: Promise<unknown>): Promise<void> {
   const outcome = await promise.then(
      (rows) => rows,
      (e: unknown) => e,
   );
   expect(outcome).toBeInstanceOf(AccessDeniedError);
}

describe("govern(): a routed answer enforces the live entry point's gate set", () => {
   it("admits a routed compile carrying the same gates, filtered as live is", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         expect(loaded.error).toBeUndefined();
         expect(
            await governRouted(loaded, "two_gates_twin", "two_gates"),
         ).toEqual([1, 2, 3]);
         expect(await governRouted(loaded, "two_gates", "two_gates")).toEqual([
            1, 2, 3,
         ]);
      });
   });

   it("denies a routed compile that kept the lock but lost the filter", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(governRouted(loaded, "lock_only", "two_gates"));
      });
   });

   it("denies a routed compile that kept the filter but lost the lock", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(governRouted(loaded, "filter_only", "two_gates"));
         // A caller the lock refuses live.
         await expectDenied(
            governRouted(loaded, "filter_only", "two_gates", {
               ...ORG1,
               ROLES: ["viewer"],
            }),
         );
      });
   });

   it("denies a routed compile that lost an inherited gate", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         // Live: the inherited lock and filter plus its own region filter.
         await expectDenied(governRouted(loaded, "east_only", "inherits_two"));
         await expectDenied(governRouted(loaded, "two_gates", "inherits_two"));
      });
   });

   it("denies a routed compile carrying a gate live does not", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(governRouted(loaded, "three_gates", "two_gates"));
      });
   });

   it("denies when the live compile is missing, so its gate set is unknown", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(governRouted(loaded, "two_gates", undefined));
      });
   });
});
