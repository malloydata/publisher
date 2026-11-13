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
         // Live: the inherited lock plus its own region filter, which
         // replaces the inherited org filter on that route (an `extend`).
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

/**
 * Adversarial review of the comparison (PR #1680 review): ways two gate sets
 * might compare equal while enforcing different rows, or unequal while
 * enforcing the same. Every expected outcome is either the live answer or a
 * denial; none may answer rows live refuses.
 */
const ADVERSARIAL = `##! experimental { givens composite_sources }
#(secure)
given: ORG :: string
#(secure)
given: ROLES :: string[]
#(secure)
given: REGION :: string

source: open is duckdb.table('accounts') extend {}

#(access_filter) org_id = $ORG
source: filter_only is duckdb.table('accounts') extend {}

#(access_filter) org_id=$ORG
source: filter_tight is duckdb.table('accounts') extend {}

#(access_filter) org_id = $ORG
source: derived_same is filter_only -> { select: * }

#(access_filter) org_id = $ORG and region = $REGION
source: one_note is duckdb.table('accounts') extend {}

#(access_filter) org_id = $ORG
#(access_filter) region = $REGION
source: two_notes is duckdb.table('accounts') extend {}

#(authorize) false
source: locked is duckdb.table('accounts') extend {}

#(authorize) true
#(access_filter) org_id = $ORG
source: reopened is locked extend {}

#(authorize) true
#(access_filter) org_id = $ORG
source: reopened_twin is duckdb.table('accounts') extend {}

source: composed is compose(filter_only, open)
`;

const ORG2: Record<string, GivenValue> = {
   ORG: "org2",
   ROLES: ["analyst"],
   REGION: "east",
};

describe("govern(): the routed gate-set comparison cannot be fooled", () => {
   it("treats differently spaced text as a different gate (fail-closed)", async () => {
      await withAuthorizeModel(ADVERSARIAL, async (loaded) => {
         expect(loaded.error).toBeUndefined();
         await expectDenied(
            governRouted(loaded, "filter_tight", "filter_only"),
         );
      });
   });

   it("counts a gate both own and inherited once, and answers as live", async () => {
      await withAuthorizeModel(ADVERSARIAL, async (loaded) => {
         expect(
            await governRouted(loaded, "filter_only", "derived_same", ORG2),
         ).toEqual([4, 5, 6]);
      });
   });

   it("compares a route's conjunction, however it is split across notes", async () => {
      await withAuthorizeModel(ADVERSARIAL, async (loaded) => {
         // `a and b` in one note is one expression; two notes are two. Unequal
         // text denies, never admits a looser form.
         await expectDenied(governRouted(loaded, "two_notes", "one_note"));
         await expectDenied(governRouted(loaded, "one_note", "two_notes"));
         await expectDenied(governRouted(loaded, "filter_only", "two_notes"));
      });
   });

   it("keeps `#(authorize) true` beside a filter as part of the set", async () => {
      await withAuthorizeModel(ADVERSARIAL, async (loaded) => {
         expect(
            await governRouted(loaded, "reopened_twin", "reopened", ORG2),
         ).toEqual([4, 5, 6]);
         // A routed form that lost the re-open, or the filter, or kept the
         // base's lock instead.
         await expectDenied(governRouted(loaded, "filter_only", "reopened"));
         await expectDenied(governRouted(loaded, "locked", "reopened"));
         await expectDenied(governRouted(loaded, "reopened", "locked"));
      });
   });

   it("denies a routed compile that resolved a composite to another branch", async () => {
      await withAuthorizeModel(ADVERSARIAL, async (loaded) => {
         // Live resolves `composed` to its gated first member.
         await expectDenied(governRouted(loaded, "open", "composed"));
         expect(
            await governRouted(loaded, "filter_only", "composed", ORG2),
         ).toEqual([4, 5, 6]);
      });
   });

   it("denies when the live entry point carries no gate (empty set is fail-closed)", async () => {
      await withAuthorizeModel(ADVERSARIAL, async (loaded) => {
         await expectDenied(governRouted(loaded, "filter_only", "open"));
         await expectDenied(governRouted(loaded, "open", "open"));
      });
   });

   it("names no gate field, expression or given in the denial", async () => {
      await withAuthorizeModel(ADVERSARIAL, async (loaded) => {
         const err = await governRouted(loaded, "one_note", "two_notes").then(
            () => undefined,
            (e: unknown) => e,
         );
         expect(err).toBeInstanceOf(AccessDeniedError);
         const text = `${(err as Error).message} ${JSON.stringify(err)}`;
         for (const leak of ["org_id", "region", "$ORG", "REGION", "ORG"]) {
            expect(text).not.toContain(leak);
         }
      });
   });
});
