// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A routed answer that writes again its run target's given-reading `where:`
 * terms (a `#(rollup)` rollup's variant, whose build left them out) is
 * re-proven in `govern()`: the routed compile's run target must apply exactly
 * the given-reading terms the live run target does, by text. A routed form
 * that lost one, added one, or reads another would answer rows live refuses,
 * so it is denied; one that kept them answers each caller's rows, exactly as
 * live.
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

const MODEL = `##! experimental.givens
given: REGION :: string
given: ORG :: string

source: scoped is duckdb.table('accounts') extend {
  where: region = $REGION
}
source: scoped_twin is duckdb.table('accounts') extend {
  where: amount > 0
  where: region = $REGION
}
source: unscoped is duckdb.table('accounts') extend {}
source: by_org is duckdb.table('accounts') extend {
  where: org_id = $ORG
}
source: both is duckdb.table('accounts') extend {
  where: region = $REGION
  where: org_id = $ORG
}
source: not_region is duckdb.table('accounts') extend {
  where: region != $REGION
}

// A source joining a given-reading source, and its twin joining it re-pointed
// at one that left the term out (a join to a rollup that does not carry it).
source: kids is duckdb.table('child') extend {
  primary_key: id
  join_many: accts is scoped on accts.child_id = id
}
source: kids_open is duckdb.table('child') extend {
  primary_key: id
  join_many: accts is unscoped on accts.child_id = id
}
`;

/** The model's own compile of `run: <source> -> { select: id }`. */
function compile(loaded: LoadedAuthorizeModel, source: string) {
   const mm = (
      loaded.model as unknown as { modelMaterializer: ModelMaterializer }
   ).modelMaterializer;
   return mm.loadRestrictedQuery(
      `\nrun: ${source} -> { select: id; order_by: id }`,
   );
}

/** Govern `routed` as a rollup's routed answer for a live compile of `live`. */
async function governRouted(
   loaded: LoadedAuthorizeModel,
   routed: string,
   live: string | undefined,
   givens: Record<string, GivenValue>,
   bypassAuthorize = false,
): Promise<number[]> {
   const governor = governorOf(loaded.model);
   const graftScope = (
      governor as unknown as { defaultGraftScope(): GraftScope }
   ).defaultGraftScope();
   const runnable: QueryMaterializer = compile(loaded, routed);
   const { runnable: governed } = await governor.govern(
      {
         givens,
         recompile: (mm: ModelMaterializer) =>
            mm.loadRestrictedQuery(
               `\nrun: ${routed} -> { select: id; order_by: id }`,
            ),
         graftScope,
         reprovesGivenTerms: true,
         ...(bypassAuthorize ? { bypassAuthorize } : {}),
      },
      {
         runnable,
         live: live === undefined ? undefined : compile(loaded, live),
      },
   );
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

describe("govern(): a routed answer re-applies the live run target's given terms", () => {
   it("admits a routed compile with the same given terms, each caller's rows exactly", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         expect(loaded.error).toBeUndefined();
         // Interleaved: east, west, east.
         for (const [region, ids] of [
            ["east", [1, 2, 4]],
            ["west", [3, 5, 6]],
            ["east", [1, 2, 4]],
         ] as const) {
            const givens = { REGION: region, ORG: "org1" };
            expect(
               await governRouted(loaded, "scoped", "scoped", givens),
            ).toEqual([...ids]);
            // A term that reads no given is not compared: only given terms
            // are left out of a build and written again.
            expect(
               await governRouted(loaded, "scoped_twin", "scoped", givens),
            ).toEqual([...ids]);
         }
      });
   });

   it("denies a routed compile that lost the given term, which would answer every caller's rows", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(
            governRouted(loaded, "unscoped", "scoped", { REGION: "east" }),
         );
      });
   });

   it("denies a routed compile reading another given, or one more", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         const givens = { REGION: "east", ORG: "org1" };
         await expectDenied(governRouted(loaded, "by_org", "scoped", givens));
         await expectDenied(governRouted(loaded, "both", "scoped", givens));
         await expectDenied(governRouted(loaded, "scoped", "both", givens));
      });
   });

   it("denies a routed compile reading the same given in another expression", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(
            governRouted(loaded, "not_region", "scoped", { REGION: "east" }),
         );
      });
   });

   it("denies a routed compile whose join lost its source's given term", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         const givens = { REGION: "east" };
         expect(await governRouted(loaded, "kids", "kids", givens)).toEqual([
            1, 2,
         ]);
         await expectDenied(governRouted(loaded, "kids_open", "kids", givens));
      });
   });

   it("re-proves under the authorize bypass too: it is the model's meaning, not access", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(
            governRouted(
               loaded,
               "unscoped",
               "scoped",
               { REGION: "east" },
               true,
            ),
         );
      });
   });

   it("denies when the live compile is missing, so its terms are unknown", async () => {
      await withAuthorizeModel(MODEL, async (loaded) => {
         await expectDenied(
            governRouted(loaded, "scoped", undefined, { REGION: "east" }),
         );
      });
   });
});
