// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's per-source declines (a gate it cannot graft, an
 * opted-out source on the chain, a zone that differs from the author's) are
 * recorded at the copy's compile and looked up per query: a query on a
 * named source walks nothing; one on a source written inline walks, and is
 * counted.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { clearQueryWalks, queryWalks } from "./admission_parity";
import { LegDecline } from "../domain/leg_decline";
import { ServedCopyServe } from "./served_copy_serve";

const ORDERS = "orders@file:///pkg/m.malloy";
const ITEMS = "items@file:///pkg/m.malloy";

/** A copy where `orders` opts out and `items` does not. */
function attemptOn(structRef: unknown) {
   const modelDef = {
      contents: {
         orders: { type: "table", name: "orders", sourceID: ORDERS },
         items: { type: "table", name: "items", sourceID: ITEMS },
      },
   };
   const decline = (reason: string, code: string): never => {
      throw new LegDecline(code as never, undefined, { message: reason });
   };
   let preparedReads = 0;
   return {
      compiled: {
         modelDef,
         optOutIDs: new Set([ORDERS]),
         rerooted: [],
      } as Record<string, unknown>,
      queryString: "run: x -> { group_by: id }",
      givens: undefined,
      runnable: {},
      virtualMap: new Map(),
      decline,
      compiling: <T>(work: () => Promise<T>) => work(),
      prepared: async () => {
         preparedReads++;
         return { structRef };
      },
      preparedReads: () => preparedReads,
      materializer: {},
   };
}

type Attempt = ReturnType<typeof attemptOn>;

function serveFor(
   attempt: Attempt,
   graftless: ReadonlySet<string>,
): Record<string, unknown> {
   const serve = Object.create(ServedCopyServe.prototype) as Record<
      string,
      unknown
   >;
   serve.beginAttempt = async () => attempt;
   serve.mayHaveRowLevelGates = () => true;
   serve.gateCannotGraft = (_c: unknown, name: string) => graftless.has(name);
   return serve;
}

/** The decline code `answer` throws, or `"passed"` past the chain checks. */
async function codeOf(
   serve: Record<string, unknown>,
   attempt: Attempt,
): Promise<string> {
   const check = serve.checkSourceDeclines as (
      a: unknown,
      which: string,
   ) => Promise<void>;
   return check.call(serve, attempt, "chain").then(
      () => "passed",
      (err: unknown) =>
         err instanceof LegDecline ? err.code : `error: ${String(err)}`,
   );
}

function record(serve: Record<string, unknown>, attempt: Attempt): void {
   const of = serve.sourceDeclinesOf as (c: unknown) => unknown;
   attempt.compiled.sourceDeclines = of.call(serve, attempt.compiled);
}

describe("the served copy's recorded per-source declines", () => {
   // Other specs in the run may have walked already.
   beforeEach(() => clearQueryWalks());
   afterEach(() => clearQueryWalks());

   it("looks a named source's declines up with no walk", async () => {
      for (const [ref, graftless, code] of [
         ["orders", new Set<string>(), "served_copy_opt_out"],
         ["orders", new Set(["orders"]), "served_copy_not_served"],
         ["items", new Set<string>(), "passed"],
         ["items", new Set(["items"]), "served_copy_not_served"],
      ] as const) {
         const attempt = attemptOn(ref);
         const serve = serveFor(attempt, graftless);
         record(serve, attempt);
         // The walk would ask whether the gate grafts; the lookup never does.
         serve.gateCannotGraft = () => {
            throw new Error("walked");
         };
         expect(await codeOf(serve, attempt)).toBe(code);
      }
      expect(queryWalks().get("served_copy_declines")).toBeUndefined();
   });

   it("walks a source written inline, and counts the walk", async () => {
      const inline = {
         type: "table",
         name: "orders",
         sourceID: ORDERS,
      };
      const attempt = attemptOn(inline);
      const serve = serveFor(attempt, new Set());
      record(serve, attempt);
      expect(await codeOf(serve, attempt)).toBe("served_copy_opt_out");
      expect(queryWalks().get("served_copy_declines")).toBe(1);
   });
});
