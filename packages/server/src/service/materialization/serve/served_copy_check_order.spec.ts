// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served copy's checks run in a fixed order, and the first that declines
 * names the decline code the path report and the route metric read. A query
 * that fails both the gate-graft check and the opt-out check is declined by
 * the gate-graft check (`served_copy_not_served`), not as an opt-out: a
 * reorder of `ServedCopyServe.answer()` fails here.
 */

import { describe, expect, it } from "bun:test";
import { LegDecline } from "../domain/leg_decline";
import { ServedCopyServe } from "./served_copy_serve";

const SOURCE_ID = "orders@file:///pkg/m.malloy";

/** A compiled copy whose one source both opts out and has an ungraftable gate. */
function attemptFailingBoth() {
   const modelDef = {
      contents: {
         orders: { type: "table", name: "orders", sourceID: SOURCE_ID },
      },
   };
   const decline = (reason: string, code: string): never => {
      throw new LegDecline(code as never, undefined, { message: reason });
   };
   return {
      compiled: { modelDef, optOutIDs: new Set([SOURCE_ID]) },
      queryString: "run: orders -> { group_by: id }",
      givens: undefined,
      runnable: {},
      virtualMap: new Map(),
      decline,
      compiling: <T>(work: () => Promise<T>) => work(),
      prepared: async () => ({ structRef: "orders" }),
      materializer: {},
   };
}

describe("the served copy's check order", () => {
   it("declines a query that fails the gate-graft and opt-out checks by the gate-graft check", async () => {
      const serve = Object.create(ServedCopyServe.prototype) as Record<
         string,
         unknown
      >;
      const attempt = attemptFailingBoth();
      serve.beginAttempt = async () => attempt;
      serve.mayHaveRowLevelGates = () => true;
      serve.gateCannotGraft = () => true;
      const answer = serve.answer as (...args: unknown[]) => Promise<unknown>;
      const declined = await answer
         .call(serve, attempt.compiled, attempt.queryString, undefined, {})
         .then(
            () => undefined,
            (err: unknown) => err,
         );
      expect(declined).toBeInstanceOf(LegDecline);
      expect((declined as LegDecline).code).toBe("served_copy_not_served");
      // Not served: the table answers nothing, and `code` says why.
      expect((declined as LegDecline).reason).toBe("NOT_SERVED");
      expect((declined as LegDecline).candidateCode).toBe("served_copy");
   });

   it("declines the same query as an opt-out when its gate can be grafted", async () => {
      const serve = Object.create(ServedCopyServe.prototype) as Record<
         string,
         unknown
      >;
      const attempt = attemptFailingBoth();
      serve.beginAttempt = async () => attempt;
      serve.mayHaveRowLevelGates = () => true;
      serve.gateCannotGraft = () => false;
      const answer = serve.answer as (...args: unknown[]) => Promise<unknown>;
      const declined = await answer
         .call(serve, attempt.compiled, attempt.queryString, undefined, {})
         .then(
            () => undefined,
            (err: unknown) => err,
         );
      expect((declined as LegDecline).code).toBe("served_copy_opt_out");
      // The reader opted out: its own reason, with no verdict code.
      expect((declined as LegDecline).reason).toBe("OPTED_OUT");
      expect((declined as LegDecline).candidateCode).toBeUndefined();
   });
});
