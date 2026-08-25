// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
/**
 * The gate proof must read the SAME run-target struct Malloy generates SQL
 * from. Malloy's precedence is `compositeResolvedSourceDef ?? structRef`
 * (query_model_impl.js:69, :141-143, query_query.js:608).
 *
 * These are white-box tests against `assertGateLanded` because the leak they
 * pin is invisible from the outside: a request whose proof passes while a
 * stale composite generates the SQL returns a normal 200 with UNFILTERED
 * rows. There is no error, no metric, and no log line to assert on — the only
 * observable is which struct the prover looked at.
 */
import { describe, expect, it } from "bun:test";
import { Model } from "./model";

/** A `Model` shell carrying only what `assertGateLanded` touches. */
function proverWithModelDef(modelDef: unknown): {
   assertGateLanded: (
      recompiled: unknown,
      grafts: ReadonlyArray<{ condition: { code: string } }>,
   ) => Promise<void>;
} {
   const shell = Object.create(Model.prototype) as Record<string, unknown>;
   shell.modelDef = modelDef;
   return shell as never;
}

/** A stand-in for the recompiled `QueryMaterializer` the prover inspects. */
function preparedQuery(query: unknown, modelDef: unknown) {
   return {
      getPreparedQuery: async () => ({ _query: query, _modelDef: modelDef }),
   };
}

const CONDITION = { code: "org_id in $GROUPS" } as const;
const GATE = { condition: CONDITION } as const;
const withGate = {
   type: "table",
   name: "t",
   filterList: [{ code: CONDITION.code }],
};
const withoutGate = { type: "table", name: "t", filterList: [] };

describe("assertGateLanded mirrors the compiler's run-target precedence", () => {
   it("DENIES when the resolved composite lacks the gate, even though structRef carries it", async () => {
      // The leak Step 0 closes. Malloy would generate SQL from the composite
      // (no filter -> every row), while a structRef-only proof sees the gate
      // and passes. Reading "either one" is just as wrong: it admits here.
      const prover = proverWithModelDef({ contents: {} });
      const recompiled = preparedQuery(
         { structRef: withGate, compositeResolvedSourceDef: withoutGate },
         { contents: {} },
      );
      await expect(prover.assertGateLanded(recompiled, [GATE])).rejects.toThrow(
         /did not land/,
      );
   });

   it("ADMITS when the resolved composite carries the gate and structRef does not", async () => {
      // The other direction, and the reason this is a mirror rather than a
      // tightening: the composite IS what executes, so a gate present only
      // there has genuinely landed. A structRef-only proof would deny a
      // correctly-filtered query.
      const prover = proverWithModelDef({ contents: {} });
      const recompiled = preparedQuery(
         { structRef: withoutGate, compositeResolvedSourceDef: withGate },
         { contents: {} },
      );
      await expect(
         prover.assertGateLanded(recompiled, [GATE]),
      ).resolves.toBeUndefined();
   });

   it("falls back to structRef when there is no resolved composite", async () => {
      const prover = proverWithModelDef({ contents: {} });
      await expect(
         prover.assertGateLanded(
            preparedQuery({ structRef: withGate }, { contents: {} }),
            [GATE],
         ),
      ).resolves.toBeUndefined();
      await expect(
         prover.assertGateLanded(
            preparedQuery({ structRef: withoutGate }, { contents: {} }),
            [GATE],
         ),
      ).rejects.toThrow(/did not land/);
   });

   it("resolves a string composite ref through contents, like a string structRef", async () => {
      const modelDef = { contents: { gated: withGate } };
      const prover = proverWithModelDef(modelDef);
      await expect(
         prover.assertGateLanded(
            preparedQuery(
               { structRef: "ungated", compositeResolvedSourceDef: "gated" },
               modelDef,
            ),
            [GATE],
         ),
      ).resolves.toBeUndefined();
   });
});
