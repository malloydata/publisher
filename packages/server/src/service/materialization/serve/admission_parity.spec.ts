// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import {
   clearParityDrifts,
   parityDrifts,
   shadowProbe,
   withParityAllowance,
} from "./admission_parity";
import type { CollationProbe } from "./gate_reads_types";
import { expectNoDrift } from "../../../test_helpers/admission_parity";

/** A port answering `source` and `stored` for every question. */
function port(
   source: string | undefined,
   stored: string | undefined,
): CollationProbe {
   return {
      sourceRefusal: async () => source,
      storedRefusal: async () => stored,
   };
}

const sourceArgs = { authorRunnable: {} as never, reads: undefined };
const storedArgs = {
   tables: [{ tier: "storage" as const, virtualHandle: "abc" }],
   answerRunnable: {} as never,
   virtualMap: undefined,
   authorRunnable: {} as never,
   reads: undefined,
};

describe("shadow admission", () => {
   afterEach(() => clearParityDrifts());

   it("answers as the probe does, whatever the lookup says", async () => {
      const shadow = shadowProbe(port("no", undefined), port(undefined, "no"));
      expect(await shadow.sourceRefusal(sourceArgs)).toBe("no");
      expect(await shadow.storedRefusal(storedArgs)).toBeUndefined();
   });

   it("records a disagreement on whether a comparison is refused, never on its wording", async () => {
      const agreeing = shadowProbe(port("a", "b"), port("other", "words"));
      await agreeing.sourceRefusal(sourceArgs);
      await agreeing.storedRefusal(storedArgs);
      expect(parityDrifts()).toEqual([]);
      const drifting = shadowProbe(port(undefined, "a"), port("b", undefined));
      await drifting.sourceRefusal(sourceArgs);
      await drifting.storedRefusal(storedArgs);
      expect(parityDrifts()).toEqual([
         { side: "source", probe: undefined, lookup: "b", tables: [] },
         { side: "stored", probe: "a", lookup: undefined, tables: ["abc"] },
      ]);
      expect(() => expectNoDrift()).toThrow(/disagreed 2 time/);
   });

   it("reads a lookup that throws as a refusal", async () => {
      const failing: CollationProbe = {
         sourceRefusal: async () => {
            throw new Error("boom");
         },
         storedRefusal: async () => undefined,
      };
      const shadow = shadowProbe(port(undefined, undefined), failing);
      expect(await shadow.sourceRefusal(sourceArgs)).toBeUndefined();
      expect(parityDrifts()[0]?.lookup).toBe("the lookup failed: boom");
   });

   it("marks a disagreement a spec allows, and lets expectNoDrift pass it by name", async () => {
      const shadow = shadowProbe(
         port("altered", undefined),
         port(undefined, undefined),
      );
      await withParityAllowance("bind_detects_alter", () =>
         shadow.sourceRefusal(sourceArgs),
      );
      expect(parityDrifts()[0]?.allowed).toBe("bind_detects_alter");
      expect(() => expectNoDrift(["bind_detects_alter"])).not.toThrow();
      await shadow.sourceRefusal(sourceArgs);
      expect(() => expectNoDrift(["bind_detects_alter"])).toThrow();
   });
});
