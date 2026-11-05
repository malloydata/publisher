// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { firstAttachClauses } from "./connection";

describe("firstAttachClauses", () => {
   it("adds ENCRYPTED and the inlining limit only when asked", () => {
      expect(firstAttachClauses({})).toBe("");
      expect(firstAttachClauses({ encrypted: true })).toBe(", ENCRYPTED true");
      expect(
         firstAttachClauses({ encrypted: true, dataInliningRowLimit: 10 }),
      ).toBe(", ENCRYPTED true, DATA_INLINING_ROW_LIMIT 10");
   });

   it("splices no text: a non-integer limit is dropped", () => {
      expect(
         firstAttachClauses({
            dataInliningRowLimit: "1; DROP" as unknown as number,
         }),
      ).toBe("");
      expect(firstAttachClauses({ dataInliningRowLimit: 1.5 })).toBe("");
   });
});
