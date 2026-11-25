// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { losslessWidening } from "./lossless_widening";

describe("losslessWidening", () => {
   it.each([
      ["INTEGER", "BIGINT", true],
      ["BIGINT", "INTEGER", false],
      ["UINTEGER", "BIGINT", true],
      ["UBIGINT", "BIGINT", false],
      ["BIGINT", "DECIMAL(19,0)", true],
      ["BIGINT", "DECIMAL(18,0)", false],
      // UBIGINT's maximum, 18446744073709551615, has 20 digits.
      ["UBIGINT", "DECIMAL(20,0)", true],
      ["UBIGINT", "DECIMAL(19,0)", false],
   ] as const)("%s into %s: %p", (from, to, lossless) => {
      expect(losslessWidening(from, to)).toBe(lossless);
   });
});
