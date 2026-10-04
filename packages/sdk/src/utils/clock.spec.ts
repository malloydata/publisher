// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { now } from "./clock";

describe("now", () => {
   it("never runs backwards, so a duration is never negative", () => {
      const start = now();
      expect(now() - start).toBeGreaterThanOrEqual(0);
   });
});
